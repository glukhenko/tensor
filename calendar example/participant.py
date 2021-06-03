"""
Модуль предназначен для обработки участников календаря
Participant <-> ЧастноеЛицо/Подразделение/Помещение
"""

import functools
import json
import sbis
import calendar_common
from calendar_base.utils import get_physic_client
from calendar_base.utils import remote_invoke_online, invoke_profiles
from calendar_base.utils import get_site, is_demo_scheme
from calendar_common.const import SITE_ONLINE
from calendar_common.const import CALENDAR_MY_PERSONAL
from calendar_common.const import PARTICIPANT_DEPARTMENT, PARTICIPANT_PERSON, PARTICIPANT_ROOM, PARTICIPANT_VEHICLE
from calendar_common.const import CALENDAR_BY_PARTICIPANT, AUTO_CREATE_CALENDAR_PARTICIPANTS
from calendar_common import filtered_dict
from calendar_common import SBISEncoder
import uuid


def _get_participant_search_query(base_query: str, query: dict, client_placeholder=1):
    """
    Формирует фильтр поиска, в зависимости от типа участника
    """
    query_parts = []
    for field_name, spec in query.items():
        field_placeholder = spec.get('placeholder')
        field_type = spec.get('type')
        query_parts.append(base_query + ' WHERE "Client" = ${client_placeholder}::bigint '
                                        '   AND "{field_name}" = ANY(${field_placeholder}::{field_type}[])'
                                        .format(client_placeholder=client_placeholder,
                                                field_name=field_name,
                                                field_placeholder=field_placeholder,
                                                field_type=field_type))

    return ' UNION ALL '.join(query_parts)


def has_all_requested_person_filled_plain_result(person_uuids: list, result) -> bool:

    def is_item_valid(item: int):
        return item is not None

    if len(result) > 0:
        dict_by_person = result.ToDict('Person', 'Face')
        item_state = [is_item_valid(dict_by_person.get(person_uuid, None)) for person_uuid in person_uuids]
        return all(item_state)

    return False


def has_all_requested_persons_filled(person_uuids: list, result) -> bool:

    def is_item_valid(item: dict):
        return item is not None

    if result.get('Count', 0) > 0:
        items = result.get('Persons', [])
        by_uuid = {item.get('Person', ''): item for item in items}
        item_state = [is_item_valid(by_uuid.get(str(item_uuid))) for item_uuid in person_uuids]
        return all(item_state)

    return False


def has_all_requested_private_persons_filled(persons: list, id_client: int, result) -> bool:

    def is_item_valid(item: dict):
        return item.get('Account', 0) == id_client and item.get('Person') is not None

    if len(result) > 0:
        by_persons = {item.get('Face', 0): item for item in result}
        item_state = [is_item_valid(by_persons.get(person, {})) for person in persons]
        return all(item_state)

    return False


def check_by_uuid(id_by_uuid, client_id):
    all_uuid = list(set(id_by_uuid.keys()) - {None})
    if not all_uuid:
        return
    sql = '''
        SELECT "PersonUUID", "Person"
          FROM "Participant"
         WHERE "Client" = $1::int
           AND "PersonUUID" = ANY($2::uuid[])
    '''
    to_update = []
    persons_db = sbis.SqlQuery(sql, client_id, all_uuid).ToDict("PersonUUID", "Person")

    for person_uuid, person_id in persons_db.items():
        if id_by_uuid.get(person_uuid) != person_id:
            to_update.append({'uuid': str(person_uuid), 'old_id': person_id, 'new_id': id_by_uuid.get(person_uuid)})
    if to_update:
        sql = '''
            WITH new_values AS (
              SELECT *
                FROM json_to_recordset($1::json) 
                  AS X("uuid" uuid,
                       "old_id" bigint,
                       "new_id" bigint
                      )            
            )
            UPDATE "Participant" pt
               SET "Person" = nv."new_id"
              FROM new_values nv
             WHERE pt."PersonUUID" = nv."uuid"
               AND (pt."Person" = nv."old_id" OR pt."Person" IS NULL)
               AND nv."new_id" IS NOT NULL
               AND pt."Client" = $2::int
               AND NOT EXISTS (SELECT 1 FROM "Participant" WHERE "Client" = $2::int AND "Person" = nv."new_id")
            RETURNING "PersonUUID"
        '''
        processed = sbis.SqlQuery(sql, json.dumps(to_update), client_id)
        lost = [person.get('uuid') for person in to_update if uuid.UUID(person.get('uuid')) not in processed]
        sbis.WarningMsg('Incorrect participants: {}, client: {}'.format(str(lost), client_id))


def sync_participants(participants, params=None):
    """
    метод получает участников по Person/Department/Room, при необходимости добавляет в базу
    Примечание: запрещено иметь в наборе участников с разными клиентами
    """

    def get_id_client(participant: list) -> int:
        """
        Возвращает идентификатор клиента, по которому будут синхронизированы участники
        :return:
        """
        id_client = sbis.Session.ClientID()
        try:
            if not params or not params.Get('ClientChecked'):
                sbis.Client.CheckClients([id_client])
        except Exception as ex:
            sbis.WarningMsg(ex)
        synced_clients = {i.get('Client') for i in participant}

        if not synced_clients:
            raise sbis.Error('В наборе участников не найдено поле Client')
        if len(synced_clients) > 1:
            raise sbis.Error('В наборе участников найдены разные клиенты, что запрещено: {}'.format(
                synced_clients))

        id_sync_client = synced_clients.pop()
        if id_sync_client in (id_client, get_physic_client()):
            id_client = id_sync_client
        else:
            raise sbis.Error('Вызов метода выполняется под клиентом {} но синхронизируются данные '
                             'для {}. Клиент ФЛ: {}'.format(id_client,
                                                            id_sync_client,
                                                            get_physic_client()))

        return id_client

    supported_fields = {'Client', 'Person', 'User', 'LastName', 'FirstName', 'MiddleName',
                        'PersonUUID', 'Department', 'Room', 'Vehicle'}

    list_participants = []

    # для ускорения запроса сформируем списки участников по типам
    id_participants = {
        'Person': [],
        'PersonUUID': [],
        'Department': [],
        'Room': [],
        'Vehicle': [],
    }

    id_by_uuid = {}
    for participant in participants:
        item_dict = filtered_dict(supported_fields, participant)
        for key in id_participants.keys():
            value = item_dict.get(key, None)
            if value is not None:
                id_participants[key].append(value)
        list_participants.append(item_dict)
        if participant.Get('PersonUUID') and participant.Get('Person'):
            id_by_uuid[participant.Get('PersonUUID')] = participant.Get('Person')

    id_client = get_id_client(list_participants)

    if id_client != get_physic_client() and id_by_uuid:
        check_by_uuid(id_by_uuid, id_client)

    if id_client == get_physic_client():
        query_params = {
            'PersonUUID': {
                'placeholder': 6,
                'type': 'uuid'
            }
        }
    else:
        query_params = {
            'Person': {
                'placeholder': 2,
                'type': 'bigint'
            },
            'Department': {
                'placeholder': 3,
                'type': 'bigint'
            },
            'Room': {
                'placeholder': 4,
                'type': 'bigint'
            },
            'Vehicle': {
                'placeholder': 5,
                'type': 'bigint'
            },
        }

    participant_search_query = _get_participant_search_query('''
        SELECT *
          FROM "Participant"
    ''', query_params)

    action_on_conflict = 'ON CONFLICT DO NOTHING'

    sql = """
    -- $7::text - данные для вставки
    -- $1::int - клиент
    -- $2::int[] - частные лица
    -- $3::int[] - подразделения
    -- $4::int[] - помещения
    -- $5::int[] - автомобили
    -- $6::uuid[] - персоны
        WITH raw_rs AS (
            SELECT * FROM json_to_recordset($7::json) AS X(
                  "Client" bigint
                , "Person" bigint
                , "User" bigint
                , "LastName" text
                , "FirstName" text
                , "MiddleName" text
                , "PersonUUID" uuid
                , "Department" bigint
                , "Room" bigint
                , "Vehicle" bigint    
            )
        )
        , participant_rs AS (
            {participant_search_query}
        )
        INSERT INTO "Participant"
            (
                "Client", "Person", "User", "LastName", "FirstName", "MiddleName",
                "PersonUUID", "Department", "Room", "Vehicle"
            )
        SELECT raw_rs."Client"
             , COALESCE(raw_rs."Person", 0) "Person"
             , raw_rs."User"
             , raw_rs."LastName"
             , raw_rs."FirstName"
             , raw_rs."MiddleName"
             , raw_rs."PersonUUID"
             , raw_rs."Department"
             , raw_rs."Room"
             , raw_rs."Vehicle"
          FROM raw_rs
     LEFT JOIN participant_rs
            ON raw_rs."Client" = participant_rs."Client"
           AND (raw_rs."Person" = participant_rs."Person"
            OR raw_rs."Department" = participant_rs."Department"
            OR raw_rs."Room" = participant_rs."Room"
            OR raw_rs."Vehicle" = participant_rs."Vehicle"
            OR raw_rs."PersonUUID" = participant_rs."PersonUUID")
         WHERE participant_rs."@Participant" IS NULL
           AND (COALESCE(raw_rs."Person", 0) != 0
               OR raw_rs."PersonUUID" IS NOT NULL
               OR raw_rs."Department" IS NOT NULL 
               OR raw_rs."Room" IS NOT NULL
               OR raw_rs."Vehicle" IS NOT NULL)
            {action_on_conflict}
    """.format(participant_search_query=participant_search_query, action_on_conflict=action_on_conflict)

    if list_participants:
        try:
            sbis.SqlQuery(sql,
                          id_client,
                          id_participants.get('Person'),
                          id_participants.get('Department'),
                          id_participants.get('Room'),
                          id_participants.get('Vehicle'),
                          id_participants.get('PersonUUID'),
                          json.dumps(list_participants, cls=calendar_common.SBISEncoder),
                          )
        except Exception as ex:
            sbis.ErrorMsg('Ошибка синхронизации. {}'.format(ex))

    # зачитаем с нуля всех участников (присутсвовавшие + добавленные)
    base_sql = '''
        SELECT "@Participant"
             , "LastName"
             , "FirstName"
             , "MiddleName"
             , "Client"
             , "Person"
             , "PersonUUID"
             , "Department"
             , "Room"
             , "Vehicle"
             , CASE
                WHEN part."Person" <> 0 OR part."PersonUUID" IS NOT NULL THEN 0
                WHEN part."Department" IS NOT NULL THEN 1
                WHEN part."Room" IS NOT NULL THEN 2
                WHEN part."Vehicle" IS NOT NULL THEN 3
               END AS "TypeParticipant"
          FROM "Participant" part
    '''
    sql = _get_participant_search_query(base_sql, query_params)
    new_participants = sbis.SqlQuery(
        sql,
        id_client,
        id_participants.get('Person'),
        id_participants.get('Department'),
        id_participants.get('Room'),
        id_participants.get('Vehicle'),
        id_participants.get('PersonUUID')
    )
    if len(participants) != len(new_participants):
        sbis.WarningMsg('some problem in sync_participants, participants: {}, new_participants: {}'.format(
            participants, new_participants))
    return new_participants


def check_person_type(record):
    """
    Проверка типа частного лица, обрабатываем только (0, 6)
    :param record: запись из результата метода ListSync
    :return: bool
    """
    return record.Get('Тип') != 0 or (record.Get('Тип') == 0 and record.Get('ЧастноеЛицоТип') in (0, 6))


def add_person_to_depart(id_person, id_department, _sync=None):
    """
    Генерирует персональное разрешение персоны, которое переведено в подразделение.
    :param id_person: частного лица, для кого генерируются персональные разрешения
    :param id_department: идентификатор подразделения
    """
    if id_person and id_department:
        sbis.КэшированиеМетодов.ОчиститьКэш('CalendarPermission.ListByPerson', 4)
        _filter = sbis.Record({
            'Person': id_person,
        })
        if _sync:
            online_person = _sync.Filter({'@Лицо': id_person})
            online_person = online_person[0] if online_person else None
            if online_person and not(check_person_type(online_person)):
                return
            _filter.AddString('OnlineParticipants')
            _filter['OnlineParticipants'] = _sync.AsJson()
        calendars = sbis.CalendarPermission.ListByPerson(None, _filter, None, None)
        id_depart_permissions = [calendar.Get('@CalendarPermission') for calendar in calendars if
                                 calendar.Get('IsDepartmentPermission')]
        if id_depart_permissions:
            sbis.CalendarPermission.CreatePersonal(id_person, id_depart_permissions)


def _get_participant_type(participant: sbis.Record):
    if participant.Get('Vehicle'):
        return PARTICIPANT_VEHICLE
    elif participant.Get('Room'):
        return PARTICIPANT_ROOM
    elif participant.Get('Department'):
        return PARTICIPANT_DEPARTMENT
    else:
        return PARTICIPANT_PERSON


def sync(ext_participants=None, ext_uuid_participants=None, by_physical=False, online_participants=None) -> dict:
    """
    Синхронизирует участников
    :param by_physical:
    :param ext_participants: синхронизирует участников (ЧЛ/подразделение/комната/машина) с online
    :param ext_uuid_participants: синхронизирует участников с сервиса профилей
    :param online_participants: Участники подготовленные на случай синхронизации (готовая замена вызову ListSync)
    :return: Возвращает словарь вида {id_ext_participant: id_new_participant}
    ВАЖНО: при вызове данного метода синхронизации, предполагается что ни ext_participants,
    ни ext_uuid_participants не будет существовать в базе Participant, иначе вызовется исключение
    """
    ext_participants = ext_participants or []
    ext_uuid_participants = ext_uuid_participants or []

    if len(ext_participants) and len(ext_uuid_participants):
        raise sbis.Error(
            'Некорректное использовние метода синхронизации. Переданы оба набора данных!')

    if len(ext_participants) and by_physical:
        raise sbis.Error(
            'Запрошена синхронизация по @Лицо и включен флаг предпочтения данных по физлицу!')

    participants = None
    _format = _get_participant_format()
    id_client = get_physic_client() if by_physical else sbis.Session.ClientID()
    sbis.Client.CheckClients([id_client])
    site = get_site()
    # Получает информацию об участниках с основного сервиса
    sbis.LogMsg(sbis.LogLevel.llMINIMAL, 'sync. ext_participants: {}, site: {}'.format(str(ext_participants), site))
    if ext_participants and site == SITE_ONLINE:
        if online_participants is None:
            online_participants = remote_invoke_online('Лицо', 'ListSync', ext_participants)
        sbis.LogMsg(sbis.LogLevel.llMINIMAL, 'sync. online_participants: {}'.format(str(online_participants)))
        participants = sbis.RecordSet() if online_participants is None else sbis.RecordSet(online_participants.Format())
        for op in online_participants or []:
            if check_person_type(op):
                op['Client'] = id_client
                participants.AddRow(op)
            else:
                sbis.WarningMsg('sync. Необслуживаемый тип частного лица. {}'.format(str(op)))
        participants.Migrate(_format)
        sbis.LogMsg(sbis.LogLevel.llMINIMAL, 'sync. participants: {}'.format(str(participants)))

    # Получает информацию об учстниках с сервиса профилей
    if ext_uuid_participants:
        participants = sbis.RecordSet(_format)
        person_uuids = filter(None, ext_uuid_participants)
        person_uuids = list(map(str, person_uuids))
        _filter = sbis.Record({'Persons': person_uuids})
        persons = remote_invoke_online('Person', 'Read', _filter)
        if persons is not None:
            for person in persons:
                participant = sbis.Record(_format)
                participant['Person'] = person.Get('PrivatePersonID') if not by_physical else 0
                participant['LastName'] = person.Get('Surname')
                participant['FirstName'] = person.Get('Name')
                participant['MiddleName'] = person.Get('Patronymic')
                participant['PersonUUID'] = person.Get('ID')
                participant['User'] = person.Get('UserID')
                participant['Client'] = id_client
                participants.AddRow(participant)

    # Обновим данные об участниках и получим новые ПК
    result = {}
    calendars = []
    sbis.LogMsg(sbis.LogLevel.llMINIMAL, 'sync. participants: {}'.format(str(participants)))
    if participants:
        # Вставим в базу отсутствовавших участников.
        new_participants = sbis.Participant.SyncParticipants(participants, sbis.Record({'ClientChecked': True}))
        for np in new_participants:
            # Если был запрос на синхронизацию по @Лицо - отдадим соответствующую колонку,
            # если по UUID - отдадим UUID персоны
            id_ext_participant = get_party_id(np) if ext_participants else get_person_uuid(np)
            id_participant = np.Get('@Participant')
            result[str(id_ext_participant)] = id_participant
            fio = '{last_name} {first_name} {middle_name}'.format(
                    last_name=np.Get('LastName') or '',
                    first_name=np.Get('FirstName') or '',
                    middle_name=np.Get('MiddleName') or '',
                )
            participant_type = _get_participant_type(np)
            if participant_type in AUTO_CREATE_CALENDAR_PARTICIPANTS:
                raw_calendar = {
                    'Owner': id_participant,
                    'Type': CALENDAR_MY_PERSONAL if by_physical else CALENDAR_BY_PARTICIPANT.get(participant_type),
                    'Name': fio
                }
                calendars.append(raw_calendar)
    if calendars:
        # создание календарей происходит в транзакции метода List открытой только на чтение
        fut_res = sbis.BLObject('Calendar').FutureInvoke('MassCreateBaseCalendars', calendars)
        _ = fut_res.get()

    return result


def get_by_ext_id(ids, by_physical=False, online_participants=None, need_sync=True) -> dict:
    """
    Возвращает идентификатор участника по идентификатору частного лица, подразделения, помещения,
    комнаты или машины
    :param ids:
        IdPersons - идентификатор частного лица
        PersonsUUID - идентификатор пользователя
        IdDepartments - идентификатор подразделения
        IdRooms - идентификатор помещения
        IdVehicles - идентификатор машины
    :param by_physical:
    :param online_participants: Участники с Online (результат Лицо.ListSync)
    :return: возвращает hashtable вида {id_ext_participant: @Participant}
    Примечание: метод дополнительно синхранизирует участника, если не найден в базе
    """

    def get_ext_participant(rec):
        return rec.Get('Person') or rec.Get('Department') or rec.Get('Room') or \
               rec.Get('Vehicle')

    def to_type(values, need_type):
        if not values:
            return
        if not isinstance(values, list):
            values = [values]
        return [need_type(value) if value is not None and not isinstance(value, need_type)
                else value for value in values]

    id_persons = to_type(ids.get('Persons'), int)
    uuid_persons = to_type(ids.get('PersonsUUID'), uuid.UUID)
    id_departments = to_type(ids.get('Departments'), int)
    id_rooms = to_type(ids.get('Rooms'), int)
    id_vehicles = to_type(ids.get('Vehicles'), int)

    result = {}
    if any((id_persons, id_departments, id_rooms, id_vehicles)):
        _filter = {}
        all_ext_id_participants = []
        for field, ext_ids in (
                ('Persons', id_persons),
                ('Departments', id_departments),
                ('Rooms', id_rooms),
                ('Vehicles', id_vehicles),
        ):
            if ext_ids:
                _filter[field] = ext_ids
                all_ext_id_participants += ext_ids
        _filter['Client'] = sbis.Session.ClientID()

        _filter = sbis.Record(_filter)
        participants = sbis.Participant.List(None, _filter, None, None)
        result = {get_ext_participant(p): p.Get('@Participant') for p in participants}

        participants_need_sync = list(set(all_ext_id_participants) - set(result.keys()))
        if need_sync and participants_need_sync:
            new_participants = sync(ext_participants=participants_need_sync, online_participants=online_participants)
            result.update(new_participants)

    if uuid_persons:
        uuid_participants = get_by_persons_uuid(uuid_persons, by_physical=by_physical)
        participants_need_sync = list(set(uuid_persons) - set(uuid_participants))
        if need_sync and participants_need_sync:
            new_participants = sync(ext_uuid_participants=participants_need_sync, by_physical=by_physical)
            uuid_participants.update(new_participants)
        result.update(uuid_participants)

    return {str(k): v for k, v in result.items()}


def get_auth_person_uuid() -> uuid.UUID:
    """
    Возвращает uuid авторизованного пользователя
    """
    uuid_from_headers = sbis.Session.GetHeader('X-SPID')
    if uuid_from_headers:
        try:
            uuid_from_headers = uuid.UUID(uuid_from_headers)
        except ValueError:
            sbis.WarningMsg('X-SPID from headers is incorrect: {}'.format(uuid_from_headers))
            uuid_from_headers = None
    if uuid_from_headers:
        return uuid_from_headers
    sbis.WarningMsg('Failed to get X-SPID from headers!')
    user_id = sbis.Session.UserID()
    if user_id:
        user_uuid = sbis.EndPoint('admin-api').Пользователь.GetPersonId(user_id)
        if user_uuid:
            try:
                return uuid.UUID(user_uuid)
            except ValueError:
                sbis.WarningMsg('Can\'t create UUID from admin-api service, user_id: {}, user_uuid: {}'.format(user_id, user_uuid))


def get_auth() -> int:
    """
    Возвращает идентификатор авторизованного участника
    """
    uuid_person = sbis.Participant.GetAuthPersonUUID()
    params = {
        'PersonsUUID': [uuid_person],
    }
    return get_by_ext_id(params).get(str(uuid_person))


def get_by_persons_uuid(uuid_persons, by_physical=False) -> dict:
    """
    Возвращает участников по uuid персон
    :param uuid_persons: uuid персон
    :param by_physical: участник по кабинету физика
    :return: hashtable вида {uuid_person: @Participant}
    """
    if not isinstance(uuid_persons, list):
        if uuid_persons is None:
            pass
        uuid_persons = [uuid_persons]

    id_participants = {}

    _filter = sbis.Record({
        'PersonsUUID': uuid_persons,
        'Client': get_physic_client() if by_physical else sbis.Session.ClientID()
    })

    participants = sbis.Participant.List(None, _filter, None, None)
    for participant in participants:
        uuid_person = participant.Get('PersonUUID')
        id_participant = participant.Get('@Participant')
        id_participants[uuid_person] = id_participant

    return id_participants


def get_auth_person() -> int:
    """
    Возвращает Person (ЧастноеЛицо) авторизованного участника, если у него есть корпоративный
    аккаунт
    """
    auth_uuid_person = sbis.Participant.GetAuthPersonUUID()
    if auth_uuid_person:
        person = get_person_by_uuid(auth_uuid_person)
        if not person:
            sbis.WarningMsg('Запрошено ЧастноеЛицо участника, не имеющего доступ на online.sbis.ru')
        return person


def has_corp_account(uuid_person: uuid.UUID) -> bool:
    """
    Проверяет, есть ли корпоративный аккаунт у персоны (доступ на onlins.sbis.ru)
    :param uuid_person:
    """

    def result_validator(result):
        # Если вернулось False попробуем переспросить
        return result

    if not uuid_person:
        uuid_person = sbis.Participant.GetAuthPersonUUID()
    if uuid_person:
        return invoke_profiles(result_validator, 'Person', 'HasCorpAccount', uuid_person) or False
    return False


def _get_exists_person(person_list, client_id):
    if not all([person_list, client_id]):
        return None
    sql = '''
        SELECT MAX("Person")
          FROM "Participant"
         WHERE "Person" = ANY($1::int[])
           AND "Client" = $2::int
    '''
    return sbis.SqlQueryScalar(sql, person_list, client_id)


def get_person_by_uuid(uuid_person: uuid.UUID, id_client=None) -> int:
    """
    Возвращает идентификатор частного лица по uuid персоны
    :param id_client:
    :param uuid_person:
    """

    id_client = id_client or sbis.Session.ClientID()
    profile_auth_person = invoke_profiles(functools.partial(has_all_requested_person_filled_plain_result, [uuid_person]),
                                          'Face', 'ByPersonsAndAccount', [uuid_person], id_client)
    sbis.LogMsg(sbis.LogLevel.llMINIMAL, 'ByPersonsAndAccount-> {}'.format(str(profile_auth_person)))
    persons = profile_auth_person.Filter({'Person': uuid_person}) if profile_auth_person else []
    if len(persons) == 1:
        result = persons[0].Get('Face')
    elif len(persons) == 0:
        result = None
    else:
        # сложная ситуация, частных лиц для одного профиля пришло несколько, надо поискать уже существующего
        persons_id = list(set(persons.ToList('Face')) - {None})
        result = _get_exists_person(persons_id, id_client)
        if not result:
            # если ни одного не нашли, пробуем синхронизировать
            persons_sync = sync(persons_id)
            result = int(list(persons_sync.keys())[0]) if persons_sync else None

    if result:
        return result

    if id_client != get_physic_client():
        sbis.WarningMsg('Failed to get auth person! UUID: {} Client: {}. Response: {}'.format(
            uuid_person,
            id_client,
            profile_auth_person))


def get_uuid_by_persons(id_persons):
    """
    Возвращает uuid персон по идентификаторам частных лиц
    :param id_persons:
    """
    result = {}
    id_client = sbis.Session.ClientID()
    _filter = [{'Account': id_client, 'Face': id_person} for id_person in id_persons]
    profile_persons = invoke_profiles(functools.partial(has_all_requested_private_persons_filled,
                                                        id_persons,
                                                        id_client),
                                      'Person', 'ByFaces', _filter, False)
    for face in profile_persons:
        id_face = face.get('Face')
        if id_client == face.get('Account') and id_face in id_persons:
            result[id_face] = uuid.UUID(face.get('Person'))
    return result


def _get_participant_format():
    """
    Формат участника
    """
    _format = sbis.RecordFormat()
    _format.AddInt32('Person')
    _format.AddInt32('User')
    _format.AddString('LastName')
    _format.AddString('FirstName')
    _format.AddString('MiddleName')
    _format.AddUuid('PersonUUID')
    _format.AddInt32('Department')
    _format.AddInt32('Room')
    _format.AddInt32('Vehicle')
    _format.AddInt32('Client')
    return _format


def get_party_id(rec):
    return rec.Get('Person') or rec.Get('Department') or rec.Get('Room') or rec.Get('Vehicle')


def get_person_uuid(rec):
    return rec.Get('PersonUUID')


def dummy_validator(*args):
    return True


def has_right_account(accounts: list, id_client: int):
    for acc in accounts:
        if acc.get('Account', 0) == id_client:
            return True


def format_person_to_participant(person: dict, id_client: int, _format):
    right = has_right_account(person.get('Faces'), id_client)
    if not right:
        sbis.WarningMsg('Не найден пользователь для текущего лица {} - {}'.format(id_client, person))
        return None
    participant = sbis.Record(_format)
    participant['Person'] = person.get('Face')
    participant['LastName'] = person.get('LastName')
    participant['FirstName'] = person.get('FirstName')
    participant['MiddleName'] = person.get('PatronymicName')
    participant['PersonUUID'] = person.get('Person')
    participant['Client'] = id_client
    return participant


def mass_create_participants(participants: sbis.RecordSet):
    sql = '''
        WITH data AS (
            SELECT * FROM json_to_recordset($1::json) AS X(
                    "Person" int
                    , "LastName" text
                    , "FirstName" text
                    , "MiddleName" text
                    , "PersonUUID" uuid
                    , "Client" int
            )
        )
        INSERT INTO "Participant" (
                    "Person"
                    , "LastName"
                    , "FirstName"
                    , "MiddleName"
                    , "PersonUUID"
                    , "Client"
        )
            SELECT
                    "Person"
                    , "LastName"
                    , "FirstName"
                    , "MiddleName"
                    , "PersonUUID"
                    , "Client"
                FROM data
            RETURNING "@Participant"
    '''
    new_persons = sbis.SqlQuery(sql, json.dumps(participants.as_list(), cls=SBISEncoder))
    return new_persons.ToList()


def check_persons(persons_uuid: list, by_physical=False):
    # вначале проверяем какие пользователи есть у нас + с учетом клиента
    id_client = get_physic_client() if by_physical else sbis.Session.ClientID()
    sbis.Client.CheckClients([id_client])
    _filter = sbis.Record({
        'PersonsUUID': persons_uuid,
        'Clients': [id_client]
    })
    exist_persons = sbis.Participant.List(None, _filter, None, None).ToList('PersonUUID')
    diff_persons = list(set(persons_uuid) - set(exist_persons))
    if not diff_persons:
        return
    persons = invoke_profiles(dummy_validator, 'Person', 'List', diff_persons, [])
    if not persons or len(diff_persons) != persons.get('Count', 0):
        sbis.WarningMsg('Сервис профилей вернул меньше данных чем было передано:\n{}\n{}/{}'.format(
                        persons_uuid, len(persons_uuid), persons.get('Count', 0)))
        return
    _format = _get_participant_format()
    participants_to_create = sbis.RecordSet(_format)
    for person in persons.get('Persons', []):
        participant = format_person_to_participant(person, id_client, _format)
        if participant:
            participants_to_create.AddRow(participant)
    new_participants = mass_create_participants(participants_to_create)
    if len(new_participants) != len(participants_to_create):
        sbis.WarningMsg('Количество созданных пользователей не равно количеству переданных: ', participants_to_create)
    return new_participants


def _from_room(new_data: sbis.Record, room: sbis.RecordFormat):
    new_data.AddString("LastName", room.Get('Помещение.Название') or room.Get('Название'))
    new_data.AddInt64("Room", room.Get('@Лицо'))


def _from_department(new_data: sbis.Record, department: sbis.RecordFormat):
    new_data.AddString("LastName", department.Get('Название'))
    new_data.AddInt64("Department", department.Get('@Лицо'))


def _from_vehicle(new_data: sbis.Record, vehicle: sbis.RecordFormat):
    new_data.AddString("LastName", vehicle.Get('Vehicle.Model'))
    new_data.AddInt64("Vehicle", vehicle.Get('@Лицо'))


def get_participant_filter(_filter: sbis.Record, person_type: str):
    new_data = sbis.Record()
    if person_type == '"ЧастноеЛицо"':
        pass
    elif person_type == '"СтруктураПредприятия"':
        _from_department(new_data, _filter)
    elif person_type == '"Помещение"':
        _from_room(new_data, _filter)
    elif person_type == '"Capital"':
        _from_vehicle(new_data, _filter)
    else:
        sbis.WarningMsg("Передан неизвестный тип Лица")
    return new_data


def get_type_from_sync(sync: sbis.RecordSet, person: int, client: int):
    person_types = {0: '"ЧастноеЛицо"', 1: '"СтруктураПредприятия"', 2: '"Помещение"', 3: '"Capital"'}
    for rec in sync:
        if rec.Get('@Лицо') == person and rec.Get('Client') == client:
            return person_types.get(rec.Get('Тип'))


def get_participant_type(_type: str):
    person_types = {'"ЧастноеЛицо"': 'Persons', '"СтруктураПредприятия"': 'Departments', '"Помещение"': 'Rooms',
                    '"Capital"': 'Vehicles'}
    return person_types.get(_type)


def get_person_type(_filter: sbis.Record, sync: sbis.RecordSet, person: int, client: int):
    _type = _filter.Get("Лицо_") or _filter.Get("Лицо.Лицо_") or get_type_from_sync(sync, person, client)
    if not _type:
        sbis.ErrorMsg('Не установлен тип Лица! {}'.format(_filter))
    return _type


def get_participant_info(_filter: sbis.Record, rec_type: str, need_create: bool):
    """
     :param  _filter: содержит поля sync/client/new/old
     :param rec_type: тип записи, которая содержит необходимые данные
     :param need_create: флаг - создавать пользователя, если не найден
     :return participant_id, _type, client
    """
    rec = _filter.Get(rec_type)
    client = _filter.Get("client")
    sync = _filter.Get("sync")
    person = rec.Get("@Лицо")
    _type = get_person_type(rec, sync, person, client)
    ids = {get_participant_type(_type): person}
    _id = sbis.Participant.GetByExtId(ids, sync, need_create)
    return _id.get(str(person), None), _type, client


def update_all(_filter: sbis.Record):
    """
    Метод для обновления ФИО/название пользователя/машины
    :param  old: старая запись
    :param  new: новая запись
    :param sync: результат вызова Лицо.ListSync
    """
    participant_id, _type, client = get_participant_info(_filter, 'new', True)
    new_data = get_participant_filter(_filter.Get("new"), _type)
    new_data.AddInt64("Client", client)
    new_data.AddInt64("@Participant", participant_id)
    sbis.Participant.Update(new_data)


def delete_all(_filter: sbis.Record):
    """
    Метод для удаления пользователя/машины/...
    :param @Лицо: лицо удяляемого пользователя
    """
    participant_id, _type, client = get_participant_info(_filter, 'old', False)
    if participant_id:
        sbis.Participant.Delete(participant_id)
