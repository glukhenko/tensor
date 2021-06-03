"""
Модуль предназначен для хранения/обработки всех типов календарей (рабочих/персональных/гугл)
"""

import json
import uuid
import datetime
import traceback
import collections
import heapq
import sbis
import event
from calendar_base.utils import remote_invoke_online
from calendar_base.utils import rgb_int_to_hex, rgb_hex_to_int, select_standard_color
from calendar_base.utils import get_site
from calendar_base.utils import get_physic_client, is_demo_scheme
from calendar_base.utils import get_initials_name
from participant.participant import get_by_ext_id, get_person_by_uuid, get_uuid_by_persons
from participant.participant import sync
from participant.participant import get_participant_info
from calendar_common.const import SITE_MY, SITE_ONLINE
from calendar_common.const import (CALENDAR_MY_CUSTOM, CALENDAR_MY_PERSONAL, CALENDAR_ONLINE_CUSTOM,
                                   CALENDAR_ONLINE_ROOM, CALENDAR_ONLINE_VEHICLE, CALENDAR_ONLINE_WORKING,
                                   CALENDAR_PERSON_QUEUE, CALENDAR_DEPARTMENT_QUEUE)
from calendar_common.const import PARTICIPANT_DEPARTMENT, PARTICIPANT_PERSON, PARTICIPANT_VEHICLE, PARTICIPANT_ROOM
from calendar_common.const import CALENDAR_BY_PARTICIPANT
from calendar_common.const import (CUSTOM_CALENDAR_COLOR, WORK_CALENDAR_COLOR, ROOM_CALENDAR_COLOR,
                                   VEHICLE_CALENDAR_COLOR, PERSONAL_CALENDAR_COLOR, DEFAULT_CALENDAR_COLOR)
from calendar_common import SBISEncoder
from calendar_common import cached_property

# Calendar property - Flags
FLAG_PUBLIC = 0
FLAG_VISIBLE_ALL = 1
FLAG_CAN_EDIT_ALL = 2

DEFAULT_NAME_CALENDAR = 'Новый календарь'

# Type access for calendar
LIMIT_READ = 0
READ = 1
EDIT = 2
ADVANCED_EDIT = 3
ADMIN = 4
ZONE_ADMIN = 6

NAME_ACCESS = {
    LIMIT_READ: 'Ограниченный просмотр',
    READ: 'Просмотр',
    EDIT: 'Редактирование',
    ADVANCED_EDIT: 'Расширенное редактирование',
    ADMIN: 'Администрирование',
}

WARNING_QUEUE_EXISTS = 'Очередь в это подразделение уже существует'
WARNING_QUEUE_EXISTS_CODE = 1

# сохранение истории можно активировать, после переноса истории от @КалендарьПользователя к @Calendar
# (аналогично для разрешений и для работ)
SAVE_HISTORY = False

create_list = lambda value: [value] if value else None


class Calendar(object):
    def __init__(self, *args, **kwargs):

        self.client = sbis.Session.ClientID()
        # store property of participant
        self.participants = {}
        self.ext_participants = {}
        self.site = get_site()
        self.participant = None
        self.type_participant = None
        self.physic_participant = None
        self.person = None
        self.uuid_person = None
        self.uuid_calendar = None
        self.type_calendar = None
        self.color = None
        self.name = None
        self.name_owner = None
        self.department = None
        self.room = None
        self.vehicle = None
        self.is_remove = False

        self.owner = None
        self.only_main = None
        self.only_can_edit = False
        self.only_active = False
        self.visible_to_all = False
        self.can_edit_all = False

        self.calc_fields = (
            # field_name, func, default_value
            ('@CalendarPermission', sbis.Record.AddInt32, None),
            ('Main', sbis.Record.AddBool, None),
            ('MainAuthPerson', sbis.Record.AddBool, None),
            ('Person', sbis.Record.AddInt32, None),
            ('PersonUUID', sbis.Record.AddUuid, None),
            ('Department', sbis.Record.AddInt32, None),
            ('Room', sbis.Record.AddInt32, None),
            ('Vehicle', sbis.Record.AddInt32, None),
            ('Owner', sbis.Record.AddInt32, None),
            ('TypeOwner', sbis.Record.AddInt32, None),
            ('FIO', sbis.Record.AddString, None),
            ('Access', sbis.Record.AddInt32, None),
            ('VisibleToAll', sbis.Record.AddBool, None),
            ('CanEditAll', sbis.Record.AddBool, None),
            ('CanRemove', sbis.Record.AddBool, None),
            ('Color', sbis.Record.AddInt32, None),
            ('ColorStr', sbis.Record.AddString, None),
            ('Order', sbis.Record.AddInt32, None),
            ('Subscribers', sbis.Record.AddArrayInt32, None),
            ('Show', sbis.Record.AddBool, None),
            ('TZ', sbis.Record.AddFloat, None),
            ('IsOwner', sbis.Record.AddBool, None),
            ('IsDepartmentPermission', sbis.Record.AddBool, None),
            ('IsRemoved', sbis.Record.AddBool, None),
        )

    @cached_property
    def auth_person_uuid(self):
        return sbis.Participant.GetAuthPersonUUID()

    @cached_property
    def auth_person(self):
        return sbis.Participant.GetAuthPerson()

    @cached_property
    def auth_participant(self):
        auth_participant = self._get_by_ext_id_cache(
            id_persons=self.auth_person
        ).get(str(self.auth_person))
        if self.site == SITE_ONLINE and auth_participant is None:
            sbis.WarningMsg('Текущий авторизованный участник не определен!')
        return auth_participant

    @cached_property
    def auth_physic_participant(self):
        auth_physic_participant = self._get_by_ext_id_cache(
            uuid_persons=self.auth_person_uuid,
            by_physical=True,
        ).get(str(self.auth_person_uuid))
        if auth_physic_participant is None:
            sbis.WarningMsg('Текущий авторизованный участник-физлицо не определен!')
        return auth_physic_participant

    def add_calc_fields(self, record):
        for field, func, default in self.calc_fields:
            if field not in record:
                if default:
                    func(record, field, default)
                else:
                    func(record, field)

    def load_filter(self, _filter):
        """
        Загружает фильтр запроса
        Примечание: при создании нового календаря, в _filter['Participant'] присутствует запись
        об участнике, для которого создается календарь
        """

        def get_owner(_filter):
            """
            Определяем участника по переданному частному лицу/помещению/машине
            Если индификаторы не переданы, то по умолчанию берем атворизованного участника
            Примечание:
                Тип календаря влияет на участника. Для календарей с online, участник идет
            с соответствующей схемой, для персональных календарей, участник идет с Client = схемы физиков, вне
            зависимости, с какого сайта пришел запрос.
            """
            if 'Owner' not in _filter:
                _filter.CopyOwnFormat()
                _filter.AddInt32('Owner')
            participant = _filter.Get('Participant')
            if participant:
                participants = sbis.RecordSet(participant.Format())
                participants.AddRow(participant)
                new_participants = sbis.Participant.SyncParticipants(participants)
                if new_participants:
                    new_participant = new_participants[0]
                    _filter['Owner'] = new_participant.Get('@Participant')
            owner = _filter.Get('Owner')
            if not owner:
                id_person = _filter.Get('Person')
                uuid_person = _filter.Get('PersonUUID')
                id_department = _filter.Get('Department')
                id_room = _filter.Get('Room')
                id_vehicle = _filter.Get('Vehicle')
                # Участник (в том числе и дефолтный) зависит от типа календаря
                ext_id_participant = id_person or uuid_person or id_department or id_room or id_vehicle
                if ext_id_participant:
                    if self.type_calendar in (CALENDAR_MY_PERSONAL, CALENDAR_MY_CUSTOM):
                        owner = self._get_by_ext_id_cache(
                            uuid_persons=uuid_person,
                            by_physical=True
                        ).get(str(uuid_person)) or self.auth_physic_participant
                    elif self.type_calendar in (CALENDAR_ONLINE_WORKING, CALENDAR_ONLINE_CUSTOM,
                                                CALENDAR_ONLINE_ROOM, CALENDAR_ONLINE_VEHICLE,
                                                CALENDAR_DEPARTMENT_QUEUE, CALENDAR_PERSON_QUEUE):
                        ext_id_participant = id_person or id_department or id_room or id_vehicle
                        owner = self._get_by_ext_id_cache(
                            id_persons=id_person,
                            id_departments=id_department,
                            id_rooms=id_room,
                            id_vehicles=id_vehicle
                        ).get(str(ext_id_participant))
                else:
                    owner = self.auth_physic_participant if get_site() == SITE_MY else self.auth_participant
                if not owner:
                    raise sbis.Error('Participant is not identified')
                _filter['Owner'] = owner
            return owner

        id_calendar = _filter.Get('@Calendar') or _filter.Get('Calendar')
        self.type_calendar = _filter.Get('Type')
        if not self.type_calendar and id_calendar:
            calendar = sbis.Calendar.Read(id_calendar)
            self.type_calendar = calendar.Get('Type')
        self.owner = get_owner(_filter)
        self.uuid_calendar = _filter.Get('CalendarUUID')
        color = rgb_hex_to_int(_filter.Get('ColorStr'))
        self.color = color or _filter.Get('Color') or self.get_default_color()
        if self.type_calendar in (CALENDAR_ONLINE_WORKING, CALENDAR_ONLINE_CUSTOM, CALENDAR_ONLINE_ROOM,
                                  CALENDAR_ONLINE_VEHICLE, CALENDAR_MY_PERSONAL, CALENDAR_MY_CUSTOM):
            self.color = select_standard_color(self.color)
        self.name = _filter.Get('Name')
        self.name_owner = self._get_short_fio_participant(_filter.Get('Owner'))
        self.type_participant = self.get_type_participant(self.owner)
        self.person = _filter.Get('Person')
        self.uuid_person = _filter.Get('PersonUUID')
        if not self.uuid_person and self.owner:
            self.uuid_person = self._get_participant(self.owner).get('PersonUUID')
        self.room = _filter.Get('Room')
        self.vehicle = _filter.Get('Vehicle')
        self.department = _filter.Get('Department')
        self.is_remove = _filter.Get('IsRemoved') or False
        self.can_edit_all = _filter.Get('CanEditAll')
        self.visible_to_all = _filter.Get('VisibleToAll')

    def create_department_permission(self, owner):
        # check department permission, create if need
        departments = self.__get_departments_by_participant(owner)
        id_depart_participants = departments.get('Departments', [])
        id_depart_permission = self.__get_permissions_by_depart(id_depart_participants)
        if id_depart_permission:
            sbis.CalendarPermission.CreatePersonal(owner, id_depart_permission)

    def on_init(self, calendar, _filter, name_method, params):
        """
        Инициализации записи при создании календаря 
        """
        self.add_calc_fields(calendar)
        self.add_calc_fields(_filter)
        self.load_filter(_filter)
        self.check_type_calendar()
        self.check_type_participant()
        self.set_default()

        auth_participant = self.auth_participant or self.auth_physic_participant
        calendar['CalendarUUID'] = self.uuid_calendar
        calendar['Name'] = self.name
        calendar['Type'] = self.type_calendar
        calendar['Flags'] = self.flags
        calendar['Color'] = self.color
        calendar['ColorStr'] = rgb_int_to_hex(self.color)
        calendar['Owner'] = self.owner
        calendar['PersonUUID'] = self.uuid_person
        calendar['Version'] = self.version
        calendar['IsDepartmentPermission'] = False
        calendar['Show'] = False
        calendar['Main'] = self.type_calendar == CALENDAR_ONLINE_WORKING
        calendar['MainAuthPerson'] = False
        calendar['VisibleToAll'] = self.flags[FLAG_VISIBLE_ALL]
        calendar['CanEditAll'] = self.flags[FLAG_CAN_EDIT_ALL]
        calendar['CanRemove'] = self.owner == auth_participant
        calendar['Access'] = ADMIN
        calendar['IsRemoved'] = False

        if self.owner:
            calendar['IsOwner'] = self.owner == auth_participant
            calendar['TypeOwner'] = self.type_participant
            participant = self._get_participant(self.owner)
            if self.site == SITE_ONLINE:
                if self.type_participant == PARTICIPANT_PERSON:
                    calendar['Person'] = participant.get('Person')
                elif self.type_participant == PARTICIPANT_ROOM:
                    calendar['Room'] = participant.get('Room')
                elif self.type_participant == PARTICIPANT_VEHICLE:
                    calendar['Vehicle'] = participant.get('Vehicle')

    def on_before_create(self, calendar, _filter, params):
        """
        Перед созданием календаря
        """
        self.load_filter(_filter)
        self._check_name()
        self.check_type_calendar()
        self.check_type_participant()
        self.set_default()
        # Установим флаги
        visible_to_all = _filter.Get('VisibleToAll')
        can_edit_all = _filter.Get('CanEditAll')
        if visible_to_all is not None:
            self.flags[FLAG_VISIBLE_ALL] = visible_to_all
        if can_edit_all is not None:
            self.flags[FLAG_CAN_EDIT_ALL] = can_edit_all

        calendar['CalendarUUID'] = self.uuid_calendar
        calendar['Name'] = self.name
        calendar['Flags'] = self.flags
        calendar['Owner'] = self.owner
        calendar['Subscribers'] = 1

        if calendar.Get('Type') == CALENDAR_DEPARTMENT_QUEUE:
            result = sbis.SqlQueryScalar('''
                SELECT pg_try_advisory_xact_lock( '"Calendar"'::regclass::integer, $1::integer )
            ''', self.owner)
            if result is not True:
                raise sbis.Warning(WARNING_QUEUE_EXISTS, WARNING_QUEUE_EXISTS_CODE)

    def on_after_create(self, calendar, _filter, params):
        """
        После создания календаря
        """
        calendar_type = calendar.Get('Type')
        self.load_filter(_filter)
        # save history
        messages = {
            CALENDAR_ONLINE_WORKING: 'Создан рабочий календарь для "{}"'.format(_filter.Get('NameOwner')),
            CALENDAR_ONLINE_CUSTOM: 'Создан личный календарь "{}"'.format(calendar.Get('Name')),
            CALENDAR_ONLINE_ROOM: 'Создан календарь для помещения "{}"'.format(_filter.Get('NameOwner')),
            CALENDAR_MY_PERSONAL: 'Создан персональный календарь для {}'.format(_filter.Get('NameOwner')),
            CALENDAR_MY_CUSTOM: 'Создан личный календарь "{}"'.format(calendar.Get('Name')),
            CALENDAR_PERSON_QUEUE: 'Создана персональная очередь "{}"'.format(_filter.Get('NameOwner')),
            CALENDAR_DEPARTMENT_QUEUE: 'Создана очередь подразделения "{}"'.format(_filter.Get('NameOwner')),
        }
        msg = messages.get(calendar_type, '')

        if msg:
            sbis.HistoryMsg(msg, 'Создание календаря', 'УправлениеРабочимВременем_Календарь',
                            str(calendar.Get('CalendarUUID')))

        if calendar_type not in [CALENDAR_PERSON_QUEUE, CALENDAR_DEPARTMENT_QUEUE]:
            # need create main permission
            owner = _filter.Get('Owner')
            if calendar_type == CALENDAR_ONLINE_ROOM:
                owner = self.auth_participant() or owner
            permission_filter = sbis.Record({
                'Calendar': calendar.Get('@Calendar'),
                'Main': True,
                'Owner': owner,
                'Access': ADMIN,
                'Color': self.color,
                'Show': list([owner]),
                'Departments': _filter.Get('Departments')
            })
            sbis.CalendarPermission.Update(permission_filter)

        if calendar_type == CALENDAR_ONLINE_WORKING:
            self.create_department_permission(owner)

    def same_name_exists(self):
        auth_participant = self.auth_participant or self.auth_physic_participant
        if auth_participant:
            _filter = sbis.Record({
                'Owner': auth_participant,
                'ExcludeRemoved': True,
                'without_handlers': True
            })
            calendars = sbis.Calendar.List(None, _filter, None, None)
            for calendar in calendars:
                if calendar.Get('Type') in (CALENDAR_ONLINE_WORKING, CALENDAR_MY_PERSONAL):
                    continue
                eq_name = calendar.Get('Name') and calendar.Get('Name') == self.name
                diff_uuid = calendar.Get('CalendarUUID') and self.uuid_calendar != calendar.Get('CalendarUUID')
                if eq_name and diff_uuid:
                    return calendar.Get('@Calendar')
        return -1

    def __get_departments_by_participant(self, id_participant, departments=None):
        """
        Получает вышестоящие подразделения для участника типа Person
        Возвращает словарь с ключами:
            - Participant, int - запрашиваемый участник-сотрудник
            - Departments, int[] - вышестоящие подразделения, для запрашиваемого участника
            - Levels, int[] - кортежи вида (Level, DepartmentParticipant)
        Примечание 1: исторически сложилось что рекорд с Level=0 это запрашиваемый сотрудник
        """
        result = {
            'Participant': id_participant,
            'Departments': [],
            'Levels': [],
        }
        departments = departments or []
        if self.site == SITE_ONLINE:
            # Примечание 2: в результате метода ЧастноеЛицо.GetDepartments Participant это
            # Лицо СтруктураПредприятия (для Level != 0)
            levels = {dep.Get('Participant'): dep.Get('Level') for dep in departments if
                      dep.Get('Level')}
            if levels:
                depart_participants = sbis.Participant.GetDictByDepartmentList(
                    self.client, list(levels.keys())) or {}
                depart_participants = depart_participants or {}
                for department, participant in depart_participants.items():
                    level = levels.get(int(department))
                    if level:
                        result['Departments'] += participant
                        result['Levels'].append((level, participant[0]))

        return result

    def _get_fio_participant(self, id_participant):
        """
        Возвращает значение ФИО
        Прмечание:
            - для персоны с доступом к online: Participant fields
            - для персоны без доступа к online: Profile fields
            - для помещения: Participant fields
            - для машины: Participant fields
        """
        if not id_participant:
            return ''
        participant = self._get_participant(id_participant)
        fio = ''
        if participant:
            fio = '{last_name} {first_name} {middle_name}'.format(
                last_name=participant.get('LastName') or '',
                first_name=participant.get('FirstName') or '',
                middle_name=participant.get('MiddleName') or '',
            )
        return fio

    def _get_short_fio_participant(self, id_participant):
        """
        Возвращает короткое значение ФИО вида: Фамилия И.О.
        """
        fio = self._get_fio_participant(id_participant)
        return get_initials_name(fio)

    def _get_by_ext_id_cache(self, id_persons=None, uuid_persons=None, id_departments=None,
                             id_rooms=None, id_vehicles=None, by_physical=False,
                             online_participants=None, need_sync=True):
        """
        Получает участника по внешним идентификаторам
        :param: by_physical - указывает что надо искать участника физика (поиск осуществляется
        только по uuid_persons)
        """

        _filter = {}
        for field, ext_ids in (
                ('Persons', id_persons),
                ('PersonsUUID', uuid_persons),
                ('Departments', id_departments),
                ('Rooms', id_rooms),
                ('Vehicles', id_vehicles),
        ):
            if ext_ids:
                if not isinstance(ext_ids, list):
                    ext_ids = [ext_ids]
                _filter[field] = ext_ids

        return get_by_ext_id(_filter, by_physical, online_participants, need_sync=need_sync)

    def get_user_calendar(self, calendar_uuid):
        """
        Возвращает запись календаря для сотрудника. Учитывается разрешение пользователя, цвет и др.
        свойства календаря
        :param calendar_uuid: идентификатор календаря
        :return: запись формата CalendarPermission.ListByPerson 
        """
        calendar = sbis.Calendar.ReadByUuid(calendar_uuid)
        if not calendar:
            sbis.WarningMsg('Не найден календарь по UUID: {}'.format(calendar_uuid))
            return sbis.Record()
        id_calendar = calendar.Get('@Calendar')
        _filter = sbis.Record({
            'Calendars': [id_calendar],
            'ShowRemoved': True,
        })
        calendars = sbis.CalendarPermission.ListByPerson(None, _filter, None, None)
        for calendar in calendars:
            if calendar.Get('@Calendar') == id_calendar:
                return calendar

    def _get_participant(self, id_participant):
        """
        Возвращает запись участника
        """
        if id_participant not in self.participants:
            participant = sbis.Participant.Read(id_participant)
            if participant:
                self.participants[id_participant] = participant.as_dict()
        return self.participants.get(id_participant, {})

    def _get_participants(self, id_participants):
        """
        Возвращает набор участников по идентификаторам
        Примечание: зачитывает, если отсутствуют в кэше
        """
        need_read = list(set(id_participants) - set(self.participants.keys()))
        if need_read:
            _filter = sbis.Record({
                '@Participant': need_read,
            })
            participants = sbis.Participant.List(None, _filter, None, None)
            participants = {p.Get('@Participant'): p.as_dict() for p in participants}
            self.participants.update(participants)
        return {id_participant: self.participants.get(id_participant)
                for id_participant in id_participants}

    def __get_permissions_by_depart(self, id_depart_participants):

        if not id_depart_participants:
            return []

        _filter = sbis.Record({
            'ByOwners': id_depart_participants,
        })
        depart_permission = sbis.CalendarPermission.List(None, _filter, None, None)
        id_depart_permission = [permission.Get('@CalendarPermission') for permission in
                                depart_permission]
        return id_depart_permission

    def on_before_update(self, calendar, _filter, params, old_calendar):
        """
        перед сохранением календаря
        Примечание: если у пользователя нет прав на редактирование календаря, просто перезальем данные от старой записи
         без уведомления клиента об ошибке
        """
        def mark_removed():
            """
            Помечаем календарь как удаленный, если пришел запрос удаления
            """
            if self.is_remove:
                if level_access != ADMIN:
                    raise sbis.Error("You are forbidden to delete the calendar! "
                                     "You haven't access.")
                type_calendars = {
                    CALENDAR_ONLINE_WORKING: 'You are forbidden to remove working calendar!',
                    CALENDAR_ONLINE_ROOM: 'You are forbidden to remove room calendar!',
                    CALENDAR_ONLINE_VEHICLE: 'You are forbidden to remove vehicle calendar!',
                    CALENDAR_MY_PERSONAL: 'You are forbidden to remove main personal calendar!',
                }
                type_calendar = old_calendar.Get('Type')
                if type_calendar in type_calendars:
                    raise sbis.Error(type_calendars.get(type_calendar))
                calendar['Removed'] = datetime.datetime.now()
                _filter.CopyOwnFormat()
                _filter.AddArrayInt64('Participant_list')
                sql_person = """
                    SELECT array_agg(cp."Owner")
                      FROM "Calendar" c
                     INNER JOIN "CalendarPermission" cp
                             ON cp."Calendar" = c."@Calendar"
                     INNER JOIN "Participant" p
                             ON cp."Owner" = p."@Participant"
                     WHERE p."Client" = c."ClientID"
                       AND c."CalendarUUID" = ANY($1::uuid[])
                """
                participant_list = sbis.SqlQueryScalar(sql_person, [old_calendar.Get('CalendarUUID')])
                _filter.Set("Participant_list", participant_list)

        def get_main_permission(id_calendar):
            """
            При создании календаря, можно сделать публичным, добавить разрешения, сменить цвет,
            сохранить, устновленный цвет не сохранится, т.к. локально нет @CalendarPermission,
            видимо клиенту нужно вызывать sbis.CalendarPermission.GetMain(id_calendar) если при 
            сохранении календаря не определено разрешение пока не перешли на сервис календарей 
            считаем что работаем с основным разрешением 
            """
            # TODO: костыль (убрать позже)
            permission = sbis.CalendarPermission.GetMain(id_calendar)
            if permission:
                return permission.Get('@CalendarPermission')

        def create_personal_permission(id_depart_permission):
            """
            Создает персональное разрешение, от подразделения
            """
            personal_permissions = sbis.CalendarPermission.CreatePersonal(self.auth_participant,
                                                                          [id_depart_permission])
            if personal_permissions:
                id_personal_permission = personal_permissions[0].Get('@CalendarPermission')
                personal_permission = sbis.CalendarPermission.Read(id_personal_permission)
                return personal_permission

        self.load_filter(_filter)
        self._check_name()
        id_calendar = old_calendar.Get('@Calendar')
        if get_site() == SITE_MY or self.type_calendar in (CALENDAR_MY_PERSONAL, CALENDAR_MY_CUSTOM):
            owner = self.auth_physic_participant
        else:
            owner = self.auth_participant
        level_access = self._get_access(id_calendar, owner)
        mark_removed()

        id_permission = _filter.Get('@CalendarPermission')
        if not id_permission:
            id_permission = get_main_permission(id_calendar)
            _filter['@CalendarPermission'] = id_permission

        permission = sbis.CalendarPermission.Read(id_permission)
        if self.get_type_participant(permission.Get('Owner')) == PARTICIPANT_DEPARTMENT:
            permission = create_personal_permission(id_permission)
        # set color
        if permission['Color'] != self.color:
            permission['Color'] = self.color
            sbis.CalendarPermission.Update(permission)
        # set flags
        if level_access == ADMIN:
            if self.visible_to_all is not None:
                calendar['Flags'][FLAG_VISIBLE_ALL] = self.visible_to_all
            if self.can_edit_all is not None:
                calendar['Flags'][FLAG_CAN_EDIT_ALL] = self.can_edit_all
        # set version
        calendar['Version'] = (old_calendar.Get('Version') or 0) + 1
        # save fields
        save_fields = ['CalendarUUID', 'Type', 'Owner']
        if level_access != ADMIN:
            save_fields.append('Name')
            save_fields.append('Description')
            save_fields.append('Flags')
        if old_calendar.Get('Type') in (CALENDAR_ONLINE_WORKING, CALENDAR_ONLINE_ROOM, CALENDAR_MY_PERSONAL):
            save_fields.append('Name')
        # forbidden to change fields of calendar
        for save_field in save_fields:
            calendar[save_field] = old_calendar.Get(save_field)

    def on_after_update(self, calendar, _filter, params, old_calendar):
        """
        После сохранения календаря
        """
        if calendar.Get('Removed'):
            msg = 'Удален календарь'
            sbis.HistoryMsg(msg, 'Удаление календаря', 'УправлениеРабочимВременем_Календарь',
                            str(old_calendar.Get('CalendarUUID')))
            participant_list = _filter.Get('Participant_list')
            if participant_list:
                user_list, persons_uuid_list = self.get_person_list_by_participant(participant_list)
                self.event_publish(calendar.Get('@Calendar'), 'delete', user=user_list,
                                   persons_uuid=persons_uuid_list)
            return True
        else:
            self.event_publish(calendar.Get('@Calendar'), 'update', user=None, persons_uuid=self.auth_person_uuid)

        # save revision history
        msgs = []
        if old_calendar.Get('Name') != calendar.Get('Name'):
            msg = 'Изменено название календаря: {}'.format(calendar.Get('Name'))
            msgs.append(msg)
        if old_calendar.Get('Description') != calendar.Get('Description'):
            msg = 'Изменено описание календаря: {}'.format(calendar.Get('Description'))
            msgs.append(msg)

        set_personal = False
        if old_calendar.Get('Flags')[FLAG_VISIBLE_ALL] != calendar.Get('Flags')[FLAG_VISIBLE_ALL]:
            if calendar.Get('Flags')[FLAG_VISIBLE_ALL]:
                msg = 'Изменено состояние календаря: публичный'
            else:
                msg = 'Изменено состояние календаря: личный'
                set_personal = True
            msgs.append(msg)

        if old_calendar.Get('Flags')[FLAG_CAN_EDIT_ALL] != calendar.Get('Flags')[FLAG_CAN_EDIT_ALL]:
            if calendar.Get('Flags')[FLAG_CAN_EDIT_ALL]:
                msg = 'Изменено состояние календаря: все могут редактировать'
            else:
                msg = 'Изменено состояние календаря: все могут просматривать'
            if not set_personal:
                msgs.append(msg)

        for msg in msgs:
            sbis.HistoryMsg(msg, 'Редактирование календаря', 'УправлениеРабочимВременем_Календарь',
                            str(old_calendar.Get('CalendarUUID')))

    def mark_as_removed(self, _filter):
        participant_id, _type, client = get_participant_info(_filter, 'old', False)
        if participant_id:
            sql = '''
                UPDATE "Calendar"
                   SET "Removed" = now()
                 WHERE "Owner" = $1::int
                   AND "ClientID" = $2::int
             RETURNING "@Calendar"
            '''
            res = sbis.SqlQuery(sql, participant_id, client)
            return bool(res)
        return False

    def unmark_as_removed(self, _filter):
        participant_id, _type, client = get_participant_info(_filter, 'old', False)
        if participant_id:
            sql = '''
                UPDATE "Calendar"
                   SET "Removed" = NULL
                 WHERE "Owner" = $1::int
                   AND "ClientID" = $2::int
             RETURNING "@Calendar"
            '''
            return bool(sbis.SqlQuery(sql, participant_id, client))
        return False

    def on_before_delete(self, calendar):
        """
        Перед удалением календаря
        """
        pass

    def set_default(self):
        self.uuid_calendar = self.uuid_calendar or uuid.uuid4()
        self.name = self.get_default_name()
        self.flags = self.get_default_flags()
        self.color = self.color or self.get_default_color()
        self.version = 0

    def get_default_name(self):
        names = {
            CALENDAR_ONLINE_WORKING: self.name_owner,
            CALENDAR_ONLINE_CUSTOM: self.name or sbis.rk(DEFAULT_NAME_CALENDAR),
            CALENDAR_ONLINE_ROOM: self.name_owner,
            CALENDAR_ONLINE_VEHICLE: self.name_owner,
            CALENDAR_MY_PERSONAL: self.name_owner,
            CALENDAR_MY_CUSTOM: self.name or sbis.rk(DEFAULT_NAME_CALENDAR),
            CALENDAR_PERSON_QUEUE: self.name or sbis.rk(DEFAULT_NAME_CALENDAR),
            CALENDAR_DEPARTMENT_QUEUE: self.name or sbis.rk(DEFAULT_NAME_CALENDAR),
        }
        return names.get(self.type_calendar) or ''

    def get_default_color(self):
        colors = {
            CALENDAR_ONLINE_WORKING: WORK_CALENDAR_COLOR,
            CALENDAR_ONLINE_CUSTOM: CUSTOM_CALENDAR_COLOR,
            CALENDAR_ONLINE_ROOM: ROOM_CALENDAR_COLOR,
            CALENDAR_ONLINE_VEHICLE: VEHICLE_CALENDAR_COLOR,
            CALENDAR_MY_PERSONAL: PERSONAL_CALENDAR_COLOR,
            CALENDAR_MY_CUSTOM: CUSTOM_CALENDAR_COLOR,
        }
        return colors.get(self.type_calendar)

    def get_default_flags(self):
        flags = {
            CALENDAR_ONLINE_WORKING: self.__get_flags(),
            CALENDAR_ONLINE_CUSTOM: self.__get_flags(),
            CALENDAR_ONLINE_ROOM: self.__get_flags(FLAG_VISIBLE_ALL, FLAG_CAN_EDIT_ALL),
            CALENDAR_MY_PERSONAL: self.__get_flags(FLAG_VISIBLE_ALL),
            CALENDAR_MY_CUSTOM: self.__get_flags(),
        }
        return flags.get(self.type_calendar) or self.__get_flags()

    @staticmethod
    def __get_flags(*args):
        return [flag in args for flag in range(3)]

    def check_type_calendar(self):
        if self.type_calendar is None:
            raise sbis.Error('You must set type calendar')
        elif self.site == SITE_MY and self.type_calendar not in (CALENDAR_MY_PERSONAL, CALENDAR_MY_CUSTOM):
            raise sbis.Error(
                'You are for forbidden to create calendar with type: {} on my.sbis.ru'.format(
                    self.type_calendar))

    def check_type_participant(self):

        if self.type_participant == PARTICIPANT_PERSON and self.type_calendar not in (
                CALENDAR_ONLINE_WORKING, CALENDAR_ONLINE_CUSTOM, CALENDAR_MY_PERSONAL, CALENDAR_MY_CUSTOM,
                CALENDAR_PERSON_QUEUE):
            raise sbis.Error(
                'You are forbidden to create calendar with type {} for person participant'.format(
                    self.type_calendar))
        elif self.type_participant == PARTICIPANT_ROOM and self.type_calendar not in (CALENDAR_ONLINE_ROOM,):
            raise sbis.Error(
                'You are forbidden to create calendar with type {} for room participant'.format(
                    self.type_calendar))
        elif self.type_participant == PARTICIPANT_VEHICLE and self.type_calendar not in (CALENDAR_ONLINE_VEHICLE,):
            raise sbis.Error(
                'You are forbidden to create calendar with type {} for vehicle participant'.format(
                    self.type_calendar))

    def check_exist_main_calendar(self):
        if self.type_calendar in (CALENDAR_ONLINE_WORKING, CALENDAR_MY_PERSONAL):
            calendars = sbis.Calendar.ReadByParticipant(self.participant, [self.type_calendar])
            return bool(len(calendars))
        return False

    def _check_name(self):
        if len(self.name) > 255:
            raise sbis.Warning(
                details='Слишком длинное название календаря.',
                user_msg='Слишком длинное название календаря.'
            )
        res = self.same_name_exists()
        if res and res > 0:
            raise sbis.Warning(
                details='Для пользователя {} уже существует календарь с именем "{}"'.format(self.owner, self.name),
                user_msg='У данного пользователя уже есть календарь с таким именем.'
            )

    def get_type_participant(self, id_participant):
        if id_participant is None:
            return None
        participant = self._get_participant(id_participant)
        type_participants = (
            ('Person', PARTICIPANT_PERSON),
            ('PersonUUID', PARTICIPANT_PERSON),
            ('Department', PARTICIPANT_DEPARTMENT),
            ('Room', PARTICIPANT_ROOM),
            ('Vehicle', PARTICIPANT_VEHICLE),
        )
        for field, type_participant in type_participants:
            if participant.get(field):
                return type_participant

    def get_owner(self, calendar_uuid):
        """
        Возвращает владельца календаря
        """
        _format = sbis.MethodResultFormat('Calendar.GetOwner', 1)
        result = sbis.Record(_format)
        if not calendar_uuid:
            return result

        calendar = sbis.Calendar.ReadByUuid(calendar_uuid)
        if calendar:
            id_participant = calendar.Get('Owner')
            participant = self._get_participant(id_participant)
            if participant:
                result['Person'] = participant.get('Person')
                result['Room'] = participant.get('Room')
                result['Vehicle'] = participant.get('Vehicle')
        return result

    def last_changes(self, _filter):
        """
        Метод возвращает актуальность календарей
        Примечание: параметр Show расчитывается только в режимах основной сетки. В режиме сетки
        сотрудника и салона красоты всегда возвращается True
        Примечание2: при запросе изменений по одному календарю всегда возвращается по основному
        разрешению календаря
        """

        def get_last_change_by_calendar(_format, calendar_uuid):
            calendar = sbis.Calendar.ReadByUuid(calendar_uuid)
            id_calendar = calendar.Get('@Calendar')
            permission = sbis.CalendarPermission.GetMain(id_calendar)
            rec = sbis.Record(_format)
            rec['@Calendar'] = id_calendar
            rec['CalendarUUID'] = calendar_uuid
            rec['Version'] = calendar.Get('Version')
            rec['Show'] = calendar.Get('Show')
            # метод не учитывает что календарь подключен авторизованному сотруднику, запрос
            # идет строго по авторизованному
            rec['Order'] = permission.Get('Order')
            return rec

        calendar_uuid = _filter.Get('CalendarUUID')
        id_person = _filter.Get('Person')
        only_main = _filter.Get('OnlyMain')
        id_participant = None

        if not calendar_uuid:
            if id_person:
                id_participant = self._get_by_ext_id_cache(id_persons=id_person).get(str(id_person))
            id_participant = id_participant or self.auth_participant or self.auth_physic_participant

        _format = sbis.MethodResultFormat('Calendar.LastChanges', 4)
        result = sbis.RecordSet(_format)
        if not any((calendar_uuid, id_participant)):
            return result

        if calendar_uuid:
            rec = get_last_change_by_calendar(_format, calendar_uuid)
            result.AddRow(rec)
        else:
            result = self.last_changes_by_person(id_participant, only_main)
        return result

    def last_changes_by_person(self, id_participant, only_main):
        """
        Возвращает список измененых календарей в аккордеоне для авторизованного сотрудника
        """
        _filter = sbis.Record({
            'Participants': [id_participant],
            'OnlyMain': only_main
        })
        _filter.AddArrayInt32('TypeCalendars')
        _filter.Set('TypeCalendars',
                    [CALENDAR_ONLINE_WORKING, CALENDAR_ONLINE_CUSTOM, CALENDAR_ONLINE_ROOM, CALENDAR_ONLINE_VEHICLE,
                     CALENDAR_MY_PERSONAL, CALENDAR_MY_CUSTOM])
        calendars = sbis.Calendar.ListByParticipant(None, _filter, None, None)
        _format = sbis.MethodResultFormat('Calendar.LastChanges', 4)
        result = sbis.RecordSet(_format)
        for calendar in calendars:
            rec = sbis.Record(_format)
            rec['@Calendar'] = calendar.Get('@Calendar')
            rec['CalendarUUID'] = calendar.Get('CalendarUUID')
            rec['Version'] = calendar.Get('Version')
            rec['Show'] = calendar.Get('Show')
            rec['Order'] = calendar.Get('Order')
            result.AddRow(rec)
        return result

    def read_by_uuid(self, uuid_calendar):
        """
        Возвращает запись календаря по uuid
        :param uuid_calendar: uuid календаря
        :return: record
        """
        sql = """
            SELECT
                *
            FROM
                "Calendar"
            WHERE
                "CalendarUUID" = $1::uuid
        """
        if uuid_calendar:
            return sbis.SqlQueryRecord(sql, uuid_calendar)

    def __get_id_person_by_uuid(self, uuid_person):
        """
        Возвращает идентификатор частного лица по uuid персоны
        """
        return get_person_by_uuid(uuid_person, self.client)

    def update_last_change(self, calendar_uuid):
        """
        Обновляет версию календаря
        Примечание: для корректного обновления календаря, необходимо передать идентификатор
        основного разрешения (пока на клиенте не полечат баг), см. Calendar.on_before_update
        """

        def get_id_permission(id_calendar):
            main_permission = sbis.CalendarPermission.GetMain(id_calendar)
            if main_permission:
                return main_permission.Get('@CalendarPermission')

        calendar = sbis.Calendar.ReadByUuid(calendar_uuid)
        if calendar:
            version = calendar.Get('Version') or 0
            calendar['Version'] = version + 1
            if '@CalendarPermission' not in calendar:
                calendar.AddInt32('@CalendarPermission')
            id_calendar = calendar.Get('@Calendar')
            calendar['@CalendarPermission'] = get_id_permission(id_calendar)
            sbis.Calendar.Update(calendar)

    def update_last_change_mass(self, calendar_uuids):
        """
        Обновление состояний календаря
        :param calendar_uuids: идентификаторы календарей
        """
        for calendar_uuid in calendar_uuids:
            self.update_last_change(calendar_uuid)

    def get_access_mass(self, calendar_uuids, id_person, uuid_person, departments=None, with_type=False):
        # получим участника
        calendar_uuids = list(set(calendar_uuids) - {None})
        if not calendar_uuids:
            return {}
        dummy_accesses = {str(calendar_uuid): READ for calendar_uuid in calendar_uuids}
        if with_type:
            dummy_types = {str(calendar_uuid): 0 for calendar_uuid in calendar_uuids}
            dummy_accesses = {'rights': dummy_accesses, 'types': dummy_types}
        id_participant = None
        if any((id_person, uuid_person)):
            id_ext_participant = id_person or uuid_person
            id_participant = self._get_by_ext_id_cache(
                id_persons=id_person,
                uuid_persons=uuid_person,
                by_physical=bool(uuid_person)
            ).get(str(id_ext_participant))
        _filter = sbis.Record({
            'ByUUID': calendar_uuids,
            'without_handlers': True,
        })
        calendars = sbis.Calendar.List(None, _filter, None, None).ToList('@Calendar')
        if not calendars:
            sbis.WarningMsg("Не найдены календари по фильтру {}".format(_filter))
            return dummy_accesses
        if not id_participant:
            id_participant = self._get_auth_participant_by_calendar(calendars[0])
        accesses = self._get_access(calendars, id_participant, departments, True, with_type)
        if not accesses:
            return dummy_accesses
        return accesses

    def get_access(self, calendar_uuid, id_person, uuid_person, departments=None):
        """
        Метод возвращает уровень доступа к календарю по лицам
        Примечание:
            - вернется ADMIN, если id_person||uuid_person является собственником календаря
            - вернется EDIT, если календарь имеет признак, что можно редактировать всем 
        """
        # получим участника
        id_participant = None
        if any((id_person, uuid_person)):
            id_ext_participant = id_person or uuid_person
            id_participant = self._get_by_ext_id_cache(
                id_persons=id_person,
                uuid_persons=uuid_person,
                by_physical=bool(uuid_person)
            ).get(str(id_ext_participant))
        calendar = sbis.Calendar.ReadByUuid(calendar_uuid)
        if not calendar:
            return READ
        id_calendar = calendar.Get('@Calendar')
        if not id_participant:
            id_participant = self._get_auth_participant_by_calendar(id_calendar)
        return self._get_access(id_calendar, id_participant, departments)

    def _get_access(self, id_calendars, id_participant, departments=None, is_mass=False, with_type=False):
        """
        Получение уровня доступа по участнику
        """

        def get_departs(id_participant):
            """
            Метод возвращает список частного лица и идентификаторы вышестоящих подразделений
            """
            result = [id_participant]
            id_person = self._get_participant(id_participant).get('Person')
            if id_person:

                if departments is None and self.client != get_physic_client():
                    online_departs = remote_invoke_online('ЧастноеЛицо', 'GetDepartments', id_person,
                                                          client=self.client)
                else:
                    online_departs = departments

                if not online_departs:
                    return result

                id_ext_participants = [online_depart.Get('Participant') for online_depart in
                                       online_departs]
                exist_participants = {}
                if id_ext_participants:
                    exist_participants = self._get_by_ext_id_cache(
                        id_persons=id_ext_participants[0],
                        id_departments=id_ext_participants[1:],
                    )
                    participants_need_sync = list(set(map(str, id_ext_participants)) - set(exist_participants.keys()))
                    if participants_need_sync:
                        new_participants = sync(ext_participants=id_ext_participants)
                        exist_participants.update(new_participants)

                result = [exist_participants.get(str(id_ext_participant)) for id_ext_participant in id_ext_participants]
            return result

        def get_owner_item(obj):
            return {
                'level': obj[0],
                'Owner': obj[1]
            }

        if not isinstance(id_calendars, list):
            id_calendars = [id_calendars]
        if not id_participant:
            sbis.WarningMsg(
                'Not received required parameter. Calendar: {}, Participant: {}'.format(
                    id_calendars, id_participant))
            return None if is_mass else READ
        _filter = sbis.Record({
            'Calendars': id_calendars,
            'without_handlers': True,
        })
        # Расширенный набор участников (Person+Departments)
        expanded_participants = [id_participant]
        calendars = sbis.Calendar.List(None, _filter, None, None).ToList('Type')
        if any(_type not in (CALENDAR_MY_PERSONAL, CALENDAR_MY_CUSTOM) for _type in calendars):
            expanded_participants += get_departs(id_participant)
        negative_participant = -1 * id_participant
        participants_levels = list(map(get_owner_item, enumerate(expanded_participants)))

        sql = '''
            WITH owner_level AS (
                SELECT * FROM json_to_recordset($3::json) AS L(
                        "level" integer
                      , "Owner" integer
                )
            )
            , permissions AS( 
                SELECT "Calendar"
                     , MAX("Access") AS "Access"
                  FROM (
                    -- get access for personal or department permission
                    (
                        SELECT DISTINCT ON (permission."Calendar")
                               permission."Calendar"
                             , greatest(permission."Access", COALESCE(c."Access", 0)) "Access"
                          FROM owner_level
                     LEFT JOIN "CalendarPermission" permission
                            ON owner_level."Owner" = permission."Owner"
                     LEFT JOIN (
                                SELECT "@Calendar"
                                     , 2 "Access"
                                  FROM "Calendar" calendar
                                 WHERE "@Calendar" = ANY($1::int[]) AND "Flags"[3] = True
                                ) c ON c."@Calendar" = permission."Calendar"                     
                         WHERE permission."Calendar" = ANY($1::int[])
                           AND NOT ($2::int = ANY(COALESCE("Show", ARRAY[]::int[])))
                         ORDER BY "Calendar", "level"  
                    )
             -- на флаг смотрим только для неперсональных календарей
                    UNION ALL

                    -- get access as "edit" if calendar have flag - can_edit_all
                    SELECT "@Calendar"
                         , 2 "Access"
                      FROM "Calendar" calendar
                     WHERE "@Calendar" = ANY($1::int[]) AND "Flags"[3] = True
                       AND "Type" NOT IN (0, 1, 10, 11)
                ) _view
                 GROUP BY "Calendar"
            )
            SELECT "CalendarUUID"
                 , "Access"
                 , calendar."Type"
              FROM permissions permission
        INNER JOIN "Calendar" calendar
                ON permission."Calendar" = calendar."@Calendar"
        '''
        result_rs = sbis.SqlQuery(sql, id_calendars, negative_participant, json.dumps(participants_levels))
        # if permission don't exist, set default access for read
        if not result_rs:
            return None if is_mass else READ
        accesses = result_rs.ToDict('CalendarUUID', 'Access')
        for k, v in accesses.items():
            if v == LIMIT_READ:
                accesses[k] = READ
        if is_mass:
            accesses = {str(k): v for k, v in accesses.items()}
            if with_type:
                types = {str(k): v for k, v in result_rs.ToDict('CalendarUUID', 'Type').items()}
                return {'rights': accesses, 'types': types}
            else:
                return accesses
        else:
            return int(next(iter(accesses.values())) or READ)

    @staticmethod
    def rename_fields_create_calendar_ext(calendar):
        """
        переименовываем поля после создания пустой записи календаря
        """
        fields = {
            'CalendarPermission.Color': 'Color',
        }

        format_calendar = calendar.Format()
        for old_field, new_field in fields.items():
            if old_field in format_calendar:
                calendar.RenameField(old_field, new_field)

    def get_by_ext_id(self, ids, need_create=True, online_participants=None):
        """
        Возвращает календарь по идентификатору частного лица, помещения, комнаты
        или машины
        :param by_physical:
        :param ids: hash table
            Person - идентификатор частного лица
            PersonUUID - идентификатор пользователя
            Department - идентификатор подразделения
            Room - идентификатор помещения
            Vehicle - идентификатор машины
        Возвращает календарь
        Примечание: нельзя перейти пока на __read_calendars, т.к. используется авторизованный 
        участник
        """
        # by_physical выпилить позже
        by_physical = True
        ids = ids or {}
        id_person = ids.get('Person')
        uuid_person = ids.get('PersonUUID')
        id_room = ids.get('Room')
        id_vehicle = ids.get('Vehicle')
        id_ext_participant = id_person or uuid_person or id_room or id_vehicle
        if id_ext_participant:
            id_participant = self._get_by_ext_id_cache(
                id_persons=id_person,
                uuid_persons=uuid_person,
                id_rooms=id_room,
                id_vehicles=id_vehicle,
                by_physical=by_physical,
                need_sync=need_create,
                online_participants=online_participants,
            ).get(str(id_ext_participant))
        else:
            id_participant = self.auth_physic_participant if by_physical else self.auth_participant
        if id_participant:
            _filter = sbis.Record({
                'Owners': [id_participant],
                'Type': [CALENDAR_ONLINE_WORKING, CALENDAR_ONLINE_ROOM, CALENDAR_ONLINE_VEHICLE, CALENDAR_MY_PERSONAL],
                'without_handlers': True,
            })
            calendars = sbis.Calendar.List(None, _filter, None, None) or {}
            if calendars:
                calendars = calendars.ToDict('Owner', 'CalendarUUID')
            elif need_create:
                type_participant = self.get_type_participant(id_participant)
                type_calendar = CALENDAR_BY_PARTICIPANT.get(type_participant)
                fio = self._get_fio_participant(id_participant)
                name = get_initials_name(fio) if type_calendar in (
                    CALENDAR_ONLINE_WORKING, CALENDAR_MY_PERSONAL) else fio
                _filter = {
                    'Type': type_calendar,
                    'Owner': id_participant,
                    'Name': name,
                }
                calendars_by_owners = self.mass_create_base_calendars([_filter])
                uuid_calendar = calendars_by_owners.get(str(id_participant))
                if uuid_calendar:
                    calendars = {
                        id_participant: uuid.UUID(uuid_calendar),
                    }
            return calendars.get(id_participant)

    def get_uuid_by_ext_ids(self, ids, need_create=True):
        """
        Возвращает uuid календарей по внешним идентификаторам собственников
        :param ids: внешние идентификаторы собственников календарей
        :param by_physical: указывает, что надо запрашивать участника физика
        :return: hash table структуры {ext_id_owner: CalendarUUID}
        """
        sbis.WarningMsg('метод Calendar.GetUuidByExtId является устаревшим, следует воспользоваться'
                        ' методом Calendar.GetByExtId')
        # by_physical выпилить позже
        by_physical = False
        type_calendars = [CALENDAR_ONLINE_WORKING, CALENDAR_ONLINE_CUSTOM, CALENDAR_ONLINE_ROOM,
                          CALENDAR_ONLINE_VEHICLE, CALENDAR_MY_PERSONAL,
                          CALENDAR_MY_CUSTOM]
        field = 'CalendarUUID'
        calendars = self.__read_calendars(ids, by_physical, type_calendars, field, need_create)
        return {str(k): str(v) if v is not None else v for k, v in calendars.items()}

    def get_by_ext_ids(self, ids, need_create=True, participants=None):
        """
        Возвращает список рабочих календарей по списку частных лиц
        :param need_create:
        :param ids: hash table
            Persons - массив частных лиц 
            PersonsUUID - массив uuid персон 
            Departments - массив идентификаторов подразделений 
            Rooms - массив идентификаторов помещений 
            Vehicles - массив идентификаторов машин
        :param participants: RecordSet с данными метода Лицо.ListSync
        :return: hash table структуры {ext_id_owner: CalendarUUID}
        """
        type_calendars = [CALENDAR_ONLINE_WORKING, CALENDAR_ONLINE_ROOM, CALENDAR_ONLINE_VEHICLE, CALENDAR_MY_PERSONAL]
        by_physical = False
        field = 'CalendarUUID'
        calendars = self.__read_calendars(ids, by_physical, type_calendars, field, need_create, participants)
        return {str(k): str(v) for k, v in calendars.items() if v}

    def __read_calendars(self, ids, by_physical, type_calendars, field=None, need_create=True,
                         online_participants=None):
        """
        Возвращает хэш набора календарей по внешним собственникам (ЧастноеЛицо||Помещение||Машина)
        :param ids: хэш внешних идентификаторов собственников
        :param by_physical: указывает, что надо запрашивать участника физика
        :return: {ext_id_owner: calendar_record}
        """
        id_persons = ids.get('Persons', [])
        uuid_persons = ids.get('PersonsUUID', [])
        id_departments = ids.get('Departments', [])
        id_rooms = ids.get('Rooms', [])
        id_vehicles = ids.get('Vehicles', [])
        if not any((id_persons, uuid_persons, id_departments, id_rooms, id_vehicles)):
            return {}
        participants = self._get_by_ext_id_cache(
            id_persons=id_persons,
            uuid_persons=uuid_persons,
            id_departments=id_departments,
            id_rooms=id_rooms,
            id_vehicles=id_vehicles,
            by_physical=by_physical,
            online_participants=online_participants,
            need_sync=need_create,
        )
        if not participants:
            return {}
        sql = '''
            WITH owners_cte AS (
                SELECT $2::bigint[] AS owners
            )
            SELECT "Owner", "{column_name}" 
            FROM 
                "Calendar"
            WHERE 
                "Owner" = ANY (SELECT UNNEST(owners) FROM owners_cte) AND
                "Type" = ANY($1::smallint[])
        '''.format(column_name=field)
        owners = list(participants.values())
        calendars = sbis.SqlQuery(sql, type_calendars, owners)
        if field:
            calendars = calendars.ToDict('Owner', field)
        else:
            calendars = {calendar.Get('Owner'): calendar for calendar in calendars}

        # TODO: перейти на метод массового создания календарей, когда будет готов
        # создаем календари если они отсутствуют
        # убрать после того, как sync будет работать и для машин\подраделений
        if need_create:
            for ext_id_participants, type_calendar in (
                    (id_persons, CALENDAR_ONLINE_WORKING),
                    (id_rooms, CALENDAR_ONLINE_ROOM),
                    (id_vehicles, CALENDAR_ONLINE_VEHICLE),
            ):
                for ext_id_participant in ext_id_participants:
                    id_participant = participants.get(str(ext_id_participant))
                    if id_participant and not calendars.get(id_participant):
                        _filter = {
                            'Type': type_calendar,
                            'Owner': id_participant,
                            'Name': self._get_fio_participant(id_participant),
                        }
                        calendars_by_owners = self.mass_create_base_calendars([_filter])
                        uuid_calendar = calendars_by_owners.get(str(id_participant))
                        if uuid_calendar:
                            new_calendar = sbis.Calendar.ReadByUuid(uuid.UUID(uuid_calendar))
                            value = new_calendar.Get(field) if field else new_calendar
                            calendars[id_participant] = value

        return {ext_id_participant: calendars.get(id_participant) for
                ext_id_participant, id_participant in participants.items()}

    @staticmethod
    def read_by_participant(id_calendar, id_participant):
        """
        чтение календаря по участнику
        """
        _format = sbis.MethodResultFormat('Calendar.ReadByParticipant', 2)
        result = sbis.Record(_format)

        calendar = sbis.Calendar.Read(id_calendar)
        _filter = sbis.Record({
            'Calendars': [id_calendar],
            'ByOwner': id_participant,
        })
        permissions = sbis.CalendarPermission.List(None, _filter, None, None)
        if permissions:
            permission = permissions[0]
        else:
            permission = sbis.CalendarPermission.GetMain(id_calendar)
        permission.CopyOwnFormat()
        permission.Remove('Owner')

        for obj in (calendar, permission):
            for field in obj:
                field_name = field.Name()
                if field_name in result:
                    if field_name == 'Flags':
                        val = obj.Get(field_name)[:3]
                    else:
                        val = obj.Get(field_name)
                    result[field_name] = val

        return result

    def __correct_only_main(self, _filter):
        """
        Корректирует флаг OnlyMain 
        """
        self.only_main = _filter.Get('OnlyMain')
        if self.only_main is None:
            self.only_main = True
        if 'OnlyMain' not in _filter:
            _filter.AddBool('OnlyMain')
        _filter['OnlyMain'] = self.only_main

    def list_by_participant(self, _filter):
        """
        Получаение списка календарей по участникам 
        """
        participants = _filter.Get('Participants')
        if not participants:
            # запрашиваем список календарей для авторизованного участника
            only_main = _filter.Get('OnlyMain')
            accordion_participant = [self.auth_participant] + [self.auth_physic_participant]
            participants_by_site = {
                SITE_ONLINE: [self.auth_participant] if only_main else accordion_participant,
                SITE_MY: [self.auth_physic_participant],
            }
            participants = participants_by_site.get(self.site)
            if not participants:
                _format = sbis.MethodResultFormat('Calendar.ListByParticipant', 4)
                return sbis.RecordSet(_format)
            _filter['Participants'] = list(filter(None, participants))

        return self.__calendars_by_participant(_filter)

    def list_by_person(self, _filter):
        """
        Получение списка календарей по лицам
        """

        def persons_to_participants(_filter):
            """
            Вычисляет участников, на основании запроса
            Person/PersonUUID - частное лицо с ONLINE/MY для базовой сетки календаря
            Persons - частные лица с ONLINE для салона красоты
            """
            _filter.AddArrayInt32('Participants')
            person = _filter.Get('Person')
            persons = _filter.Get('Persons') or []
            uuid_person = _filter.Get('PersonUUID')
            # Если запрошены календари по ЧЛ, и данное ЧЛ является текущим авторизованным
            # перейдем на обработку в режим отдачи всех календарей (как если бы ЧЛ на было передано)
            if not persons and not uuid_person and person == self.auth_person:
                return True
            participant = None
            participants = []

            # Получим участников по идентификаторам с ONLINE
            _persons = ([person] + persons) if person else persons
            _persons = list(map(str, _persons))
            if _persons:
                participants_filter = sbis.Record({
                    'Persons': _persons,
                    'Client': self.client
                })
                participants = sbis.Participant.List(None, participants_filter, None, None)
                exist_persons = {str(participant.Get('Person')): participant.Get('@Participant') for
                                 participant in participants}
                persons_need_sync = list(set(_persons) - set(exist_persons.keys()))
                service_persons = []
                if persons_need_sync:
                    if _filter.Get('OnlineParticipants'):
                        sbis.LogMsg(sbis.LogLevel.llMINIMAL,
                                    'list_by_person. online_participants: {}'.format(
                                        str(_filter.Get('OnlineParticipants'))))
                        online_participants = sbis.CreateRecordSet(_filter.Get('OnlineParticipants'), 5)
                        service_persons = [str(p.Get('Person')) for p in online_participants if
                                           p.Get('ЧастноеЛицоТип') not in (0, 6)]
                    else:
                        online_participants = None
                    new_participants = sync(ext_participants=persons_need_sync, online_participants=online_participants)
                    exist_persons.update(new_participants)
                    not_sync = list(set(persons_need_sync) - set(exist_persons.keys()))
                    if not_sync:
                        if set(not_sync) == set(service_persons):
                            sbis.WarningMsg('No calendar for this person type: {}'.format(not_sync))
                        else:
                            sbis.WarningMsg('ONLINE participant {} does not exist'.format(not_sync))
                        return False
                participant = exist_persons.get(str(person))
                participants = [exist_persons.get(str(person)) for person in persons]

            # Получим участников по uuid персоны c MY
            physic_participant = self._get_by_ext_id_cache(
                uuid_persons=uuid_person,
                by_physical=True
            ).get(str(uuid_person))
            if uuid_person and not physic_participant:
                physic_participant = sync(ext_uuid_participants=[uuid_person]).get(set(uuid_person))
                if not physic_participant:
                    sbis.ErrorMsg('Sync error for MY participant: {}'.format(uuid_person))
                    return False

            # Определимся, где каких участников показываем (в зависимости от сайта)
            only_main = _filter.Get('OnlyMain')
            accordion_participant = [participant] + [physic_participant]
            salon_participants = participants
            participants_by_site = {
                SITE_ONLINE: (
                        salon_participants or [participant]) if only_main else accordion_participant,
                SITE_MY: [physic_participant],
            }
            _filter['Participants'] = list(filter(None, participants_by_site.get(self.site)))
            return True

        sbis.LogMsg(sbis.LogLevel.llMINIMAL, 'list_by_person. parent method: {}'.format(sbis.Session.TaskMethodName()))
        calendars = _filter.Get('Calendars')
        if calendars:
            return self.__calendars_by_participant(_filter)

        self.__correct_only_main(_filter)
        _format = sbis.MethodResultFormat('Calendar.ListByParticipant', 4)
        result = sbis.RecordSet(_format)
        if not persons_to_participants(_filter):
            return result
        participants = _filter.Get('Participants')
        if participants:
            # откорректируем список календарей
            field = 'Calendars'
            if field in _filter:
                _filter.Remove(field)
            _filter.AddArrayInt32(field)
            calendar_filter = sbis.Record({
                'Owners': participants,
                'Type': [CALENDAR_ONLINE_WORKING],
            })
            calendars = sbis.Calendar.List(None, calendar_filter, None, None)
            _filter[field] = [calendar.Get('@Calendar') for calendar in calendars]
        else:
            # запрашиваем список календарей для авторизованного участника
            self.only_main = False
            participants = []
            if self.site == SITE_ONLINE:
                participants = [self.auth_participant, self.auth_physic_participant]
            elif self.site == SITE_MY:
                participants = [self.auth_physic_participant]
            participants = list(filter(None, participants))
            if not participants:
                return result
            _filter['Participants'] = participants
            if 'OnlyMain' not in _filter:
                _filter.AddBool('OnlyMain')
            _filter['OnlyMain'] = self.only_main

        return self.__calendars_by_participant(_filter)

    def __get_available_type_calendars(self):
        """
        Возвращает доступные типы календарей в зависимости от сайта
        :param person_is_auth: флаг, указывающий что требуется список типов для авторизованного 
        пользователя
        """
        personal_types = [] if is_demo_scheme() else [CALENDAR_MY_PERSONAL, CALENDAR_MY_CUSTOM]
        type_calendars = {
            # (only_main, site)
            (False, SITE_ONLINE): [CALENDAR_ONLINE_WORKING, CALENDAR_ONLINE_CUSTOM, CALENDAR_ONLINE_ROOM,
                                   CALENDAR_ONLINE_VEHICLE,
                                   CALENDAR_PERSON_QUEUE, CALENDAR_DEPARTMENT_QUEUE] + personal_types,
            (False, SITE_MY): [CALENDAR_MY_PERSONAL, CALENDAR_MY_CUSTOM],
            (True, SITE_ONLINE): [CALENDAR_ONLINE_WORKING],
            (True, SITE_MY): [CALENDAR_MY_PERSONAL],
        }
        return type_calendars.get((self.only_main, self.site))

    @staticmethod
    def _rk_my_calendar(calendar):
        _type = calendar.Get('Type')
        if _type == CALENDAR_ONLINE_WORKING:
            calendar['Name'] = sbis.rk('Рабочий календарь')
        elif _type == CALENDAR_MY_PERSONAL:
            calendar['Name'] = sbis.rk('Личный календарь')

    def rk_calendar_names(self, calendars, participants_list=None):
        """
        Метод корректирует названия календарей в зависимости от типа и принадлежности
        """
        if not participants_list:
            participants_list = [self.auth_participant, self.auth_physic_participant]
        format_names = {
            CALENDAR_ONLINE_WORKING: '{} "{}"'.format(sbis.rk('Рабочий календарь'), '{}'),
            CALENDAR_ONLINE_ROOM: '{}. {}'.format('{}', sbis.rk('По помещению')),
            CALENDAR_MY_PERSONAL: '{} "{}"'.format(sbis.rk('Личный календарь'), '{}'),
        }
        for calendar in calendars:
            if calendar.Get('Owner') in participants_list:
                self._rk_my_calendar(calendar)
            else:
                type_calendar = calendar.Get('Type')
                calendar_name = calendar.Get('Name')
                calendar['Name'] = format_names.get(type_calendar, '{}').format(calendar_name)

    def __calendars_by_participant(self, _filter):
        """
        Метод, возвращающий список календарей по сотруднику/сотрудникам
        Режимы запросов:
            1. основная сетка с подключенными календарями
            2. сетка сотрудника, календарь, открытый через карточку сотрудника
            3. сетка салона красоты
        Параметры фильтра:
            2. Participants - список участников типа Person
            3. TypeCalendars - тип календарей
            4. OnlyMain - признак возвращать только основные (для сетки сотрудника и салона красоты)
            5. OnlyCanEdit - оставить только те календари которые может редактировать авторизованный
        участник
            6. OnlyActive - отображать только включенные календари (для запроса данных календаря)
            7. MenuSort - если задан то рабочий и личный календари идут первыми, в противном случае
            8. Departments - хэш вышестоящих подразделений по авторизованному сотруднику, в виде
        {id_department: level}
        согласно персональным настройкам пользователя
        Примечание:
            1. Вне зависимости от режима, для авторизованного участника всегда учитываются
        вышестоящие подразделения, для учета уровня доступа к календарю
            2. При расшаривании и участнику и вышестоящему подразделению, возвращается ближайшее
        разрешение относительно участника, см. is_near_permission
        """

        def get_participant_levels(departments):
            """
            Корректирует набор уровней
            - id_department -> @Participant
            - добавляет авторизованных участников
            """
            participant_levels = {}
            # обработаем подразделения
            if departments:
                # TODO найти вызов откуда передается dict
                if not isinstance(departments, dict):
                    departments = {department.Get('Participant'): department.Get('Level') for department
                                   in departments}
                participants = sbis.Participant.GetDictByDepartmentList(
                    self.client, list(departments.keys())
                ) or {}
                participants = {int(id_department): participants[0] for id_department, participants
                                in participants.items()}
                participant_levels = {participants.get(id_department): level for
                                      id_department, level in departments.items() if
                                      participants.get(id_department)}
                departments = list(participant_levels.keys())
            # добавим авторизованного участника
            if self.auth_participant and self.site == SITE_ONLINE:
                participant_levels[self.auth_participant] = 0
            # добавим авторизованного участника физика
            if self.auth_physic_participant:
                participant_levels[self.auth_physic_participant] = -1
            participant_levels = [{'Participant': id_participant, 'Level': level} for
                                  id_participant, level in participant_levels.items()]
            return participant_levels, departments or []

        def prepare_calendars(id_calendars):
            """
            Корректирует фильтр запроса по календарям
            """
            if id_calendars:
                if not isinstance(id_calendars[0], int):
                    calendar_filter = sbis.Record({
                        'ByUUID': id_calendars,
                    })
                    calendars = sbis.Calendar.List(None, calendar_filter, None, None)
                    id_calendars = [calendar.Get('@Calendar') for calendar in calendars]
            return id_calendars

        def get_filter_only_active(only_active):
            return ''' AND "Show" ''' if only_active else ''

        def get_filter_only_can_edit(only_can_edit):
            _filter = ''
            if only_can_edit:
                _filter = """
                    AND "CanEdit"
                """
            return _filter

        def get_filter_only_request_calendars(id_calendars):
            _filter = ''
            if id_calendars:
                _filter = """
                    AND "@Calendar" = ANY(ARRAY{}::int[])
                """.format(id_calendars)
            return _filter

        def get_filter_show_removed(show_removed):
            _filter = ''
            if not show_removed:
                _filter = """
                    AND calendar."Removed" IS NULL
                """
            return _filter

        def correct_type_calendars(type_calendars):
            """
            Откорректируем список типов календарей, которые доступны на запрашиваемом сайте
            """
            available_types = self.__get_available_type_calendars()
            if type_calendars:
                type_calendars = list(set(type_calendars) & set(available_types))
            else:
                type_calendars = available_types
            return type_calendars

        def get_sort(menu_sort):
            if menu_sort:
                _sort = '"MenuOrder",  "Order", "Type", "Name", "@Calendar"'
            else:
                _sort = '"Order", "MainAuthPerson" desc, "Type", "Name", "@Calendar"'
            return _sort

        _format = sbis.MethodResultFormat('Calendar.ListByPerson', 4)

        self.__correct_only_main(_filter)
        participants = _filter.Get('Participants') or []
        only_can_edit = _filter.Get('OnlyCanEdit')
        only_active = _filter.Get('OnlyActive')
        show_removed = _filter.Get('ShowRemoved', False)
        if participants and not ({self.auth_participant, self.auth_physic_participant} & set(participants)):
            only_active = False
        type_calendars = _filter.Get('TypeCalendars') or []
        permissions = _filter.Get('@CalendarPermission') or []
        menu_sort = _filter.Get('MenuSort')
        request_calendars = prepare_calendars(_filter.Get('Calendars') or [])
        add_calendars = prepare_calendars(_filter.Get('AddCalendars') or [])
        departments = _filter.Get('Departments')
        if not any((participants, request_calendars)):
            return sbis.RecordSet(_format)

        # expanded_participants - расширенный набор участников, включающих помимо запрашиваемых еще
        # и авторизованного участника и вышестоящие подразделения авторизованного участника
        participant_levels, departs = get_participant_levels(departments)
        participants += departs
        # Если передали календари, то возвращаем календари всех типов
        base_calendar_types = [CALENDAR_ONLINE_WORKING, CALENDAR_ONLINE_CUSTOM, CALENDAR_ONLINE_ROOM,
                               CALENDAR_ONLINE_VEHICLE, CALENDAR_MY_PERSONAL, CALENDAR_MY_CUSTOM]
        if request_calendars:
            type_calendars = base_calendar_types
        elif not type_calendars:
            type_calendars = correct_type_calendars(base_calendar_types)

        exclude_personal_types = [CALENDAR_MY_PERSONAL, CALENDAR_MY_CUSTOM] if is_demo_scheme() else []
        type_calendars = list(set(type_calendars) - set(exclude_personal_types))
        sql = """
        WITH levels AS (
        SELECT
            *
        FROM
            json_to_recordset($6::json) AS X(
                "Level" bigint,
                "Participant" bigint
            )
        )
        , base_permission AS (
            SELECT
                *,
                ARRAY_AGG(permission."Owner") OVER (PARTITION BY permission."Calendar") "Subscribers"
                -- в случаях когда календарь не расшарен ни авторизованному сотруднику ни его
                -- вышестоящим подразделениям надо сохранить Level=NULL, т.к. MIN проигнорирует NULL
                -- это необходимо для рассчета is_near_permission
            FROM
                "CalendarPermission" permission
            LEFT JOIN
                levels
                    ON permission."Owner" = levels."Participant"
            WHERE
                -- expanded_participants, расширенный набор участников, запрашиваемые
                -- участники + авторизованный участник + вышестоящие подразделения
               (NOT $4::int[] && permission."Show" OR permission."Show" IS NULL)
                AND permission."Owner" = ANY($2::int[])
                 OR (permission."Calendar" = ANY($5::int[]) AND permission."Main" AND permission."Owner" IS NOT NULL)
        )
        , base_calendars AS (
            SELECT
                calendar."Type" = ANY(ARRAY[0, 10]) AND calendar."Owner" = ANY($2::int[]) "MainAuthPerson",
                COALESCE(calendar."Version", 0) "Version",
                calendar."Removed" IS NOT NULL "IsRemoved",
                calendar."Flags"[2] "VisibleToAll",
                calendar."Flags"[1] "Public",
                NOT (calendar."Owner" = ANY($2::bigint[])) "CanExtract",
                calendar."Type" = ANY(ARRAY[0, 10]) "Main",
                calendar."Owner" = bp."Owner" "IsOwner",
                CASE
                    WHEN $2::bigint[] && bp."Subscribers"
                    THEN ( bp."Access" >= 2 OR COALESCE(calendar."Flags"[3], FALSE) )
                    ELSE FALSE
                END "CanEdit",
                CASE
                    WHEN $2::bigint[] && bp."Subscribers"
                    THEN COALESCE(calendar."Flags"[3], FALSE)
                    ELSE FALSE
                END "CanEditAll",
                calendar."CalendarUUID",
                calendar."Name",
                calendar."Description",
                calendar."Type",
                calendar."Owner",
                calendar."Flags",
                calendar."@Calendar",
                bp."Show",
                bp."Level",
                bp."Subscribers",
                CASE WHEN bp."Owner" = ANY(
                                SELECT "Participant"
                                FROM levels
                            )
                    THEN bp."Color"
                    ELSE $7::int
                END "Color",
                bp."Access",
                bp."Order",
                bp."@CalendarPermission"
            FROM
                base_permission bp
            INNER JOIN (
                SELECT
                    *
                FROM
                    "Calendar"
                WHERE
                    "@Calendar" IN (
                    SELECT
                        "Calendar"
                    FROM
                        base_permission
                )
            ) calendar
            ON
                bp."Calendar" = calendar."@Calendar"
            WHERE
                calendar."Type" = ANY($3::int[])
                {filter_show_removed}
                {filter_only_request_calendars}
        )
        , base_participants AS (
            SELECT
                *,
                calendar."Flags" "calendar_flags",
                CASE
                    WHEN participant."Room" IS NOT NULL
                    THEN 2
                    WHEN participant."Department" IS NOT NULL
                    THEN 1
                    WHEN participant."Person" IS NOT NULL
                         AND participant."Room" IS NULL
                         AND participant."Department" IS NULL
                    THEN 0
                    ELSE 0
                END "TypeOwner",
                concat_ws(' ', NULLIF(trim(participant."LastName"), ''),
                    NULLIF(concat_ws('',    NULLIF(substr(trim(participant."FirstName"),1,1), '') || '.',
                                            NULLIF(substr(trim(participant."MiddleName"),1,1), '') || '.'
                    ), '')
                ) "FIO"
            FROM
                base_calendars calendar
            LEFT JOIN (
                SELECT
                    *
                FROM
                    "Participant"
                WHERE
                    "@Participant" IN (
                        SELECT
                            "Owner"
                        FROM
                            base_calendars
                    )
            ) participant
            ON
                calendar."Owner" = participant."@Participant"
        )
        , calendars AS (
            SELECT
                "@Calendar",
                "@CalendarPermission",
                "CalendarUUID",
                "Name",
                "Description",
                "Type",
                "Owner",
                "IsOwner",
                "TypeOwner",
                "Person",
                "PersonUUID",
                "Department",
                "Room",
                "Vehicle",
                "Main",
                "MainAuthPerson",
                "VisibleToAll",
                "Public",
                "CanEdit",
                "CanEdit" "CanCreateEvent",
                "CanEditAll",
                "Access" = 4 "CanShare",
                COALESCE("Main", FALSE) "CanRemove",
                "CanExtract",
                "Order",
                "Version",
                "IsRemoved",
                0.0::NUMERIC "TZ",
                ROW_NUMBER() OVER(PARTITION BY "@Calendar"
                                ORDER BY "Level") AS duplicate_number,
                CASE
                    WHEN "Type" = ANY(ARRAY[2, 3])
                    THEN "Name"
                    ELSE "FIO"
                END "FIO",
                ("@Participant" = "Owner" AND "Department" IS NOT NULL) "IsDepartmentPermission",
                CASE
                    WHEN $1::bool
                    THEN TRUE
                    ELSE $8::int[] && "Show"
                END "Show",
                CASE
                    WHEN $1::bool
                    THEN $7::int
                    ELSE "Color"
                END "Color",
                CASE
                    WHEN $1::bool
                    THEN '#' || lpad(UPPER(to_hex($7)), 6, '0')
                    ELSE '#' || lpad(UPPER(to_hex("Color")), 6, '0')
                END "ColorStr",
                -- Поднимать права на редактирование нужно не только для персональных разрешений но и для разрешений 
                -- подразделений (переключатель в карточке календаря "на редактирование")
                CASE
                    WHEN $2::bigint[] && "Subscribers"
                    THEN CASE
                            WHEN COALESCE("calendar_flags"[3], FALSE)
                            THEN GREATEST("Access", 2)
                            ELSE "Access"
                        END
                    ELSE 1 -- READ ACCESS
                END "Access",
                CASE
                    WHEN "MainAuthPerson" AND "Type" = 0
                    THEN 0
                    WHEN "MainAuthPerson" AND "Type" = 10
                    THEN 1
                    ELSE 2
                END "MenuOrder"
            FROM
                base_participants
        )
        SELECT
            *
        FROM
            calendars
        WHERE
            duplicate_number = 1
            {filter_only_can_edit}
            {filter_only_active}
{0}       AND "@CalendarPermission" = ANY($9::int[])
        ORDER BY
            {sort_fields}
        """.format(
            '   ' if permissions else '-- ',
            filter_only_active=get_filter_only_active(only_active),
            filter_only_can_edit=get_filter_only_can_edit(only_can_edit),
            filter_only_request_calendars=get_filter_only_request_calendars(request_calendars),
            filter_show_removed=get_filter_show_removed(show_removed),
            sort_fields=get_sort(menu_sort),
        )
        # для корректного определения включенности персонального календаря на online
        # (вкл. если участник физика входит в Show) введен массив 'активных участников'. Признак
        # включенности (как и другие) будем смотреть по пересечению 'активных участников' с Show.
        # ВАЖНО: нельзя проверять одного участника в данном случае
        active_participants = list(
            filter(None, [self.auth_participant, self.auth_physic_participant])) + departs
        # признак изъятия календаря вычисляем нWа основании отрицательных участиков
        negative_participants = list(filter(None, [self.auth_participant, self.auth_physic_participant]))
        negative_participants = list(map(lambda p: -1 * p, negative_participants))
        calendars = sbis.SqlQuery(
            sql,
            _format,
            False if request_calendars else self.only_main,
            active_participants,
            type_calendars,
            negative_participants,
            request_calendars + add_calendars,
            json.dumps(participant_levels),
            DEFAULT_CALENDAR_COLOR,
            list(filter(None, [self.auth_participant, self.auth_physic_participant])),
            permissions,
        )
        self.rk_calendar_names(calendars)
        return calendars

    def publish_set_show_events(self, permissions):
        _format = sbis.CreateRecordFormat()
        _format.AddInt32('@CalendarPermission')
        _format.AddBool('Show')
        payload = sbis.RecordSet(_format)
        for _id, show in permissions.items():
            rec = sbis.Record({
                '@CalendarPermission': _id,
                'Show': show
            })
            payload.AddRow(rec)
        try:
            event.Publish(
                "calendar.setshowevents",
                payload,
                event.Visibility.evCLIENT_ONLY,
                event.Policy.evIMMEDIATELY,
                event.Delivery.evUSER,
            )
            event.Publish(event.Event(
                name="calendar.setshowevents",
                payload=payload,
                visibility=event.Visibility.evCLIENT_ONLY,
                policy=event.Policy.evON_TRANSACTION_COMMIT,
                persons=[str(self.auth_person_uuid)],
                applications=["mobile"]
            ))
        except Exception:
            sbis.WarningMsg('Ошибка при публикации события в календаре: {}'.format(traceback.format_exc()))

    def get_payload(self, calendars, operation, switch=None):
        if not calendars:
            return
        calendars_info = None
        switch_flag = None

        sql = """ 
            SELECT cp."Color", cp."Show", c."@Calendar", c."Name" 
              FROM "Calendar" c
             INNER JOIN  "CalendarPermission" cp
                     ON cp."Calendar" = c."@Calendar" 
                    AND cp."Main" IS TRUE
             WHERE c."ClientID" = $1::int
               AND c."@Calendar" = ANY($2::int[])             
        """

        if operation in ['attach', 'create', 'update']:
            switch_flag = True
            frmt = sbis.RecordFormat()
            frmt.AddInt32('calendar')
            frmt.AddInt32('color')
            frmt.AddString('name')
            if operation != 'update':
                frmt.AddBool('show')

            additional_information_calendar = sbis.SqlQuery(sql, self.client, calendars)
            calendars_info = sbis.RecordSet(frmt)

            for information in additional_information_calendar:
                new_rec = calendars_info.AddRow()
                color = information.Get('Color')
                id_calendar = information.Get('@Calendar')
                name_calendar = information.Get('Name')
                new_rec.Set('color', color)
                if operation != 'update':
                    new_rec.Set('show', True)
                new_rec.Set('calendar', id_calendar)
                new_rec.Set('name', name_calendar)

        elif operation in ['detach', 'switch']:

            value_set = set(switch.values()) if switch is not None else []

            if operation == 'detach':
                switch_flag = False
            else:
                if len(value_set) == 1:
                    switch_flag = value_set.pop()
            frmt = sbis.RecordFormat()
            frmt.AddInt32('calendar')
            frmt.AddBool('show')
            calendars_info = sbis.RecordSet(frmt)
            for calendar in calendars:
                new_rec = calendars_info.AddRow()
                new_rec.Set('show', switch_flag if switch_flag is not None else switch.get(calendar))
                new_rec.Set('calendar', calendar)

        payload = sbis.Record({
            'operation': operation,
            'calendars_id': calendars,
            'show': switch_flag,
            'calendars_info': calendars_info
        })

        return payload

    def event_publish(self, calendars, operation, user, persons_uuid=None, switch=None):
        """
        Метод публикации события
        :param calendars: список календарей
        :param operation: какое событие обрабатывается("attch","detach"...)
        :param user: Список пользователей
        :param persons_uuid: список uuid
        :param switch: словарь с флагами
        """
        sbis.LogMsg(sbis.LogLevel.llMINIMAL,
                    'Рассылка события: {}, {}, {}, {}, {}'.format(str(calendars), str(operation),
                                                                  str(user), str(persons_uuid),
                                                                  str(switch)))
        calendars = calendars if isinstance(calendars, list) else [calendars]
        if not all([user, persons_uuid]):
            persons_uuid = [self.auth_person_uuid]
        user = user if isinstance(user, list) or user is None else [user]
        persons_uuid = persons_uuid if isinstance(persons_uuid, list) or persons_uuid is None else [persons_uuid]

        payload = self.get_payload(calendars, operation, switch)
        if not payload:
            return

        if user:
            params = {
                'name': 'calendar.operation_on_calendar',
                'payload': payload,
                'visibility': event.Visibility.evCLIENT_ONLY,
                'policy': event.Policy.evIMMEDIATELY,
                'users': user,
            }
            try:
                event.Publish(event.Event(**params))
                params.update({'applications': ['mobile']})
                event.Publish(event.Event(**params))
            except:
                sbis.WarningMsg('Не удалась публикация события для клиентской части')

        if persons_uuid:
            persons_uuid_list = list(map(str, persons_uuid))
            params = {
                'name': 'calendar.operation_on_calendar',
                'payload': payload,
                'visibility': event.Visibility.evCLIENT_ONLY,
                'policy': event.Policy.evIMMEDIATELY,
                'persons': persons_uuid_list
            }
            try:
                event.Publish(event.Event(**params))
                params.update({'applications': ['mobile']})
                event.Publish(event.Event(**params))
            except:
                sbis.WarningMsg('Не удалась публикация события для клиентской части')

    def publish_operation_on_calendar(self, calendars, operation, user=None, persons_uuid=None, switch=None):
        if not calendars:
            return
        self.event_publish(calendars, operation, user, persons_uuid, switch)
        payload = sbis.Record({
            'operation': operation,
            'calendars': calendars if isinstance(calendars, list) else [calendars]
        })

        params = {
            'name': "calendar.attach-detach",
            'payload': payload,
            'visibility': event.Visibility.evCLIENT_ONLY,
            'policy': event.Policy.evIMMEDIATELY,
            'deliv': event.Delivery.evUSER,
        }
        if user:
            params.update({'users': user})
        try:
            event.Publish(event.Event(**params))
        except:
            sbis.WarningMsg('Не удалась публикация события для клиентской части')

        try:

            event.Publish(event.Event(
                name="calendar.attach-detach",
                payload=payload,
                visibility=event.Visibility.evCLIENT_ONLY,
                policy=event.Policy.evIMMEDIATELY,
                persons=[str(user if user else sbis.Participant.GetAuthPersonUUID())],
                applications=["mobile"]
            ))
        except:
            sbis.WarningMsg('Не удалась публикация события в мобильное приложение')

    def get_department_users(self, department):
        flt = sbis.Record(
            {
                'DepartmentId': department,
                'Worked': True,
                'DateStart': datetime.date.today(),
                'DateEnd': datetime.date.today()
            }
        )
        persons_list = remote_invoke_online('Staff', 'EmployeesIDs', None, flt, None, None)
        return persons_list.ToList('UserId')

    def set_show_events(self, params):
        """
        Включает/выключает отображение календарей в аккордеоне
        :param params: hashtable вида @CalendarPermission: IsShow 
        :return: None
        """
        params = {int(id_permission): is_show for id_permission, is_show in params.items()}
        if not params:
            return
        _filter = sbis.Record({
            'CalendarPermissions': list(params.keys()),
            'without_handlers': True
        })
        permissions = sbis.CalendarPermission.List(None, _filter, None, None)
        new_values = []
        id_calendars = []
        shows = {}
        for i, permission in enumerate(permissions):
            id_permission = permission.Get('@CalendarPermission')
            type_calendar = permission.Get('Calendar.Type')
            auth_participant = self.auth_participant
            if type_calendar in (CALENDAR_MY_PERSONAL, CALENDAR_MY_CUSTOM):
                auth_participant = self.auth_physic_participant

            if auth_participant is None:
                continue

            show = permission.Get('Show') or []
            is_show = params[id_permission]
            if is_show:
                if auth_participant not in show:
                    show.append(auth_participant)
            else:
                if auth_participant in show:
                    show.remove(auth_participant)
            new_values.append({
                'id_permission': id_permission,
                'show': '{}'.format(set(show)) if show else None,
            })
            id_calendars.append(permission.Get('Calendar'))
            shows.update({permission.Get('Calendar'): bool(show)})

        self.event_publish(id_calendars, 'switch', None, self.auth_person_uuid, shows)
        self.update_show_permission(new_values)
        self.publish_set_show_events(params)

    def _update_subscribers(self, id_calendar, count_subscribers):
        """
        Обновляет количество подписчиков 
        :param id_calendar: календарь на который подписываются
        :param count_subscribers: количество подписчиков
         TODO: переделать в _base_update, с неопределенным количеством параметров
        """
        if id_calendar:
            sql = '''
            UPDATE
                "Calendar"
            SET
                "Subscribers" = GREATEST(COALESCE("Subscribers", 0) + $2::int, 0)
            WHERE
                "@Calendar" = $1::int
            '''
            sbis.SqlQuery(sql, id_calendar, count_subscribers)

    @staticmethod
    def update_show_permission(values: list):
        """
        Массово обновляет поле Show по разрешениям
        :param values: список словарей формата {@CalendarPermission: int, Show: int[]}
        """
        sql = '''
        WITH new_values (id_permission, show, color) AS (
            SELECT * FROM json_to_recordset($1::text::json) AS x1(id_permission int, show int[], color int)
        )
        UPDATE
            "CalendarPermission" permission
        SET
            "Show" = nv.show,
            "Color" = coalesce(nv.color, "Color")
        FROM
            new_values nv
        WHERE
            permission."@CalendarPermission" = nv.id_permission
        '''
        if values:
            sbis.SqlQuery(sql, json.dumps(values))

    def get_attach(self, id_calendar, id_participant):
        """
        Возвращает информацию по подключенному календарю, если таковой есть
        :param id_calendar: календарь
        :param id_participant: участник
        :return: hash table
            @CalendarPermission - идентификатор разрешения
            IsDepartmentPermission - является ли разрешение, разрешением подразделения
        """
        result = {}
        if not all((id_calendar, id_participant)):
            sbis.WarningMsg('You are forgot to set necessary params, id_calendar: {}, '
                            'id_participant: {}'.format(id_calendar, id_participant))
            return result
        _filter = sbis.Record({
            'ByOwner': id_participant,
            'Calendars': [id_calendar],
            'without_handlers': True,
        })
        calendars = sbis.CalendarPermission.List(None, _filter, None, None)
        if calendars:
            result['@CalendarPermission'] = calendars[0].Get('@CalendarPermission')
            result['IsDepartmentPermission'] = calendars[0].Get('IsDepartmentPermission')
        return result

    def attach_mass(self, calendar_uuids):
        """
        Подключает список календарей, через панель массовых операций
        Примечание: нельзя пока сделать добавление разрешений одним запросом, поскольку на 
        обработчике CalendarPermission.on_before_create проверяются права. Поскольку ситуация
        массового подключения очень редкая, оставляем пока так
        """
        result_format = sbis.MethodResultFormat('Calendar.Attach', 2)
        result = sbis.RecordSet(result_format)
        _filter = sbis.Record({
            'ByUUID': calendar_uuids,
            'without_handlers': True,
        })
        calendars = sbis.Calendar.List(None, _filter, None, None)
        id_calendars = {}
        for calendar in calendars:
            id_calendars[calendar.Get('@Calendar')] = calendar.Get('CalendarUUID')
        for id_calendar in id_calendars:
            result.AddRow(sbis.Record({
                'CalendarUUID': id_calendars[id_calendar],
                '@CalendarPermission': self.attach(id_calendar),
            }))
        return result

    def mass_attach(self, calendars_uuid, departments=None, auth_person=None):

        result_format = sbis.MethodResultFormat('Calendar.Attach', 2)
        result = sbis.RecordSet(result_format)
        self.only_main = False
        _type = self.__get_available_type_calendars()
        # self.auth_participant or self.auth_physic_participant
        # clients = [sbis.Session.ClientID(), get_physic_client()]
        if self.site == SITE_MY:
            clients = [get_physic_client()]
        else:
            clients = [sbis.Session.ClientID(), get_physic_client()]

        _filter = sbis.Record({
            'ByUUID': calendars_uuid,
            'Clients': clients,
            'Type': _type,
            'ExcludeRemoved': True,
            'without_handlers': True
        })
        # Получаем календари доступные для подключения
        calendars = sbis.Calendar.List(None, _filter, None, None)
        cal_by_dep = []
        if departments:
            _filter = sbis.Record({
                'Departments': departments.ToList('Participant'),
                'Client': self.client
            })
            departments = sbis.Participant.List(None, _filter, None, None).ToList('@Participant')
            if departments:
                _filter = sbis.Record({'ByOwners': departments, 'without_handlers': True})
                cal_by_dep = sbis.CalendarPermission.List(None, _filter, None, None).ToList('Calendar')
        uuid_by_calendar = {calendar.Get('@Calendar'): calendar.Get('CalendarUUID') for calendar in calendars
                            if calendar.Get('@Calendar') in cal_by_dep or calendar.Get('Flags')[1]}
        calendars = list(uuid_by_calendar.keys())
        if not calendars:
            return result
        # получаем все календари уже подключенные пользователю
        owners = [self.auth_participant, self.auth_physic_participant]
        _filter = sbis.Record({
            'Calendars': calendars,
            'ByOwners': owners,
        })
        attached = sbis.CalendarPermission.List(None, _filter, None, None).ToList('Calendar')
        # убираем календари уже подключенные пользователю
        need_attach = list(set(calendars) - set(attached))
        if not need_attach:
            return result
        # получаем настройки (цвет и прочее) основных разрешений
        _filter = sbis.Record({
            "Calendars": need_attach,
            "OnlyMain": True,
        })
        # по сути и есть те календари, что надо подключать
        # необходимо лишь поменять пару полей
        main_perms = sbis.CalendarPermission.List(None, _filter, None, None)
        for perm in main_perms:
            owner = self._get_auth_participant_by_calendar_type(perm.Get('Calendar.Type'))
            perm['Main'] = False
            perm['Access'] = READ
            perm['Owner'] = owner

        sql = """
           WITH data AS (
                SELECT * FROM json_to_recordset($1::json) AS X(
                       "Calendar" bigint
                     , "Color" bigint
                     , "Main" bool
                     , "Access" int
                     , "Owner" bigint
                )
            )
           INSERT INTO "CalendarPermission" (
                       "Calendar"
                     , "Color"
                     , "Main"
                     , "Access"
                     , "Owner"
                     , "Show"
                )
                SELECT data."Calendar"
                     , data."Color"
                     , data."Main"
                     , data."Access"
                     , data."Owner"
                     , array[ data."Owner" ]::bigint[]
                  FROM data
            RETURNING "@CalendarPermission", "Calendar"
        """
        permissions = sbis.SqlQuery(sql, json.dumps(main_perms.as_list(), cls=SBISEncoder))
        for perm in permissions:
            result.AddRow(sbis.Record({
                'CalendarUUID': uuid_by_calendar.get(perm.Get('Calendar')),
                '@CalendarPermission': perm.Get('@CalendarPermission'),
            }))

        self.publish_operation_on_calendar(list(set(permissions.ToList('Calendar'))), 'attach')
        return result

    def _get_auth_participant_by_calendar_type(self, c_type: int) -> int:
        if c_type in (CALENDAR_MY_PERSONAL, CALENDAR_MY_CUSTOM):
            return self.auth_physic_participant
        else:
            return self.auth_participant

    def _get_auth_participant_by_calendar(self, id_calendar: int) -> int:
        """
        Получает авторизованого участника, в зависимости от типа календаря
        :param id_calendar: идентификатор календаря
        :return: @Participant
        Примечание: нельзя ориентироваться на self.auth_participant и self.auth_physic_participant 
        поскольку на online доступна работа с персональным календарем, для которых необхоимо
        привязывать операции к self.auth_physic_participant, а не к self.auth_participant
        """
        calendar = sbis.Calendar.Read(id_calendar)
        return self._get_auth_participant_by_calendar_type(calendar.Get('Type'))

    def attach(self, id_calendar):
        """
        Подключает календарь в аккордеон 
        :param id_calendar: подключаемый календарь
        """
        auth_participant = self._get_auth_participant_by_calendar(id_calendar)
        if not all((auth_participant, id_calendar)):
            sbis.WarningMsg('You are forgot to set necessary params, Person: {}, '
                            'Calendar: {}'.format(self.auth_person, id_calendar))
            return None
        # проверим не подключен ли уже календарь
        connected_permission = self.get_attach(id_calendar, auth_participant).get(
            '@CalendarPermission')
        if connected_permission:
            sbis.WarningMsg('Calendar {} already connected, @CalendarPermission: {}'.format(
                id_calendar, connected_permission)
            )
            return None

        # подключаем календарь/создаем разрешение
        color = self.get_color(id_calendar) or WORK_CALENDAR_COLOR
        _filter = sbis.Record({
            'Calendar': id_calendar,
            'Owner': auth_participant,
            'Show': [auth_participant],
            'Color': color,
            'Access': READ,
        })
        permission = sbis.CalendarPermission.Create(_filter)
        self.publish_operation_on_calendar([id_calendar], 'attach')
        return sbis.CalendarPermission.Update(permission)

    def detach(self, calendar_uuid):
        """
        Изымает календарь у авторизованного сотрудника
        :param calendar_uuid: изымаемый календарь
        :return: ничего
        Примечание:
            Изъятия календаря может быть для:
        - участника, который явно подключил календарь раннее (или был расшарен)
        - разрешения подразделения, тогда в CalendarPermission.Show добавляется "признак изъятия"
        -1 * auth_participant
        - пероснальное разрешение (автоматически сгенерированное при установке персональных настроек
        календаря, таких как смена цвета календаря, смена расположения в аккордеоне, факт изъятия)
        Примечание 2:
            Календарь может быть расшарен нескольким вышестоящим подразделениям. Пожелание
        пользователя об изъятии календаря, проставляет для всех вышестоящих подразделений "признак
        изъятия".
        """

        def get_permissions(id_calendar, auth_participant):
            """
            Возвращает идентификаторы разрешений по календарю
            :param id_calendar: идентификатор календаря
            :return: tuple(auth_permission, depart_permissions)
                auth_permission - разрешение для авторизованного участника, int
                depart_permissions - разрешения подразделений, в аккордеоне авторизованного
            участника, list[record]
            """
            # получим все разрешения, закрепленные за календарем
            _filter = sbis.Record({
                'Calendars': [id_calendar],
                'without_handlers': True,
            })
            permissions_calendar = sbis.CalendarPermission.List(None, _filter, None, None)
            # получим информацию о типах участников
            id_participants = list(filter(None, permissions_calendar.ToList('Owner')))
            participants = self._get_participants(id_participants)
            subscribers = {p.get('@Participant'): p.get('TypeParticipant') for p in
                           participants.values()}

            # получим идентификаторы разрешений
            auth_permission = None
            depart_permissions = []
            for permission_calendar in permissions_calendar:
                id_owner = permission_calendar.Get('Owner')
                p_calendar = permission_calendar.Get('Calendar')
                type_owner = subscribers.get(id_owner)
                if type_owner == PARTICIPANT_PERSON and id_owner == auth_participant:
                    auth_permission = permission_calendar.Get('@CalendarPermission')
                if type_owner == PARTICIPANT_DEPARTMENT and id_calendar == p_calendar:
                    depart_permissions.append(permission_calendar)
            return auth_permission, depart_permissions

        def mark_department_permission(depart_permissions):
            """
            Помечаем разрешения подразделений, что авторизованный участник изъял календарь, как
            -1 * id_participant
            Примечание: поскольку подразделения только на online, не обрабатываем участника физика
            """
            new_values = []
            for depart_permission in depart_permissions:
                id_permission = depart_permission.Get('@CalendarPermission', None)
                show = depart_permission.Get('Show', None) or []
                if self.auth_participant in show:
                    show.remove(self.auth_participant)
                if -1 * self.auth_participant not in show:
                    show += [-1 * self.auth_participant]
                new_values.append({
                    'id_permission': id_permission,
                    'show': '{}'.format(set(show)) if show else None,
                })
            self.update_show_permission(new_values)

        def history(id_calendar):
            calendar = sbis.Calendar.Read(id_calendar)
            auth_participant = self.auth_participant or self.auth_physic_participant
            name_participant = self._get_fio_participant(auth_participant) or 'инкогнито'
            msg = 'Сотрудник "{}" изъял у себя календарь "{}"'.format(name_participant, calendar.Get('Name'))
            sbis.HistoryMsg(msg, 'Удаление разрешения', 'УправлениеРабочимВременем_Календарь',
                            str(calendar.Get('CalendarUUID')))

        calendar = sbis.Calendar.ReadByUuid(calendar_uuid)
        if calendar is not None:
            id_calendar = calendar.Get('@Calendar')
            auth_participant = self._get_auth_participant_by_calendar(id_calendar)
            if not auth_participant:
                sbis.WarningMsg('Forbidden detach calendar by client auth')
                return

            auth_permission, depart_permissions = get_permissions(id_calendar, auth_participant)
            # удалим разрешения
            if auth_permission:
                sbis.CalendarPermission.Delete(auth_permission)
            if depart_permissions:
                mark_department_permission(depart_permissions)
            # зафиксируем изъятие в истории
            history(id_calendar)
            self.publish_operation_on_calendar(calendar.Get('@Calendar'), 'detach')

    def get_main(self, id_calendar, owner=None):
        """
        Получение основного разрешения по календарю
        PS: основное разрешение календаря определяется как
        CalendarPermission where Calendar.Owner == CalendarPermission.Owner
        """
        if not owner:
            calendar = sbis.Calendar.Read(id_calendar)
            owner = calendar.Owner
        _filter = sbis.Record({
            'Calendar': id_calendar,
            'ByOwners': [owner],
            'without_handlers': True
        })
        permissions = sbis.CalendarPermission.List(None, _filter, None, None)
        if permissions:
            return permissions[0]

    def get_color(self, calendar):
        """
        возвращает цвет календаря
        """
        main_permission = self.get_main(calendar)
        if main_permission:
            return main_permission.Get('Color')

    def _get_popular(self, id_client, _format, pagination, departments=None,
                     available_type_calendars=None, persons_uuid=None):
        """
        Получает список популярных календарей
        :param id_client: Клиент, под которым идет запрос
        :param _format: Формат возвращаемой выборки
        :param departments: Подразделения TODO использовать для корректного заполнения IsAttached
        :param available_type_calendars: список доступных типов календарей
        :param persons_uuid: uuid'ы пользователей из фильтра
        :return:
        """

        sql = '''
        -- $1::bigint[] - клиенты, для которых ищем
        -- $2::int[] - типы календарей, которые ищем
        -- $3::bigint - участник, осуществляющий поиск
            WITH
            clients AS (
                SELECT UNNEST($1::bigint[]) AS "Client"
            )
            , calendars_recs AS (
                SELECT UNNEST(ARRAY(SELECT cal
                                      FROM "Calendar" cal
                                     WHERE cal."ClientID" = cli."Client" 
                                       AND "Flags"[2]
                                       AND "Removed" IS NULL
                                       AND "Type" = ANY($2::int[])
                                       AND NULLIF(trim(cal."Name"), '') IS NOT NULL
                                     ORDER BY "Subscribers" DESC NULLS LAST
                                     OFFSET $5
                                     -- убрали LIMIT так как резал массив, в котором был искомый календарь
                              )) C
                  FROM clients cli                                                                                                   
            )
            , calendars AS (
                SELECT (C).* 
                  FROM calendars_recs
                 ORDER BY "Subscribers" DESC NULLS LAST
            )
            , participants_data AS (
                SELECT "@Participant"
                     , "Person"
                     , "PersonUUID"
                     , CONCAT("LastName", ' ' || "FirstName", ' ' || "MiddleName") AS "FIO"
                     , CONCAT("LastName"
                            , ' ' || NULLIF(
                                        CONCAT(
                                               NULLIF(SUBSTRING("FirstName" FROM 1 FOR 1), '') || '.'
                                             , NULLIF(SUBSTRING("MiddleName" FROM 1 FOR 1), '') || '.'
                                        )
                                        , ''
                            )
                      ) AS "FIOShort"
                 FROM "Participant"
                WHERE "@Participant" = ANY(SELECT "Owner"
                                             FROM calendars)       
            )
            SELECT "CalendarUUID"
                 , "Name"
                 , NULL::integer        AS "Color"
                 , "Person"             AS "Author"
                 , p."FIO"
                 , "Type" IN (10, 11)   AS "IsPersonCalendar"
                 , EXISTS (SELECT 1 
                             FROM "CalendarPermission" p
                            WHERE "Owner" = ANY($3::bigint[])
                              AND "Calendar" = "@Calendar"
                            LIMIT 1)    AS "IsAttached"
                 , "Type"    
                 , p."FIOShort"         AS "FIOAuthor"
                 , p."PersonUUID"
                 , (SELECT array_agg("Person") FROM "Participant" WHERE "@Participant" IN (
                        SELECT "Owner" FROM "CalendarPermission" WHERE (
                        "Calendar" = c."@Calendar" AND "Owner" != c."Owner")
                    ) AND "Department" IS NULL) "SharePersons"
                 , (SELECT array_agg("Department") FROM "Participant" WHERE "@Participant" IN (
                        SELECT "Owner" FROM "CalendarPermission" WHERE (
                        "Calendar" = c."@Calendar" AND "Owner" != c."Owner")
                    ) AND "Department" IS NOT NULL) "ShareDepartments"
              FROM calendars c
        INNER JOIN participants_data p
                ON c."Owner" = p."@Participant"     
{}           WHERE p."PersonUUID" = ANY($6::uuid[])  
          ORDER BY "Subscribers" DESC NULLS LAST, "Name", "@Calendar"   
             LIMIT $4 
        '''.format('--' if not persons_uuid else '')
        page = pagination.Page() if pagination else 0
        count_on_page = pagination.RecsOnPage() if pagination else 0
        calendars = sbis.SqlQuery(
            sql,
            _format,
            list({id_client, get_physic_client()}),
            available_type_calendars,
            list(filter(None, [self.auth_participant, self.auth_physic_participant])),
            count_on_page + 1,
            page * count_on_page,
            persons_uuid or [uuid.uuid4()]
        )

        next_exist = True if calendars.Size() == count_on_page + 1 else False
        if next_exist:
            calendars.DelRow(count_on_page)
        result = sbis.MethodListResult()
        result.cursor = calendars.Cursor()
        result.nav_result = sbis.NavigationResultBool(next_exist)
        return result

    @staticmethod
    def _get_participants_by_name(search_name, clients):
        """
        Возвращает список лиц подходящих под условие поиска search_name
        :param search_name: строка поиска
        """
        id_participants = [-1]
        if search_name:
            _filter = sbis.Record({
                'Name': search_name.lower(),
                'Clients': clients,
            })
            participants = sbis.Participant.List(None, _filter, None, None)
            id_participants = [participant.Get('@Participant') for participant in participants]
        return id_participants

    def search(self, extra_fields, _filter, sorting, pagination):
        """
        Список календарей для поиска и подключения
        Примечание: по умолчанию возвращаем N популярных календарей
        """

        _format = sbis.MethodResultFormat('Calendar.Search', 4)
        result = sbis.RecordSet(_format)
        auth_participant = self.auth_participant or self.auth_physic_participant
        if not auth_participant:
            sbis.WarningMsg('Данный функицонал не доступен при входе под клиентом')
            return result

        self.only_main = _filter.Get('OnlyMain') or False
        page = pagination.Page() if pagination else 0
        count_on_page = pagination.RecsOnPage() if pagination else 0
        owners_id = _filter.Get('Owner') or []
        persons_uuid = None
        if owners_id:
            uuid_persons_dict = get_uuid_by_persons(owners_id)
            persons_uuid = list(uuid_persons_dict.values())
        try:
            type_calendar = [int(_filter.Get('Types'))] if _filter.Get('Types') is not None \
                else self.__get_available_type_calendars()
        except Exception as ex:
            sbis.LogMsg(sbis.LogLevel.llMINIMAL, 'Передан не верный тип календаря: {}'.format(ex))
            type_calendar = self.__get_available_type_calendars()
        available_type_calendars = list(set(self.__get_available_type_calendars()) & set(type_calendar))
        search_name = (_filter.Get('Name') or '').lower()
        departments = _filter.Get('Departments') or []
        if not search_name:
            return self._get_popular(sbis.Session.ClientID(), _format, pagination,
                                     departments, available_type_calendars, persons_uuid)

        if self.site == SITE_ONLINE:
            # При работе с основного сервиса при расчете подключения надо учесть авторизованного участника-физика
            # найдем все вышестоящие подразделения по авторизованому пользователю
            departments = self.__get_departments_by_participant(auth_participant, departments).get('Departments')
            all_participants = [auth_participant, self.auth_physic_participant] + departments
        else:
            all_participants = [auth_participant]

        sql = '''
        WITH calendars_by_participants AS (
            SELECT
                "@Calendar",
                "CalendarUUID",
                "Name",
                "Owner",
                "Type"
            FROM
                "Calendar"
            WHERE
                "Owner" = ANY($2::int[]) AND
                "Removed" IS NULL AND
                "Flags"[2] AND
                "Type" = ANY($1::int[])
        )
        , calendars_by_departments AS (
            SELECT
                "@Calendar",
                "CalendarUUID",
                "Name",
                "Owner",
                "Type"
            FROM
                "Calendar"
            WHERE
                "@Calendar" = ANY(
                    SELECT "Calendar"
                      FROM "CalendarPermission"
                     WHERE "Owner" = ANY($9::int[])
                ) AND
                to_tsvector('simple'::regconfig,  ("ClientID" || '_' || "Name")) @@
                to_tsquery('simple'::regconfig, $7::text || ' & ' || replace(
                    regexp_replace($3::text, '[^_[:alnum:][:space:]]', ' ', 'g')::text,
                    ' ', '_') || '_' || ':*'
                ) AND
                "Removed" IS NULL AND
                "Type" = ANY($1::int[]) AND
                "ClientID" = ANY($8::int[])
        )
        , calendars_by_name AS (
            SELECT
                "@Calendar",
                "CalendarUUID",
                "Name",
                "Owner",
                "Type"
            FROM
                "Calendar"
            WHERE
                to_tsvector('simple'::regconfig,  ("ClientID" || '_' || "Name")) @@
                to_tsquery('simple'::regconfig, $7::text || ' & ' || replace(
                    regexp_replace($3::text, '[^_[:alnum:][:space:]]', ' ', 'g')::text,
                    ' ', '_') || '_' || ':*'
                ) AND
                "Removed" IS NULL AND
                "Flags"[2] AND
                "Type" = ANY($1::int[]) AND
                "ClientID" = ANY($8::int[])
        )
        , calendars AS (
            SELECT * FROM calendars_by_participants
            UNION
            SELECT * FROM calendars_by_departments
            UNION
            SELECT * FROM calendars_by_name
        )
        , prepared_participants AS (
            SELECT *
            FROM "Participant"
            WHERE "@Participant" = ANY(
                SELECT "Owner"
                FROM calendars
            )
        )
        , prepared_permissions AS (
            SELECT *
            FROM "CalendarPermission"
            WHERE "Owner" = ANY (
                SELECT "@Participant"
                FROM prepared_participants
            )
        )
        SELECT
            DISTINCT ON (calendar."CalendarUUID")
            calendar."CalendarUUID",
            calendar."Name",
            calendar."Type",
            permission."Color",
            calendar."Owner" "Author",
            EXISTS(
                SELECT
                    1
                FROM
                    "CalendarPermission"
                WHERE
                    "Calendar" = calendar."@Calendar" AND
                    "Owner" = ANY($4::int[]) AND
                    CASE
                        WHEN array_length("Show"::bigint[], 1) IS NULL
                        THEN TRUE
                        ELSE NOT $10::int[] && "Show"
                    END
                LIMIT 1
            ) "IsAttached",
            (
                COALESCE(participant."LastName", '') || ' ' ||
                COALESCE(participant."FirstName", '') || ' ' ||
                COALESCE(participant."MiddleName", '')
            ) "FIO",
            NULL::text "FIOAuthor",
            participant."Person" IS NOT NULL "IsPersonCalendar",
            participant."PersonUUID",
            COALESCE(participant."Room", participant."Vehicle", participant."Person") AS "@Лицо"
            , (SELECT array_agg("Person") FROM "Participant" WHERE "@Participant" IN (
                SELECT "Owner" FROM "CalendarPermission" WHERE (
                "Calendar" = calendar."@Calendar" AND "Owner" != calendar."Owner")
                ) AND "Department" IS NULL) "SharePersons"
           , (SELECT array_agg("Department") FROM "Participant" WHERE "@Participant" IN (
                SELECT "Owner" FROM "CalendarPermission" WHERE (
                "Calendar" = calendar."@Calendar" AND "Owner" != calendar."Owner")
                ) AND "Department" IS NOT NULL) "ShareDepartments"
        FROM
            calendars calendar
        INNER JOIN
            prepared_permissions permission
        ON
            calendar."@Calendar" = permission."Calendar" AND
            calendar."Owner" = permission."Owner"
        INNER JOIN
            prepared_participants participant
        ON
            calendar."Owner" = participant."@Participant"
{}        WHERE participant."PersonUUID" = ANY($11::uuid[])
        ORDER BY calendar."CalendarUUID", LOWER(calendar."Name") LIKE '%'|| $3::text ||'%' DESC
        LIMIT $5 OFFSET $6
        '''.format('--' if not persons_uuid else '')
        negative_participants = list(filter(None, [self.auth_participant, self.auth_physic_participant]))
        negative_participants = list(map(lambda p: -1 * p, negative_participants))
        clients = [sbis.Session.ClientID(), get_physic_client()]
        clients_str = '(' + ' | '.join(map(str, clients)) + ')'
        calendars = sbis.SqlQuery(
            sql,
            _format,
            available_type_calendars,
            self._get_participants_by_name(search_name, clients),
            search_name,
            all_participants,
            count_on_page + 1,
            page * count_on_page,
            clients_str,
            clients,
            departments,
            negative_participants,
            persons_uuid or [uuid.uuid4()]
        )
        # сформируем короткое имя владельца календаря
        for calendar in calendars:
            fio = calendar.Get('FIO')
            calendar['FIOAuthor'] = get_initials_name(fio)
            calendar['Occurrences'] = 0

        next_exist = True if calendars.Size() == count_on_page + 1 else False
        if next_exist:
            calendars.DelRow(count_on_page)
        result = sbis.MethodListResult()
        result.cursor = calendars.Cursor()
        result.nav_result = sbis.NavigationResultBool(next_exist)
        return result

    def rename_fields_list_calendars(self, calendars):
        """
        После получения списка разрешений для карточки календаря, переименуем поля
        :param permissions: 
        :return: 
        """
        fields = {
            'РП.FIOAuthor': 'FIOAuthor',
            'РП.IsAttached': 'IsAttached',
        }
        format_calendars = calendars.Format()
        for old_field, new_field in fields.items():
            if old_field in format_calendars:
                calendars.RenameField(old_field, new_field)

    def on_after_list(self, params, calendars):
        """
        Обработчик на Calendar.List
        """
        _filter = sbis.Record()
        departments = params.filter.Get('Departments')
        if departments:
            _filter.AddRecordSet('Departments', departments.Format(), departments)
        calendars_by_auth = sbis.CalendarPermission.ListByPerson(None, _filter, None, None)
        id_calendars = [calendar.Get('@Calendar') for calendar in calendars_by_auth]
        for calendar in calendars:
            id_calendar = calendar.Get('@Calendar')
            fio = '{last_name} {first_name} {middle_name}'.format(
                last_name=calendar.Get('Owner.LastName') or '',
                first_name=calendar.Get('Owner.FirstName') or '',
                middle_name=calendar.Get('Owner.MiddleName') or '',
            )
            calendar['FIOAuthor'] = get_initials_name(fio)
            calendar['IsAttached'] = id_calendar in id_calendars
        self.rename_fields_list_calendars(calendars)

    def _get_count_employeers(self, id_departments):
        """
        Получает количество сотрудников, работающих в подразделениях
        :param id_departments: список подразделений
        :return: словарь формата {id_department: count_employee}
        """
        try:
            departments = remote_invoke_online('СтруктураПредприятия', 'КраткаяИнформация',
                                               id_departments, client=self.client)
        except Exception as err:
            raise sbis.Error(
                'some problem with СтруктураПредприятия.КраткаяИнформация: {}'.format(err))
        return {department.Get('ИдО'): department.Get('КолЧел') or 0 for department in departments}

    def _mark_subscribe(self, id_calendar=None, id_subscriber=None):
        self.__subscribe(id_calendar, id_subscriber, is_subscribe=True)

    def _mark_unsubscribing(self, id_calendar=None, id_subscriber=None):
        self.__subscribe(id_calendar, id_subscriber, is_subscribe=False)

    def __subscribe(self, id_calendar=None, id_subscriber=None, is_subscribe=True):
        """
        Обрабатывает подписку на календарь/отписку на календарь
        :param id_calendar: идентификатор календаря
        :param id_subscriber: идентификатор участника, подписчик
        """
        if id_calendar and id_subscriber:
            # Получим количество Лиц закрепленных за участником id_subscriber
            count_subscribers = 1
            type_participant = self.get_type_participant(id_subscriber)
            if type_participant == PARTICIPANT_DEPARTMENT:
                id_department = self._get_participant(id_subscriber).get('Department')
                count_subscribers = self._get_count_employeers(id_department).get(id_department, 1)
            if not is_subscribe:
                count_subscribers = -1 * count_subscribers
            self._update_subscribers(id_calendar, count_subscribers)

    @staticmethod
    def get_subscribers(calendars_uuid, only_active=False):
        """
        Возвращает подписчиков переданных календарей
        :param calendars_uuid: календари
        :param only_active: только активные
        :return: Record
        """
        if only_active:
            sql = '''
                SELECT "Person", "PersonUUID", "Department"
                  FROM "Participant"
                 WHERE "@Participant" = ANY(ARRAY(
                         SELECT DISTINCT "Owner" 
                           FROM (SELECT unnest("Show") "Owner"
                                   FROM "CalendarPermission"
                                  WHERE "Calendar" = ANY(SELECT "@Calendar"
                                                           FROM "Calendar"
                                                          WHERE "CalendarUUID" = ANY($1::uuid[])
                                                        )
                                    AND "Show" IS NOT NULL
                                ) T
                          WHERE "Owner" > 0
                       ))
            '''
        else:
            sql = '''
                SELECT "Person", "PersonUUID", "Department"
                FROM "Participant"
                WHERE "@Participant" IN (
                    SELECT "Owner" 
                    FROM "CalendarPermission"
                    WHERE "Calendar" IN (
                        SELECT "@Calendar"
                        FROM "Calendar"
                        WHERE "CalendarUUID" = ANY ($1::uuid[])
                    )
                )
            '''
        participants = sbis.SqlQuery(sql, calendars_uuid)
        persons = set(participants.ToList('Person'))
        departments = set(participants.ToList('Department'))
        uuid_persons = set(participants.ToList('PersonUUID'))
        _format = sbis.MethodResultFormat('Calendar.Subscribers', 1)
        rec = sbis.Record(_format)
        persons.discard(None)
        departments.discard(None)
        uuid_persons.discard(None)
        rec['Persons'] = list(persons)
        rec['Departments'] = list(departments)
        rec['PersonsUUID'] = list(uuid_persons)
        return rec

    def is_working_calendar_available(self, person: int, online_participants: sbis.RecordSet, access='OnRead',
                                      departments=None) -> bool:
        """
        Проверка доступности рабочего календаря для авторизованного пользователя по частному лицу
        :param person: ЧЛ
        :param online_participants: RecordSet - результат ListSync
        :param access: тип доступа: OnRead/OnWrite
        :param departments: RecordSet - результат GetDepartments
        :return: bool
        """
        calendar_uuid = self.get_by_ext_id({'Person': person}, online_participants=online_participants)
        calendar = sbis.Calendar.ReadByUuid(calendar_uuid)
        if not calendar:
            return False
        visible_to_all = calendar.Get('Flags')[FLAG_VISIBLE_ALL] if calendar.Get('Flags') else False
        if visible_to_all and access == 'OnRead':
            return True
        departments_id = departments.ToList('Participant') if departments else []
        dep_participants_id = []
        if departments_id:
            dep_participants = sbis.Participant.List(None, sbis.Record({'Departments': departments_id, 'Client': self.client}), None, None)
            dep_participants_id = dep_participants.ToList('@Participant') if dep_participants else []

        owners = list(set([self.auth_participant] + dep_participants_id) - {None})
        if not owners:
            return False
        _filter = sbis.Record({
            'ByOwners': owners,
            'Calendars': [calendar.Get('@Calendar')],
            'without_handlers': True,
        })
        permissions = sbis.CalendarPermission.List(None, _filter, None, None)
        if not permissions:
            return False

        return bool(permissions[0].Get('Access')) if access == 'OnRead' else permissions[0].Get('Access') >= 3

    def is_working_calendar_available_mass(self, persons, params=None):
        """
        Проверка доступности рабочего календаря для авторизованного пользователя по списку частных лиц
        :param persons: список ЧЛ
        :param params: Record { тип доступа: OnRead/OnWrite }
        :return: RecordSet
        """
        result_format = sbis.MethodResultFormat('Calendar.IsWorkingCalendarAvailableMass', 2)
        if not persons:
            sbis.RecordSet(result_format)
        person_access = {}
        access_type = (params.Get('AccessType') if params else None) or 'OnRead'
        participant_filter = sbis.Record({'Persons': persons, 'Client': self.client})
        participants = sbis.Participant.List([], participant_filter, None, None).ToList('@Participant')
        calendar_filter = sbis.Record({'Owners': participants,
                                       'Type': [CALENDAR_ONLINE_WORKING],
                                       })
        calendars = sbis.Calendar.List(None, calendar_filter, None, None) if participants else None
        owners_persons = calendars.ToDict('Owner', 'Owner.Person') if calendars else {}
        calendars_to_detail = []
        for calendar in calendars or []:
            if calendar.Get('Flags') and calendar.Get('Flags')[FLAG_VISIBLE_ALL] and access_type == 'OnRead':
                person_access[calendar.Get('Owner.Person')] = True
            else:
                calendars_to_detail.append(calendar.Get('@Calendar'))
        if calendars_to_detail:
            permission_filter = sbis.Record({'ByOwner': self.auth_participant,
                                             'Calendars': calendars_to_detail,
                                             'without_handlers': True,
                                             })
            permissions = sbis.CalendarPermission.List(None, permission_filter, None, None).as_list()
            for perm in permissions or []:
                access = bool(perm.get('Access')) if access_type == 'OnRead' else perm.get('Access') >= ADVANCED_EDIT
                person_access[owners_persons.get(perm.get('Calendar.Owner'))] = access
        person_access.update({p: False for p in persons if p not in person_access.keys()})
        result = sbis.RecordSet(result_format)
        for person, access in person_access.items():
            result.AddRow(sbis.Record({'Person': person, 'Access': access}))
        return result

    def mass_create_base_calendars(self, calendar_lst, participants=None):
        """
        Массовый метод создания календарей и базовых разрешений
        :param calendar_lst: - набор календарей типа hashtable
        :param participants: - набор участников типа RecordSet
        :return: hashtable в формате {id_owner: id_calendar}
        Прмечание:
            Если передан participants, то требуется синхронизация участников
        """

        def get_ext_participant(rec):
            return rec.get('Person') or rec.get('Room') or rec.get('Vehicle')

        if participants:
            # need sync participants
            new_participants = sbis.Participant.SyncParticipants(participants)
            new_owners = {get_ext_participant(p.as_dict()): p.Get('@Participant') for p in new_participants}
            for calendar in calendar_lst:
                ext_id_participant = get_ext_participant(calendar)
                id_participant = new_owners.get(ext_id_participant)
                calendar['Owner'] = id_participant

        colors = {
            CALENDAR_ONLINE_WORKING: WORK_CALENDAR_COLOR,
            CALENDAR_ONLINE_ROOM: ROOM_CALENDAR_COLOR,
            CALENDAR_ONLINE_VEHICLE: VEHICLE_CALENDAR_COLOR,
            CALENDAR_MY_PERSONAL: PERSONAL_CALENDAR_COLOR,
        }
        flags = {
            CALENDAR_ONLINE_WORKING: self.__get_flags(),
            CALENDAR_ONLINE_ROOM: self.__get_flags(FLAG_VISIBLE_ALL, FLAG_CAN_EDIT_ALL),
            CALENDAR_ONLINE_VEHICLE: self.__get_flags(FLAG_VISIBLE_ALL, FLAG_CAN_EDIT_ALL),
        }
        owners_main_calendars = []
        for calendar in calendar_lst:
            calendar['CalendarUUID'] = uuid.uuid4()
            calendar['Version'] = 0
            calendar['Subscribers'] = 1
            calendar_type = calendar.get('Type')
            calendar['Color'] = colors.get(calendar_type, WORK_CALENDAR_COLOR)
            calendar['Flags'] = flags.get(calendar_type, self.__get_flags())
            if calendar_type in (CALENDAR_ONLINE_WORKING, CALENDAR_MY_PERSONAL):
                id_participant = calendar.get('Owner')
                if id_participant:
                    name_owner = self._get_fio_participant(id_participant)
                    short_name_owner = get_initials_name(name_owner)
                    calendar['Name'] = short_name_owner
                    if calendar_type == CALENDAR_ONLINE_WORKING:
                        owners_main_calendars.append(id_participant)
                else:
                    sbis.ErrorMsg('mass_create_base_calendars: some problem with owner calendar for {}'.format(
                        calendar))
                    calendar['Name'] = 'some unknown user'

        sql = '''
            WITH data AS (
                SELECT * FROM json_to_recordset($1::json) AS X(
                       "CalendarUUID" uuid
                     , "Name" text
                     , "Version" bigint
                     , "Type" smallint
                     , "Owner" bigint
                     , "Subscribers" bigint
                     , "Color" bigint
                     , "Flags" text
                )
            )
            , raw_calendar AS (
                INSERT INTO "Calendar" (
                       "Owner"
                     , "Type"
                     , "Flags"
                     , "CalendarUUID"
                     , "Version"
                     , "Subscribers"
                     , "Name"
                )
                SELECT "Owner"
                     , "Type"
                     , translate("Flags", \'[]\', \'{}\')::bool[] AS "Flags"
                     , "CalendarUUID"
                     , "Version"
                     , "Subscribers"
                     , "Name"
                  FROM data
                    ON CONFLICT DO NOTHING
             RETURNING "@Calendar"
                     , "CalendarUUID"
                     , "Owner"
                     , "Type"
            )
            , new_permissions AS (
                INSERT INTO "CalendarPermission" (
                       "Calendar"
                     , "Main"
                     , "Owner"
                     , "Access"
                     , "Color"
                     , "Show"
                )
                SELECT raw_calendar."@Calendar"
                     , true
                     , raw_calendar."Owner"
                     , 4
                     , data."Color"
                     , array[ raw_calendar."Owner" ]::int[]
                  FROM data
            INNER JOIN raw_calendar
                    ON data."Owner" = raw_calendar."Owner"
                   AND data."Type" = raw_calendar."Type"
                    ON CONFLICT DO NOTHING
             RETURNING "Calendar"
                     , "Owner"
            )
            , present_calendars AS (
                SELECT "@Calendar" AS "Calendar"
                     , c."CalendarUUID"
                     , c."Owner"
                     , c."Type"
                  FROM "Calendar" c
            INNER JOIN data d
                    ON c."Owner" = d."Owner" 
                   AND c."Type" = d."Type"
            )
            SELECT * FROM present_calendars
            UNION
            SELECT * FROM raw_calendar
        '''
        new_calendars = sbis.SqlQuery(sql, json.dumps(calendar_lst, cls=SBISEncoder))
        result = {str(new_calendar.Get('Owner')): str(new_calendar.Get('CalendarUUID'))
                  for new_calendar in new_calendars}
        if len(calendar_lst) != len(new_calendars):
            in_clndrs = [(calendar.get('Owner'), calendar.get('Type')) for calendar in calendar_lst]
            out_clndrs = [(calendar.Get('Owner'), calendar.Get('Type')) for calendar in new_calendars]
            sbis.WarningMsg(
                'Не все переданные календари были созданы!\nПередали: {}\nСоздали: {}'.format(in_clndrs, out_clndrs))
        # создаем персональные разрешения от разрешений подразделений
        for owner in owners_main_calendars:
            self.create_department_permission(owner)
        # TODO: поскольку метод вызывается и с online, то он должен уметь возвращать по ключу ext_participant, т.е. Лицо
        # PS: Пока вызов идет с online исключительно по одному лицу, пока не обрабатываем данный случай
        return result

    def make_raw_calendar(self, id_participant, calendar_type):
        raw_calendar = {
            'Type': calendar_type,
            'Owner': id_participant,
            'Name': self._get_short_fio_participant(id_participant),
        }
        return raw_calendar

    def check_calendars(self, participants: list, by_physical=False):
        """
        Проверяет есть ли календари для переданных пользователей
        и если нет, то создает
        """
        # небезопасно, by_physical надо учитывать внутри, иначе может быть ошибка
        _type = 10 if by_physical else 0
        id_client = get_physic_client() if by_physical else self.client
        _filter = sbis.Record({
            'Owners': participants,
            'Type': [_type],
            'Client': id_client
        })
        exists = sbis.Calendar.List(None, _filter, None, None).ToList('Owner')
        new_owners = set(participants) - set(exists)
        result = None
        if new_owners:
            calendars_to_create = [self.make_raw_calendar(owner, _type) for owner in new_owners]
            result = sbis.Calendar.MassCreateBaseCalendars(calendars_to_create)
        return result

    def delete_by_persons(self, id_persons):
        """
        Удаляет календари по ЧЛ (используется при объединении сотрудников)
        """
        # найдем календари
        _filter = {
            'Persons': id_persons,
        }
        need_create = False
        uuid_calendars = [uuid.UUID(item) for item in self.get_uuid_by_ext_ids(_filter, need_create).values() if item]
        self.delete_by_uuids(uuid_calendars)

    def delete_by_uuids(self, uuid_calendars):
        """
        Удаляет календари по UUID календарей
        """
        if uuid_calendars:
            sql_person = """
                SELECT array_agg(cp."Owner") AS "OwnerList"
                  FROM "Calendar" c
                  INNER JOIN "CalendarPermission" cp
                        ON cp."Calendar" = c."@Calendar"
                 INNER JOIN "Participant" p
                         ON cp."Owner" = p."@Participant"
                 WHERE p."Client" = $1::int
                   AND  c."CalendarUUID" = ANY($2::uuid[])
            """
            res = sbis.SqlQueryScalar(sql_person, self.client, uuid_calendars)
            if res:
                user_list, persons_uuid_list = self.get_person_list_by_participant(res)
            else:
                user_list, persons_uuid_list = None, None
            sql = '''
                DELETE FROM "Calendar"
                WHERE "CalendarUUID" = ANY($1::uuid[])
                RETURNING "@Calendar"
            '''
            result = sbis.SqlQuery(sql, uuid_calendars)
            if result:
                calendar_list = result.ToList('@Calendar')
                self.event_publish(calendar_list, 'delete', user_list, persons_uuid_list)

    @staticmethod
    def get_type_by_id(calendar_id):
        return sbis.SqlQueryScalar('SELECT "Type" FROM "Calendar" WHERE "@Calendar" = $1::integer', calendar_id)

    # функция для отказа от copy/paste в attach-detach
    def get_person_list_by_participant(self, participant_id):
        participants_id_list = participant_id if isinstance(participant_id, list) else [participant_id]
        sql_person = """
                   SELECT "Person","PersonUUID","Department","User" 
                     FROM "Participant" 
                    WHERE "Client" = $1::int 
                      AND "@Participant" = ANY($2::int[]) 
                    """
        any_persons = sbis.SqlQuery(sql_person, self.client, participants_id_list)
        persons_uuid_list = []
        users_list = []
        for pers in any_persons:
            person_uuid = pers.Get('PersonUUID')
            if person_uuid:
                persons_uuid_list.append(person_uuid)
                continue

            user = pers.Get("User")
            if user:
                users_list.append(user)
                continue

            department = pers.Get('Department')
            if department:
                result_function = self.get_department_users(department)
                users_list.extend(result_function)
                continue

            person = pers.Get('Person')
            if person:
                _filter = sbis.Record({'PrivatePersons': [person], 'CalcFields': ['PersonID']})
                persons = remote_invoke_online('Person', 'Read', _filter)
                persons_uuid = persons.ToList('PersonID')
                persons_uuid_list.extend(persons_uuid)
                continue

        return users_list, persons_uuid_list


class CalendarPermission(Calendar):
    def __init__(self, *args, **kwargs):
        super(CalendarPermission, self).__init__(*args, **kwargs)

        self.history_update_permission = 'Редактирование разрешения'

        self.calc_fields = (
            ('Person', sbis.Record.AddInt32, None),
            ('PersonUUID', sbis.Record.AddUuid, None),
            ('Department', sbis.Record.AddInt32, None),
            ('Room', sbis.Record.AddInt32, None),
            ('Vehicle', sbis.Record.AddInt32, None),
            ('Owner', sbis.Record.AddInt32, None),
            ('FIO', sbis.Record.AddString, None),
            ('PhotoID', sbis.Record.AddString, None),
            ('PhotoBig', sbis.Record.AddString, None),
            ('ColorStr', sbis.Record.AddString, None),
            ('MainAuthPerson', sbis.Record.AddBool, None),
            ('CanDelete', sbis.Record.AddBool, None),
            ('VisibleAll', sbis.Record.AddBool, None),
            ('CanEditAll', sbis.Record.AddBool, None),
            ('IsDepartmentPermission', sbis.Record.AddBool, None),
        )

    def on_init(self, permission, _filter, name_method, params):
        """
        инициализация записи разрешения календаря
        """
        self.add_calc_fields(permission)
        self.add_calc_fields(_filter)

        permission['Access'] = READ
        permission['Main'] = False
        self.rename_fields_by_list_permission(permission)

    def get_orders_by_person(self, persons, is_main):
        # рабочий и личный календари должны стоять первыми, остальные (новые) добавляться в конец списка        
        sql = '''
            SELECT "Owner", 
                    CASE WHEN $2::bool 
                         THEN MIN("Order")
                         ELSE MAX("Order")
                    END as "Order"
              FROM "CalendarPermission"
             WHERE "Owner" = ANY(
                    SELECT "@Participant"
                      FROM "Participant"
                     WHERE "Client" = $2::int
                       AND ("Department" = ANY($1::int[])
                            OR "Person" = ANY($1::int[]))
                )
          GROUP BY "Owner"
        '''
        orders = sbis.SqlQuery(sql, persons, is_main, self.client)
        return orders.ToDict('Owner')

    def mass_permissions_create(self, _filter):
        """
        Вызывается в обработчике on_before_create
        """
        calendar_id = _filter.Get('Calendar')
        calendar = sbis.Calendar.Read(calendar_id)
        # Получаем Participants
        participants = self._get_by_ext_id_cache(id_persons=_filter.Get('Persons'),
                                                 id_departments=_filter.Get('Departments'),
                                                 uuid_persons=_filter.Get('PersonsUUID'),
                                                 by_physical=calendar.Get('ClientID') == get_physic_client()
                                                 )
        if not participants:
            return
        participants = list(participants.values())
        # Проверяем есть ли разрешения
        permissions_filter = sbis.Record({
            'ByOwners': participants,
            'Calendars': [calendar_id],
            'without_handlers': True,
        })
        permissions = sbis.CalendarPermission.List(None, permissions_filter, None, None)
        participants = list(set(participants) - set(permissions.ToList('Owner')))
        # Получаем цвет
        self.type_calendar = calendar.Type
        main_permission = self.get_main(calendar_id, calendar.Owner)
        main = _filter.Get('Main', False)
        if not _filter.Get('Color'):
            _filter['Color'] = self.get_default_color() if main else main_permission.Get('Color')
        color = _filter.Get('Color')
        access = _filter.Get('Access')
        auth_person = sbis.Participant.GetAuthPerson()
        departments = _filter.Get('PersonDepartments')
        if departments is None and self.client != get_physic_client():
            departments = remote_invoke_online('ЧастноеЛицо', 'GetDepartments', auth_person, client=self.client)
        persons = (departments.ToList('Participant') if departments else []) + [auth_person]
        is_main = main and self.type_calendar in (CALENDAR_ONLINE_WORKING, CALENDAR_MY_PERSONAL)
        orders_by_person = self.get_orders_by_person(persons, is_main) or {}

        permissions = []
        # создаем записи разрешений
        for participant in participants:
            permission = {
                "Main": main,
                "Calendar": calendar_id,
                "Owner": participant,
                "Access": access,
                "Order": orders_by_person.get(participant, 0),
                "Color": color,
                "Show": '{}'.format({participant}) if participant else None
            }
            permissions.append(permission)
        self.__create_permissions(json.dumps(permissions))
        # TODO: нужен массовый обработчик
        for permission in permissions:
            self.on_after_create(sbis.Record(permission), _filter, [], need_publish=False)

        user_list, persons_uuid_list = self.get_person_list_by_participant(participants)
        if user_list or persons_uuid_list:
            self.publish_operation_on_calendar(calendar_id, 'attach', user_list, persons_uuid_list)

    def __get_participants_by_departments(self, departments):
        if departments is None and self.client != get_physic_client():
            online_departs = remote_invoke_online('ЧастноеЛицо', 'GetDepartments', self.auth_person, client=self.client)
        else:
            online_departs = departments
        if online_departs:
            filter_ = sbis.Record({'Departments': online_departs.ToList('Participant'), 'Client': self.client})
            online_departs = sbis.Participant.List([], filter_, None, None)
            online_departs.RenameField('@Participant', 'Participant')
        return online_departs

    def on_before_create(self, permission, _filter, params):
        """
        Перед созданием разрешения
        """

        def is_mass(_filter):
            return any((_filter.Get('Persons'), _filter.Get('PersonsUUID'), _filter.Get('Departments')))

        def correct_format(_format):
            _format.CopyOwnFormat()
            for _field in ('Person', 'Department', 'PersonUUID'):
                if _field in _format:
                    _format.Remove(_field)
            _format.AddInt32('Person')
            _format.AddInt32('Department')
            _format.AddUuid('PersonUUID')

        def _process_first(_filter):
            """
            При массовом вызове для каждого пользователя отдельно вызывается CalendarPermission.Update
            Если не обрабатывать "первого" пользователя, то текущее разрешение будет создано с Owner = Null
            """
            single_by_mass = {'Persons': 'Person', 'PersonsUUID': 'PersonUUID', 'Departments': 'Department'}
            _fields = [_key for _key in single_by_mass.keys() if _filter.Get(_key)]
            mass_field = _fields[0]
            single_field = single_by_mass.get(mass_field)
            _filter[single_field] = _filter.Get(mass_field)[0]
            _filter[mass_field] = _filter.Get(mass_field)[1:]

        def _check_permission_exists(calendar_id, owner_id, auth_participant):
            _filter = sbis.Record({
                'Calendars': [calendar_id],
                'without_handlers': True,
            })
            perms = sbis.CalendarPermission.List(None, _filter, None, None)
            exists_perm_id, dep_perm = None, None
            for perm in perms or []:
                if perm.Get('Owner') == owner_id:
                    exists_perm_id = perm.Get('@CalendarPermission')
                if perm.Get('Owner.Department') and -owner_id in (perm.Get('Show') or []):
                    dep_perm = perm
            if exists_perm_id and not dep_perm:
                details = 'Календарь: {}, Владелец разрешения: {}, Авторизованный пользователь: {}'.format(calendar_id,
                                                                                                           owner_id,
                                                                                                           auth_participant)
                raise sbis.Warning(
                    user_msg=sbis.rk('Этот подписчик уже подключен к календарю'),
                    details=sbis.rk(details)
                )
            elif exists_perm_id and dep_perm:
                self.__delete_permission_record(exists_perm_id)
                show_list = list(set(dep_perm.Get('Show')) - {-owner_id})
                self.__update_show_field(dep_perm.Get('@CalendarPermission'), show_list)

        if is_mass(_filter):
            correct_format(_filter)
            _process_first(_filter)
            self.mass_permissions_create(_filter)

        self.load_filter(_filter)
        id_calendar = _filter.Get('Calendar')
        main_permission = self.get_main(id_calendar)
        auth_participant = self._get_auth_participant_by_calendar_type(self.type_calendar)

        departments = _filter.Get('AuthPersonDepartments')
        online_departs = self.__get_participants_by_departments(departments)

        if auth_participant:
            if self._get_access(id_calendar, auth_participant, online_departs) != ADMIN:
                raise sbis.Error('You are forbidden to create new permission')
        # Определимся с участником
        if not self.owner:
            sbis.ErrorMsg('Для разрешения не определен владелец! {} {}'.format(id_calendar, auth_participant))
        _check_permission_exists(id_calendar, self.owner, auth_participant)
        # для основного разрешения заранее извествен цвет, для второстепенных разрешений всегда
        # берем цвет календаря
        if not _filter.Get('Main') and permission.Get('Color') is None:
            permission['Color'] = main_permission.Get('Color')
        permission['Owner'] = self.owner
        permission['Main'] = _filter.Get('Main') or False

        orders = sbis.SqlQueryScalar('''SELECT ARRAY_AGG("Order") 
                                          FROM "CalendarPermission" 
                                         WHERE "Owner" = $1::int
                                           AND "Order" IS NOT NULL''',
                                     self.owner) or []
        if permission['Main'] and self.type_calendar in (CALENDAR_ONLINE_WORKING, CALENDAR_MY_PERSONAL):
            permission['Order'] = min(orders) - 1 if orders else 0
        else:
            permission['Order'] = max(orders) + 1 if orders else 0

    def on_after_create(self, permission, _filter, params, need_publish=True):
        """
        после создания разрешения
        """
        sbis.LogMsg('on_after_create run with _filter: {}'.format(_filter))
        id_calendar = _filter.Get('Calendar')
        calendar = sbis.Calendar.Read(id_calendar)
        access_level = permission.Get('Access')
        id_participant = permission.Get('Owner')
        name_owner = self._get_short_fio_participant(id_participant)
        if _filter.Get('Publish') is not None:
            need_publish = _filter.Get('Publish')
        type_owner = self.get_type_participant(id_participant)

        dep = 'подразделения ' if type_owner == PARTICIPANT_DEPARTMENT else ''
        msg = 'Создано разрешение для {dep}"{name_owner}" с уровнем доступа "{access_level}"'.format(
            dep=dep,
            name_owner=name_owner,
            access_level=NAME_ACCESS.get(access_level).lower(),
        )

        sbis.HistoryMsg(msg, 'Создание разрешения', 'УправлениеРабочимВременем_Календарь',
                        str(calendar.Get('CalendarUUID')))

        if not permission.Get('Main') \
                and calendar.Get('Type') not in [CALENDAR_DEPARTMENT_QUEUE, CALENDAR_PERSON_QUEUE]:
            self._mark_subscribe(id_calendar=id_calendar, id_subscriber=id_participant)

        user_list, persons_uuid_list = self.get_person_list_by_participant(id_participant)
        if (user_list or persons_uuid_list) and need_publish:
            self.publish_operation_on_calendar(id_calendar, 'attach', user_list, persons_uuid_list)

    def on_after_read(self, id_permission, name_method, name_link, permission):
        """
        После чтения разрешения календаря
        """
        self.add_calc_fields(permission)
        self.load_filter(permission)
        self._can_edit_one(permission)
        if self.owner:
            participant = self._get_participant(self.owner)
            if participant:
                # .get в словаре проверяет лишь на наличие ключа, если он там есть
                # и в качестве значения у него None, то без проверки словили бы неприятную багу
                l_name = '' if not participant.get('LastName', '') else participant.get('LastName')
                f_name = '' if not participant.get('FirstName', '') else participant.get('FirstName')
                m_name = '' if not participant.get('MiddleName', '') else participant.get('MiddleName')
                fio = '{} {} {}'.format(l_name, f_name, m_name)
                is_person = participant.get('Person') and participant.get('Person') != 0
                permission['FIO'] = get_initials_name(fio) if is_person else l_name
                permission['Person'] = participant.get('Person')
                permission['Department'] = participant.get('Department')
                permission['Room'] = participant.get('Room')
                permission['Vehicle'] = participant.get('Vehicle')
                permission['PersonUUID'] = participant.get('PersonUUID')
                permission['IsDepartmentPermission'] = bool(participant.get('Department'))

    def on_before_update(self, permission, _filter, params, old_permission):
        """
        перед сохранением разрешения
        """
        self.load_filter(_filter)
        id_calendar = _filter.Get('Calendar')
        auth_participant = self._get_auth_participant_by_calendar(id_calendar)
        level_access = super(CalendarPermission, self)._get_access(id_calendar, auth_participant,
                                                                   _filter.Get('Departments', None))

        save_fields = ['Calendar', 'Main', 'Owner', 'Access', 'Show']
        # цвет даем менять всегда (не требуются админские права)
        if level_access == ADMIN and not old_permission.Get('Main'):
            save_fields.remove('Access')
        # forbidden to change fields of permission
        for save_field in save_fields:
            permission[save_field] = old_permission.Get(save_field)

    def on_after_update(self, permission, _filter, params, old_permission):
        """
        после сохранения разрешения 
        """
        id_calendar = permission.Get('Calendar')
        calendar = sbis.Calendar.Read(id_calendar)
        old_access = old_permission.Get('Access')
        access = permission.Get('Access')
        id_participant = permission.Get('Owner')
        name_owner = _filter.Get('NameOwner')

        if access != old_access:
            dep = 'подразделения ' if self.get_type_participant(
                id_participant) == PARTICIPANT_DEPARTMENT else ''
            if not name_owner:
                name_owner = self._get_participant(id_participant).get('LastName')
            msg = 'Изменен уровень доступа для {dep}"{name_owner}" c "{old_access_level}" на "{access_level}"'.format(
                dep=dep,
                name_owner=name_owner,
                old_access_level=NAME_ACCESS.get(old_access).lower(),
                access_level=NAME_ACCESS.get(access).lower(),
            )
            sbis.HistoryMsg(msg, 'Редактирование разрешения', 'УправлениеРабочимВременем_Календарь',
                            str(calendar.Get('CalendarUUID')))

    @staticmethod
    def _calendars_admins(calendars):
        sql = '''
            SELECT ARRAY_AGG("Owner")
              FROM "CalendarPermission"
             WHERE "Calendar" = ANY($1::int[])
               AND "Access" = 4
        '''
        return list(filter(None, sbis.SqlQueryScalar(sql, calendars) or []))

    @staticmethod
    def _calendars_owners(calendars):
        sql = '''
            SELECT "@Calendar"
                 , "Owner"
              FROM "Calendar"
             WHERE "@Calendar" = ANY($1::int[])
        '''
        return sbis.SqlQuery(sql, calendars).ToDict('@Calendar', 'Owner')

    def _can_edit_mass(self, params, permissions):

        # Обратная совместимость между 19.2хх и 19.310
        # В 19.2хх приходит 1 календарь в единичном параметре, в 310 приходит множественный параметр с массивом
        one_calendar = params.filter.Get('Calendar')
        calendar_list = params.filter.Get('Calendars', [])

        departments = params.filter.Get('AuthPersonDepartments')
        online_departs = self.__get_participants_by_departments(departments) or []
        departments_prtc = {rec.Get('Participant') for rec in online_departs}

        if not calendar_list:
            calendar_list = [one_calendar]
        auth_participants = {self.auth_physic_participant, self.auth_participant} | departments_prtc
        cln_admins = self._calendars_admins(calendar_list)
        cln_owners = self._calendars_owners(calendar_list)
        # Есть ли у авторизованного пользователя админские права на календарь
        is_admin = bool(auth_participants & set(cln_admins))
        for perm in permissions:
            is_cln_owner = cln_owners.get(perm.Get('Calendar')) in auth_participants
            if is_admin and (perm.Get('Owner') not in auth_participants or is_cln_owner) and not perm.Get('Main'):
                perm['CanDelete'] = True

    def has_admin_permission(self, participant, calendar):
        return participant in self._calendars_admins([calendar])

    def _can_edit_one(self, permission):
        auth_participant = self._get_auth_participant_by_calendar_type(permission.Get('Calendar.Type'))
        is_admin = self.has_admin_permission(auth_participant, permission.Get('Calendar'))
        if is_admin and permission.Get('Owner') != auth_participant and not permission.Get('Main'):
            permission['CanDelete'] = True

    def _remove_personal_permission(self, id_calendar, id_exclude_participants):
        """
        Удаляет персональные разрешения, в случае удаления разрешения подразделения
        :param id_calendar: обрабатываемый календарь
        :param id_exclude_participants: участники, которые имеют персональные разрешения
        :return:
        """
        _filter = sbis.Record({
            'Calendars': [id_calendar],
            'ByOwners': id_exclude_participants,
            'without_handlers': True,
        })
        id_personal_permission = sbis.CalendarPermission.List(None, _filter, None, None).ToList('@CalendarPermission')
        if id_personal_permission:
            sbis.CalendarPermission.Delete(id_personal_permission)

    def remove_personal_permissions(self, permissions):
        for permission in permissions:
            if permission.Get('IsDepartmentPermission'):
                id_calendar = permission.Get('Calendar')
                show = permission.Get('Show') or []
                id_exclude_participants = list(map(abs, filter(lambda x: x < 0, show)))
                if id_exclude_participants:
                    self._remove_personal_permission(id_calendar, id_exclude_participants)

    def on_before_delete(self, permission):
        """
        Перед удалением разрешения
        - Проверим права, можно ли текущему авторизованному удалить это разрешение
        - Удалим все персональные разрешения которые были созданы при отключении от календаря подразделения (отрицательные)
        - У календаря, к которому было разрешение, уменьшим количество подписчиков
        """
        id_calendar = permission.Get('Calendar')
        id_owner = permission.Get('Owner')
        show = permission.Get('Show') or []
        departments = permission.Get('Departments')
        online_departs = self.__get_participants_by_departments(departments)
        calendar = sbis.Calendar.Read(id_calendar)
        auth_participant = self._get_auth_participant_by_calendar_type(calendar.Get('Type'))
        if permission.Get('IsAdmin', False):
            access = ADMIN
        else:
            access = self._get_access(id_calendar, auth_participant, departments=online_departs, calendar=calendar)
        # удаляем разрешение, если авторизованный участник удаляет свое или имеет на это право
        # запрещено удалять, если календарь рабочий/личный и основное разрешение
        is_main_permission = calendar.Get('Type') in (CALENDAR_ONLINE_WORKING, CALENDAR_MY_PERSONAL) \
                             and calendar.Get('Owner') == id_owner
        if is_main_permission or (id_owner != auth_participant and access != ADMIN):
            raise sbis.Error('You are forbidden to delete permission')

        # удалим все персональные разрешения, которые создавались от разрешения подразделения
        type_participant = self.get_type_participant(id_owner)
        if type_participant == PARTICIPANT_DEPARTMENT:
            id_exclude_participants = list(map(abs, filter(lambda x: x < 0, show)))
            if id_exclude_participants:
                self._remove_personal_permission(id_calendar, id_exclude_participants)

        # Уменьшим количество подписчиков
        self._mark_unsubscribing(id_calendar=id_calendar, id_subscriber=id_owner)

    def on_after_delete(self, permission):
        """
        после удаления разрешения
        """
        calendar_uuid = permission.Get('CalendarUUID')
        if not calendar_uuid:
            id_calendar = permission.Get('Calendar')
            calendar = sbis.Calendar.Read(id_calendar)
            calendar_uuid = calendar.Get('CalendarUUID')
        id_participant = permission.Get('Owner')
        name_owner = permission.Get('FIO')
        dep = ''
        if not name_owner:
            name_owner = self._get_short_fio_participant(id_participant)
        if (permission.Get('IsDepartmentPermission')
                or self.get_type_participant(id_participant) == PARTICIPANT_DEPARTMENT):
            dep = 'подразделения '
        msg = 'Удалено разрешение для {dep}"{name_owner}"'.format(
            dep=dep,
            name_owner=name_owner,
        )
        sbis.HistoryMsg(msg, 'Удаление разрешения', 'УправлениеРабочимВременем_Календарь', str(calendar_uuid))

        user_list, persons_uuid_list = self.get_person_list_by_participant(id_participant)
        if user_list:
            id_calendar = permission.Get('Calendar')
            self.publish_operation_on_calendar(id_calendar, 'detach', user_list, persons_uuid_list)

    def is_available_for_remove(self, permission):
        auth_participant = self._get_auth_participant_by_calendar_type(permission.Get('Calendar.Type'))
        id_calendar = permission.Get('Calendar')
        id_owner = permission.Get('Owner')
        departments = permission.Get('Departments')
        online_departs = self.__get_participants_by_departments(departments)
        access = self._get_access(id_calendar, auth_participant, online_departs)
        is_main_permission = permission.Get('Calendar.Type') in (CALENDAR_ONLINE_WORKING, CALENDAR_MY_PERSONAL) \
                             and permission.Get('Calendar.Owner') == id_owner
        if is_main_permission or (id_owner != auth_participant and access != ADMIN):
            return False
        return access == ZONE_ADMIN or access == ADMIN

    def _get_available_for_remove(self, permissions: sbis.RecordSet, access: int) -> sbis.RecordSet:
        available_for_remove = sbis.RecordSet(permissions.Format())
        for permission in permissions:
            if self.is_available_for_remove(permission):
                available_for_remove.AddRow(permission)
        return available_for_remove

    def delete(self, permission_id, current_access, permission=None):
        if not permission:
            permission = sbis.CalendarPermission.Read(permission_id)
        # ZONE_ADMIN - это уровень доступа "полный" для зоны "Календарь редактирование событий"
        # полученный при помощи метода sbis.CheckRights.ZoneAccess()
        permission.AddBool('IsAdmin', current_access == ZONE_ADMIN)
        self.on_before_delete(permission)
        result = self.__delete_permission_record(permission_id)
        self.on_after_delete(permission)
        return bool(result)

    @staticmethod
    def __delete_permission_record(permission_id: int):
        sql = '''
            DELETE FROM "CalendarPermission"
            WHERE "@CalendarPermission" = $1::int
            RETURNING *
        '''
        return sbis.SqlQuery(sql, permission_id)

    @staticmethod
    def __update_show_field(permission_id, show_list):
        sql = '''
            UPDATE "CalendarPermission"
               SET "Show" = $2::int[] 
             WHERE "@CalendarPermission" = $1::int
            RETURNING *
        '''
        return sbis.SqlQuery(sql, permission_id, show_list)

    def delete_mass(self, permissions: sbis.RecordSet, access: int, calendar_id = None) -> bool:
        """
        Массовое удаление разрешений
        :param permissions:
        :param access:
        :return:
        """
        auth_person = sbis.Participant.GetAuth()
        if calendar_id is not None:
            access_calendars = sbis.CalendarPermission.List2(None, sbis.Record({'Calendars': [calendar_id]}), None,
                                                             None)
            deletion_subscribers = sbis.RecordSet(access_calendars.Format())
            for access in access_calendars:
                if access.Get('Main') is not True and auth_person != access.Get('Owner'):
                    deletion_subscribers.AddRow(access)
            permissions = deletion_subscribers

        if not permissions:
            return False
        available_for_remove = self._get_available_for_remove(permissions, access)
        self.remove_personal_permissions(available_for_remove)
        sql = '''
            DELETE FROM "CalendarPermission"
            WHERE "@CalendarPermission" = ANY($1::int[])
            RETURNING "@CalendarPermission"
        '''
        result = sbis.SqlQuery(sql, available_for_remove.ToList('@CalendarPermission'))
        for permission in available_for_remove:
            self.on_after_delete(permission)
        return bool(result.ToList('@CalendarPermission'))

    def rename_fields_by_list_permission(self, permissions):
        """
        После получения списка разрешений для карточки календаря, переименуем поля
        :param permissions: 
        :return: 
        """
        if isinstance(permissions, sbis.Record):
            _format = permissions.Format()
            new_permissions = sbis.RecordSet(_format)
            new_permissions.AddRow(permissions)
            permissions = new_permissions

        fields = {
            'Owner.Person': 'Person',
            'Owner.Department': 'Department',
            'Owner.Room': 'Room',
            'Owner.Vehicle': 'Vehicle',
            'Calendar.Name': 'Name',
            'Calendar.CalendarUUID': 'CalendarUUID',
            'РП.CanDelete': 'CanDelete',
            'РП.IsDepartmentPermission': 'IsDepartmentPermission',
            'РП.FIO': 'FIO',
            'РП.MainAuthPerson': 'MainAuthPerson',
            'РП.Flags': 'Flags',
            'РП.VisibleAll': 'VisibleAll',
            'РП.CanEditAll': 'CanEditAll',
            'РП.TypeOwner': 'TypeOwner',
        }

        format_permissions = permissions.Format()
        for old_field, new_field in fields.items():
            if old_field in format_permissions:
                permissions.RenameField(old_field, new_field)
        for permission in permissions:
            permission['ColorStr'] = rgb_int_to_hex(permission.Get('Color'))
            permission['MainAuthPerson'] = (permission.Get('Owner') == self.auth_participant
                                            or permission.Get('Owner') == self.auth_physic_participant)
            flags = permission.Get('Flags') or [False, False, False]
            permission['VisibleAll'] = flags[1]
            permission['CanEditAll'] = flags[2]

    def rename_fields_create_permission_ext(self, permission):
        """
        переименовываем поля после создания пустой записи разрешения календаря
        """
        fields = {
            'РП.Person': 'Person',
            'РП.Department': 'Department',
        }
        format_calendar = permission.Format()
        for old_field, new_field in fields.items():
            if old_field in format_calendar:
                permission.RenameField(old_field, new_field)

    def get_online_participant(self, id_participant):
        if id_participant:
            participant = self._get_participant(id_participant)
            return participant.get('Person') or participant.get('Department') or \
                   participant.get('Room') or participant.get('Vehicle')

    def _get_access(self, id_calendar, id_participant, departments=None, calendar=None):
        """
        Метод проверяет, может ли авторизованный сотрудник создать/изменить разрешение календаря
        Разрешено, если:
        - есть доступ к календарю уровня ADMIN
        - отсутствует основное разрешение календаря (в процессе создания календаря нельзя
        блокировать создание разрешения)
        - календарь расшарен участнику через подразделение (создание персонального разрешения
        при смене цвета/положения/должности)
        """
        calendar = sbis.Calendar.Read(id_calendar) if not calendar else calendar
        main_permission = self.get_main(id_calendar, calendar.Owner)
        if not main_permission or calendar.Type in (CALENDAR_DEPARTMENT_QUEUE, CALENDAR_PERSON_QUEUE):
            return ADMIN
        participants = [id_participant]
        if departments:
            participants.extend(departments.ToList('Participant'))
        _filter = sbis.Record({
            'Calendar': id_calendar,
            'ByOwners': participants,
            'without_handlers': True
        })
        permissions = sbis.CalendarPermission.List(None, _filter, None, None)
        if permissions:
            return max(permissions.ToList('Access'))
        else:
            return READ

    def create_personal(self, id_participant, id_permissions):
        """
        Создает персональные разрешения
        """
        # определимся с участником, для кого будут созданы персональные разрешения
        if not id_participant:
            id_participant = self.auth_participant
        return self._create_personal(id_participant, id_permissions)

    def _create_personal(self, id_participant, id_permissions):
        """
        Создает персональные разрешения календарей от разрешений подразделений
        Персональное разрешение должно создаваться по действиям:
           - действия по сотруднику (прием, перевод, объединение)
           - смена цвета календаря
           - drag&drop календаря в аккордеоне
        :param id_participant: участник, для которого создаются персональные разрешения
        :param id_permissions: CalendarPermission, int[]
        :return: RS
            Calendar, int
            CalendarPermission, int
        :return: 
        """

        def create_permissions(new_owner, dep_permissions):
            """
            Создает персональные разрешения
            :param new_owner: участник, для кого создаются персональные разрешения
            :param dep_permissions: разрешения подразделений
            """
            new_values = []
            for dep_permission in dep_permissions:
                show = dep_permission.Get('Show') or []
                dep_permission['Owner'] = new_owner
                dep_permission['Show'] = [id_participant] if id_participant in show else []
                temp = dep_permission.as_dict()
                temp['Show'] = '{}'.format(set(show)) if show else None
                new_values.append(temp)
            return self.__create_permissions(json.dumps(new_values))

        def mark_permissions(new_owner, dep_permissions):
            """
            Помечает разрешения подразделений: устанавливаем признак в Show, что участник 
            new_owner изъял календарь
            :param dep_permissions: разрешения подразделений
            """
            new_show = []
            for dep_permission in dep_permissions:
                show = dep_permission.Get('Show') or []
                negative_owner = -1 * new_owner
                if new_owner in show:
                    show.remove(new_owner)
                if negative_owner not in show:
                    show += [negative_owner]
                new_show.append({
                    'id_permission': dep_permission.Get('@CalendarPermission'),
                    'show': '{}'.format(set(show)) if show else None,
                })
            self.update_show_permission(new_show)

        # получим список разрешений подразделений
        _filter = sbis.Record({
            'CalendarPermissions': id_permissions,
            'OnlyDepartment': True,
        })
        permissions = sbis.CalendarPermission.List(None, _filter, None, None)
        permission_format = sbis.TableFormat('CalendarPermission')
        permissions.Migrate(permission_format)
        # создадим персональные разрешения
        personal_permissions = create_permissions(id_participant, permissions)
        mark_permissions(id_participant, permissions)
        return personal_permissions

    def create_mass_permissions(self, values):
        self.__create_permissions(json.dumps(values))

    def __create_permissions(self, values):
        """
        Массовая вставка "персональных" разрешений
        """
        sql = '''
        WITH new_values ("Main", "Calendar", "Owner", "Access", "Order", "Color", "Show") AS (
            SELECT * FROM json_to_recordset($1::text::json) AS x1(
                  "Main" bool
                , "Calendar" int
                , "Owner" bigint
                , "Access" smallint
                , "Order" bigint
                , "Color" int
                , "Show" int[]
            )
        )
        INSERT INTO "CalendarPermission"
            ("Main", "Calendar", "Owner", "Access", "Order", "Color", "Show")
        SELECT
            "Main", "Calendar", "Owner", "Access", "Order", "Color", "Show"
        FROM
            new_values
        ON CONFLICT DO NOTHING
        RETURNING
            "@CalendarPermission",
            "Calendar" "@Calendar"
        '''

        if values:
            with sbis.CreateTransaction(sbis.TransactionLevel.READ_COMMITTED,
                                        sbis.TransactionMode.WRITE):
                return sbis.SqlQuery(sql, values)

    def list(self, addit, _filter, sort, _nav):
        """
        Временный метод, до тех пор, пока в платформе не будет реализован
        обработчик ДО для декларативных списков
        """
        filter_fields = {'OnlyMain', 'ByOwner', 'ExtIds', 'ByOwners', 'ByTypes', 'Calendars',
                         'CalendarPermissions', 'OnlyDepartment', 'CalendarsUUID', 'ByPersons', 'without_handlers'}

        # Если переданы два поля фильтра Calendar и Person то можно пропустить - на эти два поля есть четкий индекс
        indexed_permission_list = set(_filter.GetFieldNames()) == {'Calendar', 'Person'}
        if set(_filter.GetFieldNames()) & filter_fields or indexed_permission_list:
            return sbis.CalendarPermission.List2(addit, _filter, sort, _nav)
        return sbis.RecordSet(sbis.MethodResultFormat('CalendarPermission.List2', 4))

    def on_after_list(self, params, permissions):
        """
        Обработчик на список разрешений
        """
        self.rename_fields_by_list_permission(permissions)
        self._can_edit_mass(params, permissions)

    def set_access_mass(self, id_permissions, access_level, departments=None, calendar_id=None):
        """
        Массово назначает права для разрешений
        """
        auth_person = sbis.Participant.GetAuth()
        sbis.LogMsg(sbis.LogLevel.llMINIMAL, 'авторизованный пользователь: {}'.format(str(auth_person)))
        if calendar_id is not None:
            access_calendars = sbis.CalendarPermission.List2(None, sbis.Record({'Calendars': [calendar_id]}), None,
                                                             None)
            sbis.LogMsg(sbis.LogLevel.llMINIMAL, 'списочный метод: {}'.format(str(access_calendars)))
            for access in access_calendars:
                if access.Get('Main') is not True and auth_person != access.Get('Owner'):
                    if access.Get('TypeOwner') >= 3:
                        department = access.Get('Department')
                        if department not in departments:
                            id_permissions.append(access.Get('@CalendarPermission'))
                    else:
                        id_permissions.append(access.Get('@CalendarPermission'))
        if not id_permissions:
            return
        for id_permission in id_permissions:
            permission = sbis.CalendarPermission.Read(id_permission)
            if permission:
                permission['Access'] = access_level
                if departments and isinstance(departments, sbis.RecordSet):
                    permission.AddRecordSet('Departments', departments.Format(), departments)
                sbis.CalendarPermission.Update(permission)

    def merge(self, id_person, id_removed_person, id_departments_by_removed_person):
        """
        Прикрепляет разрешения от старого участника к новому
        """
        self.__merge_online(id_person, id_removed_person, id_departments_by_removed_person)

    def move(self, object_name, object_id, destination_id, index_number,
             hierarchy_name, order, read_method=None, update_method=None):
        sbis.IndexNumber.Move(object_name, object_id, destination_id, index_number,
                              hierarchy_name, order, read_method, update_method)

    def get_persons_with_calendar(self, params):
        """
        Возвращает частных лиц, у которых календари видны.
        :param params: Фильтр для отбора календарей.
        :return: Массив частных лиц.
        """
        permissions_rs = sbis.CalendarPermission.List(None, params, None, None)
        return self.__get_persons_from_permission(permissions_rs)

    def __get_persons_from_permission(self, permission_rs):
        filtered_list = permission_rs.ToList(('Person', 'Department', 'Show'))
        participants_to_show = set()
        participants_to_hide = set()

        # Получаем список участников, для которых показывать и скрывать календарь.
        for item in filtered_list:
            self.__process_permission(item, participants_to_hide, participants_to_show)

        result_info = sbis.RecordSet(sbis.MethodResultFormat('CalendarPermission.PersonsWithVisibleCalendar', 4))

        # По списку участников получим необходимую информацию.
        part_info = self._get_participants(list(participants_to_show - participants_to_hide))

        for item in part_info:
            self.__fill_part_info_rs(item, part_info, result_info)

        return result_info

    def __process_permission(self, item, participants_to_hide, participants_to_show):
        person = item[0]
        department = item[1]
        show_calendar = item[2]
        if person:
            # Проверяем, нужно ли показывать календарь у пользователя.
            if show_calendar and show_calendar[0] > 0:
                participants_to_show.add(show_calendar[0])
        if department:
            self.__process_department_permission(participants_to_hide, participants_to_show, show_calendar)

    def __process_department_permission(self, participants_to_hide, participants_to_show, show_calendar):
        if not show_calendar:
            return
        for participant in show_calendar:
            if participant > 0:
                participants_to_show.add(participant)

    def __fill_part_info_rs(self, item, part_info, result_info):
        dict_value = part_info[item]
        if dict_value:
            fill_dict = {'Person': dict_value['Person'],
                         'User': dict_value['User'],
                         'Client': dict_value['Client'],
                         'PersonUUID': dict_value['PersonUUID']}
            row = result_info.AddRow()
            row.Fill(fill_dict)

    def __merge_online(self, id_person, id_removed_person, id_departments_by_removed_person):
        """
        Мержит разрешения частных лиц
        """
        _filter = {
            'Persons': [id_person, id_removed_person],
        }
        if id_departments_by_removed_person:
            _filter['Departments'] = id_departments_by_removed_person

        participants = get_by_ext_id(_filter)

        id_participant = participants.get(str(id_person))
        id_remote_participant = participants.get(str(id_removed_person))
        id_depart_participants = [participants.get(p) for p in participants if
                                  int(p) in id_departments_by_removed_person]

        if all((id_participant, id_remote_participant)):
            sbis.LogMsg('try merge participants: id_participant: {}, id_remote_participant: {}'.format(id_participant,
                                                                                                       id_remote_participant))
            self.__merge_permissions(id_participant, id_remote_participant, id_depart_participants)

    @staticmethod
    def __get_permissions(id_participants):
        """
        Возвращает разрешения участников
        """
        _filter = sbis.Record({
            'ByOwners': id_participants,
        })
        all_permissions = sbis.CalendarPermission.List(None, _filter, None, None)
        format_permissions = all_permissions.Format()
        permissions = sbis.RecordSet(format_permissions)
        depart_permissions = sbis.RecordSet(format_permissions)
        for permission in all_permissions:
            if permission.Get('IsDepartmentPermission'):
                depart_permissions.AddRow(permission)
            else:
                permissions.AddRow(permission)
        return permissions, depart_permissions

    def __mark_as_exclude(self, id_participant, id_removed_participant, depart_permissions):
        """
        Переназначает для разрешений подразделений признак иъятия, за новым участником
        """
        new_values = []
        id_exclude_participant = -1 * id_participant
        id_exclude_removed_participant = -1 * id_removed_participant
        for depart_permission in depart_permissions:
            show = depart_permission.Get('Show') or []
            if id_exclude_removed_participant in show:
                show.remove(id_exclude_removed_participant)
                show.append(id_exclude_participant)
                new_values.append({
                    'id_permission': depart_permission.Get('@CalendarPermission'),
                    'show': '{}'.format(set(show)),
                })
        self.update_show_permission(new_values)

    def __merge_permissions(self, id_participant, id_removed_participant, id_depart_participants):
        """
        Мержит разрешения при объединении участников
        PS: в случае подключения одних и тех же календарей, оставляем разрешение с max AccessLevel
        """

        id_participants = [id_participant, id_removed_participant] + (id_depart_participants or [])
        permissions, depart_permissions = self.__get_permissions(id_participants)
        # получим разрешения, с максимальным уровнем доступа, закрепленные за календарем
        calendars = collections.defaultdict(list)
        calendar_to_remove = None
        for permission in permissions:
            id_permission = permission.Get('@CalendarPermission')
            is_main_permission = permission.Get('Main')
            id_calendar = permission.Get('Calendar')
            level_access = permission.Get('Access')
            owner = permission.Get('Owner')
            type_calendar = permission.Get('Calendar.Type')
            if type_calendar == CALENDAR_ONLINE_WORKING and owner == id_removed_participant and is_main_permission:
                calendar_to_remove = id_calendar
                continue

            # Для каждого календаря соберем все его пермишены,
            # heap queue обеспечит в 0 элементе всегда запись с самым минимальный разрешением
            # Но по скольку разрешения инвертированы по факту мы получим самое широкое разрешение
            # https://docs.python.org/3.0/library/heapq.html
            heapq.heappush(calendars[id_calendar], (-level_access, id_permission))

        # переопределим собственника разрешений, и удалим лишние
        # самое широкое разрешение переопределим - сменим собственника
        # остальные разрешения удалим
        all_permissions = [permission.Get('@CalendarPermission') for permission in permissions]
        widest_permissions = [calendar_permissions[0] for calendar_permissions in calendars.values()]
        need_update = [permission_id for _, permission_id in widest_permissions]
        need_remove = list(set(all_permissions) - set(need_update))
        if need_update:
            sql = '''
                UPDATE
                    "CalendarPermission"
                SET
                    "Owner" = $1::int
                WHERE
                    "@CalendarPermission" = ANY($3::int[]) AND
                    "Owner" = $2::int
            '''
            sbis.SqlQuery(sql, id_participant, id_removed_participant, need_update)

        # Удалим разрешения.
        # Здесь не надо проверять права. Слияние - это административная операция,
        # она полностью должна игнорировать права. Даже если нет прав на календарь, но есть на слияние то они важнее
        if need_remove:
            sbis.LogMsg('remove permissions: {}'.format(need_remove))
            for permission_id in need_remove:
                # от стандартного удаления нам вообще ничего не нужно, даже подписчиков уменьшать не надо
                # просто был один, вместо него стал другой
                self.__delete_permission_record(permission_id)

        # Удалим основной рабочий календарь - события переносятся на основном сервисе
        if calendar_to_remove:
            sbis.LogMsg('remove main calendar: {}'.format(calendar_to_remove))
            sbis.Calendar.Delete([calendar_to_remove])

        # Остальные календари перекидываем на нового владельца
        sql = '''
            UPDATE "Calendar"
               SET "Owner" = $1::bigint
             WHERE "Owner" = $2::bigint
        '''
        sbis.SqlQuery(sql, id_participant, id_removed_participant)

        # Перепривяжем признак изъятия календаря к новому участнику
        self.__mark_as_exclude(id_participant, id_removed_participant, depart_permissions)
        sbis.LogMsg('remove participant: {}'.format(id_removed_participant))
        sbis.Participant.Удалить(id_removed_participant)

    def __merge_personal_events(self, id_participant, id_removed_participant):
        """
        Мержит персональные события
        """
        # смержим персональные события
        _filter = sbis.Record({
            'Owners': [id_participant, id_removed_participant],
            'Type': [CALENDAR_MY_PERSONAL],
        })
        calendars = sbis.Calendar.List(None, _filter, None, None)
        calendars = {calendar.Get('Owner'): calendar.Get('CalendarUUID') for calendar in
                     calendars}
        uuid_calendar = calendars.get(id_participant)
        uuid_remote_calendar = calendars.get(id_removed_participant)
        sbis.Event.Merge(uuid_calendar, uuid_remote_calendar)
