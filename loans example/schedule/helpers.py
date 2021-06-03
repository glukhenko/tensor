"""
Вспомогательный модуль, предоставляющий функционал по работе с графиками платежей.

Реализованы следуюшие фукнции:
1. Получение фильтра по идентификатору договора, необходимого для построения графика платежей, get_filter_schedule
2. Получение списка платежей за один день, get_payments
3. Получение названий регламетов, для списка платежей за один день, get_name_regls
"""


__author__ = 'Glukhenko A.V.'

import sbis
from loans.loanConsts import LC
from .sql import PAYMENTS_BY_DATE, FILTER_FOR_SCHEDULE


def get_filter_schedule(id_docs, type_schedules=None, lang_filter='en', only_empty_monthly_payment=False):
    """
    Возвращает набор фильтров для рассчета графиков займов
    :param id_docs: массив идентификаторов документов, при None обрабатывает все документы договоров займов
    :param type_schedules: список типов графиков платежей, который может приниманть значения:
        - LC.REPAYMENT_ON_DEMAND
        - LC.DIFFERENTIATED_SCHEDULE
        - LC.ANNUITY_SCHEDULE
        - LC.REPAYMENT_DEBT_AND_PERCENTS_AT_THE_END
    :param lang_filter: указывает язык полей, возможные значения 'ru'/'en'
    :param only_empty_monthly_payment: возвращает графики с незаполненой суммой ежемесячного платежа
    :return: набор фильтров в виде {id_doc: _filter}, dict
    """
    type_docs = (LC.ISSUED_LOAN_DOC_TYPE, LC.RECEIVED_LOAN_DOC_TYPE)

    if id_docs:
        filter_by_docs = '''
            doc."@Документ" = ANY(ARRAY{})
        '''.format(id_docs)
    else:
        # все документы
        filter_by_docs = '''
            doc."ТипДокумента" = ANY(
                SELECT "@ТипДокумента" FROM "ТипДокумента" WHERE "ТипДокумента" = ANY(ARRAY[{}])
            )
        '''.format(','.join("'{}'".format(type_doc) for type_doc in type_docs))

    if type_schedules:
        filter_by_type_schedules = '''
            AND diff_doc."ТипГрафикаПогашения" = ANY(ARRAY{})
        '''.format(type_schedules)
    else:
        filter_by_type_schedules = ''
    name_fields = get_name_fields_filter_shcedule(lang_filter)

    if only_empty_monthly_payment:
        filter_without_rate = 'AND diff_doc."Состав" IS NULL'
    else:
        filter_without_rate = ''

    sql = FILTER_FOR_SCHEDULE.format(
        filter_by_docs=filter_by_docs,
        filter_by_type_schedules=filter_by_type_schedules,
        filter_without_rate=filter_without_rate,
        **name_fields,
    )
    return {rec.Get('@Документ'): rec for rec in sbis.SqlQuery(sql)}


def get_name_fields_filter_shcedule(lang_filter):
    """
    Возвращает словарь с названием полей, для фильтра запроса графика платежей
    :param lang_filter: указывает язык полей, возможные значения 'ru'/'en'
    :return: словарь вида {'date_start': 'ДатаНачало' | 'DateBegin', ...}
    PS: используется для поддержки старых графиков платежей
    """
    name_fields = {}
    if lang_filter not in ('en', 'ru'):
        raise sbis.Error('lang_filter может принимать значения (en, ru)')
    if lang_filter == 'en':
        name_fields = {
            'date_start': 'DateBegin',
            'date_stop': 'DateEnd',
            'loan': 'IdLoan',
            'face_loan': 'IdFaceLoan',
            'rate': 'Rate',
            'monthly_payment': 'MonthlyPayment',
            'id_organization': 'IdOrganization',
            'first_payment_date': 'FirstPaymentDate',
            'size_payment': 'SizePayment',
            'type_schedule': 'TypeSchedule',
            'type_doc': 'TypeDoc',
            'name_type_doc': 'NameTypeDoc',
        }

    if lang_filter == 'ru':
        name_fields = {
            'date_start': 'ДатаНачало',
            'date_stop': 'ДатаКонец',
            'loan': 'ДоговорЗайма',
            'face_loan': 'ДоговорЗайма.Лицо',
            'rate': 'ДоговорЗайма.Ставка',
            'monthly_payment': 'ЕжемесячныйПлатеж',
            'id_organization': 'НашаОрганизация',
            'first_payment_date': 'ПервыйПлатеж',
            'size_payment': 'РазмерЗайма',
            'type_schedule': 'ТипГрафика',
            'type_doc': 'ТипДокумента',
            'name_type_doc': 'ТипДокументаНазвание',
        }
    return name_fields


def get_payments(_filter):
    """
    Возвращает список платежей на дату по договору займа
    :param _filter: фильтр запроса
    """
    _format = sbis.MethodResultFormat('ЗаймыКредиты.Payments', 1)
    result = sbis.RecordSet(_format)
    obj = sbis.Session.ObjectName()
    type_dc = {
        LC.RECEIVED_LOAN_DOC_TYPE: 1,
        LC.ISSUED_LOAN_DOC_TYPE: 2,
    }
    payments_type_docs = {
        LC.ISSUED_LOAN_DOC_TYPE: [
            'ВходящийПлатеж',
            'ПриходныйОрдер',
            'БухгалтерскаяСправка',
        ],
        LC.RECEIVED_LOAN_DOC_TYPE: [
            'ИсходящийПлатеж',
            'РасходныйОрдер',
            'БухгалтерскаяСправка',
        ],
    }

    id_loan = _filter.Get('@Документ')
    date = _filter.Get('Дата')

    if all((id_loan, date)):
        payments = sbis.SqlQuery(
            PAYMENTS_BY_DATE,
            id_loan,
            date,
            type_dc.get(obj),
            payments_type_docs.get(obj),
        )
        id_regls = payments.ToList("Регламент")
        name_regls = get_name_regls(id_regls)
        for payment in payments:
            number = payment.Get('Номер')
            rec = sbis.Record({
                '@Документ': payment.Get('@Документ'),
                'Описание': '{name_regl} {date}{doc_number}, {org_name} на сумму {doc_sum}'.format(
                    name_regl=name_regls.get(payment.Get('Регламент')),
                    date=payment.Get('Дата').strftime('%d.%m.%y'),
                    doc_number=f' №{number}' if number else '',
                    org_name=payment.Get('НазваниеОрганизации'),
                    doc_sum=payment.Get('Сумма'),
                ),
            })
            result.AddRow(rec)
    return result


def get_name_regls(id_regls):
    """
    Возвращает названия регламентов по их идентификаторам
    :param id_regls: список идентификаторов регламентов
    :return: словарь вида {id_regl: name_regl}
    """
    name_regls = {}

    if id_regls:
        try:
            regls_filter = sbis.Record({
                'Действующий': True,
                'regl_int_ids': id_regls,
            })
            regls = sbis.Regulation.List(regls_filter).Get('Регламент')
            name_regls = {regl.Get('@Регламент'): regl.Get('Название') for regl in regls}
        except sbis.Error as err:
            sbis.WarningMsg('some problem with reglaments, {}'.format(err))

    return name_regls


def swap_id_type_row(schedule, to_new_const):
    """
    Меняет значения идентификаторов записей в графике платежей
    :param schedule: график платежей, RecordSet
    :param to_new_const: заменяет значения полей ТипЗаписи на новые или старые константы
    True - заменяет старые идентификаторы на новые
    False - заменяет новые идентификаторы на старые
    Примечание: Благодаря данному методу мы можем менять сортировку графика на БЛ на лету,
    не меняя API типов записей перед клиентом. Например нам надо поднять строку год платежа
    SCHEDULE_DATE на первую позицию согласно сортировке. Старое значение идентификатора = 3
    (на которое ориентируется клиент), а новое 0. После сортировки графика мы возвращаем
    старые идентификаторы, необходимые для клиента. Данные подмены необходимы т.к. часто
    меняются условия построения графика, добавляются новые типы (например открытая просрочка)
    и меняются приорететы по сортировке. При закрытии проекта, данную функцию можно будет
    убрать.
    """
    new_id_rows = {
        LC.SCHEDULE_DATE: 0,
        LC.SCHEDULE_DELAY: 1,
        LC.SCHEDULE_OPEN_DELAY: 2,
        LC.SCHEDULE_PAYMENT: 3,
        LC.SCHEDULE_PAYMENTS: 4,
        LC.SCHEDULE_INITIAL_BALANCE: 5,
        LC.SCHEDULE_PLAN: 6,
        LC.SCHEDULE_OUTCOME: 7,
        LC.SCHEDULE_CORRECTION: 8,
    }
    old_id_rows = {value: key for key, value in new_id_rows.items()}
    map_type_rows = new_id_rows if to_new_const else old_id_rows

    for i in range(schedule.Size()):
        old_value = schedule.Get(i, 'ТипЗаписи')
        new_value = map_type_rows.get(old_value)
        schedule.Set(i, 'ТипЗаписи', new_value)


def get_x_point(abscissa1, ordinate1, abscissa2, ordinate2):
    """
    :param [abscissa1:ordinate1]: первая точка
    :param [abscissa2:ordinate2]: вторая точка
    :return: возвращает значение абциссы, при которой ордината = 0, int
    """
    coefficient = (ordinate2 - ordinate1) / (abscissa2 - abscissa1)
    return (0 - ordinate2) / coefficient + abscissa2
