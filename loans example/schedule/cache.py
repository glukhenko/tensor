"""
Модуль отвечает за кэширование графиков платежей нового формата. Кэш хранится в поле ДокументРасширение.Параметры
в виде hstore
"""


__author__ = 'Glukhenko A.V.'

import decimal

import sbis
from loans.cache.base import BaseCacheLoan
from loans.loanConsts import LC

KEY_SCHEDULE_PARAMS = '_schedule'


# BaseCacheLoan, поскольку чтение/кодирование/декодирование не должно происходить в классе SchedulePaymentCache
# (loans\cache\base)
class SchedulePaymentCache:
    """
    Класс предназначен для кэширования графика платежей по договорам займа.
    Хранение: кэш хранится в ДокументРасширение.Параметры в виже HSTORE
    Особенности хранения: кэш по графикам платежей хранится только в парах с окончаниям KEY_SCHEDULE_PARAMS, остальные
    пары используются для других целей
    """
    def __init__(self, _filter=sbis.Record(), navigation=None):
        self._filter = _filter
        self.navigation = navigation
        self.type_rows = {
            LC.SCHEDULE_PLAN: LC.SCHEDULE_PLAN_NAME,
            LC.SCHEDULE_PAYMENT: LC.SCHEDULE_PAYMENT_NAME,
            LC.SCHEDULE_DELAY: LC.SCHEDULE_DELAY_NAME,
            LC.SCHEDULE_DATE: LC.SCHEDULE_DATE_NAME,
            LC.SCHEDULE_OUTCOME: LC.SCHEDULE_OUTCOME_NAME,
            LC.SCHEDULE_PAYMENTS: LC.SCHEDULE_PAYMENTS_NAME,
        }
        self.id_type_rows = dict((v, k) for k, v in self.type_rows.items())

        self.__format_cache = (
            ("@Документ", sbis.Record.AddObjectId),
            ("Дата", sbis.Record.AddDate),
            ("ТипЗаписи", sbis.Record.AddInt16),
            ("ОсновнойДолг", sbis.Record.AddMoney),
            ("ОсновнойДолгПлан", sbis.Record.AddMoney),
            ("НачисленныеПроценты", sbis.Record.AddMoney),
            ("НачисленныеПроцентыПлан", sbis.Record.AddMoney),
            ("РазмерПлатежа", sbis.Record.AddMoney),
            ("РазмерПлатежаПлан", sbis.Record.AddMoney),
            ("ОстатокДолга", sbis.Record.AddMoney),
            ("СведенияОПлатеже", sbis.Record.AddString),
            ("Описание", sbis.Record.AddString),
            ("ОписаниеДата", sbis.Record.AddString),
            ("ДатаКонца", sbis.Record.AddDate),
            ("ДатаНачалаПросрочки", sbis.Record.AddDate),
            ("ДатаОкончанияПросрочки", sbis.Record.AddDate),
        )
        self.format_cache = self.__create_format(self.__format_cache)

    def __create_format(self, fields):
        """Создает формат записи"""
        rec_format = sbis.Record()
        for field, func in fields:
            func(rec_format, field)
        return rec_format.Format()

    def load(self, id_loan):
        """
        Загружает график начислений из кэша
        :param id_loan: идентификатор договора
        :return: график платежей, RecordSet
        """
        schedule = self.__read_schedule_payment(id_loan)
        # откорректируем outcome
        for i in range(schedule.Size()):
            if schedule.Get(i, 'ТипЗаписи') == LC.SCHEDULE_OUTCOME:
                schedule.outcome = schedule[i]
                schedule.DelRow(schedule[i])
                break
        return schedule

    def save(self, id_loan, schedule):
        """
        Сохраняет график начислений в кэш
        :param id_loan: идентификатор договора
        :param schedule: график начислений, RecordSet
        :return: None
        """
        hstore_schedule = self.encode(schedule)
        self.mass_update_schedule_params({
            id_loan: hstore_schedule
        })

    def encode(self, schedule):
        """
        Преобразует график начислений из RecordSet в hstore
        :param schedule: график платежей, RecordSet
        :return: график платежей, hstore
        """
        return self.__encode(schedule)

    def mass_update_schedule_params(self, data):
        """
        Обновляет параметры документа, сохраняя доп. поля (если по регламенту используются)
        :param data: словарь данных в виде {id_doc [int]: schedule [hstore]}, dict
        :return: None
        Примечание: обновляется поле ДокументРасширение.Параметры, причем обрабатываем hashtable только с ключами
        в формате "%_schedule", остальные не трогаем. Учитывается ситуация, что количество месяцев в графике начислений
        может измениться.
        """
        data = [{'id_doc': id_doc, 'params': params} for id_doc, params in data.items()]
        BaseCacheLoan().mass_update(key_cache=KEY_SCHEDULE_PARAMS, values=data)

    def __encode(self, schedule):
        """
        Преобразует график начислений из RecordSet в hstore
        :param schedule: график платежей, RecordSet
        :return: график платежей, hstore
        """
        data = self.__encode_data(schedule)
        outcome_data = self.__encode_outcome(schedule.outcome)
        data.update(outcome_data)
        return sbis.CreateHstore(data)

    def __encode_data(self, schedule):
        """
        Кодирует строки графика начислений
        :param schedule:
        :return:
        """
        data = {}
        rs_format = schedule.Format()
        for i in range(len(schedule)):
            row = []
            for field, func in self.__format_cache:
                if field in rs_format:
                    value = schedule.Get(i, field)
                    if field == 'ТипЗаписи':
                        type_row = schedule[i]['@Документ'].RefObjectId().Name
                        value = str(self.id_type_rows.get(type_row))
                    if isinstance(value, decimal.Decimal):
                        value = '{0:.2f}'.format(value)
                    value = str(value) if value is not None else 'NULL'
                    row.append(value)
            key = '{}{}'.format(i, KEY_SCHEDULE_PARAMS)
            value = '{{{}}}'.format(', '.join(row))
            data[key] = value
        return data

    def __encode_outcome(self, outcome):
        """
        Кодирует строку итогов графика начислений
        :param outcome: Record
        :return: {'outcome_schedule': outcome_row}, dict
        """
        row = []
        for field, func in self.__format_cache:
            value = outcome.Get(field)
            if field == 'ТипЗаписи':
                value = str(LC.SCHEDULE_OUTCOME)
            if isinstance(value, decimal.Decimal):
                value = '{0:.2f}'.format(value)
            value = str(value) if value is not None else 'NULL'
            row.append(value)

        key = 'outcome{}'.format(KEY_SCHEDULE_PARAMS)
        value = '{{{}}}'.format(', '.join(row))
        return {key: value}

    def __read_schedule_payment(self, id_loan):
        """
        Чтение графика из поля ДокументРасширение.Параметры
        :param id_loan: идентификатор договора
        :return: график платежей, RecordSet
        """
        # позже завязаться на константы
        sql = '''
            WITH schedule AS (
                SELECT
                    CASE
                        WHEN (value::text[])[3]::int = 0 THEN (value::text[])[1]::int || ',ГрафикПлатежей'
                        WHEN (value::text[])[3]::int = 1 THEN (value::text[])[1]::int || ',Документ'
                        WHEN (value::text[])[3]::int = 2 THEN (value::text[])[1]::int || ',Просрочка'
                        WHEN (value::text[])[3]::int = 3 THEN (value::text[])[1]::int || ',ГодПлатежа'
                        WHEN (value::text[])[3]::int = 4 THEN (value::text[])[1]::int || ',СтрокаИтогов'
                        WHEN (value::text[])[3]::int = 5 THEN (value::text[])[1]::int || ',Документы'                    
                    END "@Документ",
                    (value::text[])[1]::int "Документ",
                    (value::text[])[2]::date "Дата",
                    (value::text[])[3]::int "ТипЗаписи",
                    ("value"::text[])[4]::numeric "ОсновнойДолг",
                    ("value"::text[])[5]::numeric "ОсновнойДолгПлан",
                    ("value"::text[])[6]::numeric "НачисленныеПроценты",
                    ("value"::text[])[7]::numeric "НачисленныеПроцентыПлан",
                    ("value"::text[])[8]::numeric "РазмерПлатежа",
                    ("value"::text[])[9]::numeric "РазмерПлатежаПлан",
                    ("value"::text[])[10]::numeric "ОстатокДолга",
                    ("value"::text[])[11]::text "СведенияОПлатеже",
                    ("value"::text[])[12]::text "Описание",
                    ("value"::text[])[13]::text "ОписаниеДата",
                    ("value"::text[])[14]::date "ДатаКонца",
                    ("value"::text[])[15]::date "ДатаНачалаПросрочки",
                    ("value"::text[])[16]::date "ДатаОкончанияПросрочки"
                FROM
                    EACH(
                        (
                            SELECT
                                ARRAY_AGG("Параметры"::hstore)
                            FROM
                                "ДокументРасширение"
                            WHERE
                                "@Документ" = $1::int
                        )[1]
                    )
                WHERE
                    "key" like '%_schedule'
            )
            SELECT
                *
            FROM
                schedule
        '''
        return sbis.SqlQuery(sql, id_loan)
