"""
Модуль для построения списка планируемых документов к начислению процентов.
"""


__author__ = 'Glukhenko A.V.'

import datetime

import sbis
from loans.loanConsts import LC
from loans.loanDBConsts import LCDB
from loans.loansRightsHelpers import AllowedOrgsChecker
from loans.percentsCommon import LoansDates
from loans.percentsToAccrued import LinkedRangesMerger
from loans.version_loans import get_date_build
from .helpers import cut_by_navigation
from .const import FORWARD, BACKWARD
from .const import LIST_PERCENT_PLAN


class PlanPercentsList:
    """Класс отвечает за формирование списка плановых документов начисления процентов"""
    def __init__(self, _filter, navigation, type_obj=None):
        self._filter = _filter
        self._lcdb = LCDB()
        self.navigation = navigation
        self.today = datetime.date.today()

        self.date_begin = self.__get_date_begin()
        self.date_end = self.__get_date_end()
        self.obj = type_obj or sbis.Session.ObjectName()

        self.id_type_doc_loan = self.__get_id_type_doc_loan()
        self._format = sbis.MethodResultFormat(self.obj + '.' + LC.LIST_TO_ACCRUED, 4)
        self._orgs_checker = AllowedOrgsChecker.create(_filter)
        self.is_first_page = self._filter.Get('is_first_page')
        self.is_script_init_cache = self._filter.Get('ScriptInitCache')

    def __get_date_begin(self):
        """Возвращает начало периода"""
        date_begin = self._filter.Get('ФильтрДатаС')
        date_period = self._filter.Get('ФильтрДатаПериод')
        if not date_begin and date_period:
            date_begin = LoansDates.getDateAgo(self.today, str(date_period))
        return date_begin

    def __get_date_end(self):
        """
        Возвращает окончание периода.
        Примечние: возвращается последний день месяца (текущего или от даты начала)
        """
        date_end = self._filter.Get('ФильтрДатаП')
        if not date_end:
            if self.date_begin:
                date_end = max(self.today, self.date_begin)
            else:
                date_end = self.today
        return LoansDates.add_months(date_end, count_months=2, return_last_day_of_month=True)

    def __get_id_type_doc_loan(self):
        """Возвращает идентификатор типа договора"""
        id_type_docs_by_obj = {
            LC.PERCENTS_ON_ISSUED_LOANS: self._lcdb.issued_id(),
            LC.PERCENTS_ON_RECEIVED_LOANS: self._lcdb.received_id(),
        }
        return id_type_docs_by_obj.get(self.obj)

    def __get_type_dc(self):
        """Возвращает тип записи ДебетКредит по входному объекту"""
        types_dc = {
            LC.PERCENTS_ON_ISSUED_LOANS: 1,
            LC.PERCENTS_ON_RECEIVED_LOANS: 2,
        }
        return types_dc.get(self.obj)

    def __build_loans_list_by_orgs(self):
        """
        Построить список договоров, по которым могут быть начислены проценты
        На выходе получим упорядоченный по организациям набор договоров, по которым можно начислить проценты.
        Для каждого договора указан период начисления [ДатаС, ДатаПо]
        Периоды отсортированы по нижней границе
        """
        # Раскоментить, когда нужно будет переходить на построение по кешу
        # return self.___build_cache_loans_list_by_orgs

        sql = PercentsToAccruedSqlMaker(self._orgs_checker, self.is_script_init_cache).create_sql()
        return sbis.SqlQuery(
            sql,
            self._lcdb.debt_analytic(),
            self.id_type_doc_loan,
            self.__get_type_dc(),
            self._lcdb.percents_id(),
            self.id_type_doc_loan == self._lcdb.issued_id(),
            self._lcdb.percent_analytic(),
            self.date_end,
            self._lcdb.accounts_ids(),
            [LC.LINK_TYPE, LC.NORMAL_LINK_TYPE],
        )

    def ___build_cache_loans_list_by_orgs(self):
        """
        Построить список договоров, по которым могут быть начислены проценты
        На выходе получим упорядоченный по организациям набор договоров, по которым можно начислить проценты.
        Для каждого договора указан период начисления [ДатаС, ДатаПо]
        Периоды отсортированы по нижней границе
        """
        sql = PercentsToAccruedSqlMaker(self._orgs_checker).create_cache_sql()
        return sbis.SqlQuery(
            sql,
            self.id_type_doc_loan,
            self.date_end,
        )

    def __build_docs_list(self, loans):
        """
        Формирует список документов к начислению по диапазонам
        :param loans: список займов
        """
        merged_ranges = LinkedRangesMerger(loans).mergeAll(self.date_begin)
        for merged_range in merged_ranges:
            last_days_of_month = LoansDates.getMonthEndsForPeriod(merged_range.get('ДатаС'), merged_range.get('ДатаПо'))
            for date in last_days_of_month:
                if date <= self.date_end:
                    percent = sbis.Record({
                        '@Документ': None,
                        'Дата': date,
                        'ТипДокумента': self._lcdb.percents_id(),
                        'ДокументНашаОрганизация': merged_range.get('@Лицо'),
                        'ДокументНашаОрганизация.Контрагент.Название': merged_range.get('Название'),
                        'РП.Лицо1.СписокНазваний': self.__get_name_contractors(merged_range, date),
                        'ТипЗаписи': LIST_PERCENT_PLAN,
                    })
                    self.result.AddRow(percent)

        self.__sort_result()
        self.__calc_id_doc()
        self.__filter_by_navigation()
        self.__filter_by_period()
        self.__calc_navigation()

    def __calc_id_doc(self):
        """Рассчитывает идентификаторы записей"""
        for i, rec in enumerate(self.result, start=1):
            id_row = sbis.ObjectId('ПроцентыКНачислению', -i)
            rec['@Документ'].From(id_row)

    def __filter_by_navigation(self):
        """Фильтрует результат согласно навигации"""
        for i in reversed(range(self.result.Size())):
            id_doc = self.result.Get(i, '@Документ')
            date = self.result.Get(i, 'Дата')
            if not self.__is_correct_by_navigation(id_doc, date):
                self.result.DelRow(i)

    def __is_correct_by_navigation(self, id_doc, date):
        """
        Проверяет корректность документа по дате согласно навигации
        :param id_doc: идентификатор планового документа
        :param date: дата документа
        """
        is_correct = False
        direction = self.navigation.Direction()
        id_doc_position, date_doc_position = self.__get_data_cursor()
        if all((id_doc_position is not None, date_doc_position)):
            if self.is_first_page:
                if direction == FORWARD:
                    is_correct = date < date_doc_position
                if direction == BACKWARD:
                    is_correct = date >= date_doc_position
            else:
                if direction == FORWARD:
                    is_correct = (id_doc < id_doc_position and date == date_doc_position) or date < date_doc_position
                if direction == BACKWARD:
                    is_correct = (id_doc > id_doc_position and date == date_doc_position) or date > date_doc_position
        else:
            if direction == FORWARD:
                is_correct = date <= self.today
            if direction == BACKWARD:
                is_correct = date > self.today
        return is_correct

    def __get_data_cursor(self):
        """Возвращает данные курсора"""
        position = self.navigation.Position()
        if position:
            id_doc = position.Get('id_plan_doc')
            if id_doc is not None:
                id_doc = int(id_doc)
            date_doc = position.Get('date_plan_doc')
            if date_doc is not None:
                date_doc = datetime.datetime.strptime(date_doc, '%Y-%m-%d').date()
        else:
            id_doc = None
            date_doc = None

        return id_doc, date_doc

    def __filter_by_period(self):
        """Фильтрует результат согласно заданному периоду"""
        date_begin = self._filter.Get('ФильтрДатаС') or datetime.date.min
        date_end = self._filter.Get('ФильтрДатаП') or datetime.date.max
        for i in reversed(range(self.result.Size())):
            if not (date_begin <= self.result.Get(i, 'Дата') <= date_end):
                self.result.DelRow(i)

    def __calc_navigation(self):
        """Рассчитывает навигацию"""
        more_exist = self.result.Size() > self.navigation.Limit()
        self.result.nav_result = sbis.NavigationResult(more_exist)
        if more_exist:
            cut_by_navigation(self.result, self.navigation)

    def __get_name_contractors(self, merged_range, date):
        """Возвращает список названий контрагентов"""
        contractors_by_date = merged_range.get('Лицо1.СписокНазваний')
        contractors = contractors_by_date.get(date)
        if contractors:
            contractors = '; '.join(sorted(contractors))
        return contractors

    def __sort_result(self):
        """Сортировка результата"""
        self.result.sort(key=lambda rec: (
            -1 * int(rec.Get('Дата').strftime("%Y%m%d")),
            rec.Get('ДокументНашаОрганизация.Название')
        ))

    def get_documents(self, only_contractors=False):
        """Возвращает список документов"""
        self.result = sbis.CreateRecordSet(self._format)
        if not self._orgs_checker.is_target_org_blocked():
            loans = self.__build_loans_list_by_orgs()
            if only_contractors:
                return loans
            self.__build_docs_list(loans)
            self.__sort_result()
        return self.result


class PercentsToAccruedSqlMaker:
    """Класс отвечает за построение SQL запроса"""
    def __init__(self, orgs_checker, is_script_init_cache=False):
        self._orgs_checker = orgs_checker
        self.is_script_init_cache = is_script_init_cache

    def __get_filter_by_orgs(self, cte_prefix=None, org_field='НашаОрганизация'):
        """Возвращает фильтр по организациям"""
        _filter = ''
        allowed_orgs = self._orgs_checker.get_allowed_orgs()
        blocked_orgs = self._orgs_checker.get_blocked_orgs()

        org_field = '"{}"'.format(org_field)
        if cte_prefix:
            org_field = '{}.{}'.format(cte_prefix, org_field)

        if allowed_orgs is not None:
            _filter = '''AND {} = any(array{}::integer[])'''.format(org_field, allowed_orgs)
        elif blocked_orgs:
            _filter = '''AND {} != all(array{}::integer[])'''.format(org_field, blocked_orgs)
        return _filter

    def __get_addition_fields(self):
        """
        Возвращает список дополнительных полей, необходимых для работы скрипта (инициализация кеша для ускорения
        плановых процентов).
        Примечание:
        Для работы скрипта добавляли дополнительные поля: id_contract, debt, date_last_payment. Но это повлияло на
        работу метода объединения периодов LinkedRangesMerger(loans).mergeAll. В следствие чего при обычном построении
        реестра, начали появляться дубли плановых документов.
        https://online.sbis.ru/opendoc.html?guid=b39c4f74-ba5f-443d-b387-9730d331a560
        Быстро ошибку не нашел как починить, ибо логика LinkedRangesMerger().mergeAll не тривиальна.
        Примечание2: для работоспособности скрипта впринципе не надо чинить метод mergeAll, поскольку до объединения
        периодов дело не доходит.
        """
        if self.is_script_init_cache:
            addition_fields = '''
                doc."@Документ" "id_contract",
                plan_percents."ОсновнойДолг" "debt",
                plan_percents."ДатаПогашения" "date_last_payment",
            '''
        else:
            addition_fields = ''
        return addition_fields

    def create_sql(self):
        """Формирует запрос в базу данных"""
        base_filter_by_org = self.__get_filter_by_orgs(cte_prefix='dc')
        dc_filter_by_org = self.__get_filter_by_orgs(cte_prefix='dc')
        filter_by_org = self.__get_filter_by_orgs(org_field='ДокументНашаОрганизация')
        result_filter_by_org = self.__get_filter_by_orgs(cte_prefix='doc', org_field='ДокументНашаОрганизация')

        return f'''
            WITH raw_data AS (
                SELECT
                    dc."Тип",
                    dc."Сумма",
                    dc."Дата",
                    dc."Лицо2",
                    dc."Лицо3",
                    $5::bool AS "Выданный"
                FROM
                    "ДебетКредит" dc
                LEFT JOIN
                    "Документ" doc
                    ON doc."Лицо" = dc."Лицо2"
                LEFT JOIN
                    "РазличныеДокументы" diff_doc
                    ON doc."@Документ" = diff_doc."@Документ"
                WHERE
                    dc."Лицо2" IS NOT NULL AND
                    dc."Лицо3" = $1::integer AND -- Основной долг
                    dc."Счет" = ANY($8::integer[]) AND -- только по счетам займов
                    dc."Тип" = ANY(ARRAY[1, 2]) AND
                    -- не уверен что требуется этот отграничитель (генерирует последний день месяца '2020-11-30)
                    dc."Дата" < $7::date AND -- не интересуют проводки за последний день и новее
                    doc."ТипДокумента" = $2::integer AND
                    doc."$Черновик" IS NULL AND
                    (diff_doc."Коэффициент" IS NOT NULL AND diff_doc."Коэффициент" IS DISTINCT FROM 0)
                    {base_filter_by_org}
            )
            , percent_dates AS (
                SELECT
                    "Лицо2",
                    MAX("Дата") AS "ДатаПроцентов"
                FROM (
                    (
                    -- Из проводок выбираем проценты - это проведенные начисления процентов, бух. справки и фиксации остатков
                    SELECT
                        dc."Лицо2",
                        MAX(
                            CASE
                                WHEN
                                    (dc."Тип" = $3::int ) OR -- 1 or 2
                                    (dc."Тип" = 5) -- фиксация остатков
                                THEN
                                    dc."Дата"
                                ELSE
                                    NULL::date
                            END
                        ) AS "Дата"
                    FROM
                        "ДебетКредит" dc
                    LEFT JOIN
                        "Документ" doc
                        ON doc."Лицо" = dc."Лицо2"
                        -- без этого в плановом реестре будут учитываться общие начисления процентов (старые)
                        -- ON doc."@Документ" = dc."Документ"
                    WHERE
                        dc."Лицо2" IS NOT NULL AND -- договор должен быть заполнен
                        dc."Лицо3" = $6::integer AND -- проценты
                        dc."Тип" = ANY(ARRAY[1, 2, 5]) AND
                        dc."Счет" = ANY($8::integer[]) AND -- только по счетам займов
                        -- doc."ТипДокумента" = $2::integer AND -- только займы
                        doc."ТипДокумента" = $2::integer AND -- только проценты по ~~~~входящим договорам
                        doc."$Черновик" IS NULL
                        {dc_filter_by_org}
                    GROUP BY
                        dc."Лицо2"
                    )
                    UNION ALL
                    (
                        -- Проценты по займам. Их учитываем всегда, так как некоторые начисления процентов могут быть не проведены
                        WITH percents AS (
                            SELECT
                                "@Документ"
                            FROM
                                "Документ" 
                            WHERE
                                "ТипДокумента" = $4::integer AND
                                "$Черновик" IS NULL AND
                                "Удален" IS NOT TRUE
                                {filter_by_org}
                        )
                        , loans AS (
                            SELECT
                                MAX(link_docs."Дата") AS "Дата",
                                link_docs."ДокументОснование" AS "@Документ"
                            FROM
                                "СвязьДокументов" link_docs
                            WHERE
                                link_docs."ВидСвязи" = ANY($9::integer[]) AND
                                link_docs."ДокументСледствие" = ANY(SELECT "@Документ" FROM percents)
                            GROUP BY
                                link_docs."ДокументОснование"
                        )
                        SELECT
                            doc."Лицо" AS "Лицо2",
                            loans."Дата"
                        FROM
                            "Документ" doc
                        JOIN
                            loans
                            ON loans."@Документ" = doc."@Документ"
                        WHERE
                            doc."@Документ" IN (SELECT "@Документ" FROM loans)
                    )
                ) _percent_dates
                GROUP BY
                    "Лицо2"
            )
            , plan_percents AS (
                SELECT
                    "Лицо2",
                    MIN(
                        CASE
                            WHEN "Лицо3" = $1::integer AND (("Выданный" IS TRUE AND "Тип" = 1) OR ("Выданный" IS FALSE AND "Тип" = 2))
                            THEN "Дата"
                            ELSE NULL
                        END
                    ) AS "ДатаВыдачи",
                    MAX(
                        CASE
                            WHEN "Лицо3" = $1::integer AND (("Выданный" IS TRUE AND "Тип" = 2) OR ("Выданный" IS FALSE AND "Тип" = 1))
                            THEN "Дата"
                            ELSE NULL
                        END
                    ) AS "ДатаПогашения",
                    SUM(
                        CASE
                            WHEN "Лицо3" = $1::integer
                            THEN
                                CASE
                                    WHEN ("Выданный" IS TRUE AND "Тип" = 1) OR ("Выданный" IS FALSE AND "Тип" = 2)
                                    THEN "Сумма"
                                    ELSE -"Сумма"
                                END
                            ELSE
                                NULL
                        END
                    ) AS "ОсновнойДолг"
                FROM
                    raw_data
                GROUP BY
                    "Лицо2"
            )
            SELECT
                {self.__get_addition_fields()}
                doc."ДокументНашаОрганизация" AS "@Лицо",
                org."Название",
                GREATEST(plan_percents."ДатаВыдачи", percent_dates."ДатаПроцентов") + 1 AS "ДатаС", -- ни дата выдачи, ни дата предыдущего начисления не должны попасть в расчет, поэтому +1 день
                LEAST(
                    CASE
                        WHEN
                            plan_percents."ДатаПогашения" IS NOT NULL AND
                            (
                                plan_percents."ОсновнойДолг" IS NOT NULL AND
                                plan_percents."ОсновнойДолг" <= 0.0
                            )
                        THEN
                            plan_percents."ДатаПогашения"
                        ELSE
                            null::date
                    END,
                    $7::date
                ) AS "ДатаПо",
                contractor."Название" AS "Лицо1.Название"
            FROM
                plan_percents
            LEFT JOIN
                percent_dates
                ON plan_percents."Лицо2" = percent_dates."Лицо2"
            LEFT JOIN
                "Документ" doc
                ON plan_percents."Лицо2" = doc."Лицо"
            LEFT JOIN
                "Лицо" org
                ON doc."ДокументНашаОрганизация" = org."@Лицо"
            LEFT JOIN
                "Лицо" contractor
                ON doc."Лицо1" = contractor."@Лицо"
            WHERE
                plan_percents."ДатаВыдачи" IS NOT NULL
                {result_filter_by_org}
            ORDER BY
                1, 3 -- обязательно отсорт
        '''

    def create_cache_sql(self):
        """Формирует запрос в базу данных"""
        filter_by_org = self.__get_filter_by_orgs(cte_prefix='doc', org_field='ДокументНашаОрганизация')

        return f'''
            WITH raw_data AS (
                SELECT
                    doc."@Документ" id_contract,
                    doc."Лицо" face,
                    doc."Лицо1" id_contractor,
                    doc."ДокументНашаОрганизация" id_org,
                    (string_to_array(doc_ext."Параметры"::hstore->'plan_docs', ';', ''))[1]::date "date_begin",
                    (string_to_array(doc_ext."Параметры"::hstore->'plan_docs', ';', ''))[2]::date "date_last_payment",
                    (string_to_array(doc_ext."Параметры"::hstore->'plan_docs', ';', ''))[3]::numeric debt
                FROM
                    "Документ" doc
                LEFT JOIN
                    "ДокументРасширение" doc_ext
                ON doc."@Документ" = doc_ext."@Документ"
                WHERE
                    doc."ТипДокумента" = $1::int
                    AND doc_ext."Параметры"::hstore->'plan_docs' IS NOT NULL
                    {filter_by_org}
            )
            SELECT
                raw_data."id_contract",
                raw_data."face",
                raw_data."date_begin" "ДатаС",
                LEAST(
                    CASE
                        WHEN
                            raw_data."date_last_payment" IS NOT NULL AND
                            raw_data."debt" IS NOT NULL AND
                            raw_data."debt" <= 0.0
                        THEN
                            raw_data."date_last_payment"
                        ELSE
                            NULL::date
                    END,
                    $2::date
                ) AS "ДатаПо",
                org."Название",
                contractor."Название" AS "Лицо1.Название"
            FROM
                raw_data
            LEFT JOIN
                "Лицо" org
            ON raw_data."id_org" = org."@Лицо"
            LEFT JOIN
                "Лицо" contractor
            ON raw_data."face" = contractor."@Лицо"
        '''
