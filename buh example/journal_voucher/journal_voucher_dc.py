"""
Глухенко А.В.
Реализация ЖурналаОрдера по режиму ДебетКредит
"""

import sbis

from .base_sql import DC_BASE_SQL, DC_WITH_HIERARCHY_BASE_SQL, DC_WITH_ANALYTICS_HIERARCHY_SQL
from .journal_voucher_base import JournalVoucherBase
from ..jo_helpers import Columns, DEBIT, CREDIT, FLAG_ACCOUNTING, FLAG_TAXING, FLAG_CURRENCY, Helpers


class JournalVoucherDC(JournalVoucherBase):
    """Отчет журнал-ордер, стоящийся по таблице ДебетКредит"""

    def _get_report_from_dc(self):
        """Строит отчет по таблице ДебетКредит"""
        sql = DC_BASE_SQL.format(
            fields_group_by_doc=self.__get_fields_group_by_doc(),
            filter_by_internal_turnover=self._get_filter_by_inter_turn(),
            raw_fields=self.__get_raw_fields_dc(),
            join_corresponds=self.__get_join_corresponds(),
            dc_filter=self.where_filters_dc,
            group_fields_by_curr=self.__get_group_fields(),
            group_fields=self.__get_group_fields('gbc'),
            having_fields=self.__get_having_fields(),
            fields_for_sort=self.__get_fields_for_sort(),
            tables_for_sort=self.__get_tables_for_sort(),
            search_filter=self.__get_search_filter(),
            sort_fields=self.__get_sort_fields('gbdd'),
            result_fields=self.__get_result_fields(),
            addition_tables=self.__get_addition_tables(),
            sort_result=self.__get_sort_fields('pr'),
            filter_by_ido=self._get_filter_by_ido(params_offset=7),
        )
        # временный костыль, нельзя убрать не затронув другие режимы отчетов в ЖО
        is_node = None if self.is_node == 'NULL' else self.is_node
        result = sbis.SqlQuery(
            sql,
            self.parent_id,
            is_node,
            self.date_start,
            self.date_end,
            self.limit_on_page,
            self.offset_of_page,
            self.ido_filter,
        )

        if self.current_group == 'document':
            result.DelCol('Дата')
            result.RenameField('ДатаДокумента', 'Дата')
            Helpers.document_names(result)

        self.__set_outcome_dc(result)
        return result

    def __get_field_for_group(self, curr):
        """Возвращает поле группировки"""
        templates_by_curr = {
            False: 'MIN({group_field_name}) {group_field_name}',
            True: 'MIN({group_field}) {group_field_name}',
        }

        field = ''
        if self.current_group != 'periodicity':
            field = templates_by_curr.get(curr).format(
                group_field=self.group_col_dc,
                group_field_name=self.group_col_dc.split('.')[-1],
            )
        return field

    def __get_group_correspond_field(self):
        """Вычисляет поле корреспонденции, учитывая возможные группировки"""
        field = ''
        if any((self.is_detail_turnovers, self.is_corr_request)):
            field = ''' string_agg(corr_arr, ',') corr_arr '''
        return field

    def __get_join_corresponds(self):
        """При наличии корреспонденции достает нужные поля"""
        join_condition = ''
        if self.is_corr_request:
            join_condition = '''
                JOIN
                (
                    SELECT
                        DISTINCT ON ("Проводка", "Счет", "Тип") "Проводка",
                        "Счет",
                        "Тип"
                    FROM
                        "ДебетКредит" dk2
                    WHERE
                        {dc_corresponds_cond}
                    ORDER BY
                        "Проводка",
                        "Счет"
                ) dk2 
                ON
                    dk."Проводка" = dk2."Проводка" AND
                    dk."Тип" <> dk2."Тип"
            '''.format(
                dc_corresponds_cond=' AND '.join(self.dc_corresponds_cond),
            )
        elif self.is_detail_turnovers:
            join_condition = '''
                LEFT JOIN LATERAL (
                    SELECT
                        DISTINCT "Счет"
                    FROM
                        "ДебетКредит" dk2
                    WHERE
                        dk."Проводка" = dk2."Проводка" AND
                        (
                            dk."Тип" <> dk2."Тип" AND
                            {dc_corresponds_cond}
                        ) IS TRUE
                ) dk2 ON TRUE
            '''.format(
                dc_corresponds_cond=' AND '.join(self.dc_corresponds_cond),
            )
        return join_condition

    def __get_id_fields(self, table=None):
        """Формирует идентификаторы записей"""
        ids = {
            'document': '''
                COALESCE($1::TEXT || ',', '') || 
                COALESCE({group_col_dc}, -11) ||
                '|' || 
                LPAD(
                    EXTRACT(DOY FROM dk."Дата")::TEXT || 
                    EXTRACT(YEAR FROM dk."Дата")::TEXT,
                    7,
                    '0'
                ) ||
                '@document' id 
            ''',
            'periodicity': '''
                COALESCE($1::text || ',', '') ||
                COALESCE({group_col_dc}, '-11')::text ||
                '@{current_group}' id
            ''',
            'default': '''
                COALESCE($1::text || ',', '') ||
                COALESCE({group_col_dc}, -11)::text ||
                '@{current_group}' id
            ''',
        }
        real_ids = {
            'document': '''
                {group_col_dc} || ',Документ' real_id
            ''',
            'default': '''
                {group_col_dc} real_id
            ''',
        }
        _id = (ids.get(self.current_group) or ids.get('default')).format(
            group_col_dc=self.group_col_dc,
            current_group=self.current_group,
        )
        real_id = (real_ids.get(self.current_group) or real_ids.get('default')).format(
            group_col_dc=self.group_col_dc,
        )
        result = ',\n'.join([_id, real_id])
        return result.replace('dk', table) if table else result

    def __get_having_fields(self):
        """Формирует having часть (убираем нулевые записи)"""

        template = '''
            NULLIF(SUM(CASE WHEN dk."Тип" = {dc_value} AND {curr_col} THEN dk."Сумма{postfix}" END), 0)
        '''
        cols = {
            DEBIT: self.cols[Columns.TURNOVER_DEBIT],
            CREDIT: self.cols[Columns.TURNOVER_CREDIT],
        }

        having_fields = []
        args = [(flag, dc) for flag in self.on_flags for dc in (DEBIT, CREDIT)]
        for flag, dc in args:
            dc_col = 'NULL'
            if cols.get(dc):
                curr_col = 'dk."Валюта" IS NOT NULL' if flag == FLAG_CURRENCY else 'dk."Валюта" IS NULL'
                dc_col = template.format(
                    dc_value=dc,
                    curr_col=curr_col,
                    postfix=self.postfixes.get(flag)
                )
            having_fields.append(dc_col)

        return ', \n'.join(having_fields)

    def __get_fields_for_sort(self):
        """Возвращает поля, необходимые для сортировки"""
        if self.current_group not in ('account', 'document', 'periodicity') and self.sort_by_name:
            result = '''
                , TO_DATE(
                    SUBSTRING(
                        participant."Название" FROM ' от ([0-9]{2}.[0-9]{2}.[0-9]{2})'
                    ),
                    'dd.mm.yy'
                ) for_order_1
                , SUBSTRING(participant."Название" FROM '№(.*) от ([0-9]{2}.[0-9]{2}.[0-9]{2})') for_order_2
                , participant."Название" for_order_3
            '''
        else:
            result = ''
        return result

    def __get_turnover_fields(self):
        """Расчет полей по оборотам"""

        turnover_template = '''
            SUM("{dc_field}{postfix}") FILTER(WHERE gbc."Валюта" IS NULL) {field_name}
        '''
        curr_turnover_template = '''
            jsonb_object_agg(gbc."Валюта", gbc."{dc_field}{postfix}")
            FILTER(WHERE gbc."Валюта" IS NOT NULL AND COALESCE(gbc."{dc_field}{postfix}", 0) <> 0 ) {field_name}
        '''

        total_turnover_template = '''
            SUM(SUM(CASE WHEN gbc."Валюта" IS NULL THEN gbc."{dc_field}{postfix}" END))
            OVER() {field_name}
        '''
        total_curr_turnover_template = '''
        (
            SELECT
                jsonb_object_agg("Валюта", "{dc_field}{postfix}") FILTER (WHERE "{dc_field}{postfix}" <> 0)
            FROM (
                SELECT
                    gbc."Валюта",
                    SUM(COALESCE(gbc."{dc_field}{postfix}", 0)) "{dc_field}{postfix}"
                FROM
                    group_by_curr gbc
                WHERE
                    gbc."Валюта" IS NOT NULL
                GROUP BY
                    gbc."Валюта"
            ) z
        ) {field_name}
        '''

        dc_fields = {
            DEBIT: 'Дебит',
            CREDIT: 'Кредит',
        }

        fields = []
        turnover_args = [(flag, dc) for flag in self.on_flags for dc in (DEBIT, CREDIT)]
        for flag, dc in turnover_args:
            # расчет поля по оборотам
            field_name = 'v{dc_part}{postfix_ab}'.format(
                dc_part=self.dc_parts.get(dc),
                postfix_ab=self.postfixes_ab.get(flag),
            )
            template = curr_turnover_template if flag == FLAG_CURRENCY else turnover_template
            fields.append(template.format(
                postfix=self.postfixes.get(flag),
                dc_field=dc_fields.get(dc),
                field_name=field_name,
            ))
            # расчет суммарного поля по оборотам
            template = total_curr_turnover_template if flag == FLAG_CURRENCY else total_turnover_template
            fields.append(template.format(
                postfix=self.postfixes.get(flag),
                dc_field=dc_fields.get(dc),
                field_name=field_name + '_s',
            ))
        return ',\n'.join(fields)

    def __get_tables_for_sort(self):
        """Возвраащет таблицы, необходимые для сортировки"""
        # Уверен что логика значительно проще, но так было реализовано
        join_for_order = ''
        if self.current_group == 'document':
            if self.search:
                join_for_order = 'LEFT JOIN "Документ" doc ON (doc."@Документ" = gbdd."Документ")'
        elif self.current_group == 'account':
            join_for_order = 'LEFT JOIN "ПланСчетов" ps ON (ps."@ПланСчетов" = gbdd.real_id)'
        elif self.current_group == 'periodicity':
            pass
        elif any((self.sort_by_name, self.search)):
            join_for_order = 'LEFT JOIN "Лицо" participant ON (participant."@Лицо" = gbdd.real_id)'
        # add by search
        if self.search and self.current_group not in ('account', 'document', 'periodicity'):
            join_for_order += '''
                \nLEFT JOIN (SELECT * FROM "Контрагент" WHERE "ИНН" = '{0}') ca ON ca."@Лицо" = gbdd.real_id
            '''.format(self.search)
        return join_for_order

    def __get_search_filter(self):
        """Фильтрует набор по строке поиска"""
        search_filters = {
            'document': '''
                doc."Номер" ILIKE '{0}%'
            ''',
            'periodicity': '',
            'organization': '',
            'account': '''
                ps."Номер" ILIKE '{0}%' OR
                ps."Название" ILIKE '{0}%'
            ''',
            'default': '''
                participant."Название" ILIKE '%{0}%' OR
                ca."ИНН" = '{0}'
            ''',
        }

        _filter = ''
        if self.search:
            key = self.current_group if self.current_group in search_filters else 'default'
            _filter = search_filters.get(key).format(
                self.search,
            )
        return _filter or 'TRUE'

    def __get_sort_by_flags(self, direction):
        """Формирует поля сортировки"""
        # Warning!: не понятно почему только по оборотам идет сортировка, ну оставим как было
        _fields = []
        turnover_args = [(f, dc) for f in self.on_flags for dc in (DEBIT, CREDIT)]

        for flag, dc in turnover_args:
            field = 'v{dc_part}{postfix_ab}'.format(
                dc_part=self.dc_parts.get(dc),
                postfix_ab=self.postfixes_ab.get(flag),
            )
            if flag == FLAG_CURRENCY:
                field = self._get_curr_order(field)
            _fields.append(self._get_order(field, direction))
        return ','.join(_fields)

    def __get_sort_fields(self, table):
        """Порядок выдачи результата"""
        sort_fields = {
            'document': ', '.join([
                '{}.{}'.format(table, self._get_order('"Дата"', 'DESC')),
                '{}.{}'.format(table, self._get_order('"Документ"', 'ASC')),
            ]),
            'periodicity': self._get_order('real_id', 'DESC'),
            'account': self._get_order('ps."Признаки"[3]', 'ASC') + ', ' + self._get_order('ps."Номер"', 'ASC'),
            'sort_by_name_default': ', '.join([
                self._get_order("real_id IS NULL", 'ASC'),
                self._get_order('for_order_1', 'DESC'),
                self._get_order('for_order_2', 'DESC'),
                self._get_order('for_order_3', 'ASC', 'NULLS FIRST'),
            ]),
            'default': ', '.join([
                self._get_order("real_id IS NULL", 'ASC'),
                self.__get_sort_by_flags('ASC')
            ]),
        }
        if self.sort_by_name or self.current_group == 'periodicity':
            fields = sort_fields.get(self.current_group)
            if not fields:
                fields = sort_fields.get('sort_by_name_default')
        else:
            fields = sort_fields.get('default')
        return fields

    def __get_result_fields(self):
        """Результирующие поля в наборе"""
        result_fields = {
            'document': '''
                "Документ" "@Документ",
                d."Дата" "ДатаДокумента",
                '№' || d."Номер" "ДокументНомер",
                d."Регламент",
                '' "РегламентНазвание",
                td."НазваниеКраткое" "ТипДокументаНазвание",
                participant."Название" "КонтрагентНазвание",
                '' "Название",
                to_char(pr."Дата", 'DD.MM.YY') "ДатаПроводки",
                "Документ" IS NULL special
            ''',
            'periodicity': '''
                {} "Название"
            '''.format(
                Helpers.date_col_name(self.periodicity_type, 'real_id')
            ),
            'account': '''
                ps."Номер",
                ps."Название",
                real_id IS NULL special
            ''',
            'default': '''
                CASE
                    WHEN real_id IS NOT NULL
                    THEN COALESCE(participant."Название", 'Без названия')
                    ELSE 'Без аналитики'
                END "Название",
                participant."Лицо_" regclass,
                COALESCE(ca."ИНН", pp."ИНН") || COALESCE(' ' || ca."КПП", '') AS "code",
                ca."АдресЮридический" AS "address",
                participant."Название" IS NULL special
            ''',
        }

        fields = result_fields.get(self.current_group)
        if not fields:
            fields = result_fields.get('default')
        return fields

    def __get_addition_tables(self):
        """Добавляет необходимые таблицы для результата"""
        tables_by_group = {
            'document': '''
                LEFT JOIN
                    "Документ" d
                        ON d."@Документ" = pr."Документ"
                LEFT JOIN
                    "ТипДокумента" td
                        ON td."@ТипДокумента" = d."ТипДокумента"
                LEFT JOIN
                    "Лицо" participant
                        ON participant."@Лицо" = d."Лицо1"
            ''',
            'account': '''
                LEFT JOIN
                    "ПланСчетов" ps
                        ON ps."@ПланСчетов" = real_id
            ''',
            'default': '''
                LEFT JOIN
                    "Лицо" participant
                        ON participant."@Лицо" = real_id
                LEFT JOIN
                    "Контрагент" ca
                        ON ca."@Лицо" = real_id
                LEFT JOIN
                    "ЧастноеЛицо" pp
                        ON pp."@Лицо" = real_id
            ''',
        }

        tables = tables_by_group.get(self.current_group) or tables_by_group.get('default')
        if self.current_group == 'periodicity':
            tables = ''
        return tables

    def __get_group_fields(self, table=None):
        """Поля для группировки"""
        if self.current_group == 'document':
            fields = '{}, dk."Дата"'.format(self.group_col_dc)
        else:
            fields = self.group_col_dc
        # к сожалению нельзя пока заменить префикс таблицы для self.group_col_dc
        return fields.replace('dk', table) if table else fields

    def _get_report_from_dc_acc_hier(self):
        """Строим отчет по таблице ДебетКредит с иерархией по счетам"""
        search_filter = ''
        if self.search:
            search_filter = '''
                AND (
                    "Номер" ILIKE '{0}%' OR
                    "Название" ILIKE '{0}%'
                )
            '''.format(self.search)

        last_account = self._get_last_parent_id()

        if last_account:
            filter_by_parent_acc = 'parent = ' + str(last_account)
        else:
            filter_by_parent_acc = "real_id = ANY(ARRAY[{}]::INT[])".format(
                ', '.join(map(str, self.id_accounts_without_doubles)))

        correspond_array_field, group_correspond_array_field = '', ''
        if any((self.is_detail_turnovers, self.is_corr_request)):
            correspond_array_field = 'corr_arr,'
            group_correspond_array_field = ''' string_agg(corr_arr, ',') corr_arr, '''

        sql = DC_WITH_HIERARCHY_BASE_SQL.format(
            # raw_data
            raw_fields=self.__get_raw_fields_dc_hierarchy(),
            corresponds_tables=self.__get_join_corresponds(),
            where_cond=self.where_filters_dc,
            filter_by_internal_turnover=self._get_filter_by_inter_turn(),
            # data_with_parents
            amount_fields=self.__get_amount_fields(),
            correspond_array_field=correspond_array_field,
            # total_curr_cte
            total_curr_cte=self.__get_total_curr_cte(),
            # result cte
            result_amount_fields=self.__get_result_amount_fields(),
            result_curr_amount_fields=self.__get_result_curr_amount_fields(external_table_alias='dwp_external'),
            result_curr_total_fields=self.__get_result_curr_total_fields(),
            is_node=self.is_node,
            group_correspond_array_field=group_correspond_array_field,
            filter_by_parent_acc=filter_by_parent_acc,
            search_filter=search_filter,
            having_fields=self.__get_having_fields_hier(),
            filter_by_ido=self._get_filter_by_ido(params_offset=2),
            sort_result=self.__get_sort_fields('')

        )
        result = sbis.SqlQuery(
            sql,
            self.parent_id,
            self.ido_filter,
            self.limit_on_page,
            self.offset_of_page,
        )

        self.__set_outcome_dc(result)
        return result

    def _get_report_from_dc_analytics_hier(self):
        """Строим отчет по таблице ДебетКредит с иерархией по аналитикам"""
        params = [self.parent_id, self.limit_on_page, self.offset_of_page]

        params.append(self.ido_filter)
        ido_filter = self._get_filter_by_ido(params_offset=len(params))

        last_parent_id = self._get_last_parent_id(self.current_group)

        if last_parent_id:
            filter_by_parent = 'parent = ' + str(last_parent_id)
        else:
            ids = self.analytics.get_ids_by_group(self.current_group)
            if ids:
                filter_by_parent = "real_id = ANY(ARRAY[{}]::INT[])".format(', '.join(map(str, ids)))
            else:
                filter_by_parent = 'parent is null'

        correspond_array_field, group_correspond_array_field = '', ''
        if any((self.is_detail_turnovers, self.is_corr_request)):
            correspond_array_field = 'corr_arr,'
            group_correspond_array_field = ''' string_agg(corr_arr, ',') corr_arr, '''

        sql = DC_WITH_ANALYTICS_HIERARCHY_SQL.format(
            # raw_data
            raw_fields=self.__get_raw_fields_dc_hierarchy(is_analytics_hierarchy=True),
            analytics_num=self.current_group[-1],
            cte_hierarchy=self.analytics.get_cte_hierarchy(),
            corresponds_tables=self.__get_join_corresponds(),
            where_cond=self.where_filters_dc,
            filter_by_internal_turnover=self._get_filter_by_inter_turn(),
            # data_with_parents
            amount_fields=self.__get_amount_fields(),
            correspond_array_field=correspond_array_field,
            # total_curr_cte
            total_curr_cte=self.__get_total_curr_cte(),
            # result cte
            result_amount_fields=self.__get_result_amount_fields(),
            result_curr_amount_fields=self.__get_result_curr_amount_fields(external_table_alias='dwp_external'),
            result_curr_total_fields=self.__get_result_curr_total_fields(),
            is_node=self.is_node,
            group_correspond_array_field=group_correspond_array_field,
            filter_by_parent=filter_by_parent,
            having_fields=self.__get_having_fields_hier(),
            filter_by_ido=ido_filter,
        )
        result = sbis.SqlQuery(
            sql,
            *params,
        )

        self.__set_outcome_dc(result)
        return result

    def __get_having_fields_hier(self):
        """Формирует having часть (убирает нулевые записи)"""
        template = 'NULLIF(SUM(COALESCE("v{dc_part}{postfix_ab}", 0)) FILTER(WHERE {curr_filter}), 0)'
        fields = []

        cols = {
            DEBIT: self.cols[Columns.TURNOVER_DEBIT],
            CREDIT: self.cols[Columns.TURNOVER_CREDIT],
        }

        args = [(flag, dc) for flag in self.on_flags for dc in (DEBIT, CREDIT)]
        for flag, dc in args:
            if cols.get(dc):
                if flag == FLAG_CURRENCY:
                    curr_filter = '"Валюта" IS NOT NULL'
                    postfix_ab = ''
                else:
                    curr_filter = '"Валюта" IS NULL'
                    postfix_ab = self.postfixes_ab.get(flag)

                field = template.format(
                    dc_part=self.dc_parts.get(dc),
                    postfix_ab=postfix_ab,
                    curr_filter=curr_filter,
                )
                fields.append(field)

        return ',\n'.join(fields) or 'NULL'

    def __add_turnover_to_outcome(self, result, outcome):
        """Добавляет поля оборотов в строку итогов"""
        outcome_format = outcome.Format()

        for flag in self.on_flags:
            for dc in (DEBIT, CREDIT):
                dc_part = self.dc_parts.get(dc)
                postfix_ab = self.postfixes_ab.get(flag)
                if None not in (dc_part, postfix_ab):
                    field = 'v{dc_part}{postfix_ab}'.format(
                        dc_part=dc_part,
                        postfix_ab=postfix_ab,
                    )
                    if field not in outcome_format:
                        if flag == FLAG_CURRENCY:
                            outcome.AddHashTable(field)
                        else:
                            outcome.AddMoney(field)
                    field_total = field + '_s'
                    if result:
                        outcome[field].From(result[0].Get(field_total))

    def __add_balance_to_outcome(self, result, outcome):
        """Добавляет поля баланса в строку итогов"""
        result_format = result.Format()
        outcome_format = outcome.Format()

        saldo = self._get_saldo()
        for flag in self.on_flags:
            for balance in self.on_ab_cols:
                balance_postfix = self.balance_postfixes.get(self.map_columns_to_types.get(balance))
                dc_part = self.dc_parts.get(self.map_columns_to_dc.get(balance))
                postfix_ab = self.postfixes_ab.get(flag)
                if None not in (balance_postfix, dc_part, postfix_ab):
                    field = 'b{dc_part}{balance_postfix}{postfix_ab}'.format(
                        dc_part=dc_part,
                        balance_postfix=balance_postfix,
                        postfix_ab=postfix_ab,
                    )
                    if field not in result_format:
                        if flag == FLAG_CURRENCY:
                            result.AddColHashTable(field)
                        else:
                            result.AddColMoney(field)

                    if field not in outcome_format:
                        if flag == FLAG_CURRENCY:
                            outcome.AddHashTable(field, saldo.Get(field))
                        else:
                            outcome.AddMoney(field, saldo.Get(field) or 0)
                    outcome[field] = saldo.Get(field)

    def __set_outcome_dc(self, result):
        """
        Рассчитывает outcome
        Примечание: fields - набор полей для отчета _get_report_from_ab_by_period
        """
        outcome = sbis.Record()
        self.__add_turnover_to_outcome(result, outcome)
        self.__add_balance_to_outcome(result, outcome)
        self.__remove_total_fields(result)
        result.outcome = outcome

        if result:
            field_names = result[0].GetFieldNames()
            # Надо как то убрать валютные суммы с отчета
            for field_name in field_names:
                if field_name.endswith("cs"):
                    result.DelCol(field_name)

            if 'НашаОрганизация' not in field_names:
                result.AddColInt32('НашаОрганизация')

    def __remove_total_fields(self, result):
        """Чистим набор от суммарных колонок, данные которых ушли в outcome"""
        total_fields = [f.Name() for f in result.Format() if '_s' in f.Name()]
        for total_field in total_fields:
            result.DelCol(total_field)

    def __get_total_curr_cte(self):
        """Формирует рассчет валютных полей "отчета" """

        template = '''
        , total_curr AS (
            -- Общие итоги считаем без конечных родителей, потому что у них не может быть проводок, там будет 
            -- содержаться только суммарная сумма по детям, поэтому берем данные только из raw_data
            SELECT 
                jsonb_object_agg("Валюта", "vdb") FILTER (WHERE "vdb" <> 0) "vdb{postfix_ab}_s", 
                jsonb_object_agg("Валюта", "vcr") FILTER (WHERE "vcr" <> 0) "vcr{postfix_ab}_s" 
            FROM (
                SELECT
                    gbc."Валюта",
                    SUM(COALESCE(gbc."vdb", 0)) "vdb",
                    SUM(COALESCE(gbc."vcr", 0)) "vcr"
                FROM
                    raw_data gbc
                WHERE
                    gbc."Валюта" IS NOT NULL
                GROUP BY
                    gbc."Валюта"
                ) z
        )
        '''

        cte = ''
        if FLAG_CURRENCY in self.on_flags:
            cte = template.format(
                postfix_ab=self.postfixes_ab.get(FLAG_CURRENCY),
            )

        return cte

    def __get_raw_amount_fields(self, table_alias):
        """Формирует рассчет невалютных полей в select блоке"""
        template = 'SUM(CASE WHEN {table_alias}."Тип" = {dc} THEN {table_alias}."{field_name}" END) ' \
                   '"v{dc_part}{postfix_ab}"'

        fields = []

        args = [(flag, dc) for flag in (FLAG_ACCOUNTING, FLAG_TAXING) for dc in (DEBIT, CREDIT)]
        for flag, dc in args:
            field_name = 'СуммаН' if flag == FLAG_TAXING else 'Сумма'
            fields.append(
                template.format(
                    table_alias=table_alias,
                    dc=dc,
                    dc_part=self.dc_parts.get(dc),
                    postfix_ab=self.postfixes_ab.get(flag),
                    field_name=field_name,
                )
            )
        return ',\n'.join(fields)

    def __get_amount_fields(self):
        """Формирует названия невалютных полей в select блоке"""
        template = '"v{dc_part}{postfix_ab}"'

        fields = []

        args = [(flag, dc) for flag in (FLAG_ACCOUNTING, FLAG_TAXING) for dc in (DEBIT, CREDIT)]
        for flag, dc in args:
            fields.append(
                template.format(
                    dc_part=self.dc_parts.get(dc),
                    postfix_ab=self.postfixes_ab.get(flag),
                )
            )
        fields = ',\n'.join(fields)
        if fields:
            fields += ','
        return fields

    def __get_result_amount_fields(self):
        """Формирует рассчет невалютных полей в результирующем запросе"""
        template = 'SUM("v{dc_part}{postfix_ab}") FILTER(WHERE "Валюта" IS NULL) "v{dc_part}{postfix_ab}"'
        template_total = '''
            SUM(
                SUM("v{dc_part}{postfix_ab}") FILTER(WHERE "Валюта" IS NULL) 
            ) OVER ()  AS "v{dc_part}{postfix_ab}_s"
        '''

        flags = self.on_flags[:]
        if FLAG_CURRENCY in flags:
            flags.remove(FLAG_CURRENCY)

        fields = []

        args = [(flag, dc) for flag in flags for dc in (DEBIT, CREDIT)]
        for flag, dc in args:
            fields.append(
                template.format(
                    dc_part=self.dc_parts.get(dc),
                    postfix_ab=self.postfixes_ab.get(flag),
                )
            )
            fields.append(
                template_total.format(
                    dc_part=self.dc_parts.get(dc),
                    postfix_ab=self.postfixes_ab.get(flag),
                )
            )
        fields = ',\n'.join(fields)
        if fields:
            fields += ','
        return fields

    def __get_result_curr_amount_fields(self, external_table_alias):
        """Формирует рассчет валютных полей в результирующем запросе"""
        template = '''
            (
                SELECT 
                    jsonb_object_agg("Валюта", "v{dc_part}") FILTER (WHERE "v{dc_part}" <> 0)
                FROM (
                    SELECT
                        gwp."Валюта",
                        SUM(COALESCE(gwp."v{dc_part}", 0)) "v{dc_part}"
                    FROM
                        data_with_parents gwp
                    WHERE
                        gwp."real_id" = {table_alias}."real_id" AND
                        gwp."Валюта" IS NOT NULL
                    GROUP BY
                        gwp."Валюта"
                ) z
            ) "v{dc_part}{postfix_ab}"
        '''
        flags = set(self.on_flags) & {FLAG_CURRENCY}

        fields = []

        args = [(flag, dc) for flag in flags for dc in (DEBIT, CREDIT)]
        for flag, dc in args:
            fields.append(
                template.format(
                    dc_part=self.dc_parts.get(dc),
                    postfix_ab=self.postfixes_ab.get(flag),
                    table_alias=external_table_alias,
                )
            )

        fields = ',\n'.join(fields)
        if fields:
            fields += ','
        return fields

    def __get_result_curr_total_fields(self):
        """Формирует рассчет валютных полей "отчета" в результирующем запросе"""
        template_without_curr = ''' NULL::jsonb "v{dc_part}{postfix_ab}s" '''
        template_by_curr = '''
            (
                SELECT
                    "v{dc_part}{postfix_ab}_s"
                FROM
                    total_curr
            ) "v{dc_part}{postfix_ab}_s"
        '''

        fields = []

        for dc in (DEBIT, CREDIT):
            template = template_by_curr if self.flags[FLAG_CURRENCY] else template_without_curr
            fields.append(
                template.format(
                    dc_part=self.dc_parts.get(dc),
                    postfix_ab=self.postfixes_ab.get(FLAG_CURRENCY),
                )
            )

        return ',\n'.join(fields)

    def __get_calc_corr_arr(self):
        """Рассчет корреспондирующей строки"""
        field_corr_arr = ''
        if any((self.is_detail_turnovers, self.is_corr_request)):
            field_corr_arr = '''
                STRING_AGG(
                    (
                        '{' ||
                        CASE WHEN dk."Тип" = 1 THEN 2 ELSE 1 END ||
                        '|' ||
                        COALESCE(dk2."Счет", 0) ||
                        '|' ||
                        COALESCE(dk."Сумма", 0) ||
                        '|' ||
                        COALESCE(dk."СуммаН", 0) ||
                        '|' ||
                        COALESCE(dk."Валюта", 'RUB') ||
                        '}'
                    ),
                    ','
                ) corr_arr
            '''
        return field_corr_arr

    def __get_raw_fields_dc(self):
        """Формирует список полей для сырой выборки"""
        fields = [
            'dk."Валюта"',
            'MIN(dk."Дата") "Дата"',
            'SUM(dk."Сумма") FILTER(WHERE dk."Тип" = 1) "Дебит"',
            'SUM(dk."Сумма") FILTER(WHERE dk."Тип" = 2) "Кредит"',
            'SUM(dk."СуммаН") FILTER(WHERE dk."Тип" = 1) "ДебитН"',
            'SUM(dk."СуммаН") FILTER(WHERE dk."Тип" = 2) "КредитН"',
        ]
        doc_field_with_curr = 'MIN(dk."Документ") doc_id' if self.current_group == 'document' else ''
        for field in (
                self.__get_field_for_group(curr=True),
                self.__get_calc_corr_arr(),
                doc_field_with_curr,
        ):
            if field:
                fields.append(field)
        return ',\n'.join(fields)

    def __get_raw_fields_dc_hierarchy(self, is_analytics_hierarchy=False):
        """Формирует список полей для сырой выборки по иерархии счетов"""
        fields = [
            'dk."Валюта"',
            f'dk."Лицо{self.current_group[-1]}" AS "real_id"' if is_analytics_hierarchy else 'dk."Счет" AS "real_id"',
        ]
        for field in (
                self.__get_raw_amount_fields(table_alias='dk'),
                self.__get_calc_corr_arr(),
        ):
            if field:
                fields.append(field)
        return ',\n'.join(fields)

    def __get_fields_group_by_doc(self):
        """Формирует список полей при группировке по документам"""
        fields = [
            'MIN("Дата") "Дата"',
        ]
        doc_field_without_curr = 'MIN(doc_id) doc_id' if self.current_group == 'document' else ''
        for field in (
                self.__get_field_for_group(curr=False),
                doc_field_without_curr,
                self.__get_id_fields('gbc'),
                self.__get_group_correspond_field(),
                self.__get_turnover_fields()

        ):
            if field:
                fields.append(field)
        return ',\n'.join(fields)
