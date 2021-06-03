"""
Глухенко А.В.
Реализация ЖурналаОрдера по режиму acc_balance
"""

import datetime

import sbis

from entry.account.main import get_cte_accounts_with_marks
from .base_sql import AB_BASE_SQL, AB_WITH_PERIOD_BASE_SQL, AB_WITH_ANALYTICS_HIERARCHY_SQL
from .journal_voucher_base import JournalVoucherBase
from .sql import CORR_WITH_ANALYTICS_PERIOD_SQL, CORR_WITHOUT_ANALYTICS_PERIOD_SQL, CORR_AB_SQL, BASE_CORR_QUERY, \
    CORR_AB_ACCOUNT_WITHOUT_ANALYTICS_SQL, CORR_AB_ACCOUNT_WITH_ANALYTICS_SQL, CALC_CORR_ARR, CALC_CORR_ARR_AB
from ..helpers import get_dates_box_filters
from ..jo_helpers import Columns, DEBIT, CREDIT, FLAG_ACCOUNTING, FLAG_TAXING, FLAG_CURRENCY, Helpers, ColumnTypes


class JournalVoucherAB(JournalVoucherBase):
    """Отчет журнал-ордер, стоящийся по таблице acc_balance"""
    def __init__(self, _filter, navigation):
        super().__init__(_filter, navigation)
        self.fields_by_col = {
            # {base_field: field_by_col}
            Columns.BALANCE_START_DEBIT: {
                'bdb': 'bdbs',
            },
            Columns.BALANCE_START_CREDIT: {
                'bcr': 'bcrs',
            },
            Columns.BALANCE_END_DEBIT: {
                'bdb': 'bdbe',
            },
            Columns.BALANCE_END_CREDIT: {
                'bcr': 'bcre',
            },
            Columns.TURNOVER_DEBIT: {
                'vdb': 'vdb',
            },
            Columns.TURNOVER_CREDIT: {
                'vcr': 'vcr',
            },
        }
        # границы периода для колонок в виде: {col: (date_start, date_end)}
        self.dates_by_col = {
            ColumnTypes.BALANCE_START: (
                self.date_start - datetime.timedelta(days=1),
                None,
            ),
            ColumnTypes.BALANCE_END: (
                self.date_end,
                None,
            ),
            ColumnTypes.TURNOVER: (
                self.date_start,
                self.date_end,
            )
        }
        self.calc_fields_by_col = {
            ColumnTypes.BALANCE_START: '''
                COALESCE(SUM(CASE WHEN dt <= '{date_start}'::DATE THEN {col_name} END), 0)
            ''',
            ColumnTypes.BALANCE_END: '''
                COALESCE(SUM(CASE WHEN dte >= '{date_start}'::DATE THEN {col_name} END), 0)
            ''',
            ColumnTypes.TURNOVER: '''
                COALESCE(SUM(CASE WHEN dt BETWEEN '{date_start}' AND '{date_end}' THEN {col_name} END), 0)
            '''
        }

        self.fields_by_on_col = self.__get_fields_by_on_col()
        self.fields_by_on_flags = self.__get_fields_by_on_flags()
        self.ab_current_group = self.__get_ab_current_group()
        self.is_need_corresponds = self.__check_need_corresponds()

    def __get_ab_current_group(self):
        """Рассчитывает текущую группу для режима acc_balance"""
        return 'account' if self.current_group == 'document' else self.current_group

    def __check_need_corresponds(self):
        """Проверяет, требуется ли корреспонденция в режиме acc_balance"""
        is_need_corresponds = self.is_detail_turnovers
        if self.current_group == 'document':
            is_need_corresponds = False
        return is_need_corresponds

    def _get_report_from_ab(self):
        """Строит отчет по таблице acc_balance"""
        if self.current_group == 'document':
            turnovers = False
            group_col = 'acc'
            page_limit = 1
            offset = 0
        else:
            turnovers = self.is_turnovers
            group_col = self.group_col
            page_limit = self.limit_on_page
            offset = self.offset_of_page

        real_id_col = group_col + ' AS real_id'
        main_data_fields, calc_main_data_fields = self.__get_main_data_fields()
        main_data_cols = ',\r\n'.join([real_id_col] + list(calc_main_data_fields))

        dates_filter = get_dates_box_filters(
            self.cols[Columns.BALANCE_START_DEBIT] or self.cols[Columns.BALANCE_START_CREDIT],
            turnovers,
            self.cols[Columns.BALANCE_END_DEBIT] or self.cols[Columns.BALANCE_END_CREDIT],
            self.date_start,
            self.date_end
        )

        cte_accounts_with_marks = ''
        if self.is_need_corresponds:
            cte_accounts_with_marks = get_cte_accounts_with_marks(self.id_accounts_with_children)

        sql = AB_BASE_SQL.format(
            cte_with_accounts_hier=cte_accounts_with_marks,
            result_sum_cols=self.__get_result_sum_cols(),
            window_agg_cols=self.__get_window_agg_cols(),
            main_data_cols=main_data_cols,
            where_flt=' AND '.join(self.where_filters_ab),
            dates_filter=dates_filter,
            group_col=group_col,
            zero_sum_filters=self.__get_zero_sum_having(),
            saldo_filters=self.__get_saldo_filters(),
            order_by=self.__get_order_cols(main_data_fields),
            additional_columns=self.__get_additional_fields(),
            corresponds=self.__get_ab_corresponds(),
            join_extended_tables=self.__get_extended_tables(self.ab_current_group),
            join_tables_for_sort=self.__get_tables_for_sort(self.ab_current_group),
            join_tables_for_search=self.__get_tables_for_search(self.ab_current_group),
            field_for_name_order=self.__get_field_for_name_order(self.ab_current_group),
            filter_by_search=self.__get_filter_by_search(),
            filter_by_ido=self._get_filter_by_ido(params_offset=2),
            curr_aggregate=self.__get_calc_curr_aggregate(),
        )
        result_rs = sbis.SqlQuery(
            sql,
            self.parent_id,
            self.ido_filter,
            page_limit,
            offset,
        )

        return self.__post_proccess_ab(result_rs)

    def _get_report_from_ab_analytics_hier(self):
        """Строит отчет по таблице acc_balance"""
        if self.current_group == 'document':
            turnovers = False
            group_col = 'acc'
            page_limit = 1
            offset = 0
        else:
            turnovers = self.is_turnovers
            group_col = self.group_col
            page_limit = self.limit_on_page
            offset = self.offset_of_page

        real_id_col = group_col + ' AS real_id'
        main_data_fields, calc_main_data_fields = self.__get_main_data_fields()
        main_data_cols = ',\r\n'.join([real_id_col] + list(calc_main_data_fields))

        dates_filter = get_dates_box_filters(
            self.cols[Columns.BALANCE_START_DEBIT] or self.cols[Columns.BALANCE_START_CREDIT],
            turnovers,
            self.cols[Columns.BALANCE_END_DEBIT] or self.cols[Columns.BALANCE_END_CREDIT],
            self.date_start,
            self.date_end
        )

        params = [self.parent_id, page_limit, offset]

        last_parent_id = self._get_last_parent_id(self.current_group)

        params.append(self.ido_filter)
        ido_filter = self._get_filter_by_ido(params_offset=len(params))

        if last_parent_id:
            filter_by_parent = f'parent = {last_parent_id}'
        else:
            filter_by_parent = 'parent is null'

        sum_fields = ',\n'.join(self.__get_fields_by_col())

        if self.is_need_corresponds:
            correspondence_field = ', corr_arr'
            grouped_correspondence_field = ", string_agg(corr_arr, ',') corr_arr"
        else:
            correspondence_field = grouped_correspondence_field = ''

        sql = AB_WITH_ANALYTICS_HIERARCHY_SQL.format(
            cte_hierarchy=self.analytics.get_cte_hierarchy(),
            sum_fields=sum_fields,
            result_sum_cols=self.__get_result_sum_cols(),
            window_agg_cols=self.__get_window_agg_cols(),
            main_data_cols=main_data_cols,
            where_flt=' AND '.join(self.where_filters_ab),
            dates_filter=dates_filter,
            group_col=group_col,
            zero_sum_filters=self.__get_zero_sum_having(),
            saldo_filters=self.__get_saldo_filters(),
            order_by=self.__get_order_cols(main_data_fields),
            additional_columns=self.__get_additional_fields(),
            corresponds=self.__get_ab_corresponds(is_hierarchy=True),
            correspondence_field=correspondence_field,
            grouped_correspondence_field=grouped_correspondence_field,
            join_extended_tables=self.__get_extended_tables(self.ab_current_group),
            join_tables_for_sort=self.__get_tables_for_sort(self.ab_current_group),
            join_tables_for_search=self.__get_tables_for_search(self.ab_current_group),
            field_for_name_order=self.__get_field_for_name_order(self.ab_current_group),
            filter_by_search=self.__get_filter_by_search(),
            curr_aggregate=self.__get_calc_curr_aggregate(),
            filter_by_parent=filter_by_parent,
            filter_by_ido=ido_filter
        )
        result_rs = sbis.SqlQuery(
            sql,
            *params
        )
        result_rs.DelCol('Раздел@')
        return self.__post_proccess_ab(result_rs)

    def __get_fields_by_on_col(self):
        """
        Возвращает словарь полей для запрашиваемой колонки, вида:
        {col: {base_field: field_by_col} }
        PS: дело в том что в запросе для расчета к примеру валютной колонки {field}c требуется название базовой колонки
        {field}. fields_by_on_col содержит соответствие базовой колонки и итогового названия колонки
        """
        result = {}
        for col in self.on_ab_cols:
            all_fields = {}
            fields = self.fields_by_col.get(col)
            if any((self.flags[FLAG_ACCOUNTING], self.flags[FLAG_CURRENCY])):
                all_fields.update(fields)
            if self.flags[FLAG_TAXING]:
                t_fields = {'{}t'.format(k): '{}t'.format(v) for k, v in fields.items()}
                all_fields.update(t_fields)
            result[col] = all_fields
        return result

    def __get_fields_by_on_flags(self):
        """
        Возвращает список полей, согласно включенным флагам
        """
        fields_by_flag = {
            FLAG_ACCOUNTING: ['bdbe', 'bcre', 'vdb', 'vcr', 'bdbs', 'bcrs'],
            FLAG_TAXING: ['bdbet', 'bcret', 'vdbt', 'vcrt', 'bdbst', 'bcrst'],
            FLAG_CURRENCY: ['bdbec', 'bcrec', 'vdbc', 'vcrc', 'bdbsc', 'bcrsc'],
        }
        fields = []
        for flag in self.on_flags:
            fields += fields_by_flag.get(flag)
        return fields

    def __get_main_data_fields(self):
        """Возвращает список колонок и их рассчеты"""
        result = {}
        for col, fields in self.fields_by_on_col.items():
            template = self.calc_fields_by_col.get(self.map_columns_to_types.get(col))
            date_start, date_end = self.dates_by_col.get(self.map_columns_to_types.get(col))
            for base_field, field_by_col in fields.items():
                result[field_by_col] = '{} AS {{alias}}'.format(template).format(
                    date_start=date_start,
                    date_end=date_end,
                    col_name=base_field,
                    alias=field_by_col,
                )
        return result.keys(), result.values()

    def __get_result_sum_cols(self):
        """Возвращет колонки для результирующего вывода"""
        # из-за того что в рассчете валюты используются невалютные названия полей, не удобно генерировать приставки t/c
        templates = {
            FLAG_ACCOUNTING: {
                'row': 'SUM(CASE WHEN curr IS NULL THEN {alias} END) AS {alias}',
                'total': 'MIN({alias}_result) AS {alias}_result',
            },
            FLAG_TAXING: {
                'row': 'SUM({alias}t) AS {alias}t',
                'total': 'MIN({alias}t_result) AS {alias}t_result',
            },
            FLAG_CURRENCY: {
                'row': '''
                    JSONB_OBJECT_AGG(curr, {alias}) 
                    FILTER (WHERE curr IS NOT NULL AND {alias} <> 0) AS {alias}c
                ''',
                'total': '''
                    (ARRAY_AGG({alias}c_result))[1] AS {alias}c_result
                ''',
            },
        }

        result_sum_cols = []
        for col in self.on_ab_cols:
            for field in self.fields_by_col.get(col).values():
                for flag in self.on_flags:
                    row = templates.get(flag).get('row')
                    total = templates.get(flag).get('total')
                    result_sum_cols.append(row.format(
                        alias=field,
                    ))
                    result_sum_cols.append(total.format(
                        alias=field,
                    ))

        return ',\r\n'.join(result_sum_cols)

    def __get_calc_curr_aggregate(self):
        """Возвращает расчет агрегатора для валюты"""
        template = '''
            JSONB_OBJECT_AGG(curr, {alias}c_result_base)
            FILTER (WHERE curr IS NOT NULL AND {alias}c_result_base <> 0) OVER () AS {alias}c_result
        '''

        curr_aggregate = list(self.__get_agg_columns_curr(template))

        if curr_aggregate:
            return ', {}'.format(',\n'.join(curr_aggregate))
        return ''

    def __get_agg_columns_curr(self, template):
        """подставляет в строку с валютными итогами именя поля"""
        for col in self.on_ab_cols:
            for field in self.fields_by_col.get(col).values():
                if self.flags[FLAG_CURRENCY]:
                    yield template.format(alias=field)

    def __get_window_agg_cols(self):
        """Возвращает окно агрегата"""
        template = ''' SUM(CASE WHEN curr IS NULL THEN {alias} END) OVER () AS {alias}_result '''
        template_curr = ''' SUM(CASE WHEN curr IS NOT NULL THEN {alias} END) OVER w AS {alias}c_result_base '''

        window_agg_cols = [template.format(alias=field_by_col) for field_by_col in self.__get_fields_by_col()]

        window_agg_cols.extend(self.__get_agg_columns_curr(template_curr))

        return ',\r\n'.join(window_agg_cols)

    def __get_fields_by_col(self):
        """
        получить названия включенных колонок
        """
        for fields in self.fields_by_on_col.values():
            yield from fields.values()

    def __get_saldo_filters(self):
        """Возвращает фильтр по сальдо"""
        flags = []
        if any((self.flags[FLAG_ACCOUNTING], self.flags[FLAG_CURRENCY])):
            flags.append(FLAG_ACCOUNTING)
        if self.flags[FLAG_TAXING]:
            flags.append(FLAG_TAXING)
        columns = []

        dual_side_filters = []
        negative_filters = []
        if self.cols[Columns.BALANCE_END_DEBIT]:
            columns += list(self.fields_by_col.get(Columns.BALANCE_END_DEBIT).values())
        if self.cols[Columns.BALANCE_END_CREDIT]:
            columns += list(self.fields_by_col.get(Columns.BALANCE_END_CREDIT).values())
        if columns:
            dual_side_filters = self.__get_dual_side_saldo_filters(flags, columns)
            negative_filters = self.__get_negative_saldo_filters(flags, columns)

        saldo_filters = ''
        if any((dual_side_filters, negative_filters)):
            saldo_filters = '''({}) AND ({})'''.format(
                ' OR '.join(dual_side_filters) or 'TRUE',
                ' OR '.join(negative_filters) or 'TRUE',
            )

        return saldo_filters or ' TRUE '

    def __get_negative_saldo_filters(self, flags, columns):
        """Возвращает фильтр по отрицательному сальдо"""
        templates = {
            FLAG_ACCOUNTING: ''' {alias} < 0 ''',
            FLAG_TAXING: ''' {alias}t < 0 '''
        }
        negative_filters = []
        if self.is_negative_balance_end_day:
            for flag in flags:
                template = templates.get(flag)
                negative_filters.append(
                    '({})'.format(
                        ' OR '.join(template.format(alias=col) for col in columns)
                    )
                )
        return negative_filters

    def __get_dual_side_saldo_filters(self, flags, columns):
        """Возвращает фильтр по дебету и кредиту сальдо"""
        templates = {
            FLAG_ACCOUNTING: ''' {alias} <> 0 ''',
            FLAG_TAXING: ''' {alias}t <> 0 '''
        }
        dual_side_filters = []
        if self.is_dual_side_balance_at_end:
            for flag in flags:
                template = templates.get(flag)
                dual_side_filters.append(
                    '({})'.format(
                        ' AND '.join(template.format(alias=col) for col in columns)
                    )
                )
        return dual_side_filters

    def __get_zero_sum_having(self):
        """Возвращает фильтр по нулевым суммам для HAVING блока"""
        zero_sum_filters = []
        for col, fields in self.fields_by_on_col.items():
            template = self.calc_fields_by_col.get(self.map_columns_to_types.get(col))
            date_start, date_end = self.dates_by_col.get(self.map_columns_to_types.get(col))
            for base_field in fields.keys():
                zero_sum_filters.append('{} <> 0'.format(template.format(
                    date_start=date_start,
                    date_end=date_end,
                    col_name=base_field,
                )))

        return ' OR '.join(zero_sum_filters)

    def __get_additional_fields(self):
        """Возвращает дополнительные поля для выборки"""
        additional_columns = [
            '$1::text AS parent',
        ]
        if self.ab_current_group == 'account':
            additional_columns.append(
                "coalesce($1::text || ',', '') || ABS(real_id) || '@{current_group}' AS id".format(
                    current_group=self.ab_current_group,
                )
            )
            additional_columns += ['ps."Номер"', 'ps."Название"']
            if self.account_hierarchy:
                additional_columns.append(
                    'ps."Раздел@" OR {is_node}::bool AS "parent@"'.format(is_node=self.is_node)
                )
            else:
                additional_columns.append(
                    '{is_node}::bool AS "parent@"'.format(is_node=self.is_node))
        else:
            additional_columns.append(
                "coalesce($1::text || ',', '') || real_id || '@{current_group}' AS id".format(
                    current_group=self.ab_current_group
                )
            )
            additional_columns.append('''
                CASE
                    WHEN real_id IS NOT NULL
                    THEN
                        CASE
                            WHEN l."@Лицо" IS NOT NULL
                            THEN coalesce(l."Название", 'Без названия')
                            ELSE 'Без аналитики'
                        END
                    ELSE NULL
                END AS "Название"
            ''')
            additional_columns.append('l."Лицо_" AS regclass')
            additional_columns.append('''COALESCE(ca."ИНН", pp."ИНН") || COALESCE(' ' || ca."КПП", '') AS "code"''')
            additional_columns.append('ca."АдресЮридический" AS "address"')
            additional_columns.append('l."Название" IS NULL AS special')
            if self.analytics.is_hierarchical(self.ab_current_group):
                additional_columns.append(f'limited_data."Раздел@" OR {self.is_node}::bool AS "parent@"')
            else:
                additional_columns.append('{is_node}::bool AS "parent@"'.format(is_node=self.is_node))

        return ',\n'.join(additional_columns)

    def __get_order_cols(self, request_fields):
        """
        Возвращает колонки с сортировкой
        :param request_fields: поля, которые запрашивает клиент
        :return: условие сортировки результата
        """
        order_cols = None
        if self.sort_by_name:
            if self.ab_current_group == 'account':
                order_cols = self._get_order('"Признаки"[3]', 'ASC') + ', ' + self._get_order('"Номер"', 'ASC')
            else:
                order_cols = []
                for field, default_order, default_null_order in (
                        ('real_id = -11', 'ASC', None),
                        ('for_order_1', 'DESC', None),
                        ('for_order_2', 'DESC', None),
                        ('for_order_3', 'ASC', 'NULLS FIRST'),
                ):
                    order_cols.append(self._get_order(field, default_order, default_null_order))
                order_cols = ','.join(order_cols)
        if not order_cols:
            order_cols = self.__get_base_order_cols(request_fields)
        return order_cols

    def __get_base_order_cols(self, request_fields):
        """Возвращает колонки с базовой сортировкой"""
        order_cols = []
        if self.only_currency:
            order_fields = ['bdbe', 'bcre', 'vdb', 'vcr', 'bdbs', 'bcrs']
            for field in order_fields:
                if field in request_fields:
                    field = self._get_curr_order(field + 'c')
                    order_cols.append(self._get_order(field, 'ASC'))
        else:
            order_cols.append(self._get_order('real_id = -11', 'ASC'))
            for field in self.fields_by_on_flags:
                if field in request_fields:
                    order_cols.append(self._get_order(field, 'ASC', sum_fields=self.fields_by_on_flags))

        order_cols.append(self._get_order('real_id', 'ASC'))
        return ',\r\n'.join(order_cols)

    def __get_extended_tables(self, current_group):
        """Возвращает расширенную версию запроса"""
        if current_group == 'account':
            extension_skeleton = '''
                LEFT JOIN "ПланСчетов" ps ON "@ПланСчетов" = ABS(real_id)
            '''
        else:
            extension_skeleton = '''
                LEFT JOIN "Лицо" l ON l."@Лицо" = real_id
                LEFT JOIN "Контрагент" ca ON ca."@Лицо" = real_id
                LEFT JOIN "ЧастноеЛицо" pp ON pp."@Лицо" = real_id
            '''
        return extension_skeleton

    def __get_tables_for_sort(self, current_group):
        """
        Возвращает набор таблиц, необходимых для сортировки
        PS: тут и поля для сортировки и поиск (пока оставим как было)
        """
        tables_for_sort = ''
        if current_group == 'account':
            tables_for_sort = '''
                LEFT JOIN "ПланСчетов" ps ON "@ПланСчетов" = ABS(real_id)
            '''
        else:
            if any((self.search, self.sort_by_name)):
                tables_for_sort = 'LEFT JOIN "Лицо" AS face_names ON (md.real_id = face_names."@Лицо")'
        return tables_for_sort

    def __get_tables_for_search(self, current_group):
        """
        Возвращает набор таблиц, необходимых для поиска
        """
        tables_for_search = ''
        if current_group != 'account' and self.search:
            tables_for_search += '''
                LEFT JOIN (SELECT * FROM "Контрагент" WHERE "ИНН" = '{0}') ca ON ca."@Лицо" = md.real_id
            '''.format(self.search)
        return tables_for_search

    def __get_field_for_name_order(self, current_group):
        """Возвращает колонку, необходимую для сортировки до имени"""
        col_for_alphabetical_order = ''
        if current_group != 'account' and self.sort_by_name:
            col_for_alphabetical_order = '''
                , TO_DATE(
                    SUBSTRING(
                        face_names."Название" FROM ' от ([0-9]{2}.[0-9]{2}.[0-9]{2})'
                    ),
                    'dd.mm.yy'
                ) AS for_order_1
                , SUBSTRING(face_names."Название" FROM '№(.*) от ([0-9]{2}.[0-9]{2}.[0-9]{2})') AS for_order_2
                , face_names."Название" AS for_order_3
            '''
        return col_for_alphabetical_order

    def __get_filter_by_search(self):
        """Возвращает фильтр по поиску"""
        filter_by_search = ''
        if self.search:
            if self.ab_current_group == 'account':
                filter_by_search = '''
                    ps."Номер" ILIKE '{0}%' OR
                    ps."Название" ILIKE '{0}%'
                '''.format(self.search)
            else:
                filter_by_search = '''
                    face_names."Название" ILIKE '%{0}%' OR
                    ca."ИНН" = '{0}'
                '''.format(self.search)
        return filter_by_search or ' TRUE '

    def __post_proccess_ab(self, result_rs):
        """Пост обработка результата"""

        result_rs.DelCol('real_id')
        result_rs.RenameField('correct_real_id', 'real_id')

        outcome = sbis.Record()
        self.__add_actual_date(outcome)
        result_rs.outcome = outcome
        if result_rs:
            for col in result_rs.Format():
                if col.Name()[-7:] == '_result':
                    if col.Type() == sbis.FieldType.ftHASH_TABLE:
                        result_rs.outcome.AddHashTable(col.Name()[:-7], result_rs.Get(0, col.Name()))
                    else:
                        result_rs.outcome.AddMoney(col.Name()[:-7], result_rs.Get(0, col.Name()))
        return result_rs

    def _get_report_from_ab_by_period(self):
        """Строит отчет по таблице acc_balance с периодичностью"""

        select_fields = self.__get_select_fields()
        corresponds = self.__get_corresponds_sql_ab_period()
        result_select_fields = select_fields[:]
        if corresponds:
            result_select_fields.append(corresponds)
        fields = 'dt, org, acc, vdb, vcr, vdbt, vcrt, bdb, bcr, bdbt, bcrt'
        sql = AB_WITH_PERIOD_BASE_SQL.format(
            main_fields_ab_period=fields,
            name_col=Helpers.date_col_name(self.periodicity_type, 'real_id'),
            where_flt=' AND '.join(self.where_filters_ab),
            grp_dt_col=self.group_col,
            filter_by_empty=self.__get_filter_by_empty(),
            filter_by_saldo=self.__get_filter_by_saldo(),
            result_select_fields=',\n'.join(result_select_fields),
            curr_turnover_fields=self.__get_curr_turnover_fields(),
            is_node=self.is_node,
            filter_by_ido=self._get_filter_by_ido(params_offset=6),
        )
        result = sbis.SqlQuery(
            sql,
            self.parent_id,
            self.date_start,
            self.date_end,
            self.limit_on_page,
            self.offset_of_page,
            self.ido_filter,
        )
        self.__post_proccess_ab_period(result, fields=select_fields)

        return result

    def __get_select_fields(self):
        """Возвращает набор колонок"""
        fields = []

        for flag in self.on_flags:
            # balance fields
            for col in self.on_balance_cols:
                fields.append('b{dc_part}{balance_postfix}{postfix_ab}'.format(
                    dc_part=self.dc_parts.get(self.map_columns_to_dc.get(col)),
                    balance_postfix=self.balance_postfixes.get(self.map_columns_to_types.get(col)),
                    postfix_ab=self.postfixes_ab.get(flag),
                ))
            # turnover fields
            if self.is_turnovers:
                for dc in (DEBIT, CREDIT):
                    fields.append('v{dc_part}{postfix_ab}'.format(
                        dc_part=self.dc_parts.get(dc),
                        postfix_ab=self.postfixes_ab.get(flag),
                    ))
        return fields

    def __get_filter_by_empty(self):
        """Возвращает поля фильтрации для пустых значений"""
        turnover_template = {
            FLAG_ACCOUNTING: 'COALESCE(v{dc_part}{postfix_ab}, 0) <> 0',
            FLAG_TAXING: 'COALESCE(v{dc_part}{postfix_ab}, 0) <> 0',
            FLAG_CURRENCY: '''COALESCE(v{dc_part}{postfix_ab}::text, '{{}}'::text) <> '{{}}'::text''',
        }
        balance_template = {
            FLAG_ACCOUNTING: '''
                NULLIF(b{dc_part}e{postfix_ab}, 0) IS DISTINCT FROM 
                NULLIF(b{dc_part}s{postfix_ab}, 0)
            ''',
            FLAG_TAXING: '''
                NULLIF(b{dc_part}e{postfix_ab}, 0) IS DISTINCT FROM 
                NULLIF(b{dc_part}s{postfix_ab}, 0)
            ''',
            FLAG_CURRENCY: '''
                NULLIF(b{dc_part}e{postfix_ab}::text, '{{}}'::text) IS DISTINCT FROM
                NULLIF(b{dc_part}s{postfix_ab}::text, '{{}}'::text)
            ''',
        }

        fields = []
        dc_fields = [(f, dc) for f in self.on_flags for dc in (DEBIT, CREDIT)]

        for flag, dc in dc_fields:
            active_columns = list(filter(lambda x: self.map_columns_to_dc.get(x) == dc, self.on_ab_cols))
            # turnovers
            if ColumnTypes.TURNOVER in map(self.map_columns_to_types.get, active_columns):
                fields.append(turnover_template.get(flag).format(
                    dc_part=self.dc_parts.get(dc),
                    postfix_ab=self.postfixes_ab.get(flag),
                ))
            # balance
            if {ColumnTypes.BALANCE_START, ColumnTypes.BALANCE_END} & set(
                    map(self.map_columns_to_types.get, active_columns)):
                fields.append(balance_template.get(flag).format(
                    dc_part=self.dc_parts.get(dc),
                    postfix_ab=self.postfixes_ab.get(flag),
                ))

        return ' OR \n'.join(fields)

    def __checker_negative_json_exist(self, field):
        """Проверяет наличие отрицательного значения в json"""
        return '''
        (
            SELECT
                jsonb_object_agg(key, value)
            FROM
            (
                SELECT
                    key, value
                FROM
                    jsonb_each({field})
                WHERE
                    "value"::text::numeric < 0.0
            ) z
        ) IS NOT NULL
        '''.format(field=field)

    def __get_filter_by_saldo(self):
        """Фильтр по сальдо"""
        neg_fields = []
        dual_fields = []

        for flag in self.on_flags:
            # check_saldo_neg/check_saldo_dual
            fields = []
            if self.cols[Columns.BALANCE_END_DEBIT]:
                fields.append('bdbe')
            if self.cols[Columns.BALANCE_END_CREDIT]:
                fields.append('bcre')

            dual_fields_tmp = []
            postfix_ab = self.postfixes_ab.get(flag)
            for field in fields:
                if self.is_negative_balance_end_day:
                    if flag == FLAG_CURRENCY:
                        col_name = f'{field}{postfix_ab}'
                        neg_fields.append(self.__checker_negative_json_exist(col_name))
                    else:
                        neg_fields.append('{field}{postfix_ab} < 0'.format(
                            field=field,
                            postfix_ab=postfix_ab,
                        ))
                if self.is_dual_side_balance_at_end:
                    dual_fields_tmp.append('{field}{postfix_ab} {check}'.format(
                        field=field,
                        postfix_ab=postfix_ab,
                        check=' IS NOT NULL ' if flag == FLAG_CURRENCY else ' <> 0 ',
                    ))
            if dual_fields_tmp:
                dual_fields.append(' AND '.join(dual_fields_tmp))
        if any((neg_fields, dual_fields)):
            fields = '({}) AND ({})'.format(
                ' OR '.join(neg_fields) or 'TRUE',
                ' OR '.join(dual_fields) or 'TRUE'
            )
        else:
            fields = 'TRUE'
        return fields

    def __get_curr_turnover_fields(self):
        """Расчитывает валюты оборотов"""

        if self.flags[FLAG_CURRENCY]:
            fields = '''
                jsonb_object_agg(curr, vdb) FILTER(WHERE curr IS NOT NULL AND COALESCE(vdb, 0) <> 0) vdbc,
                jsonb_object_agg(curr, vcr) FILTER(WHERE curr IS NOT NULL AND COALESCE(vcr, 0) <> 0) vcrc,

                -- сложный рассчет суммарных валютных оборотов vdbc_s/vcrc_s
                (	
                    SELECT
                    (
                        SELECT
                            -- схлопнем все в json
                            jsonb_object_agg(key, value)
                        FROM (
                            -- развернем json и просуммируем значения по валютам
                            SELECT key, sum(value::text::numeric) "value"
                            FROM jsonb_each(j_vdb)
                            GROUP BY key
                        ) x
                    )
                    -- получим набор валют с данными vdb
                    FROM (
                        SELECT
                            jsonb_object_agg(curr, vdb) j_vdb
                        FROM (
                            SELECT curr, vdb
                            FROM group_by_id_curr
                            WHERE curr IS NOT NULL AND
                            COALESCE(vdb, 0) <> 0
                        ) x
                    ) xx
                ) vdbc_s,

                (	
                    SELECT
                    (
                        SELECT
                            -- схлопнем все в json
                            jsonb_object_agg(key, value)
                        FROM (
                            -- развернем json и просуммируем значения по валютам
                            SELECT key, sum(value::text::numeric) "value"
                            FROM jsonb_each(j_vcr)
                            GROUP BY key
                        ) x
                    )
                    -- получим набор валют с данными vdb
                    FROM (
                        SELECT
                            jsonb_object_agg(curr, vcr) j_vcr
                        FROM (
                            SELECT curr, vcr
                            FROM group_by_id_curr
                            WHERE curr IS NOT NULL AND
                            COALESCE(vcr, 0) <> 0
                        ) x
                    ) xx
                ) vcrc_s,
            '''
        else:
            fields = '''
                NULL::jsonb vdbc,
                NULL::jsonb vcrc,
                NULL::jsonb vdbc_s,
                NULL::jsonb vcrc_s,
            '''
        return fields

    def __get_sql_join_json(self, field_start_balance, field_turnover, field_result):
        """Возвращает sql запрос рассчета объединения и суммирования json полей"""
        # метод устаревший но вдруг пригодится, пока оставим
        fields = (field_start_balance, field_turnover)
        return '''
        (
            SELECT
                -- схлопнем все в json
                jsonb_object_agg(key, value) result
            FROM (
                SELECT
                    key,
                    sum(value) "value"
                FROM (
                    -- развернем json и просуммируем значения по валютам
                    SELECT
                        key,
                        value::text::numeric
                    FROM jsonb_each(
                        (
                            SELECT
                                jsonb_object_agg(key, value)
                            FROM
                                ({fields}) z,
                                jsonb_each("field")
                        )
                    )
                ) zz
                GROUP BY
                    key
            ) zzz
        ) {name_field_result}
        '''.format(
            name_field_result=field_result,
            fields=' UNION ALL '.join(' SELECT {} "field"'.format(field) for field in fields),
        )

    def __get_corresponds_sql_ab_period(self):
        """
        Возвращает подзапрос для получения корреспонденций в режиме построения по таблице acc_balance с периодичностью
        """
        if not self.is_detail_turnovers:
            return ''

        period_st, period_end = self.__get_period_condition()

        if not any(self.analytics.basic):
            corresponds = CORR_WITHOUT_ANALYTICS_PERIOD_SQL.format(
                calc_corr_arr_ab=CALC_CORR_ARR_AB,
                org_flt=self.to_org_filter,
                date_st=self.date_start,
                date_end=self.date_end,
                acc_list=', '.join(map(str, self.id_accounts_with_children)),
                period_st=period_st,
                period_end=period_end,
                sum_field=self._get_turnover_field_by_type(),
            )
        else:
            corresponds = CORR_WITH_ANALYTICS_PERIOD_SQL.format(
                calc_corr_arr=CALC_CORR_ARR,
                other_conditions=' AND '.join(self.ab_corresponds_cond),
                date_st=self.date_start,
                date_end=self.date_end,
                acc_list=', '.join(map(str, self.id_accounts_with_children)),
                period_st=period_st,
                period_end=period_end,
                filter_by_internal_turnover=self._get_filter_by_inter_turn(),
            )

        return corresponds

    def __get_period_condition(self):
        """Возвращает рассчеты даты"""
        templates = {
            'day': {
                'begin': "TO_DATE(SUBSTRING(real_id FROM 2), 'YYYYDDD')",
                'end': "TO_DATE(SUBSTRING(real_id FROM 2), 'YYYYDDD')",
            },
            'week': {
                'begin': "TO_DATE(SUBSTRING(real_id FROM 2), 'IYYY0IW')",
                'end': "TO_DATE(SUBSTRING(real_id FROM 2), 'IYYY0IW') + 6",
            },
            'month': {
                'begin': "TO_DATE(SUBSTRING(real_id FROM 2), 'YYYY0MM')",
                'end': "(TO_DATE(SUBSTRING(real_id FROM 2), 'YYYY0MM') + INTERVAL '1 MONTH - 1 DAY')::DATE",
            },
            'quarter': {
                'begin': '''
                    (
                        TO_DATE(
                            SUBSTRING(real_id FROM 2 FOR 4) ||
                            SUBSTRING(real_id FROM 8)::INT * 3,
                            'YYYYMM'
                        ) -
                        INTERVAL '2 MONTH'
                    )::DATE
                ''',
                'end': '''
                    (
                        TO_DATE(
                            SUBSTRING(real_id FROM 2 FOR 4) ||
                            SUBSTRING(real_id FROM 8)::INT * 3,
                            'YYYYMM'
                        ) +
                        INTERVAL '1 MONTH - 1 DAY'
                    )::DATE
                ''',
            },
            'default': {
                'begin': "TO_DATE(SUBSTRING(real_id FROM 2 FOR 4), 'YYYY')",
                'end': "TO_DATE(SUBSTRING(real_id FROM 2 FOR 4), 'YYYY') + INTERVAL '1 YEAR'",
            },
        }
        template = templates.get(self.periodicity_type) or templates.get('default')
        return template.get('begin'), template.get('end')

    def __get_ab_corresponds(self, is_hierarchy=False):
        """Подзапрос для получения корреспонденции в режиме построения по таблице acc_balance"""
        if not self.is_need_corresponds:
            return ''

        group_col = 'acc' if self.current_group == 'document' else self.group_col
        entity_field = group_col if is_hierarchy else 'real_id'

        corresponds = CORR_AB_SQL.format(
            calc_corr_arr=CALC_CORR_ARR,
            base_corr_query_amount=self.__get_base_corr_query(entity_field, is_amount=True),
            base_corr_query=self.__get_base_corr_query(entity_field, is_amount=False),
        )
        if self.ab_current_group == 'account':
            if self.groups[0] == 'account' and not any(self.analytics.basic):
                corresponds = CORR_AB_ACCOUNT_WITHOUT_ANALYTICS_SQL.format(
                    calc_corr_arr_ab=CALC_CORR_ARR_AB,
                    base_acc_turnover=self._get_base_acc_turnover(ab_account=True)
                )
            else:
                corresponds = CORR_AB_ACCOUNT_WITH_ANALYTICS_SQL.format(
                    acc_list=', '.join(map(str, self.id_accounts_with_children)),
                    date_st=self.date_start,
                    date_end=self.date_end,
                    other_conditions=' AND '.join(self.ab_corresponds_cond),
                    filter_by_internal_turnover=self._get_filter_by_inter_turn(),
                    calc_corr_arr=CALC_CORR_ARR,
                )
        return corresponds

    def __post_proccess_ab_period(self, result, fields):
        """
        Рассчитывает outcome
        Примечание: fields - набор полей для отчета _get_report_from_ab_by_period
        """
        fields = fields or []

        if self.current_group == 'periodicity':
            outcome = sbis.Record()
            self.__add_actual_date(outcome)
            if result:
                for field in fields:
                    if field in self.currency_fields:
                        outcome.AddHashTable(field)
                        outcome[field] = result[0].Get(field + '_s')
                    else:
                        outcome.AddMoney(field)
                        outcome[field] = result[0].Get(field + '_s', 0)

                sum_fields = [field_name for field_name in result[0].GetFieldNames() if field_name.endswith("_s")]
                for sum_field in sum_fields:
                    result.DelCol(sum_field)

            result.outcome = outcome

    def __add_actual_date(self, outcome):
        """Добавляет актуальные даты в outcome"""
        actual_date, wh_actual_text = Helpers.get_actual_date(
            turnover=self.is_turnovers,
            balance_end=self.cols[Columns.BALANCE_END_DEBIT] or self.cols[Columns.BALANCE_END_CREDIT],
            date_start=self.date_start,
            date_end=self.date_end,
            ab_org_flt=self.to_org_filter,
            org_ids=self.organizations,
            access_zone=self._filter.Get(self.access_zone_field),
            accounts=self.id_accounts,
        )
        outcome.AddDate('actual_date', actual_date)
        outcome.AddString('wh_actual_text', wh_actual_text)

    def __get_base_corr_query(self, entity_field, is_amount):
        """Формирует базовый запрос для корреспонденции"""
        filter_by_amount = '{entity_field} = {join_col} AND'
        if is_amount:
            filter_by_amount = '''
                {join_col} IS NULL AND
                {entity_field} = -11 AND
            '''
        filter_by_amount = filter_by_amount.format(
            entity_field=entity_field,
            join_col=self.group_col_dc,
        )
        return BASE_CORR_QUERY.format(
            date_st=self.date_start,
            date_end=self.date_end,
            acc_list=', '.join(map(str, self.id_accounts_with_children)),
            filter_by_amount=filter_by_amount,
            other_conditions=' AND '.join(self.ab_corresponds_cond),
            filter_by_internal_turnover=self._get_filter_by_inter_turn(),
        )
