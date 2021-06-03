"""
Глухенко А.В.
Реализация базовых методов по отчету ЖурналаОрдера
"""

import datetime

import sbis
from docflow_helpers import badCharFilter

from acc_reports.reports_core.base import BaseReportClass
from entry.account.const import get_actual_currencies
from entry.account.utils import is_salary_acc
from entry.core.docface import get_formated_face_name
from .analytics import Analytics
from .sql import CURR_ORDER_SQL, CORR_WITHOUT_ANALYTICS_SQL, CORR_WITH_ANALYTICS_SQL, BASE_CTE_ACCOUNT, \
    BASE_ACC_TURNOVER
from ..accounts import Accounts
from ..helpers import parse_date_key, EMPTY_NAV_LIMIT
from ..jo_helpers import FLAG_ACCOUNTING, FLAG_TAXING, FLAG_CURRENCY, Columns, DEBIT, CREDIT, INFINITY_DATE, Helpers, \
    ColumnTypes, MAP_COLUMNS_TO_DC, MAP_COLUMNS_TO_TYPES

HUGE_FILTER_THRESHOLD = 50


class JournalVoucherBase(BaseReportClass):
    """Основной класс для построения отчета"""
    def __init__(self, _filter, navigation):
        super().__init__(_filter)
        self.__backward_compatibility()
        # navigation
        self.navigation = navigation
        cur_page = navigation.Page()

        if self.ido_filter:
            # при наличии фильтра по СписокИдО, будет работать не навигация, а ограничение по id
            recs_on_page = EMPTY_NAV_LIMIT
        else:
            recs_on_page = navigation.RecsOnPage() or EMPTY_NAV_LIMIT

        self.limit_on_page = recs_on_page + 1
        self.offset_of_page = cur_page * recs_on_page
        self.postfixes = {
            FLAG_ACCOUNTING: '',
            FLAG_TAXING: 'Н',
            FLAG_CURRENCY: '',
        }
        self.postfixes_ab = {
            FLAG_ACCOUNTING: '',
            FLAG_TAXING: 't',
            FLAG_CURRENCY: 'c',
        }
        self.balance_postfixes = {
            ColumnTypes.BALANCE_START: 's',
            ColumnTypes.BALANCE_END: 'e',
        }
        self.dc_parts = {
            DEBIT: 'db',
            CREDIT: 'cr',
        }
        self.map_columns_to_types = MAP_COLUMNS_TO_TYPES
        self.map_columns_to_dc = MAP_COLUMNS_TO_DC
        # кэшируем получение набора дочерних счетов
        self.child_accounts = {}
        self.currency_fields = (
            'bdbsc', 'bcrsc',
            'bdbec', 'bcrec',
            'vdbc', 'vcrc',
        )
        self.type_corr_accounting = {
            FLAG_ACCOUNTING: 0,
            FLAG_TAXING: 1,
            FLAG_CURRENCY: 0,
        }
        self.__parse_filter()
        self.__check_restrictions()
        self.currencies = get_actual_currencies()

    def __backward_compatibility(self):
        """Поддержка обратной совместимости для смены параметров фильтра"""
        self.accounts_field = self._get_filter_field('Accounts', 'account')
        self.corr_accounts_field = self._get_filter_field('CorrAccounts', 'corr_account')
        self.date_begin_field = self._get_filter_field('DateBegin', 'date_start')
        self.date_end_field = self._get_filter_field('DateEnd', 'date_end')
        self.access_zone_field = self._get_filter_field('AccessZone', 'access_zone')
        self.organization_field = self._get_filter_field('Organization', 'organization')
        self.exclude_internal_turnover_field = self._get_filter_field('ExcludeInternalTurnover',
                                                                      'exclude_internal_turnover')
        self.dual_side_balance_at_end_field = self._get_filter_field('DualSideBalanceAtEnd', 'dualSideBalanceAtEnd')
        self.negative_balance_end_day_field = self._get_filter_field('NegativeBalanceEndDay', 'negativeBalanceEndDay')
        self.order_by_field = self._get_filter_field('OrderBy', 'order_by')
        self.group_by_field = self._get_filter_field('GroupBy', 'group_by')
        self.periodicity_type_field = self._get_filter_field('PeriodicityType', 'periodicity_type')
        self.account_hierarchy_field = self._get_filter_field('AccountHierarchy', 'account_hierarchy')
        self.parent_field = self._get_filter_field('Parent', 'parent')
        self.search_field = self._get_filter_field('Search', 'search_string')

    def __check_restrictions(self):
        """
        Накладывает ограничения при построении отчета
        """
        negative_bed = bool(self._filter.Get(self.negative_balance_end_day_field))
        dual_side = bool(self._filter.Get(self.dual_side_balance_at_end_field))
        if self.exclude_internal_turnover is not None and any((negative_bed, dual_side)):
            msg = 'В фильтре включен режим проверки сальдо, но колонка с конечным сальдо не отображается. ' \
                  'Настройте отображение колонки или выключите режим проверки на панели фильтров'
            raise sbis.Warning(*(msg,) * 2)

    def _get_columns(self):
        """Разбор запроса по колонкам отчета"""
        if 'Columns' in self._filter:
            columns = self._filter.Get('Columns') or {}
            if isinstance(columns, sbis.Record):
                columns = columns.as_dict()
            balance_start = bool(columns.get('BalanceStart'))
            balance_start_debit = bool(columns.get('BalanceStartDebit'))
            balance_start_credit = bool(columns.get('BalanceStartCredit'))
            balance_end = bool(columns.get('BalanceEnd'))
            balance_end_debit = bool(columns.get('BalanceEndDebit'))
            balance_end_credit = bool(columns.get('BalanceEndCredit'))
            debit = bool(columns.get('Debit'))
            credit = bool(columns.get('Credit'))
            turnover_debit_node = bool(columns.get('CorrDebitNode'))
            turnover_debit_leaf = bool(columns.get('CorrDebitLeaf'))
            turnover_credit_node = bool(columns.get('CorrCreditNode'))
            turnover_credit_leaf = bool(columns.get('CorrCreditLeaf'))
        else:
            balance_start_debit = None
            balance_start_credit = None
            balance_end_debit = None
            balance_end_credit = None
            balance_start = bool(self._filter.Get('balance_start'))
            balance_end = bool(self._filter.Get('balance_end'))
            debit = bool(self._filter.Get('debit'))
            credit = bool(self._filter.Get('credit'))
            turnover_debit_node = bool(self._filter.Get('correspondence_d_node'))
            turnover_debit_leaf = bool(self._filter.Get('correspondence_d_leaf'))
            turnover_credit_node = bool(self._filter.Get('correspondence_c_node'))
            turnover_credit_leaf = bool(self._filter.Get('correspondence_c_leaf'))

        # поддержа старых колонок
        if balance_start:
            balance_start_debit = True
            balance_start_credit = True

        if balance_end:
            balance_end_debit = True
            balance_end_credit = True

        negative_bed = bool(self._filter.Get(self.negative_balance_end_day_field))
        dual_sbae = bool(self._filter.Get(self.dual_side_balance_at_end_field))

        if any((negative_bed, dual_sbae)):
            if negative_bed and not any((balance_end_debit, balance_end_credit)) or \
               dual_sbae and not all((balance_end_debit, balance_end_credit)):
                raise sbis.Warning(
                    *['В фильтре включен режим проверки сальдо, но колонки с конечным сальдо не отображаются. '
                      'Настройте отображение колонок или выключите режим проверки на панели фильтров'] * 2)
            # зануляем все колонки кроме конечного сальдо
            cols = {
                Columns.BALANCE_START_DEBIT: False,
                Columns.BALANCE_START_CREDIT: False,
                Columns.TURNOVER_DEBIT: False,
                Columns.TURNOVER_DEBIT_NODE: False,
                Columns.TURNOVER_DEBIT_LEAF: False,
                Columns.TURNOVER_CREDIT: False,
                Columns.TURNOVER_CREDIT_NODE: False,
                Columns.TURNOVER_CREDIT_LEAF: False,
                Columns.BALANCE_END_DEBIT: balance_end_debit,
                Columns.BALANCE_END_CREDIT: balance_end_credit
            }
        else:
            turnover_debit = any((debit, turnover_debit_node, turnover_debit_leaf))
            turnover_credit = any((credit, turnover_credit_node, turnover_credit_leaf))

            cols = {
                Columns.BALANCE_START_DEBIT: balance_start_debit,
                Columns.BALANCE_START_CREDIT: balance_start_credit,
                Columns.TURNOVER_DEBIT: turnover_debit,
                Columns.TURNOVER_DEBIT_NODE: turnover_debit_node,
                Columns.TURNOVER_DEBIT_LEAF: turnover_debit_leaf,
                Columns.TURNOVER_CREDIT: turnover_credit,
                Columns.TURNOVER_CREDIT_NODE: turnover_credit_node,
                Columns.TURNOVER_CREDIT_LEAF: turnover_credit_leaf,
                Columns.BALANCE_END_DEBIT: balance_end_debit,
                Columns.BALANCE_END_CREDIT: balance_end_credit
            }
        if not any(cols.values()):
            raise sbis.Warning(*['Не выбраны отображаемые колонки'] * 2)
        return cols

    def _get_parent_id(self):
        """ Если запрос с клиента был нетипизирован, а в parent положили null, его тип будет Bool, дропнем поле """
        val = self._filter.Get(self.parent_field)
        if not val and self.parent_field in self._filter:
            self._filter.Remove(self.parent_field)
            return None
        return val

    def __parse_filter(self):
        """Основная функция, которая разбирает фильтры. Состоит почти полностью из вызова других, вспомогательных."""
        self.parent_id = self._get_parent_id()
        self.periodicity_type = self._filter.Get(self.periodicity_type_field)
        self.account_hierarchy = bool(self._filter.Get(self.account_hierarchy_field))
        self.exclude_internal_turnover = self._filter.Get(self.exclude_internal_turnover_field)
        self.flags = self._get_flags()
        self.on_flags = self._get_on_flags()
        self.only_currency = self._get_only_currency()
        self.parents = self.__get_parents()
        self.analytics = Analytics(self._filter, self.parents)
        self.date_start, self.date_end = self.__get_dates()
        self.groups = self.__get_groups()
        self.organizations = self.__get_organizations()
        self.id_corr_accounts = self.__get_corr_accounts()
        self.is_corr_request = any((self.id_corr_accounts, any(self.analytics.corresponding)))
        self.is_dc = any(('document' in self.parents, self.is_corr_request, self.exclude_internal_turnover is not None))
        self.current_group = self.__get_current_group()
        # флаг получения данных из ДебетКредит или acc_balance
        self.is_dc = any((self.current_group == 'document', self.is_dc))
        self.cols = self._get_columns()
        self.is_detail_turnovers = self.__check_is_detail_turnovers()
        self.is_turnovers = self.__check_is_turnovers()
        self.is_negative_balance_end_day = self.__check_negative_balance()
        self.is_dual_side_balance_at_end = self.__check_dual_side_balance()
        self.on_cols = self.__get_on_columns()
        self.on_balance_cols = self.__get_on_balance_columns()
        self.on_ab_cols = self.__get_on_ab_columns()
        self.is_node = self._check_is_node()
        # поиск должен распространяться только на первый уровень
        self.search = not self.parents and badCharFilter(self._filter.Get(self.search_field) or '')
        self.sort_by_name = self._filter.Get(self.order_by_field) == 'name'
        self.asc_sort = self._filter.Get('AscSort') in (True, None)
        self.accounts = self.__get_base_accounts()
        if not self.accounts:
            self.all_accounts = None
            return
        self.id_accounts = self.__get_id_accounts()
        self.id_correct_accounts = self.__get_id_correct_accounts()
        self.parent_accounts = Accounts.unique_accounts(self.id_accounts)
        self.is_personal_calc_acc = any(map(is_salary_acc, self.parent_accounts.Get('num')))
        self.id_accounts_without_doubles = self.parent_accounts.Get('acc')
        self.id_correct_accounts_ab = self.__get_id_correct_accounts_ab()
        self.id_accounts_with_children = self.__get_id_accounts_with_child()
        # временное решение, пока для двух режимов возвращаются всегда с - (где требуется)
        # тут лишь постфактум для dc режима будем зачитывать по abs.
        self.id_correct_accounts = list(map(abs, self.id_correct_accounts))
        # все счета (необходимо для корреспонденции)
        self.all_accounts = self.__get_all_accounts()
        self.group_col, self.group_col_dc = self._get_group_col()
        self.ab_corresponds_cond, self.dc_corresponds_cond = self.__get_corresponds_filter()
        self.ab_org_filter = self.__get_org_filter()
        self.to_org_filter = self.__get_to_org_filter()
        self.where_filters_ab, self.where_filters_dc = self.__get_ab_filter(), self.__get_dc_filter()
        # мы внутри печати
        self.is_printing = self._filter.Get('IsPrinting') or self._filter.Get('IsUnloading')

    def __get_dates(self):
        """Разбор дат"""
        date_start = self._filter.Get(self.date_begin_field) or datetime.date(1900, 1, 1)
        date_end = self._filter.Get(self.date_end_field) or INFINITY_DATE
        periodicity = self.parents.get('periodicity')
        if periodicity:
            first_day, last_day = parse_date_key(periodicity)
            date_start = max((first_day, date_start))
            date_end = min((last_day, date_end))
        return date_start, date_end

    def __get_parents(self) -> dict:
        """Разбор parent_id"""
        parents = {}
        if self.parent_id:
            for item in self.parent_id.split(','):
                value, key = item.split('@')
                if key == 'document':
                    id_doc, datetime_code = value.split('|')
                    day, year = int(datetime_code) // 10000, int(datetime_code) % 10000
                    date = datetime.date(year, 1, 1) + datetime.timedelta(day - 1)
                    parents['document'] = int(id_doc)
                    parents['date_document'] = date
                elif key in ('organization', 'face1', 'face2', 'face3', 'face4'):
                    parents[key] = int(value)
                elif key == 'account':
                    parents[key] = abs(int(value))
                elif key == 'periodicity':
                    parents[key] = value
        return parents

    def __get_groups(self):
        """Разбор группы"""
        return self._filter.Get(self.group_by_field) or ['account']

    def _get_last_parent_id(self, group='account'):
        """Возвращает последний счет из parent_id"""
        if self.parent_id:
            for item in self.parent_id.split(',')[::-1]:
                value, key = item.split('@')
                if key == group:
                    return int(value)
        return None

    def __get_organizations(self):
        """Разбор организаций"""
        organization = self._filter.Get(self.organization_field)
        organizations = self._filter.Get('Organizations')
        if organization and not organizations:
            organizations = [organization]
        if self.parents.get('organization'):
            organizations = [self.parents.get('organization')]
        return organizations

    def __get_corr_accounts(self):
        """Разбор счетов корресонденции"""
        corr_accounts = self._filter.Get(self.corr_accounts_field, [])
        if not isinstance(corr_accounts, (list, set, tuple)):
            corr_accounts = [corr_accounts]
        if corr_accounts:
            corr_accounts = Accounts.get_all_children(corr_accounts)
        return [acc.Get('acc') for acc in corr_accounts]

    def __check_is_detail_turnovers(self):
        """Проверяет, запрошена ли детализация по оборотам"""
        cols = (
            Columns.TURNOVER_DEBIT_NODE,
            Columns.TURNOVER_DEBIT_LEAF,
            Columns.TURNOVER_CREDIT_NODE,
            Columns.TURNOVER_CREDIT_LEAF,
        )
        return any(filter(lambda c: self.cols[c], cols))

    def __check_is_turnovers(self):
        """Проверяет, запрошены ли обороты"""
        if 'Columns' in self._filter:
            columns = self._filter.Get('Columns') or {}
            if isinstance(columns, sbis.Record):
                columns = columns.as_dict()
            debit = bool(columns.get('Debit'))
            credit = bool(columns.get('Credit'))
        else:
            debit = bool(self._filter.Get('debit'))
            credit = bool(self._filter.Get('credit'))
        negative_bed = bool(self._filter.Get(self.negative_balance_end_day_field))
        dual_sbae = bool(self._filter.Get(self.dual_side_balance_at_end_field))
        if any((negative_bed, dual_sbae)):
            is_turnovers = False
        else:
            is_turnovers = any((debit, credit, self.is_detail_turnovers))
        return is_turnovers

    def __check_negative_balance(self):
        """Проверяет запрошено ли отрицательное сальдо"""
        negative_bed = bool(self._filter.Get(self.negative_balance_end_day_field))
        balance_end = self.cols[Columns.BALANCE_END_CREDIT] or self.cols[Columns.BALANCE_END_DEBIT]
        return all((negative_bed, not self.parent_id, balance_end))

    def __check_dual_side_balance(self):
        """Проверяет запрошено ли сальдо по дебету и кредиту"""
        dual_side = bool(self._filter.Get(self.dual_side_balance_at_end_field))
        # а проверить сальдо двустороннее мы можем только если запрошены и дебет и кредит
        balance_end = self.cols[Columns.BALANCE_END_CREDIT] and self.cols[Columns.BALANCE_END_DEBIT]
        return all((dual_side, not self.parent_id, balance_end))

    def __get_on_columns(self):
        """Рассчитывает включенные колонки"""
        return list(filter(lambda c: self.cols[c], self.cols.keys()))

    def __get_on_balance_columns(self):
        """Рассчитывает включенные колонки для баланса"""
        balance_cols = (
            Columns.BALANCE_START_CREDIT,
            Columns.BALANCE_START_DEBIT,
            Columns.BALANCE_END_CREDIT,
            Columns.BALANCE_END_DEBIT
        )
        return list(filter(lambda b: self.cols[b], balance_cols))

    def __get_on_ab_columns(self):
        """Рассчитывает включенные колонки для режима acc_balance"""
        ab_cols = (
            Columns.BALANCE_START_DEBIT,
            Columns.BALANCE_START_CREDIT,
            Columns.BALANCE_END_DEBIT,
            Columns.BALANCE_END_CREDIT,
            Columns.TURNOVER_DEBIT,
            Columns.TURNOVER_CREDIT
        )
        return list(filter(lambda c: self.cols[c], ab_cols))

    def __get_current_group(self):
        """
        Разбор текущей группы
        Примечание:
            Когда мы раскрываем 'account' узел надо понимать спускаться ли вниз по иерархии счетов или переходить
            к другой группе (проверяем, есть ли дети у текущего счета). При данной проверке мы проверяем
            - ближайших детей (ab_mode)
            - всех детей (dc_mode)
            На данном этапе проверки, dc_mode означает что:
            - отчет строился ранее по группе 'document' или
            - запрашивается коррекспондения или их аналитики
        Важно:
            dc_mode может включиться позже, после перехода на 'document' группу.
        """
        if self.parent_id:
            value_last_group, key_last_group = self.__get_last_group()
            current_group = None
            if all((key_last_group == 'account', self.account_hierarchy)):
                # проверка, есть ли дети еще
                children_accounts = self.__get_children_accounts([value_last_group])
                if children_accounts and not self._filter.Get('stop_falling'):
                    current_group = key_last_group
            elif 'face' in key_last_group:
                if self.analytics.is_hierarchical(key_last_group):
                    if any(self.analytics.children_by_parent(value_last_group)):
                        current_group = key_last_group
            current_group = current_group or self.__get_next_group(key_last_group)
        else:
            current_group = self.groups[0]

        if current_group in ('account', 'face1', 'face2', 'face3', 'face4', 'periodicity', 'organization', 'document'):
            return current_group
        raise sbis.Error('В фильтре детализации указана несуществующая группа: {}'.format(current_group))

    def _check_is_node(self):
        """Определяет какой признак parent@ проставлять для текущего списка, можем ли дальше проваливаться"""
        return 'NULL' if self.current_group == self.groups[-1] else 'TRUE'

    def __get_base_accounts(self):
        """Возвращает базовый набор счетов"""
        id_accounts = self.parents.get('account') or self._filter.Get(self.accounts_field) or []
        if not id_accounts:
            raise sbis.Warning(*('Отчет не может быть построен, необходимо указать счет.',) * 2)
        if not isinstance(id_accounts, (list, set, tuple)):
            id_accounts = [id_accounts]
        return self.__get_accounts_for_jv(id_accounts)

    def __get_id_accounts(self):
        """Возвращает идентификаторы базовых счетов"""
        return [rec.Get('acc') for rec in self.accounts]

    def __get_id_correct_accounts(self):
        """Возвращает идентификаторы 'корректных' счетов, с учетом знака"""
        return [-rec.Get('acc') if self.__is_neg_acc(rec) else rec.Get('acc') for rec in self.accounts]

    def __get_id_correct_accounts_ab(self):
        """
        Разбор набора счетов
        PS: для режима ab характерно:
        1. current_group == 'account' and hierarchy = False    Родители+Дети (минусов нет никогда)
        2. current_group == 'account' and hierarchy = True     Родители (с минусами если подходят под условие)
        3. current_group != 'account'
        """
        id_correct_accounts_ab = list(self.id_correct_accounts)
        if self.current_group != 'account' or self.account_hierarchy:
            id_correct_accounts_ab = list(
                filter(lambda acc: abs(acc) in self.id_accounts_without_doubles, self.id_correct_accounts))
        return id_correct_accounts_ab

    def __get_accounts_for_jv(self, id_accounts):
        """Возвращает список счетов"""
        accounts = None
        if self.parent_id:
            value_last_group, key_last_group = self.__get_last_group()
            if all((key_last_group == 'account', self.account_hierarchy, not self._filter.Get('stop_falling'))):
                id_accounts = [value_last_group]
                accounts = self.__get_children_accounts(id_accounts)
                if not accounts:
                    accounts = Accounts.get_acc_info(id_accounts)
        if not accounts:
            if not self.is_dc and any((self.current_group != 'account', self.account_hierarchy)):
                accounts = Accounts.get_acc_info(id_accounts)
            else:
                accounts = Accounts.get_all_children(id_accounts)
        return accounts

    def __is_neg_acc(self, rec):
        """Проверяет, является ли счет отрицательным"""
        is_node = rec.Get("Раздел@", False)
        # не уверен что is_hierarchy лучшее название
        is_hierarchy = not any((self.current_group == 'account', 'account' in self.parents)) or self.account_hierarchy
        return is_node and is_hierarchy

    def __get_id_accounts_with_child(self):
        """
        Получаем набор дочерних счетов
        Примечание: детей запрашиваем только для отрицательных счетов с account_balance
        """
        id_accounts_with_children = []
        if self.is_detail_turnovers:
            id_correct_accounts = list(map(abs, filter(lambda x: x < 0, self.id_correct_accounts)))
            accounts_with_children = Accounts.get_all_children(id_correct_accounts)
            id_accounts_with_children = self.id_correct_accounts + [a.Get('acc') for a in accounts_with_children]
        return id_accounts_with_children

    def __get_org_filter(self):
        """Разбор фильтра по организации"""
        faces_in_groupby = set(self.groups) & {'face1', 'face2', 'face3', 'face4', 'document'}
        need_zpl_check = self.is_personal_calc_acc and (any(self.analytics.basic) or faces_in_groupby)
        ab_org_filter = 'TRUE' + self.cached_rights.mass_get_org_ab_flt_70acc_rights_check(
            any(self.analytics.basic),
            need_zpl_check,
            self.organizations,
            self._filter.Get(self.access_zone_field),
            '',
            'COALESCE(org, 0)' if any(self.analytics.basic) else 'org',
        )
        if ab_org_filter == 'TRUE':
            # Если детализируем по чему-то другому, суперагрегаты нельзя использовать
            can_use_super_aggregates = self.current_group in ('face1', 'account', 'periodicity')
            if any(self.analytics.basic):
                # если в фильтре есть аналитики 2,3,4 не можем использовать суперагрегаты, они только по acc и fc1 есть
                if can_use_super_aggregates and not any(self.analytics.basic[1:4]):
                    ab_org_filter = 'COALESCE(org, 0) = 0'
                else:
                    ab_org_filter = 'COALESCE(org, 0) = ANY(ARRAY(SELECT "@Лицо" FROM "НашаОрганизация"))'
            else:
                if can_use_super_aggregates:
                    ab_org_filter = 'org IS NULL'
                else:
                    ab_org_filter = 'org IS NOT NULL'

        return ab_org_filter

    def __get_to_org_filter(self):
        """Разбор фильтра по организации"""
        to_org_filter = 'TRUE' + self.cached_rights.mass_get_org_filter(
            self.organizations,
            self._filter.Get(self.access_zone_field),
            '',
            'org'
        )

        return to_org_filter

    def __get_last_group(self):
        """Возвращает последнюю группу по parent_id"""
        return self.parent_id.split(',')[-1].split('@')

    def __get_next_group(self, group):
        """Возвращает следующую группу"""
        index_group = self.groups.index(group)
        next_group = None
        try:
            next_group = self.groups[index_group + 1]
        except IndexError:
            sbis.ErrorMsg('Ошибка получения следующей группы. group: {}, groups: {}, parent_id: {}'.format(
                group,
                self.groups,
                self.parent_id,
            ))
        return next_group

    def __get_children_accounts(self, id_accounts):
        """Возвращает набор из дочерних счетов"""
        id_accounts = tuple(id_accounts)
        if id_accounts:
            if id_accounts not in self.child_accounts:
                only_near_children = not self.is_dc
                if only_near_children:
                    self.child_accounts[id_accounts] = Accounts.get_near_children(id_accounts)
                else:
                    # может быть ситуация что вернется родительский счет (запрашиваемый)
                    self.child_accounts[id_accounts] = Accounts.get_all_children(id_accounts, without_parent=True)
        return self.child_accounts.get(id_accounts, [])

    def __get_format_date_group(self):
        """Вовзращает формат для колонок дат"""
        dt_group_col = Helpers.get_sql_for_date_group(self.periodicity_type)
        if not dt_group_col:
            detail_msg = 'Параметр group_by: {groups}, параметр periodicity_type: {periodicity_type}'.format(
                groups=self.groups,
                periodicity_type=self.periodicity_type,
            )
            user_msg = 'Непредвиденная ошибка. Предполагается построение отчета с периодичностью, но её шаг ' \
                       'не указан или указан некорректно. Попробуйте сбросить фильтр и набрать его снова.'
            raise sbis.Warning(detail_msg, user_msg)
        return dt_group_col

    def _get_group_col(self):
        """Вычисляет значения колонок на основе текущей группы"""
        groups_col = {
            'account': 'acc',
            'face1': 'fc1',
            'face2': 'fc2',
            'face3': 'fc3',
            'face4': 'fc4',
            'document': 'acc',
            'periodicity': 'dt',
            'organization': 'org',
        }
        groups_col_dc = {
            'account': 'dk."Счет"',
            'face1': 'dk."Лицо1"',
            'face2': 'dk."Лицо2"',
            'face3': 'dk."Лицо3"',
            'face4': 'dk."Лицо4"',
            'document': 'dk."Документ"',
            'periodicity': 'dk."Дата"',
            'organization': 'dk."НашаОрганизация"',
        }
        group_col = groups_col.get(self.current_group)
        group_col_dc = groups_col_dc.get(self.current_group)
        if self.current_group == 'periodicity':
            dt_group_col = self.__get_format_date_group()
            group_col = dt_group_col.format(dt_col='dt')
            group_col_dc = dt_group_col.format(dt_col='dk."Дата"')
        return group_col, group_col_dc

    def __get_corresponds_filter(self):
        """Возвращает фильты для корреспонденции"""
        ab_corresponds_cond = self.__get_filter_by_corr(self.analytics.basic, is_correspondence=False)
        dc_corresponds_cond = self.__get_filter_by_corr(self.analytics.corresponding, is_correspondence=True)
        return ab_corresponds_cond, dc_corresponds_cond

    def __get_filter_by_corr(self, analytics, is_correspondence):
        """Возвращает фильты для отчетов"""
        filter_fields = []
        table = 'dk2' if is_correspondence else 'dk'
        templates = {
            ('analytic', True): '{table}."Лицо{i}" IS NULL ',
            ('analytic', False): '{table}."Лицо{i}" IN ({acc}) ',
            ('document', True): '{table}."Документ" IS NULL ',
            ('document', False): '{table}."Документ" = {id_doc} ',
        }
        # фильтрация по аналитикам
        for i, _analytics in enumerate(analytics, 1):
            if _analytics:
                is_special_analytic = _analytics == ['-11']
                if not is_correspondence:
                    _analytics = {*_analytics, *self.analytics.children_by_group_num(i)}
                filter_fields.append(templates.get(('analytic', is_special_analytic)).format(
                    table=table,
                    i=i,
                    acc=', '.join(_analytics),
                ))
        if is_correspondence:
            filter_fields.append('{table}."Тип" IN (1, 2)'.format(
                table=table,
            ))

            if self.id_corr_accounts:
                filter_fields.append('{table}."Счет" IN ({acc})'.format(
                    table=table,
                    acc=', '.join(map(str, self.id_corr_accounts))
                ))
        id_doc = self.parents.get('document')
        if id_doc:
            date = self.parents.get('date_document')
            filter_fields.append(''' {table}."Дата" = '{date}' '''.format(
                table=table,
                date=date,
            ))
            is_special_analytic = id_doc == -11
            filter_fields.append(templates.get(('document', is_special_analytic)).format(
                table=table,
                id_doc=id_doc,
            ))
        else:
            filter_fields.append(''' {table}."Дата" BETWEEN '{date_start}' AND '{date_end}' '''.format(
                table=table,
                date_start=self.date_start,
                date_end=self.date_end,
            ))

        org_filter = 'TRUE' + self.cached_rights.mass_get_org_filter(
            self.organizations,
            self._filter.Get(self.access_zone_field),
            table,
        )
        filter_fields.append(org_filter)
        return filter_fields

    def __get_ab_filter(self):
        """Формирует фильтр для режима acc_balance"""
        filters = ['acc IN ({})'.format(', '.join(map(str, self.id_correct_accounts_ab)))]
        if self.only_currency:
            filters.append('curr IS NOT NULL')
        elif not self.flags[FLAG_CURRENCY]:
            filters.append('curr IS NULL')
        is_huge = self.___is_huge_filter()
        filters += self.__get_filter_by_analytics(is_huge)
        filters += self.__get_filter_by_null_analytics(is_huge)
        filters.append(self.ab_org_filter)
        return filters

    def ___is_huge_filter(self):
        """
        В исключительном случае для оптимизации используем частичный индекс.
        Если аналитика в фильтре всего одна
        Если в массиве очень много значений
        Если она не контрагент
        Если текущий разворот по этой самой аналитике
        """
        filled_analytics = [i for i in range(4) if self.analytics.basic[i]]
        if len(filled_analytics) == 1:
            filled_analytic_idx = filled_analytics[0]
            if len(self.analytics.basic[filled_analytic_idx]) >= HUGE_FILTER_THRESHOLD:
                if filled_analytic_idx not in Accounts.get_contractor_analytics(self.id_accounts):
                    if self.current_group == 'face{}'.format(filled_analytic_idx + 1):
                        return True
        return False

    def __get_filter_by_analytics(self, is_huge):
        """Возвращает фильтр по аналитикам"""
        filters = []
        for i in range(4):
            if self.analytics.basic[i]:
                analytics = {*self.analytics.basic[i], *self.analytics.children_by_group_num(i + 1)}
                if is_huge:
                    # обманываем оптимизатор, мы сами лучше знаем как надо
                    filters.append("(fc{0} = ANY('{{{1}}}')) IS TRUE AND fc{0} IS NOT NULL".format(
                        i + 1,
                        ', '.join(analytics)
                    ))
                else:
                    filters.append('COALESCE(fc{}, 0) IN ({})'.format(i + 1, ', '.join(analytics)))
            elif 'face{}'.format(i + 1) == self.current_group:
                if any(self.analytics.basic):
                    filters.append('COALESCE(fc{}, 0) <> 0 '.format(i + 1))
                else:
                    filters.append('fc{} IS NOT NULL '.format(i + 1))
        return filters

    def __get_filter_by_null_analytics(self, is_huge):
        """Возвращает фильтр по нулевым аналитикам"""
        filters = []
        null_analytics = ['fc{}'.format(i + 1) for i in range(4) if
                          not any((self.analytics.basic[i], 'face{}'.format(i + 1) == self.current_group))]
        if null_analytics:
            if any(self.analytics.basic) and not is_huge:
                filters += ['COALESCE({}, 0) = 0 '.format(i) for i in null_analytics]
            else:
                filters += ['ROW({}) IS NULL'.format(', '.join(null_analytics))]
        return filters

    def __get_dc_filter(self):
        """Формирует фильтр для режима dc (ДебетКредит)"""
        filter_ = ['dk."Счет" in ({acc})'.format(acc=', '.join(map(str, self.id_correct_accounts)))]
        if self.only_currency:
            filter_.append('dk."Валюта" IS NOT NULL')
        elif not self.flags[FLAG_CURRENCY]:
            filter_.append('dk."Валюта" IS NULL')
        return ' AND '.join((*self.ab_corresponds_cond, *filter_))

    def _get_filter_by_inter_turn(self):
        """Возвращает фильтр по внутренним оборотам для ДебетКредит"""
        templates = {
            # без фильтрации (дефолтное значение)
            None: '',
            # только внутрифирменные обороты
            False: 'AND dk."ВнутреннийОборот" IS NOT NULL',
            # только внешние обороты
            True: 'AND dk."ВнутреннийОборот" IS NULL',
        }
        return templates.get(self.exclude_internal_turnover)

    def _get_turnover_field_by_type(self):
        """
        Возвращает расчет поля оборотов в зависимости от типа оборота: внутренние (компания/холдинг) или внешние
        Прмечание: используется только при работе с таблицей acc_turnovers
        """
        templates = {
            # без фильтрации (дефолтное значение)
            None: 'suma',
            # только внутрифирменные обороты
            False: 'COALESCE(sumi, 0) + COALESCE(sumh, 0) suma',
            # только внешние обороты
            True: 'COALESCE(suma, 0) - COALESCE(sumi, 0) - COALESCE(sumh, 0) suma',
        }
        return templates.get(self.exclude_internal_turnover)

    def _get_saldo(self):
        """Возвращает сальдо"""
        saldo = sbis.Record()
        if self.on_balance_cols and self.current_group == 'document':
            saldo = self._get_report_from_ab().outcome
        return saldo

    def __get_all_accounts(self):
        """Получает все счета, используемые для корреспонденции"""
        all_accounts = None
        if self.is_detail_turnovers:
            all_accounts = Accounts.get_all_accounts()
            all_accounts.SetKeyName('acc')
        return all_accounts

    def _get_order(self, field: str, default_order, null_order=None, sum_fields=()):
        """
        Формирует приставки сортировки
        PS: учитывается, что по умолчанию поле может быть DESC отсортировано
        :param field: поле сортировки
        :param default_order: строка сортировки ASC/DESC
        :param null_order: строка сортировки NULL значений NULL FIRST/NULL FIRST/None
        :return: строка отсортированого поля
        """
        convert_order = {
            'ASC': 'DESC',
            'DESC': 'ASC',
        }
        convert_null_order = {
            'NULLS FIRST': 'NULLS LAST',
            'NULLS LAST': 'NULLS FIRST',
        }
        order = default_order
        if not self.asc_sort:
            order = convert_order.get(default_order)
            null_order = convert_null_order.get(null_order)

        if null_order is None:
            null_order = ''
            if field in sum_fields and not field.endswith('c'):
                field = f'COALESCE({field}, 0)'

        return '{field} {order} {null_order}'.format(
            field=field,
            order=order,
            null_order=null_order,
        )

    def __parse_debit_credit_array(self, record, is_result=False):
        """Разбирает данные из особых колонок, возвращенных sql запросом. В них информация об корреспонденции.
        is_result означает что в колонке с информацией о корр счетах дополнительно содержится информация о наличии
        оборотов, так как сумма оборотов может быть 0 (+5 и -5, например), а колонку показать при этом все равно нужно.
        Парсим и вставляем в record в понятном фронту формате
        """
        fields_by_flag = {
            FLAG_ACCOUNTING: '{field}n',
            FLAG_TAXING: '{field}t',
            FLAG_CURRENCY: '{field}c',
        }
        methods_by_flag = {
            FLAG_ACCOUNTING: (self.__get_sum_row, sbis.Record.AddMoney),
            FLAG_TAXING: (self.__get_sum_row, sbis.Record.AddMoney),
            FLAG_CURRENCY: (self.__get_curr_sum_row, sbis.Record.AddHashTable),
        }
        record.correspondence = sbis.Record()
        corr_arr = record.Get('corr_arr')
        if corr_arr:
            data_correspondence = self.__parse_correspondence(corr_arr, is_result)
            correspondence = self.__calc_correspondence(data_correspondence, is_result)
            correspondence_field = record.Get('correspondence')
            for field, value in correspondence.items():
                for flag in self.on_flags:
                    m_get_sum, m_add_field = methods_by_flag.get(flag)
                    sum_row = m_get_sum(value, flag, is_result)
                    if sum_row is not None:
                        name_field = fields_by_flag.get(flag).format(field=field)
                        m_add_field(correspondence_field, name_field, sum_row)

    @staticmethod
    def __parse_correspondence(corr_arr, is_result):
        """Парсит корреспонденцию"""
        data_correspondence = []
        debet_credit_arr = corr_arr.replace('},{', '@').replace('{', '').replace('}', '').split('@')
        for debet_credit_row in debet_credit_arr:
            if is_result:
                turn_type, account, suma, sumt, currency, has_turns_normal, has_turns_tax = debet_credit_row.split('|')
                has_turns_normal = bool(sbis.Money(has_turns_normal))
                has_turns_tax = bool(sbis.Money(has_turns_tax))
            else:
                turn_type, account, suma, sumt, currency, = debet_credit_row.split('|')
                has_turns_normal = True
                has_turns_tax = True

            data_correspondence.append(
                {
                    'account': int(account),
                    'turnover_type': int(turn_type),
                    'sum': sbis.Money(suma),
                    'sumt': sbis.Money(sumt),
                    'currency': currency,
                    'has_turns_normal': has_turns_normal,
                    'has_turns_tax': has_turns_tax,
                }
            )
        return data_correspondence

    def __calc_correspondence(self, data_correspondence, is_result):
        """Возвращает correspondence с учетом входных параметров отчета"""
        correspondence = {}
        fields_by_col = {
            Columns.TURNOVER_DEBIT_LEAF: 'К{acc_num}',
            Columns.TURNOVER_CREDIT_LEAF: 'Д{acc_num}',
            Columns.TURNOVER_DEBIT_NODE: '@К{acc_par_num}',
            Columns.TURNOVER_CREDIT_NODE: '@Д{acc_par_num}',
        }

        for item_corr in data_correspondence:
            account_number, account_parent_number = self.__get_account_correspondence(item_corr.get('account'))
            cols_to_create = set()
            for col in (
                Columns.TURNOVER_DEBIT_LEAF,
                Columns.TURNOVER_CREDIT_LEAF,
                Columns.TURNOVER_DEBIT_NODE,
                Columns.TURNOVER_CREDIT_NODE
            ):
                need_turnover_type = 2 if col in (Columns.TURNOVER_DEBIT_LEAF, Columns.TURNOVER_DEBIT_NODE) else 1
                if self.cols[col] and item_corr.get('turnover_type') == need_turnover_type:
                    field = fields_by_col.get(col).format(
                        acc_num=account_number,
                        acc_par_num=account_parent_number,
                    )
                    cols_to_create.add(field)
            for f_name in cols_to_create:
                current_branch = correspondence.setdefault(f_name, {}).setdefault(item_corr.get('currency'), {})
                current_branch.setdefault('vals', [0, 0])
                current_branch['vals'][0] += item_corr.get('sum')
                current_branch['vals'][1] += item_corr.get('sumt')
                if is_result and f_name[0] == '@':
                    # считаем количество уникальных детей, с которых мы собираем суммы в счете верхнего уровня
                    current_branch.setdefault('uniq_children', [set(), set()])
                    if account_number != account_parent_number:
                        if item_corr.get('sum') is not None:
                            current_branch['uniq_children'][0].add(account_number)
                        if item_corr.get('sumt') is not None:
                            current_branch['uniq_children'][1].add(account_number)
                current_branch.setdefault('has_turns', [False, False])
                current_branch['has_turns'][0] |= item_corr.get('has_turns_normal')
                current_branch['has_turns'][1] |= item_corr.get('has_turns_tax')
        # удаляем поля-дубли. Они могут появиться из-за неправильных проводок по корню
        # и тогда в результате получится, например К62: 100р и @К62: 999999 (с учетом себя и субсчетов)
        is_full_debit_turnover = all((self.cols[Columns.TURNOVER_DEBIT_LEAF], self.cols[Columns.TURNOVER_DEBIT_NODE]))
        is_full_credit_turnover = all((self.cols[Columns.TURNOVER_CREDIT_LEAF], self.cols[Columns.TURNOVER_CREDIT_NODE]))
        if any((is_full_debit_turnover, is_full_credit_turnover)):
            for field in correspondence.copy():
                if '@' + field in correspondence:
                    del correspondence[field]
            # Также удалим дубли другого рода. только для итогов (оптимизация)
            if is_result:
                self._clear_extra_columns(correspondence, is_full_debit_turnover, is_full_credit_turnover)
        return correspondence

    @staticmethod
    def _clear_extra_columns(correspondence, is_full_debit_turnover, is_full_credit_turnover):
        """
        При наличии оборота только по одному субсчету, информация по родителю
        будет дублирующей и избыточной. Например @К60: 100, @K60-01: 100. @К60 нужно удалить
        https://online.sbis.ru/opendoc.html?guid=5d91794a-ee21-48bb-9299-8550d09319e5
        """
        for field, value in correspondence.items():
            if field[0] == '@':
                if is_full_debit_turnover and field[1] == 'К' or is_full_credit_turnover and field[1] == 'Д':
                    # если детей всего один (как в примере) то такой родитель нам не нужен
                    # особая ситуация когда детей нет. Это значит что счет сам себя содержит. Обычное дело для 51го
                    # счета. Д51 и @Д51. Мы исключали таких детей выше. Удалять нельзя
                    threshold = 1
                    curr_uniq_children = set()
                    for curr, curr_val in value.items():
                        if curr_val:
                            # нужно собрать детей со всех валютных данных, потом посчитаем их
                            uniq_children = curr_val.get('uniq_children')
                            if curr != 'RUB':
                                curr_uniq_children |= uniq_children[0]
                            else:
                                # рубли
                                if len(uniq_children[0]) == threshold:
                                    # зануляем суммы
                                    curr_val['has_turns'][0] = False
                                    curr_val['vals'][0] = None
                                # налоги
                                if len(uniq_children[1]) == threshold:
                                    # зануляем суммы
                                    curr_val['has_turns'][1] = False
                                    curr_val['vals'][1] = None
                    if len(curr_uniq_children) == threshold:
                        for curr in list(value.keys()):
                            if curr != 'RUB':
                                del value[curr]

    def __get_account_correspondence(self, id_account):
        """Возвращает удобочитаемое представление счета для корреспонденции"""
        if id_account:
            account_by_id = self.all_accounts.RowByKey(id_account)
            acc_num = account_by_id.num
            acc_par_num = account_by_id.parent_num or account_by_id.num
        else:
            acc_num = '--'
            acc_par_num = '--'
        return acc_num, acc_par_num

    def __get_sum_row(self, value, flag, is_result):
        """Возвращает сумму корреспонденции"""
        type_acc = self.type_corr_accounting.get(flag)
        ruble = value.get('RUB', {})
        sum_row = ruble.get('vals', [0, 0])[type_acc]
        has_turnover = ruble.get('has_turns', [False, False])[type_acc]
        if (is_result and has_turnover) or sum_row:
            return sum_row
        return None

    def __get_curr_sum_row(self, value, flag, is_result):
        """Возвращает валютную сумму корреспонденции"""
        curr_value = {}
        for curr in value:
            if curr == 'RUB':
                continue
            type_acc = self.type_corr_accounting.get(flag)
            sum_row = value[curr]['vals'][type_acc]
            has_turnover = value[curr]['has_turns'][type_acc]
            if (is_result and has_turnover) or sum_row:
                curr_value[curr] = sum_row
        # пустые колонки не нужны, вернем None
        return curr_value or None

    @staticmethod
    def _get_curr_order(field):
        """Возвращает шаблон для сортировки по полю валютного счета"""
        return CURR_ORDER_SQL.format(
            field=field,
        )

    def __results_row_correspondence(self, outcome):
        """Заполняет переданный Record (предполагается что это строка итогов) данными с корреспонденцией"""
        if 'corr_arr' not in outcome:
            outcome.AddString('corr_arr')
        if any(self.analytics.basic) or any(self.analytics.corresponding):
            get_corr = self.__get_corr_with_analytics
        else:
            get_corr = self.__get_corr_without_analytics
        outcome.corr_arr = get_corr()

    def __get_corr_without_analytics(self):
        """Возвращает набор корреспонденций без аналитик"""
        sql = CORR_WITHOUT_ANALYTICS_SQL.format(
            base_cte_account=BASE_CTE_ACCOUNT,
            base_acc_turnover=self._get_base_acc_turnover(ab_account=False)
        )
        return sbis.SqlQueryScalar(
            sql,
            self.id_accounts_with_children,
            self.date_start,
            self.date_end,
        )

    def _get_base_acc_turnover(self, ab_account):
        """Формирует базовый запрос в acc_turnover"""
        if ab_account:
            join_acc_field = 'ps.child'
            filter_acc_debit = filter_acc_credit = 'ps.top = ABS(real_id) AND'
        else:
            join_acc_field = 'ps.acc'
            if self.id_corr_accounts:
                corr_acc = ','.join(map(str, self.id_corr_accounts))
                filter_acc_debit = 'adb IN ({}) AND'.format(corr_acc)
                filter_acc_credit = 'acr IN ({}) AND'.format(corr_acc)
            else:
                filter_acc_debit = filter_acc_credit = ''

        return BASE_ACC_TURNOVER.format(
            acc_list=', '.join(map(str, self.id_accounts_with_children)),
            date_st=self.date_start,
            date_end=self.date_end,
            join_acc_field=join_acc_field,
            filter_acc_debit=filter_acc_debit,
            filter_acc_credit=filter_acc_credit,
            sum_field=self._get_turnover_field_by_type(),
            org_flt=self.to_org_filter,
        )

    def __get_corr_with_analytics(self):
        """Возвращает набор корреспонденций с аналитиками"""
        sql = CORR_WITH_ANALYTICS_SQL.format(
            base_cte_account=BASE_CTE_ACCOUNT,
            ab_corresponds_cond=' AND ' + ' AND '.join(self.ab_corresponds_cond or ['TRUE']),
            dc_corresponds_cond=' AND '.join(self.dc_corresponds_cond or ['TRUE']),
            type_join=' ' if self.is_corr_request else 'LEFT',
            filter_by_internal_turnover=self._get_filter_by_inter_turn(),
        )
        return sbis.SqlQueryScalar(
            sql,
            self.id_accounts_with_children,
            self.date_start,
            self.date_end,
        )

    def __calc_curr_types(self, record):
        """Вычисляет список валют, использующихся при построении отчета"""
        fields_template = [
            'bdbs{postfix_ab}',
            'bcrs{postfix_ab}',
            'bdbe{postfix_ab}',
            'bcre{postfix_ab}',
            'vdb{postfix_ab}',
            'vcr{postfix_ab}',
        ]
        curr_fields = [ft.format(postfix_ab=self.postfixes_ab.get(FLAG_CURRENCY)) for ft in fields_template]
        all_curr = set()
        for field in curr_fields:
            list_curr = (record.Get(field) or {}).keys()
            all_curr.update(list_curr)
        curr_types = {curr_type: self.currencies.get(curr_type) for curr_type in all_curr}
        record['curr_types'].From(curr_types)

    def __add_zero_curr(self, outcome):
        """Добавляет нулевые валюты в каждое поле, если встречаются в других полях"""
        curr_types = outcome.Get('curr_types')
        fields_template = [
            'bdbs{postfix_ab}',
            'bcrs{postfix_ab}',
            'bdbe{postfix_ab}',
            'bcre{postfix_ab}',
            'vdb{postfix_ab}',
            'vcr{postfix_ab}',
        ]
        curr_fields = [ft.format(postfix_ab=self.postfixes_ab.get(FLAG_CURRENCY)) for ft in fields_template]
        for curr_field in curr_fields:
            if curr_field in outcome:
                value = outcome.Get(curr_field) or {}
                value = {k: value.get(k, 0) for k in curr_types}
                outcome[curr_field] = value

    def _post_processing(self, record_set):
        """Пост обработка. В основном создает пустые колонки, которые по каким-то причинам не заполнились в основных
        методах."""
        # set navigation result
        if len(record_set) == self.limit_on_page:
            record_set.DelRow(self.limit_on_page - 1)
            record_set.nav_result = sbis.NavigationResultBool(True)
        else:
            record_set.nav_result = sbis.NavigationResultBool(False)

        self.__add_fields_to_result(record_set)
        self.__add_correspondence(record_set)
        record_set.AddColHashTable('curr_types')
        record_set.outcome.AddHashTable('curr_types')
        # set correct name
        for rec in record_set:
            new_name = self.__get_name_result(rec)
            rec['Название'].From(new_name)
            self.__calc_curr_types(rec)
        self.__calc_curr_types(record_set.outcome)
        self.__add_zero_curr(record_set.outcome)

        result_format = record_set.Format()
        for order_field in ('for_order_1', 'for_order_2', 'for_order_3'):
            if order_field in result_format:
                record_set.DelCol(order_field)

        return record_set

    @staticmethod
    def __add_fields_to_result(record_set):
        """Добавляет поля, необходимые для клиента"""
        fields = (
            ('special', sbis.RecordSet.AddColBool),
            ('code', sbis.RecordSet.AddColString),
            ('address', sbis.RecordSet.AddColString),
            ('regclass', sbis.RecordSet.AddColString),
            ('Номер', sbis.RecordSet.AddColString),
        )
        outcome_fields = (
            ('actual_date', sbis.Record.AddDate),
            ('wh_actual_text', sbis.Record.AddString),
        )
        if record_set.outcome is None:
            record_set.outcome = sbis.Record()
        _format = record_set.Format()
        _outcome_format = record_set.outcome.Format()
        for field, func in fields:
            if field not in _format:
                func(record_set, field)
        for field, func in outcome_fields:
            if field not in _outcome_format:
                func(record_set.outcome, field)

    def __add_correspondence(self, record_set):
        """Добавляет корреспонденцию к результату"""
        if self.is_detail_turnovers:
            record_set.AddColRecord('correspondence')
            record_set.outcome.AddRecord('correspondence')
            self.__results_row_correspondence(record_set.outcome)
            self.__parse_debit_credit_array(record_set.outcome, is_result=True)
            outcome_rec_field_names = record_set.outcome.correspondence.GetFieldNames()
            outcome_correspondence = record_set.outcome.correspondence
            for col_name in outcome_rec_field_names:
                if col_name[-1] == 'c':
                    record_set.AddColHashTable(col_name)
                    record_set.outcome.AddHashTable(col_name, outcome_correspondence.Get(col_name))
                else:
                    record_set.AddColMoney(col_name)
                    record_set.outcome.AddMoney(col_name, outcome_correspondence.Get(col_name))
            for rec in record_set:
                self.__parse_debit_credit_array(rec)
                current_correspondence = rec.Get('correspondence')
                if current_correspondence:
                    for col in outcome_rec_field_names:
                        rec.Set(col, current_correspondence.Get(col))

    def __get_name_result(self, rec):
        """Возвращает корректное название строки отчета"""
        name = rec.Get('Название')
        if self.current_group == 'periodicity' and self.periodicity_type == 'month':
            return Helpers.get_month_name(name)
        return get_formated_face_name(name, (rec.Get('regclass') or '').strip('"'))
