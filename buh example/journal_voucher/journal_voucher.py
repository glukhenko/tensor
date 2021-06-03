"""
Глухенко А.В.
Реализация обертки ЖурналаОрдера
"""

import sbis
from common_utils import cached_property
from .journal_voucher_ab import JournalVoucherAB
from .journal_voucher_dc import JournalVoucherDC
from ..helpers import digit_with_capacity

LIMIT_RECORD_EXPAND_REPORT = 1000
LIMIT_RECORD_EXPAND_REPORT_XML = 10000

MSG_ERROR_EXPORT = 'Невозможно вывести отчет на экран, в нем более {} строк. Выгрузите отчет в файл.'.format(
    digit_with_capacity(LIMIT_RECORD_EXPAND_REPORT)
)


class JournalVoucher(JournalVoucherAB, JournalVoucherDC):
    """Отчет ЖурналОрдер (обертка)"""

    def __init__(self, _filter, navigation):
        super().__init__(_filter, navigation)
        self.order_counter = 0
        self.nodes = {}
        # для разворота. тут лежит порядковый номер строки внутри своего раздела
        self.sort_cols = {}

        self.structure_order = {
            # int
            'Документ': sbis.RecordSet.AddColInt32,
            'НашаОрганизация': sbis.RecordSet.AddColInt32,
            'Лицо1': sbis.RecordSet.AddColInt32,
            'Лицо2': sbis.RecordSet.AddColInt32,
            'Лицо3': sbis.RecordSet.AddColInt32,
            'Лицо4': sbis.RecordSet.AddColInt32,
            'Счет': sbis.RecordSet.AddColInt32,
            # date
            'Дата': sbis.RecordSet.AddColDate,
            'doc_id': sbis.RecordSet.AddColInt32,
            # str
            'Номер': sbis.RecordSet.AddColString,
            'Название': sbis.RecordSet.AddColString,
            # money
            'bdbs': sbis.RecordSet.AddColMoney,
            'bcrs': sbis.RecordSet.AddColMoney,
            'bdbst': sbis.RecordSet.AddColMoney,
            'bcrst': sbis.RecordSet.AddColMoney,
            'vdb': sbis.RecordSet.AddColMoney,
            'vcr': sbis.RecordSet.AddColMoney,
            'vdbt': sbis.RecordSet.AddColMoney,
            'vcrt': sbis.RecordSet.AddColMoney,
            'bdbe': sbis.RecordSet.AddColMoney,
            'bcre': sbis.RecordSet.AddColMoney,
            'bdbet': sbis.RecordSet.AddColMoney,
            'bcret': sbis.RecordSet.AddColMoney,

            'bdbs_result': sbis.RecordSet.AddColMoney,
            'bcrs_result': sbis.RecordSet.AddColMoney,
            'bdbst_result': sbis.RecordSet.AddColMoney,
            'bcrst_result': sbis.RecordSet.AddColMoney,
            'vdb_result': sbis.RecordSet.AddColMoney,
            'vcr_result': sbis.RecordSet.AddColMoney,
            'vdbt_result': sbis.RecordSet.AddColMoney,
            'vcrt_result': sbis.RecordSet.AddColMoney,
            'bdbe_result': sbis.RecordSet.AddColMoney,
            'bcre_result': sbis.RecordSet.AddColMoney,
            'bdbet_result': sbis.RecordSet.AddColMoney,
            'bcret_result': sbis.RecordSet.AddColMoney,
            # money curr
            'vdbc': sbis.RecordSet.AddColHashTable,
            'vcrc': sbis.RecordSet.AddColHashTable,
            'bdbsc': sbis.RecordSet.AddColHashTable,
            'bcrsc': sbis.RecordSet.AddColHashTable,
            'bdbec': sbis.RecordSet.AddColHashTable,
            'bcrec': sbis.RecordSet.AddColHashTable,

            'bdbsc_result': sbis.RecordSet.AddColHashTable,
            'bcrsc_result': sbis.RecordSet.AddColHashTable,
            'vdbc_result': sbis.RecordSet.AddColHashTable,
            'vcrc_result': sbis.RecordSet.AddColHashTable,
            'bdbec_result': sbis.RecordSet.AddColHashTable,
            'bcrec_result': sbis.RecordSet.AddColHashTable,
        }

    def get_report(self):
        """Основной метод. Строит отчет. Возвращает RecordSet"""
        if not self.accounts:
            return sbis.RecordSet()
        if self._filter.Get('ExpandAll'):
            return self.get_expand_report()
        rs = self._post_processing(self._get_data())
        # если rs пустой, попробуем просто построить следующий уровень детализации
        if all((
                not rs,                                 # если ничего не нашли
                not self._filter.Get('stop_falling'),   # ещё не пытались прекратить провал в счета
                self.current_group == 'account',        # текущая группа - счета
                self.account_hierarchy,                 # влючена иерархия по счетам
                self.current_group != self.groups[-1],  # есть куда провалиться помимо счетов
                self.parent_id                          # а мы вообще проваливаемся? если нет, то зачем прекращать его
        )):
            # копируем фильтр
            filter_copy = sbis.Record(self._filter)
            # ставим признак что провал будем пытаться делать не в субсчета, в следующий уровень детализации
            filter_copy.AddBool('stop_falling', True)
            return JournalVoucher(filter_copy, self.navigation).get_report()
        return rs

    def _correct_nav_expand_order(self, size_order=0):
        """Вносит корректировки в навигацию в развернутом отчете"""
        count_on_page = self._get_limit_expand_report() - size_order
        if count_on_page < 0:
            if not self.is_xml_export:
                raise sbis.Warning(MSG_ERROR_EXPORT, MSG_ERROR_EXPORT)
            else:
                self.navigation = None
        else:
            self.navigation = sbis.Navigation(count_on_page + 1, 0, True)

    def get_expand_report(self):
        """Основной метод. Строит развернутый отчет. Возвращает RecordSet"""
        if 'ExpandAll' in self._filter:
            self._filter.Remove('ExpandAll')
        # корректировать первый вызов будем только если детализаций несколько
        if len(self.groups) > 1:
            self._correct_nav_expand_order()
        result = sbis.BuhReports.GetJournalVoucher(None, self._filter, None, self.navigation)
        self.__convert_real_id(result)

        self._add_inner_report([], result)

        if self.nodes:
            while self.nodes:
                level = max(self.nodes)
                parent = self.nodes[level].pop(0)
                if not self.nodes[level]:
                    self.nodes.pop(level)

                if 'parent' not in self._filter:
                    self._filter.AddString('parent')
                self._filter['parent'] = parent
                self._correct_nav_expand_order(size_order=len(result))

                if self.navigation:
                    rs = sbis.BuhReports.GetJournalVoucher(None, self._filter, None, self.navigation)
                    self._add_inner_report(result, rs)

                len_result = len(result)
                if len_result > self._get_limit_expand_report():
                    if self.is_xml_export:
                        # обрежем до limit
                        for i in range(len_result, self._get_limit_expand_report(), -1):
                            result.DelRow(i - 1)
                        result.nav_result = sbis.NavigationResultBool(True)
                        break
                    else:
                        raise sbis.Warning(MSG_ERROR_EXPORT, MSG_ERROR_EXPORT)
            result.SortRows(self.sort_by_branches)

        return result

    def __convert_real_id(self, result):
        """Конвертирует поле real_id в строку"""
        result_format = result.Format()
        if 'real_id' not in result_format:
            result.AddColString('real_id')
        else:
            result.RenameField('real_id', 'real_id_old')
            result.AddColString('real_id')
            for rec in result:
                rec['real_id'] = str(rec.Get('real_id_old')) if rec.Get('real_id_old') else None
            result.DelCol('real_id_old')

    def __correct_structure(self, result, new_rs):
        """Корректировка формата результата, по новому набору данных"""
        if not result:
            return

        result_fields = []
        if result:
            result_format = result.Format()
            result_fields = sbis.Record(result_format).GetFieldNames()
        new_rs_fields = []
        if new_rs:
            new_rs_format = new_rs.Format()
            new_rs_fields = sbis.Record(new_rs_format).GetFieldNames()

        for field in set(new_rs_fields) - set(result_fields):
            add_func = self.structure_order.get(field)
            if add_func:
                add_func(result, field)
            else:
                sbis.WarningMsg('Не найдена функция для поля: {}'.format(field))

    def __correct_correspondence_field(self, result, new_rs):
        """ добавляет в result отсутствующие колонки с корреспонденцией """
        res_format = result.Format()
        for fld in new_rs.Format():
            fld_name = fld.Name()
            if fld_name not in res_format:
                res_format.Add(fld_name, fld.Type(), 0)
        result.Migrate(res_format)

        outcome_correspondence = result.outcome.Get('correspondence')
        new_correspondence = new_rs.outcome.Get('correspondence')
        if outcome_correspondence and new_correspondence:
            for fld in new_correspondence.Format():
                fld_name = fld.Name()
                if fld_name not in outcome_correspondence:
                    # добавляем в поле correspondence
                    outcome_correspondence.AppendField(fld_name, fld.Type(), 0)
                    # и просто в outcome тоже. Такие требования фронта
                    result.outcome.AppendField(fld_name, fld.Type(), 0)

    @cached_property
    def is_xml_export(self):
        """Проверяет, выгружается ли отчет в excel"""
        return 'GetReportInExcelFromObject' in sbis.Session.TaskMethodName() or self.is_printing

    def _get_limit_expand_report(self):
        """Возвращает количество строк отчета, которые можно сформировать в развернутом отчете"""
        limit = LIMIT_RECORD_EXPAND_REPORT
        if self.is_xml_export:
            limit = LIMIT_RECORD_EXPAND_REPORT_XML
        return limit

    def _log_inner_report(self, current_rs, total_rs):
        """Служебное логирование, для развертывания ЖО"""
        if self.order_counter == 0:
            sbis.LogMsg('[BASE] rows: {}, [RESULT] {} rows'.format(
                len(current_rs), len(total_rs)
            ))
        else:
            sbis.LogMsg('[STEP {}] rows: {}, [RESULT] {} rows'.format(
                self.order_counter, len(current_rs), len(total_rs)
            ))
        self.order_counter += 1

    def _add_inner_report(self, result, rec_set):
        """
        Добавляет к результату данные по вложенному отчету
        Сначала строим список в корне, затем разворачиваем по очереди каждую ветку
        """
        self.__correct_structure(result, rec_set)

        for i, rec in enumerate(rec_set):
            self._set_sort_cols(i, rec)
            if rec.Get('parent@'):
                id_node = rec.Get('id')
                level = len(id_node.split(','))
                if level not in self.nodes:
                    self.nodes[level] = []
                self.nodes[level].append(id_node)
            if result:
                self.__correct_correspondence_field(result, rec_set)
                result.AddRow(rec)

        # отладочное логирование
        # self._log_inner_report(rec_set, result)

    def _set_sort_cols(self, i, rec):
        """
        заполняет колонку для сортировки
        """
        id_node = rec.Get('id')
        self.sort_cols[id_node] = i

    def sort_by_branches(self, left, right):
        """Сортируем отчет с учетом вложенности"""
        left_parent = left.Get('parent')
        left_parent = left_parent.split(',') if left_parent else []
        right_parent = right.Get('parent')
        right_parent = right_parent.split(',') if right_parent else []

        # если предок у записей один, то просто сравним наши записи
        if left_parent == right_parent:
            return self.sort_cols[left.Get('id')] < self.sort_cols[right.Get('id')]

        # ищем общего предка
        common_level = 0
        for i in range(min(len(left_parent), len(right_parent))):
            if left_parent[i] != right_parent[i]:
                break
            common_level = i + 1

        # теперь нужно взять sort_cols у тех записей, которые лежат в этом общем предке и сравнить
        # берем не общего предка (он же один, чего его сравнивать), а следующего по иерархии, поэтому +1
        x = common_level + 1

        # иногда следующая после предка запись это сама исходная запись (проверка по len(left_parent))
        left_cmp = self.sort_cols[left.Get('id') if len(left_parent) < x else ','.join(left_parent[0:x])]
        right_cmp = self.sort_cols[right.Get('id') if len(right_parent) < x else ','.join(right_parent[0:x])]

        if left_cmp == right_cmp:
            # похоже что записи вложены. Родитель идет раньше
            return len(left_parent) < len(right_parent)

        return left_cmp < right_cmp

    def _get_data(self):
        """Вычисляет на основе входных параметров отчета какой из методов построения отчета будет вызван и вызывает
        его"""
        if self.is_dc:
            if self.current_group == 'account' and self.account_hierarchy:
                return self._get_report_from_dc_acc_hier()
            elif 'face' in self.current_group and self.analytics.is_hierarchical(self.current_group):
                return self._get_report_from_dc_analytics_hier()
            else:
                return self._get_report_from_dc()
        else:
            if self.current_group == 'periodicity':
                return self._get_report_from_ab_by_period()
            elif 'face' in self.current_group and self.analytics.is_hierarchical(self.current_group):
                return self._get_report_from_ab_analytics_hier()
            else:
                return self._get_report_from_ab()
