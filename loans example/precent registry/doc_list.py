"""
Модуль для настройки доклиста
"""


__author__ = 'Glukhenko A.V.'

import abc
import datetime

import sbis
import doclist
import loans.loansCommon as cmn
from loans.loanConsts import LC
from loans.loanDBConsts import LCDB
from loans.version_loans import get_date_build
from .const import FORWARD, BACKWARD, LIST_PERCENT_FACT
from .helpers import is_last_month_day


class PercentsDocListMaker(metaclass=abc.ABCMeta):
    """Класс конструктор доклиста"""
    def __init__(self, filter_rec, navigation, doc_type, method_name):
        # поскольку необходимо сохранить фильтр в дочерних классах неизменным, то пересоздаем фильтр,
        # необходимый для док листа (тем самым убирается корректировка через __is_all_our_org_or_our_company)
        self.filter_rec = sbis.Record(filter_rec)
        self.navigation = navigation
        self._lcdb = LCDB()
        self._doc_type = doc_type
        self._org_id = self.filter_rec.Get(LC.FLD_FILTER_OUR_ORG, None)
        # Полное название списочного метода
        self.__method = self._doc_type + '.' + method_name
        self.is_first_page = self.filter_rec.Get('is_first_page')
        self.today = get_date_build() or datetime.date.today()

    def __is_all_our_org_or_our_company(self):
        if self._org_id is not None:
            if self._org_id == LC.ALL_OUR_ORG or self._org_id == LC.OUR_COMPANY:
                self.filter_rec.Remove(LC.FLD_FILTER_OUR_ORG)
                return True
        return False

    def __get_all_our_org_where_text(self):
        filter_by_org = None
        our_comp_id = self._lcdb.our_company_id()
        if our_comp_id is not None:
            filter_by_org = '"Д"."ДокументНашаОрганизация" != {0}::integer'.format(our_comp_id)
        return filter_by_org

    def __get_doc_list_where_text(self):
        if self.__is_all_our_org_or_our_company():
            if self._lcdb.is_accounting_used() and self._org_id == LC.ALL_OUR_ORG:
                return self.__get_all_our_org_where_text()
        return None

    def __get_cursor_filter(self):
        """Фильтр по курсору навигации"""
        direction = self.navigation.Direction()
        id_doc_position, date_doc_position = self.__get_data_cursor()

        if all((id_doc_position, date_doc_position)):
            if self.is_first_page:
                sign_by_directions = {FORWARD: '<=', BACKWARD: '>'}
            else:
                sign_by_directions = {FORWARD: '<', BACKWARD: '>'}
            cursor_filter = '''
                ("Д"."Дата", "Д"."@Документ") {sign} ('{date_doc_position}'::DATE, {id_doc_position}::INT)
            '''
        else:
            sign_by_directions = {FORWARD: '<=', BACKWARD: '>'}
            cursor_filter = '''
                ("Д"."Дата"::date {sign} '{date}'::date)
            '''

        return cursor_filter.format(
            sign=sign_by_directions.get(direction),
            date_doc_position=date_doc_position,
            id_doc_position=id_doc_position,
            date=self.today,
        )

    def __get_data_cursor(self):
        """Возвращает данные курсора"""
        position = self.navigation.Position()
        if position:
            id_doc_position = position.Get('id_doc')
            date_doc_position = position.Get('date_doc')
        else:
            id_doc_position = None
            date_doc_position = None
        return id_doc_position, date_doc_position

    def _get_search_mask_strategy(self):
        return None

    def __create_doc_list(self):
        """Создает объект доклиста"""
        self.filter_rec.AddBool('ФильтрБезЧерновиков', True)
        doc_list = doclist.DocList(self.filter_rec, self.navigation, '', '', LC.DOCLIST_OPTIONS,
                                   self._get_search_mask_strategy())
        _filters = [
            self.__get_doc_list_where_text(),
            self.__get_cursor_filter(),
        ]
        _filters = filter(None, _filters)
        if _filters:
            doc_list.whereText += ' AND ' + ' AND '.join(_filters)
        return doc_list

    def _prepare_sql(self, doc_list):
        """
        Добавляет необходимые поля в запрос доклиста
        :param doc_list: объект доклиста
        """
        pass

    def _post_processing(self, doc_list):
        """
        Постобработка списка документов
        :param doc_list: объект доклиста
        """
        doc_list.rsPtr.AddColRecord('РП.Документ')
        cmn.fill_rp_document(doc_list.rsPtr)
        self.__set_last_accruals(doc_list.rsPtr)

    def get_documents(self):
        """Возвращает список документов"""
        doc_list = self.__create_doc_list()
        self._prepare_sql(doc_list)
        doc_list.CreateAndExecuteSql(self.__method)
        self._post_processing(doc_list)
        return doc_list

    def __set_last_accruals(self, rs):
        """
        Устанавливает признак ПоследниеНачисления для набора документов
        :param rs: набор документов
        """
        if rs:
            count_link_docs = self.__get_count_link_docs(rs)
            for rec in rs:
                if rec.Get('ТипЗаписи') == LIST_PERCENT_FACT:
                    id_doc = rec.Get('@Документ')
                    _count_link_docs = count_link_docs.get(id_doc)
                    rec['ПоследниеНачисления'] = self.__is_last_accruals(rec, _count_link_docs)

    @staticmethod
    def __get_count_link_docs(rs):
        """
        Возвращает количество документов оснований по документам начислений
        :param rs: набор документов начислений
        """
        _filter = sbis.Record({'Route': 'up'})
        count_link_docs = sbis.Документ.КоличествоОснований(rs, _filter)
        return {rec.Get('@Документ'): rec.Get('Количество') for rec in count_link_docs}

    @staticmethod
    def __is_last_accruals(doc_percent, count_link_docs):
        """
        Проверяем, является ли документ начисления процентов последним, т.е. процедура окончательной выплаты по займу.
        :param doc_percent: документ начисления процентов
        :param count_link_docs: количество документов оснований у документа начисления процентов
        PS: документ считаем последним, если КоличествоОснований == 1 и дата != последнего дня месяца
        """
        date = doc_percent.Get('Дата')
        return all((
            count_link_docs == 1,
            not is_last_month_day(date),
        ))
