"""
Модуль для построения списка существующих документов начисления процентов.
"""


__author__ = 'Glukhenko A.V.'

import sbis
from loans.loanConsts import LC
from .doc_list import PercentsDocListMaker
from .const import LIST_PERCENT_FACT


class PercentsListNew(PercentsDocListMaker):
    """Класс отвечает за формирование списка фактических документов начисления процентов на основании доклиста"""
    def __init__(self, _filter, navigation, doc_type, result_format):
        super().__init__(_filter, navigation, doc_type, 'СписокЛесенка')
        self._filter = _filter
        self.navigation = navigation
        self.doc_type = doc_type
        self._doc_ids = []
        self.debts = {}

        self.sum_docs = {}
        self.result = sbis.RecordSet(result_format)

    def _prepare_sql(self, doc_list):
        """
        Добавляет необходимые поля в запрос доклиста
        :param doc_list: объект доклиста
        """
        doc_list.additionalFields = f'''
            NULL::text AS "{LC.FLD_FACE1_NAMES_LIST}",
            0::numeric(32,2) AS "{LC.FLD_PERCENTS_ISSUED_LOANS}",
            0::numeric(32,2) AS "{LC.FLD_PERCENTS_RECEIVED_LOANS}",
            NULL AS "РП.Документ",
            "ДР"."ДатаВремяСоздания" AS "ДР.ДатаВремяСоздания",
            {LIST_PERCENT_FACT} "ТипЗаписи"
        '''
