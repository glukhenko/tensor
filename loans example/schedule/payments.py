"""
Модуль отвечает за получение документов выдачи займов, начислений процентов и погашений
"""

__author__ = 'Glukhenko A.V.'


import sbis
from loans.loanConsts import LC
from .sql import LIST_PAYMENTS


class Payments:
    """
    Расчет списка платежей по договору займа
    """
    def __init__(self, _filter, lcdb):
        self._filter = _filter
        self.lcdb = lcdb
        self.is_issued = self.lcdb.isIssuedLoanTypeByID(self._filter.Get('TypeDoc'))
        self.debt_field = 'ДебетДолг' if self.is_issued else 'КредитДолг'

    def _is_valid_filter(self):
        """Проверяет валидность фильтра, для получения данных о платежах"""
        return all((
            self._filter.Get('DateBegin'),
            self._filter.Get('IdOrganization'),
            self._filter.Get('IdFaceLoan'),
        ))

    def get_list(self):
        """
        Возвращает список документов, связанных с займов, а именно: выдачи, начисления процентов и погашения
        """
        docs = self.__get_docs()
        disbursements = {}
        percents = {}
        payments = {}

        for i in range(docs.Size()):
            if docs.Get(i, 'Платеж'):
                payments[docs.Get(i, 'Дата')] = docs[i]
            elif docs.Get(i, self.debt_field):
                disbursements[docs.Get(i, 'Дата')] = docs[i]
            else:
                percents[docs.Get(i, 'Дата')] = docs[i]

        return (
            disbursements,
            percents,
            payments,
        )

    def __get_docs(self):
        """Возвращает данные о платежах и выдачах денег"""
        payments = sbis.RecordSet()
        if self._is_valid_filter():
            payments = sbis.SqlQuery(
                LIST_PAYMENTS,
                self.lcdb.accounts_ids(),
                self._filter.Get('IdOrganization'),
                self._filter.Get('IdFaceLoan'),
                self.lcdb.debt_analytic(),
                self.lcdb.percent_analytic(),
                self._filter.Get('DateBegin'),
                LC.MAX_DATE,
                self.is_issued,
            )
        return payments
