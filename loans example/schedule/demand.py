"""
Модуль отвечает за построение графика с погашением по требованию
"""


__author__ = 'Glukhenko A.V.'


from collections import defaultdict

import sbis
from loans.loanConsts import LC
from .real import RealPaymentSchedule


class ShowPaymentsForSchedule(RealPaymentSchedule):
    """Класс строит график платежей с погашением по требованию"""
    def __init__(self, _filter, navigation):
        super().__init__(_filter, navigation)

    def build(self):
        """Построение графика платежей"""
        return super().build()

    def _build_schedule(self):
        """
        Строит график по займу
        PS: при расчете признака is_payment учитывается что даты у плана и факта может совпасть, в таком случае
        сначала надо обработать платеж, а потом плановую запись.
        """
        debt = sbis.Money() if self._is_registered() else self._filter.Get('SizePayment')

        payment_dates = list(self.payments.keys())
        payment_dates.sort()
        period_rows = defaultdict(list)

        for payment_date in payment_dates:
            self._processing_payment(payment_date, payment_date, debt, period_rows)
            row = self.__get_row_debt(period_rows)
            if row:
                debt = row.Get('ОстатокДолга')

        for row in period_rows[LC.SCHEDULE_PAYMENT]:
            self.result.AddRow(row)

    def __get_row_debt(self, period_rows):
        """
        Возвращает строку графика, от которой нужно взять остаток долга
        :param period_rows: записи графика по плановому периоду, defaultdict
        """
        if period_rows[LC.SCHEDULE_CORRECTION]:
            return period_rows[LC.SCHEDULE_CORRECTION][0]

        delay_row = period_rows[LC.SCHEDULE_DELAY][0] if period_rows[LC.SCHEDULE_DELAY] else None
        payment_row = period_rows[LC.SCHEDULE_PAYMENT][-1] if period_rows[LC.SCHEDULE_PAYMENT] else None
        return payment_row or delay_row
