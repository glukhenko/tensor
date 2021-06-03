"""
Модуль отвечает за построение нового графика начислений с просрочками и сгруппированными платежами
"""


__author__ = 'Glukhenko A.V.'


from loans.loanConsts import LC
from .demand import ShowPaymentsForSchedule
from .deposit import DepositPaymentSchedule
from .real import RealPaymentSchedule


class PaymentSchedule:
    """
    Расчет нового графика платежей по договору займа.
    """
    def __init__(self, _filter, navigation):
        self._filter = _filter
        self.navigation = navigation

    def get_schedule(self):
        """
        Возвращает график платежей
        Примечание: депозиты и погашение по требованию имеют расхождения с RealPaymentSchedule, поэтомустроятся
        через дочерние классы. Остальные же графики платежей строятся по RealPaymentSchedule
        """
        schedules = {
            LC.REPAYMENT_ON_DEMAND: self.__get_demand_schedule,
            LC.DEPOSIT: self.__get_deposit_schedule,
        }
        type_schedule = self._filter.Get('TypeSchedule')
        method = schedules.get(type_schedule, self.__get_real_schedule)
        return method()

    def __get_demand_schedule(self):
        return ShowPaymentsForSchedule(self._filter, self.navigation).build()

    def __get_deposit_schedule(self):
        return DepositPaymentSchedule(self._filter, self.navigation).build()

    def __get_real_schedule(self):
        return RealPaymentSchedule(self._filter, self.navigation).build()
