"""
Модуль отвечает за построение идеального графика платежей, который подходит под все типы графиков.

Идеальный план может быть использован для:
1. Формирования графика платежей для не зарегестрированных графиков (по которым не была выдача денежных средств)
2. Расчета "идеальной" суммы ежемесячного платежа, методом подгона.
3. Расчета сумм графика платежей

"""


__author__ = 'Glukhenko A.V.'

from functools import lru_cache

import sbis
from loans.loanConsts import LC
from .base import BasePaymentsSchedule
from .helpers import get_x_point


class IdealPaymentSchedule(BasePaymentsSchedule):
    """Класс для построения идеального графика платежей"""
    def __init__(self, _filter, navigation):
        super().__init__(_filter, navigation)
        self.monthly_payment = self._filter.Get('MonthlyPayment') or self._calc_best_monthly_payment()
        # суммы по идеальному плану
        self.ideal_plans = self.get_sum_schedule()
        self.result = sbis.RecordSet(self.result_format)

    def build_schedule(self):
        """Построение графика платежей"""
        if not self._is_valid_filter():
            return sbis.RecordSet(self.result_format)

        schedule = self.__build_schedule()
        self.__post_processing(schedule)
        return schedule

    def __post_processing(self, schedule):
        """
        Постобработка графика платежей
        ВАЖНО:
        Перед вычислением просрочки необходимо:
        - отсортировать по дате
        - рассчитанное поле ближайшего платежа, __calc_near_payment
        - использовать устаревшие плановые записи, т.е. __remove_old_plan строго после расчета просрочки
        """
        schedule.sort(key=lambda rec: (rec.Get('Дата')))
        self._add_years(schedule)
        self._mark_separator_line(schedule)
        self._calc_show_total(schedule)
        self._sort_result(schedule)
        self._calc_outcome(schedule)

    def __build_schedule(self):
        """Построение графика платежей"""
        schedule = sbis.RecordSet(self.result_format)

        sum_schedule = self.get_sum_schedule(monthly_payment=None)
        for date, plan in sum_schedule.items():
            id_row = str(date).replace('-', '')
            row = sbis.Record(self.result_format)
            row['@Документ'].From('{},{}'.format(id_row, LC.SCHEDULE_PLAN_NAME))
            row['Дата'] = date
            row['НачалоПериода'] = plan.get('begin_period')
            row['КонецПериода'] = plan.get('end_period')
            row['РазмерПлатежа'] = sbis.Money()
            row['РазмерПлатежаПлан'] = plan.get('size_payment')
            row['ОсновнойДолг'] = sbis.Money()
            row['ОсновнойДолгПлан'] = plan.get('body_debt')
            row['НачисленныеПроценты'] = sbis.Money()
            row['НачисленныеПроцентыПлан'] = plan.get('percent')
            row['ОстатокДолга'] = plan.get('end_debt')
            row['ТипЗаписи'] = LC.SCHEDULE_PLAN
            schedule.AddRow(row)
        return schedule

    def get_sum_schedule(self, monthly_payment=None):
        """
        Рассчитывает идельаный план
        :param monthly_payment: сумма ежемесячного платежа, по которой надо сформировать график
        Примечание: в качестве ключа используется дата окончания периода.!!!
        """
        sum_schedule = {}

        if self._is_valid_filter():
            debt = self._filter.Get('SizePayment')
            monthly_payment = monthly_payment or self.monthly_payment or 0

            ideal_plan_dates = self._get_plan_dates(use_date_prolongation=False)
            plan_periods = self._get_periods(use_date_prolongation=False)

            for date_begin, date_end in plan_periods:
                is_last_sub_period = date_end == ideal_plan_dates[-1]

                size_payment, body_debt, percent = self.__calc_ideal_plan(
                    debt=debt,
                    date_begin=date_begin,
                    date_end=date_end,
                    is_last_sub_period=is_last_sub_period,
                    monthly_payment=monthly_payment,
                )
                sum_schedule[date_end] = {
                    'size_payment': size_payment,
                    'percent': percent,
                    'body_debt': body_debt,
                    'begin_debt': debt,
                    'end_debt': debt - body_debt,
                    'begin_period': date_begin,
                    'end_period': date_end,
                }
                debt = debt - body_debt
                if not debt:
                    break

        return sum_schedule

    def __calc_ideal_plan(self, **kwargs):
        """Возвращает плановые суммы по графику"""
        percent = self.percents_calc.calc(
            kwargs.get('debt'),
            self.get_rate(),
            kwargs.get('date_begin'),
            kwargs.get('date_end'),
        )
        percent = round(percent, 2)

        if kwargs.get('is_last_sub_period'):
            body_debt = kwargs.get('debt')
            size_payment = body_debt + percent
        else:
            if self._filter.Get('TypeSchedule') == LC.DIFFERENTIATED_SCHEDULE:
                body_debt = sbis.Money(kwargs.get('monthly_payment'))
                size_payment = body_debt + percent
            else:
                size_payment = sbis.Money(kwargs.get('monthly_payment'))
                body_debt = size_payment - percent
            if self.__has_overflow(size_payment, body_debt, percent, kwargs.get('debt') - body_debt):
                body_debt = kwargs.get('debt')
                size_payment = body_debt + percent

        return size_payment, body_debt, percent

    @staticmethod
    def __has_overflow(size_payment, body_debt, percent, debt):
        """
        Проверяет есть ли переполнение при построении графика
        :param size_payment: размер платежа
        :param body_debt: основной долг
        :param percent: проценты
        :param debt: остаток долга
        """
        is_overflow = any(value < 0 for value in (size_payment, body_debt, percent, debt))
        if is_overflow:
            msg = f'Произошло переполнение при рассчете строки графика Платеж: {round(size_payment, 2)}, ' \
                  f'Основной долг: {round(body_debt, 2)}, Проценты: {round(percent, 2)}, ' \
                  f'Остаток долга: {round(debt, 2)}. Пометим данный период как последний и пересчитаем.'
            sbis.WarningMsg(msg)
        return is_overflow

    def _calc_best_monthly_payment(self):
        """Рассчитывает наилучшую сумму ежемесячного платежа (в дочернем классе)"""
        monthly_payment = None
        if self._is_valid_filter():
            if self._filter.Get('TypeSchedule') == LC.ANNUITY_SCHEDULE:
                monthly_payment = self.__get_best_monthly_payment()
            else:
                monthly_payment = self._get_monthly_payment()
        return monthly_payment

    def __get_best_monthly_payment(self):
        """
        Рассчитывает наилучшую сумму ежемесячного платежа
        Примечание: наилучшую сумму определяем на основании уровнения прямой через точки points
        """
        points = []
        monthly_payment = self._get_monthly_payment()
        delta = self.__get_delta_payment(monthly_payment)

        if abs(delta) < sbis.Money(0.01):
            return monthly_payment

        # first point
        points.append((monthly_payment, delta))
        # second point
        monthly_payment += sbis.Money(0.01)
        delta = self.__get_delta_payment(monthly_payment)
        points.append((monthly_payment, delta))

        for i in range(5):
            if abs(delta) < sbis.Money(0.01):
                break
            monthly_payment = get_x_point(*points[i], *points[i + 1])
            delta = self.__get_delta_payment(monthly_payment)
            points.append((monthly_payment, delta))

        return monthly_payment

    def _get_monthly_payment(self):
        """
        Рассчитывает сумму ежемесячного платежа
        PS: учитываем что если график пролонгируется, нельзя учитывать один месяц пролонгации
        """
        month_sum = None
        if self._is_valid_filter():
            if self._filter.Get('TypeSchedule') != LC.ANNUITY_SCHEDULE:
                month_sum = self.__calc_simple_monthly_payment()
            else:
                month_sum = self.__calc_monthly_payment()
        return month_sum

    @lru_cache(maxsize=1)
    def __calc_simple_monthly_payment(self):
        """
        Возвращает сумму простого ежемесячного платежа
        """
        payments_count = self.__get_payments_count()
        loan_sum = self._get_total_sum_disbursement() or self._filter.Get('SizePayment')
        return loan_sum / payments_count

    @lru_cache(maxsize=1)
    def __calc_monthly_payment(self):
        """
        Возвращает сумму ежемесячного платежа
        """
        monthly_rate = self.get_monthly_rate()
        payments_count = self.__get_payments_count()
        loan_sum = self._get_total_sum_disbursement() or self._filter.Get('SizePayment')
        return loan_sum * monthly_rate * (1 + 1 / ((1 + monthly_rate) ** payments_count - 1))

    def __get_payments_count(self):
        """
        Возвращает количество предполагаемых платежей исключительно для расчета ежемесячного платежа
        """
        if self._filter.Get('TypeSchedule') == LC.REPAYMENT_ON_DEMAND:
            payments_count = 1
        else:
            payments_count = len(self._get_plan_dates(use_date_prolongation=False))
        return payments_count

    def __get_delta_payment(self, monthly_payment):
        """
        Возвращает погрешность в рассчете ежемесячного платежа
        Показывает на сколько различается сумма последнего платежа графика с ежемесячным платежом
        :param monthly_payment: сумма ежемесячного платежа, для которого рассчитывается погрешность
        :return: сумма погрешности
        """
        sum_schedule = self.get_sum_schedule(monthly_payment)

        id_last_row = max(sum_schedule)
        last_debts = sum_schedule.get(id_last_row).get('size_payment')
        return last_debts - monthly_payment

    def _get_fine_percent(self, date, percent):
        """Возвращает сумму штрафа, из-за недоплаты по платежу"""
        plan_date_end = self._get_plan_period_by_date().get(date)[1]
        ideal_percent = self.ideal_plans.get(plan_date_end, {}).get('percent') or sbis.Money()
        fine = percent - ideal_percent
        return fine
