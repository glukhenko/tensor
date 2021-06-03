"""
Модуль отвечает за построение графика платежей по договору депозита
"""


__author__ = 'Glukhenko A.V.'


import sbis
from loans.loanConsts import LC
from .real import RealPaymentSchedule


class DepositPaymentSchedule(RealPaymentSchedule):
    """Класс строит график платежей по депозитам"""
    def __init__(self, _filter, navigation):
        super().__init__(_filter, navigation)
        # рассчитанные планы графика
        self.plans = {}

    def build(self):
        """Построение графика платежей"""
        if not self.is_valid_filter:
            return self.result

        self.__add_plans()
        self.__add_facts()
        self._post_processing()
        return self.result

    def __add_plans(self):
        """
        Добавляет плановые строки графика
        """
        prev_date = self.first_date_disbursement or self.date_begin
        debt = sbis.Money() if self.is_registered else self.loan_sum
        schedule_dates = self._get_schedule_dates()

        for i, date in enumerate(schedule_dates, start=1):
            is_last_plan = date == schedule_dates[-1]

            plan = self.get_plan_sum(debt, prev_date, date, is_last_plan)
            fact = self._get_agg_payments().get(date, {}).get('total', {})
            debt = self._correct_debt(debt, date, plan)
            self.plans[date] = plan
            self.__calc_overpayment(plan)
            self.__calc_underpayment(date, plan)
            self._calc_total_sum(date, debt, plan, fact)

            rec = sbis.Record(self.result_format)
            rec['@Документ'].From('{},{}'.format(-i, LC.SCHEDULE_PLAN_NAME))
            rec['Дата'] = date
            rec['РазмерПлатежа'] = fact.get('size_payment', sbis.Money())
            rec['РазмерПлатежаПлан'] = max((plan.get('size_payment'), sbis.Money(0)))
            rec['ОсновнойДолг'] = fact.get('body_debt', sbis.Money())
            rec['ОсновнойДолгПлан'] = max((plan.get('body_debt'), sbis.Money(0)))
            rec['НачисленныеПроценты'] = fact.get('percent', sbis.Money())
            rec['НачисленныеПроцентыПлан'] = max((plan.get('percent'), sbis.Money(0)))
            rec['ОстатокДолга'] = debt
            rec['ТипЗаписи'] = LC.SCHEDULE_PLAN
            self.result.AddRow(rec)

            prev_date = date
            if self.__check_paid_out():
                break

    def __add_facts(self):
        """
        Добавляет платежи
        """
        plan_date_by_payment = self._get_plan_date_by_payment()
        for date_payment, payment in self.payments.items():
            date_plan = plan_date_by_payment.get(date_payment)
            if int(payment.Get('@Документ').split(',')[0]) < 0:
                id_type_row = LC.SCHEDULE_PAYMENTS
            else:
                id_type_row = LC.SCHEDULE_PAYMENT

            plan = self.plans.get(date_plan, {})
            fact = self.__get_fact_sum(payment)
            debt = payment.Get('ОстатокДолга')

            rec = sbis.Record(self.result_format)
            rec['@Документ'].From(payment.Get('@Документ'))
            rec['Дата'] = date_payment
            rec['РазмерПлатежа'] = fact.get('size_payment')
            rec['РазмерПлатежаПлан'] = plan.get('size_payment', sbis.Money())
            rec['ОсновнойДолг'] = fact.get('body_debt')
            rec['ОсновнойДолгПлан'] = plan.get('body_debt', sbis.Money())
            rec['НачисленныеПроценты'] = fact.get('percent')
            rec['НачисленныеПроцентыПлан'] = plan.get('percent', sbis.Money())
            rec['ОстатокДолга'] = debt
            rec['ТипЗаписи'] = id_type_row
            self.result.AddRow(rec)

    def get_plan_sum(self, debt, date_begin, date_end, is_last_plan):
        """
        Возвращает суммы по плановому начислению
        :param debt: остаток долга
        :param date_begin: дата начала начисления процентов
        :param date_end: дата окончания начисления процентов
        :param is_last_plan: признак последнего строки графика (механизм расчета различен)
        :return: словарь вида
            plan = {
                size_payment: sum_size_payment, # сумма платежа
                percent: sum_percent, # сумма по процентам
                body_debt: sum_body_debt, # сумма основного долга
            }
        """
        method = self.__get_last_plan_sum if is_last_plan else self.__get_plan_sum
        return method(debt, date_begin, date_end)

    def __get_plan_sum(self, debt, date_begin, date_end):
        """
        Рассчитывает планновые суммы по графику платежа.
        PS: при рассчете учитываются
            - сумма недоплат с предыдущих периодов underpayment
            - сумма переплат с предыдущих периодов overpayment
            - итоговая сумма новых платежей payments_by_month в рамках расчитываемого месяца
        """
        if self.is_registered:
            payments = self._get_agg_payments().get(date_end, {}).get('total', {})

            percent = self._calc_percent(debt, date_begin, date_end) \
                + self.underpayment.get('percent', sbis.Money()) \
                - self.overpayment.get('percent', sbis.Money()) \
                - payments.get('percent', sbis.Money())
            size_payment = sbis.Money(self.monthly_payment) \
                + self.underpayment.get('size_payment', sbis.Money()) \
                - self.overpayment.get('size_payment', sbis.Money()) \
                - payments.get('size_payment', sbis.Money())
            body_debt = size_payment - percent
        else:
            percent = self._calc_percent(debt, date_begin, date_end)
            size_payment = sbis.Money(self.monthly_payment)
            body_debt = size_payment - percent

        return {
            'size_payment': size_payment,
            'percent': percent,
            'body_debt': body_debt,
        }

    def __get_last_plan_sum(self, debt, date_begin, date_end):
        """
        Рассчитывает планновые суммы по последней строке графика платежа.
        PS: при рассчете учитываются
            - сумма недоплат с предыдущих периодов underpayment
            - сумма переплат с предыдущих периодов overpayment
            - итоговая сумма новых платежей payments_by_month в рамках расчитываемого месяца
            - сумма изменения остатка долга для плановой записи date_end
        """
        if self.is_registered:
            payments = self._get_agg_payments().get(date_end, {}).get('total', {})

            percent = self._calc_percent(debt, date_begin, date_end) \
                + self.underpayment.get('percent', sbis.Money()) \
                - self.overpayment.get('percent', sbis.Money()) \
                - payments.get('percent', sbis.Money())
            body_debt = debt + self._get_change_debts_by_month(date_end)
            size_payment = body_debt + percent
        else:
            percent = self._calc_percent(debt, date_begin, date_end)
            body_debt = debt
            size_payment = body_debt + percent

        return {
            'size_payment': size_payment,
            'percent': percent,
            'body_debt': body_debt,
        }

    def _correct_debt(self, debt, date, plan):
        """
        Корректирует остаток, по которому рассчитываются следующие плановые записи
        Корректируется сумма остатка на
            - сумму изменения остатка в течении месяца в зависимости от новых выдач или платежей
            - плановую сумму в графике, если он позже текущего дня и отсутствуют переплаты
        """
        if self.is_registered:
            change_debts = self._get_change_debts_by_month(date)
            if change_debts:
                debt += change_debts
            if date >= self.today and plan.get('body_debt') > 0:
                debt -= plan.get('body_debt')
        else:
            debt = debt - plan.get('body_debt')
        return debt

    def __get_fact_sum(self, payment):
        """
        Возвращает фактические суммы по платежу
        """
        body_debt = payment.Get(self.payment_dc_field_name)
        percent = payment.Get(self.payment_percent_dc_field_name)
        size_payment = body_debt + percent
        return {
            'size_payment': size_payment,
            'percent': percent,
            'body_debt': body_debt,
        }

    def _calc_total_sum(self, date, debt, plan, fact):
        """
        Рассчитывает итоговые суммы графика платежей
        - при наличии платежей, итоги содержат сумму всех платежей
        - при отсутствии платежей, итоги содержат сумму всех плановых записей (суммы со старых плановых записей
        переносятся на следующие планы, поэтому учитываем текущий день)
        :param date: дата графика
        :param debt: остаток
        :param plan: плановые суммы
        :param fact: фактические суммы
        """
        if self.is_registered:
            if self.payments:
                self.total.update({
                    'size_payment': self.total.get('size_payment') + fact.get('size_payment'),
                    'body_debt': self.total.get('body_debt') + fact.get('body_debt'),
                    'percent': self.total.get('percent') + fact.get('percent'),
                    'debt': fact.get('debt') if fact.get('debt') is not None else self.total.get('debt'),
                })
            else:
                if date >= self.today:
                    self.total.update({
                        'size_payment': self.total.get('size_payment') + plan.get('size_payment'),
                        'body_debt': self.total.get('body_debt') + plan.get('body_debt'),
                        'percent': self.total.get('percent') + plan.get('percent'),
                        'debt': debt,
                    })
        else:
            self.total.update({
                'size_payment': self.total.get('size_payment') + plan.get('size_payment'),
                'body_debt': self.total.get('body_debt') + plan.get('body_debt'),
                'percent': self.total.get('percent') + plan.get('percent'),
                'debt': debt,
            })

    def __check_paid_out(self):
        """
        Метод проверяет по наличию платежей (сумма которых лежит в итогах) что займ выплачен,
        и нет необходимости строить график дальше
        """
        return all((self.payments, self.total_sum_disbursement)) \
               and self.total.get('body_debt') >= self.total_sum_disbursement

    def __calc_overpayment(self, plan):
        """
        Рассчитывает переплату по строке графика платежа
        PS: в плановых суммах уже учтены платежи, и если сумма отрицательная значит есть переплата
        """
        self.overpayment.clear()
        self.overpayment.update({
            'size_payment': abs(min((plan.get('size_payment'), sbis.Money()))),
            'percent': abs(min((plan.get('percent'), sbis.Money()))),
            'body_debt': abs(min((plan.get('body_debt'), sbis.Money()))),
        })

    def __calc_underpayment(self, date, plan):
        """
        Рассчитывает недоплату по плановому графику платежа
        PS: в "прошедших" плановых суммах уже учтены платежи, и если сумма положительная значит есть недоплата
        """
        self.underpayment.clear()
        if date < self.today:
            self.underpayment.update({
                'size_payment': max((plan.get('size_payment'), sbis.Money())),
                'percent': max((plan.get('percent'), sbis.Money())),
                'body_debt': max((plan.get('body_debt'), sbis.Money())),
            })
        else:
            self.underpayment.update({
                'size_payment': sbis.Money(),
                'percent': sbis.Money(),
                'body_debt': sbis.Money(),
            })
