"""
Модуль отвечает за формирование плановых строк графика.

"""


__author__ = 'Glukhenko A.V.'


from collections import defaultdict

import sbis
from loans.loanConsts import LC
from .ideal import IdealPaymentSchedule


class PlanRow(IdealPaymentSchedule):
    """Класс хранящий обработчики по формированию плановых строк графика"""

    def __init__(self, _filter, navigation):
        super().__init__(_filter, navigation)
        self.field_monthly_payment = self.__get_field_monthly_payment()
        # сумма плановых начислений по предыдущему плановому периоду (см. метод _correct_underpayment_by_delay)
        self.prev_plan = defaultdict(sbis.Money)

    def __get_field_monthly_payment(self):
        """
        Возвращает название поле сумма которого должна соответствовать ежемесячному платежу.
        Например сумма ежемесячного платежа должна совпадать с:
        - суммой платежа (аннуитентный график)
        - суммой основного долга (диффиренцированный график)
        """
        fields = {
            LC.ANNUITY_SCHEDULE: 'size_payment',
            LC.DIFFERENTIATED_SCHEDULE: 'body_debt',
            LC.REPAYMENT_DEBT_AND_PERCENTS_AT_THE_END: 'size_payment',
        }
        return fields.get(self._filter.Get('TypeSchedule'))

    def _calc_real_plan(self, date_begin, date_end, debt, is_last_sub_period, limit_date_payment=None):
        """
        Осуществляет расчет плановых сумм
        :param date_begin: начало периода плановой записи
        :param date_end: окончание периода плановой записи
        :param debt: остаток долга за прошлый период, Money
        :param is_last_sub_period: признак расчета последнего планового периода, bool
        :param limit_date_payment: ограничитель платежа (подробнее в методе _calc_percent)
        :return: возвращает словарь расчитанных плановых сумм, dict
        Важно: для случаев переплаты основного долга (3.1) или переплаты процентов (4.1) необходимо навешивать пострбработку
        поскольку, не зная заранее плановые суммы, мы не можем зафиксировать факт переплаты по показателям.
        """
        cases = {
            LC.CASE_DEFAULT_PLAN: self.__case_default,
            LC.CASE_LAST_ROW_PLAN: self.__calc_last_row_plan,
            LC.CASE_OVER_PERCENT: self.__case_reduce_to_zero_percent,
            LC.CASE_OVER_BODY_DEBT: self.__case_reduce_to_zero_percent,
            LC.CASE_PREPAYMENT: self.__case_prepayment,
            LC.CASE_IDEAL: self.__case_ideal_plan,
        }
        case = self.__get_case_plan(is_last_sub_period, date_end, debt)
        method = cases.get(case)
        if case == LC.CASE_IDEAL:
            plan = method(date_end)
        else:
            plan = method(date_begin, date_end, debt, limit_date_payment)

        case_overpayment = self.__get_case_overpayment_plan()
        if case_overpayment:
            print(f'case_overpayment: {case_overpayment}')
            print('>' + '#' * 100)
            print(f'Найден случай 3.1 4.1. \n plan: {plan} \n Сведем проценты в ноль balance\n{self._get_detail_store()}!!!')
            method = cases.get(case_overpayment)
            plan = method(date_begin, date_end, debt, limit_date_payment)
            print(f'Свели проценты в ноль plan: {plan} balance\n{self._get_detail_store()}')
            print('<' + '#' * 100)

        plan['size_payment'] = plan.get('size_payment') or sbis.Money(0)
        plan['body_debt'] = plan.get('body_debt') or sbis.Money(0)
        plan['percent'] = plan.get('percent') or sbis.Money(0)
        return plan

    def __get_case_plan(self, is_last_sub_period, date_end, debt):
        """
        Возвращает кейс расчета сумм для плановой строки
        :param is_last_sub_period: признак построения последней строки графика
        :param date_end: окончание планового периода
        :param debt: остаток долга
        """
        if is_last_sub_period:
            case = LC.CASE_LAST_ROW_PLAN
        else:
            if date_end > self.today and self.__balance_is_normalized(date_end, debt):
                case = LC.CASE_IDEAL
            # elif self.overpayment.get('percent') > 0:

            # TODO: на самом деле при классическом погашении, у нас будет возвращать этот кейс CASE_OVER_PERCENT, хотя
            # переплаты по процентам как таковой нету еще. Важно: а как понять что у нас кейс переплаты процентов, пока
            # мы не сформировали план??? Нооо, после формирования плановой записи, значит мы обработали все факты и план
            # видимо можно навесить постобработку - корректирующий суммы плана, исходя из необходимости выровнять в ноль
            # проценты.
            # elif self._is_overpayment_exist('percent'):
            #     case = LC.CASE_OVER_PERCENT
            # # elif self.overpayment.get('body_debt') > 0:
            # elif self._is_overpayment_exist('body_debt'):
            #     case = LC.CASE_OVER_BODY_DEBT
            elif any(self.prepayment.values()):
                case = LC.CASE_PREPAYMENT
            else:
                case = LC.CASE_DEFAULT_PLAN
        return case

    def __get_case_overpayment_plan(self):
        """
        Возвращает кейс переплаты основного долга (3.1) или процентов (4.1), при наступлении такого случая.
        Важно: в случае переплаты и по основному долгу и по процентам, в приоритете случай 4.1, т.е. переплата по
        процентам
        """
        case = None
        if self._is_overpayment_exist('percent'):
            case = LC.CASE_OVER_PERCENT
        elif self._is_overpayment_exist('body_debt'):
            case = LC.CASE_OVER_BODY_DEBT
        return case

    def __calc_last_row_plan(self, date_begin, date_end, debt, limit_date_payment):
        """
        Рассчет сумм плана для последней строки графика
        :param date_begin: начало планового периода
        :param date_end: окончание планового периода
        :param debt: остаток долга
        :param limit_date_payment: ограничитель платежа (подробнее в методе _calc_percent)
        Примечание: для графика с погашением в конце срока или для графика с периодом в один месяц остаток долга еще
        не определен, возмем сумму ежемесячного платежа. Также учитываем наличие недоплаты, ибо основной долг к этому
        моменту может быть погашен.
        """
        percent = self._calc_percent(debt, date_begin, date_end, limit_date_payment)
        body_debt = debt
        if not debt and not self._is_underpayment_exist():
            body_debt = self.monthly_payment
        total_payments = self._get_agg_payments().get(date_end, {}).get('total')
        if total_payments:
            percent -= total_payments.get('percent', sbis.Money())
            body_debt -= total_payments.get('body_debt', sbis.Money())

        size_payment = body_debt + percent

        plan = {
            'percent': percent,
            'size_payment': size_payment,
            'body_debt': body_debt,
            'detail': f'Для расчета плановой строки использовался случай "{LC.CASE_LAST_ROW_PLAN_NAME}"\n'
                      f'Порядок расчета: Проценты: {percent}, Основной долг: {body_debt}, Платеж: {size_payment}',
        }
        self._calc_balance(date_end, plan, {})
        return plan

    def __case_overpayment_percent(self, date_begin, date_end, debt, limit_date_payment):
        """
        Случай наличия переплаты по процентам (сценарий 4.1)
        :param date_begin: начало планового периода
        :param date_end: окончание планового периода
        :param debt: остаток долга
        :param limit_date_payment: ограничитель платежа (подробнее в методе _calc_percent)
        Примечание: переплату процентов учли, в следующих периодах она не понадобится
        """
        percent = self._calc_percent(debt, date_begin, date_end, limit_date_payment)
        percent = percent # + self._get_balance('percent')

        # видимо нужно все таки учитывать баланс, пусть будут отрицательные суммы процентов в апреле

        if self.field_monthly_payment == 'size_payment':
            size_payment = self.monthly_payment # + self._get_balance('size_payment')
            body_debt = size_payment - percent
        elif self.field_monthly_payment == 'body_debt':
            body_debt = self.monthly_payment # + self._get_balance('body_debt')
            size_payment = body_debt + percent
        else:
            size_payment = sbis.Money(0)
            body_debt = sbis.Money(0)



        plan = {
            'percent': percent,
            'size_payment': size_payment,
            'body_debt': body_debt,
            'detail': f'Для расчета плановой строкСведем проценты в ноль!!!и использовался случай "{LC.CASE_OVER_PERCENT_NAME}"\n'
                      f'Порядок расчета: Проценты: {percent}, Основной долг: {body_debt}, Платеж: {size_payment}',
        }

        print('>>>>')
        self._calc_balance(date_end, plan, {})

        print(f'[{date_begin}, {date_end}] plan_before percent: {percent} body_debt: {body_debt}, size_payment: {size_payment} \nbalance\n{self._get_detail_store()} ')

        self._rebalance_by_overpayment()

        print(f'[{date_begin}, {date_end}] plan_after percent: {percent} body_debt: {body_debt}, size_payment: {size_payment} \nbalance\n{self._get_detail_store()} ')


        # if date_end > self.today and self._is_overpayment_exist('percent'):
        if self._is_overpayment_exist('percent'):
            print('REBALANCING')
            percent = self._get_balance('percent')
            self.overpayment['percent'] = sbis.Money(0)
            self.prepayment['percent'] = sbis.Money(0)
            self.timely_payment['percent'] = sbis.Money(0)
            self.delay_payment['percent'] = sbis.Money(0)

            size_payment = self.monthly_payment
            body_debt = size_payment + abs(percent)

        print(
            f'[{date_begin}, {date_end}] plan_after2 percent: {percent} body_debt: {body_debt}, size_payment: {size_payment} \nbalance\n{self._get_detail_store()} ')

        print('<<<<')

        plan = {
            'percent': percent,
            'size_payment': size_payment,
            'body_debt': body_debt,
            'detail': f'Для расчета плановой строки использовался случай "{LC.CASE_OVER_PERCENT_NAME}"\n'
                      f'Порядок расчета: Проценты: {percent}, Основной долг: {body_debt}, Платеж: {size_payment}',
        }

        return plan

    # def __case_overpayment_body_debt(self, date_begin, date_end, debt, limit_date_payment):
    #     """
    #     Случай наличия переплаты по основному долгу
    #     :param date_begin: начало планового периода
    #     :param date_end: окончание планового периода
    #     :param debt: остаток долга
    #     :param limit_date_payment: ограничитель платежа (подробнее в методе _calc_percent)
    #     """
    #     percent = self._calc_percent(debt, date_begin, date_end, limit_date_payment)
    #     body_debt, size_payment = self.__get_base_sums(percent)
    #
    #     plan = {
    #         'percent': percent,
    #         'size_payment': size_payment,
    #         'body_debt': body_debt,
    #         'detail': f'Для расчета плановой строки использовался случай "{LC.CASE_OVER_BODY_DEBT_NAME}"\n'
    #                   f'Порядок расчета: Проценты: {percent}, Основной долг: {body_debt}, Платеж: {size_payment}',
    #     }
    #     self._calc_balance(date_end, plan, {})
    #
    #     return plan

    def __case_prepayment(self, date_begin, date_end, debt, limit_date_payment):
        """
        Случай наличия предоплаты (сценарий 5.1)
        :param date_begin: начало планового периода
        :param date_end: окончание планового периода
        :param debt: остаток долга
        :param limit_date_payment: ограничитель платежа (подробнее в методе _calc_percent)
        Примечание: переплату процентов учли, в следующих периодах она не понадобится
        """
        percent = self._calc_percent(debt, date_begin, date_end, limit_date_payment)
        percent = percent - self.prepayment.get('percent')
        body_debt, size_payment = self.__get_base_sums(percent)
        self.prepayment.clear()
        plan = {
            'percent': percent,
            'size_payment': size_payment,
            'body_debt': body_debt,
            'detail': f'Для расчета плановой строки использовался случай "{LC.CASE_PREPAYMENT_NAME}"\n'
                      f'Порядок расчета: Проценты: {percent}, Основной долг: {body_debt}, Платеж: {size_payment}\n'
                      f'{self._get_detail_store()}',
        }
        self._calc_balance(date_end, plan, {})
        return plan

    def __case_ideal_plan(self, date_end):
        """
        Случай расчета будущего периода при идеальном балансе
        :param date_end: окончание планового периода
        Примечание: плановые суммы берутся с идеального графика платежей
        """
        date_row = self._get_plan_period_by_date().get(date_end)[1]
        ideal_plan = self.ideal_plans.get(date_row, {})
        percent = ideal_plan.get('percent')
        size_payment = ideal_plan.get('size_payment')
        body_debt = ideal_plan.get('body_debt')
        plan = {
            'percent': percent,
            'size_payment': size_payment,
            'body_debt': body_debt,
            'detail': f'Для расчета плановой строки использовался случай "{LC.CASE_IDEAL_NAME}"\n'
                      f'Порядок расчета: Проценты: {percent}, Основной долг: {body_debt}, Платеж: {size_payment}',
        }
        self._calc_balance(date_end, plan, {})
        return plan

    def __case_default(self, date_begin, date_end, debt, limit_date_payment):
        """
        Случай по умолчанию
        :param date_begin: начало планового периода
        :param date_end: окончание планового периода
        :param debt: остаток долга
        :param limit_date_payment: ограничитель платежа (подробнее в методе _calc_percent)
        """
        date_row = self._get_plan_period_by_date().get(date_end)[1]

        percent = self._calc_percent(debt, date_begin, date_end, limit_date_payment)
        percent_paid_off = self._get_balance('percent') <= 0
        if percent_paid_off:
            fine_percent = self._get_fine_percent(date_row, percent)
        else:
            fine_percent = sbis.Money(0)

        type_schedule = self._filter.Get('TypeSchedule')
        if type_schedule == LC.REPAYMENT_DEBT_AND_PERCENTS_AT_THE_END:
            body_debt = self.monthly_payment
            if not percent_paid_off:
                percent += fine_percent
                body_debt += fine_percent
            size_payment = body_debt + percent
        elif self.field_monthly_payment == 'size_payment':
            size_payment = self.monthly_payment
            if not percent_paid_off:
                percent += fine_percent
                size_payment += fine_percent
            body_debt = size_payment - percent
        elif self.field_monthly_payment == 'body_debt':
            body_debt = self.monthly_payment
            if not percent_paid_off:
                percent += fine_percent
                body_debt += fine_percent
            size_payment = body_debt + percent
        else:
            size_payment = sbis.Money(0)
            body_debt = sbis.Money(0)

        plan = {
            'percent': percent,
            'size_payment': size_payment,
            'body_debt': body_debt,
            'detail': f'Для расчета плановой строки использовался случай "{LC.CASE_DEFAULT_PLAN_NAME}"\n'
                      f'Порядок расчета: Проценты: {percent}, Основной долг: {body_debt}, Платеж: {size_payment}',
        }
        self._calc_balance(date_end, plan, {})
        return plan

    def __case_reduce_to_zero_percent(self):
        """
        Сводит в ноль проценты для случаев 3.1 и 4.1
        Примечание: сумма процентов состо
        """

        pass

    def __get_base_sums(self, percent):
        """
        Возвращает базовые плановые сумммы
        :param percent: сумма расчитанных процентов
        """
        if self.field_monthly_payment == 'size_payment':
            size_payment = self.monthly_payment
            body_debt = size_payment - percent
        elif self.field_monthly_payment == 'body_debt':
            body_debt = self.monthly_payment
            size_payment = body_debt + percent
        else:
            size_payment = sbis.Money(0)
            body_debt = sbis.Money(0)
        return body_debt, size_payment

    def __balance_is_normalized(self, date_end, debt):
        """
        Провряет, что выплачена сумма по основному долгу и остаток долга сравнялся с идеальным графиком
        :param date_end: окончание планового периода
        :param debt: остаток долга
        Примечание: остаток долга хранит значение на начало планового периода, если были платежи в периоде - учтем это
        и возьмем остаток последнего платежа в периоде.
        """
        date_row = self._get_plan_period_by_date().get(date_end)[1]
        ideal_plan = self.ideal_plans.get(date_row, {})

        payments = self._get_agg_payments().get(date_end)
        if payments:
            debt = payments.get('total').get('debt')

        return ideal_plan.get('begin_debt') == debt

    def _create_plan_row(self, date_begin, date_end, debt, is_last_sub_period):
        """
        Создает плановую запись графика платежей
        :param date_begin: начало периода плановой записи
        :param date_end: окончание периода плановой записи
        :param debt: остаток долга за прошлый период, Money
        :param is_last_sub_period: признак расчета последнего планового периода, bool
        :return: запись, Record
        """
        debt = debt or sbis.Money()
        fact = defaultdict(sbis.Money)
        plan = self._calc_real_plan(date_begin, date_end, debt, is_last_sub_period)
        self.prev_plan = plan
        # self._calc_balance(date_end, plan, fact)

        id_row = str(date_end).replace('-', '')
        row = sbis.Record(self.result_format)
        row['@Документ'].From('{},{}'.format(id_row, LC.SCHEDULE_PLAN_NAME))
        row['Дата'] = date_end
        row['НачалоПериода'] = date_begin
        row['КонецПериода'] = date_end
        row['РазмерПлатежа'] = sbis.Money()
        row['РазмерПлатежаПлан'] = plan.get('size_payment')
        row['ОсновнойДолг'] = sbis.Money()
        row['ОсновнойДолгПлан'] = plan.get('body_debt')
        row['НачисленныеПроценты'] = sbis.Money()
        row['НачисленныеПроцентыПлан'] = plan.get('percent')
        row['ОстатокДолга'] = self._correct_debt(debt, date_end, plan)
        row['ТипЗаписи'] = LC.SCHEDULE_PLAN
        row['already_paid'] = self._is_already_paid()
        row['Детализация'] = self._get_detail(plan.get('detail'))
        return row
