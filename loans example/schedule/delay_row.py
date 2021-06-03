"""
Модуль отвечает за формирование строк по просрочкам платежа.

Виды просрочек
Закрытая просрочка - просрочка которая была в прошлом, суммы статичны
Открытая просрочка - состояние просрочки по графику на текущий день, суммы зависят от текущего дня

Формула для расчета суммы процентов по просрочке: Xф - Xнп - Xупл, где
Xф - проценты на текущую дату по фактическому долгу
Xнп - проценты за последний незавершенный период по плановому графику
Xупл - возвращает фактически уплаченные проценты на текущую дату

"""


__author__ = 'Glukhenko A.V.'


import datetime
from collections import defaultdict

import sbis
from loans.loanConsts import LC
from .ideal import IdealPaymentSchedule


class DelayRow(IdealPaymentSchedule):
    """Класс хранящий обработчики по формированию просрочки"""

    def __init__(self, _filter, navigation):
        super().__init__(_filter, navigation)
        # период действия просрочки
        self.delay_period = {
            'begin': None,
            'end': None,
        }
        # все периоды просрочек
        self.delay_periods = []

    def _calc_delay_plan(self, date_begin_delay, date_end_delay, debt, is_last_row):
        """
        Осуществляет расчет плановых сумм для просрочки
        :param date_begin_delay: начало периода просрочки
        :param date_end_delay: окончание периода просрочки, она же дата обрабатываемого платежа
        :param debt: остаток долга за прошлый период, Money
        :param is_last_row: признак расчета последней строки графика, bool
        :return: возвращает словарь расчитанных плановых сумм для просрочки, dict
        """
        result = {
            'percent': sbis.Money(),
            'size_payment': sbis.Money(),
            'body_debt': sbis.Money(),
            'detail': '',
        }
        case = self.__get_case_delay(is_last_row, date_begin_delay)
        if case == LC.CASE_LAST_ROW_DELAY:
            result = self._calc_last_row_delay(date_begin_delay, date_end_delay, debt)
        else:
            if case == LC.CASE_OVER_BODY_DEBT_DELAY:
                result = self._calc_over_body_debt_delay()
            elif case == LC.CASE_EARLY_FIRST_PAYMENT:
                result = self._calc_early_first_payment_delay(date_begin_delay, date_end_delay, debt)
            elif case == LC.CASE_DEFAULT_DELAY:
                result = self._calc_default_delay(date_end_delay, debt)
        return result

    def _calc_last_row_delay(self, date_begin_delay, date_end_delay, debt):
        """
        Рассчет сумм просрочки для последней строки графика
        :param date_begin_delay: начало периода просрочки
        :param date_end_delay: окончание периода просрочки, она же дата обрабатываемого платежа
        :param debt: остаток долга
        """
        percent_paid_off = self._get_balance('percent') <= 0
        if percent_paid_off:
            percent = self.__get_fine_delay(date_begin_delay, date_end_delay, debt)
            detail_percent = ''
        else:
            percent_by_fact_debt = self.__get_percent_by_fact_debt(date_end_delay)
            percent_by_plan_debt = self.__get_percent_by_plan_debt(date_end_delay)
            paid_percents = self.__get_paid_percents(date_end_delay)
            percent = percent_by_fact_debt - percent_by_plan_debt - paid_percents
            detail_percent = f'Xф={percent_by_fact_debt}, Xнп={percent_by_plan_debt}, Xупл={paid_percents}\n'

        body_debt = debt
        size_payment = body_debt + percent
        return {
            'percent': percent,
            'size_payment': size_payment,
            'body_debt': body_debt,
            'detail': f'Для расчета использовался случай "{LC.CASE_LAST_ROW_DELAY_NAME}"\n'
                      f'{detail_percent}'
                      f'Порядок расчета: Проценты: {percent}, Основной долг: {body_debt}, Платеж: {size_payment}',
        }

    def _calc_over_body_debt_delay(self):
        """
        Рассчет сумм при переплате только по основному долгу (в ТЗ описан как случай 6.2)
        """
        percent = self._get_balance('percent')
        body_debt = max((self._get_balance('body_debt'), sbis.Money()))
        size_payment = percent + body_debt
        return {
            'percent': percent,
            'size_payment': size_payment,
            'body_debt': body_debt,
            'detail': f'Для расчета использовался случай "{LC.CASE_OVER_BODY_DEBT_DELAY_NAME}"\n'
                      f'Порядок расчета: Проценты: {percent}, Основной долг: {body_debt}, Платеж: {size_payment}',
        }

    def _calc_early_first_payment_delay(self, date_begin_delay, date_end_delay, debt):
        """
        Рассчет сумм просрочки при слишком раннем платеже
        :param date_begin_delay: начало периода просрочки
        :param date_end_delay: окончание периода просрочки, она же дата обрабатываемого платежа
        :param debt: остаток долга
        :return:
        """
        fine_delay = self.__get_fine_delay(date_begin_delay, date_end_delay, debt)
        percent = fine_delay
        body_debt = max((self._get_balance('body_debt'), sbis.Money()))
        size_payment = percent + body_debt
        return {
            'percent': percent,
            'size_payment': size_payment,
            'body_debt': body_debt,
            'detail': f'Для расчета процентов использовался случай "{LC.CASE_EARLY_FIRST_PAYMENT_NAME}"\n'
                      f'Порядок расчета: Проценты: {percent}, Основной долг: {body_debt}, Платеж: {size_payment}',
        }

    def _calc_default_delay(self, date_end_delay, debt):
        """
        Рассчет сумм просрочки по умолчанию. Используется формула Xф - Xнп - Xупл
        где Xф - проценты на текущую дату по фактическому долгу
        Xнп - проценты за последний незавершенный период по плановому графику
        Xупл - возвращает фактически уплаченные проценты на текущую дату
        :param date_end_delay: окончание периода просрочки, она же дата обрабатываемого платежа
        :param debt: отстаток долга по графику
        """
        date_row = self._get_plan_period_by_date().get(date_end_delay)[0]
        ideal_plan = self.ideal_plans.get(date_row, {})

        percent_by_fact_debt = self.__get_percent_by_fact_debt(date_end_delay)
        percent_by_plan_debt = self.__get_percent_by_plan_debt(date_end_delay)
        paid_percents = self.__get_paid_percents(date_end_delay)
        percent = percent_by_fact_debt - percent_by_plan_debt - paid_percents

        body_debt = debt - ideal_plan.get('end_debt', sbis.Money())
        size_payment = percent + body_debt
        return {
            'percent': percent,
            'size_payment': size_payment,
            'body_debt': body_debt,
            'detail': f'Для расчета процентов использовался случай "{LC.CASE_DEFAULT_DELAY_NAME}" '
                      f'Xф={percent_by_fact_debt}, Xнп={percent_by_plan_debt}, Xупл={paid_percents}\n'
                      f'Порядок расчета: Проценты: {percent}, Основной долг: {body_debt}, Платеж: {size_payment}',
        }

    def __get_case_delay(self, is_last_row, date_begin_delay):
        """
        Возвращает кейс расчета сумм для строки просрочки
        :param is_last_row: признак построения последней строки графика
        :param date_begin_delay: начало периода просрочки
        """
        if is_last_row:
            case = LC.CASE_LAST_ROW_DELAY
        else:
            if self.__is_overpayment_body_debt():
                case = LC.CASE_OVER_BODY_DEBT_DELAY
            elif self.__is_early_first_payment(date_begin_delay):
                case = LC.CASE_EARLY_FIRST_PAYMENT
            else:
                case = LC.CASE_DEFAULT_DELAY
        return case

    def __get_fine_delay(self, date_begin, date_end, debt):
        """
        Возвращает сумму штрафа за просрочку.
        :param date_begin: дата начала просрочки
        :param date_end: дата окончания просрочки
        :param debt: остаток долга
        :return: сумма штрафа, sbis.Money
        Примечание: рассчитывается как разница между фактическим и плановыми начислениями на дату просрочки
        """
        date_row = self._get_plan_period_by_date().get(date_end)[0]
        ideal_plan = self.ideal_plans.get(date_row, {})
        plan_debt = ideal_plan.get('end_debt')

        fine_fact = self._calc_percent(debt, date_begin, date_end)
        fine_plan = self._calc_percent(plan_debt, date_begin, date_end)
        fine_old_period = sum(
            [self._calc_percent(debt, _date_begin, _date_end) for _date_begin, _date_end in self.delay_periods]
        )
        return fine_old_period + fine_fact - fine_plan

    def __get_percent_by_fact_debt(self, date):
        """
        Проценты на текущую дату по фактическому долгу (Xф)
        :param date: дата на которую рассчитывается просрочка
        """
        return self._calc_percent_by_accrual(self._get_date_begin_schedule(), date)

    def __get_percent_by_plan_debt(self, date_end):
        """
        Проценты за последний незавершенный период по плановому графику (Xнп)
        :param date_end: дата окончания планового периода
        """
        date_row = self._get_plan_period_by_date().get(date_end)[0]
        ideal_plan = self.ideal_plans.get(date_row, {})
        ideal_debt = ideal_plan.get('end_debt')
        date_begin = self.delay_period.get('end') or self.delay_period.get('begin')
        percent_by_plan_debt = self._calc_percent(ideal_debt, date_begin, date_end)
        return percent_by_plan_debt

    def __get_paid_percents(self, date_end_delay):
        """
        Возвращает фактически уплаченные проценты на текущую дату (Xупл)
        :param date_end_delay: окончание периода просрочки, она же дата обрабатываемого платежа
        """
        paid_percents = sbis.Money()
        for date_payment, payment in self.payments.items():
            if date_payment < date_end_delay:
                paid_percents += payment.Get(self.payment_percent_dc_field_name)
        return paid_percents

    def __is_overpayment_body_debt(self):
        """
        Проверяет случай переплаты только по основному долгу (в ТЗ описан как случай 6.2)
        """
        return all((
            self.overpayment.get('body_debt') > 0,
            self.overpayment.get('percent') == 0,
        ))

    def __is_early_first_payment(self, date_begin):
        """
        Проверяет, что клиенту требуется совершить первый платеж слишком рано. Ситуация когда только-только
        выдали деньги, а уже надо совершать первый платеж, и по факту клиент не пользовался деньгами. В первый платеж
        проценты ожидаются нулевыми, а основной долг - согласно сумме ежемесячного платежа.
        Примечание: https://online.sbis.ru/opendoc.html?guid=12abc6b7-2a17-488f-802e-44d1b6519142
        :param date_begin: дата начала строки графика
        :return: bool
        """
        return date_begin <= self.get_first_date_disbursement()

    def _create_open_delay_row(self, date_begin, date_end, debt, is_last_row):
        """
        Создает запись открытой просрочки
        :param date_begin: начало периода просрочки
        :param date_end: окончание периода просрочки
        :param debt: остаток долга за прошлый период, Money
        :param is_last_row: признак расчета последней строки графика, bool
        :return: запись, Record
        """
        debt = debt or sbis.Money()
        prev_day = date_end - datetime.timedelta(days=1)
        plan = self._calc_delay_plan(date_begin, date_end, debt, is_last_row)
        self._correct_underpayment_by_delay(self.prev_plan, plan)
        debt = debt - plan.get('body_debt')
        id_row = int(str(date_end).replace('-', ''))

        row = sbis.Record(self.result_format)
        row['@Документ'].From(sbis.ObjectId(LC.SCHEDULE_OPEN_DELAY_NAME, id_row))
        row['Описание'] = self._get_description_delay(date_begin, date_end)
        row['ОписаниеДата'] = self._get_description_date_delay(date_begin, prev_day)
        row['Дата'] = date_end
        row['НачалоПериода'] = date_begin
        row['КонецПериода'] = date_end
        row['ТипЗаписи'] = LC.SCHEDULE_OPEN_DELAY
        row['РазмерПлатежа'] = sbis.Money()
        row['РазмерПлатежаПлан'] = plan.get('size_payment')
        row['ОсновнойДолг'] = sbis.Money()
        row['ОсновнойДолгПлан'] = plan.get('body_debt')
        row['НачисленныеПроценты'] = sbis.Money()
        row['НачисленныеПроцентыПлан'] = plan.get('percent')
        row['ОстатокДолга'] = debt
        row['Детализация'] = self._get_detail(plan.get('detail'))
        return row

    def _create_close_delay_row(self, date_begin, date_end, debt, is_last_row):
        """
        Создает запись закрытой просрочки
        :param date_begin: начало периода просрочки
        :param date_end: окончание периода просрочки, она же дата обрабатываемого платежа
        :param debt: остаток долга за прошлый период, Money
        :param is_last_row: признак расчета последней строки графика, bool
        :return: запись, Record
        """
        debt = debt or sbis.Money()
        prev_day = date_end - datetime.timedelta(days=1)
        plan = self._calc_delay_plan(date_begin, date_end, debt, is_last_row)
        fact = defaultdict(sbis.Money)
        id_row = int(str(date_end).replace('-', ''))
        debt = debt - plan['body_debt']

        row = sbis.Record(self.result_format)
        row['@Документ'].From(sbis.ObjectId(LC.SCHEDULE_DELAY_NAME, id_row))
        row['Описание'] = self._get_description_delay(date_begin, date_end)
        row['ОписаниеДата'] = self._get_description_date_delay(date_begin, prev_day)
        row['Дата'] = date_end
        row['НачалоПериода'] = date_begin
        row['КонецПериода'] = date_end
        row['ТипЗаписи'] = LC.SCHEDULE_DELAY
        row['РазмерПлатежа'] = fact['size_payment']
        row['РазмерПлатежаПлан'] = plan.get('size_payment')
        row['ОсновнойДолг'] = fact['body_debt']
        row['ОсновнойДолгПлан'] = plan.get('body_debt')
        row['НачисленныеПроценты'] = fact['percent']
        row['НачисленныеПроцентыПлан'] = plan.get('percent')
        row['ОстатокДолга'] = debt
        row['Детализация'] = self._get_detail(plan.get('detail'))
        return row
