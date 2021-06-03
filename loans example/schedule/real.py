"""
Модуль отвечает за построение реального графика платежей, который подходит под все типы графиков.

Примечание.
В конкретном типе графика различается лишь порядок расчета плановых сумм, но механизм расчета строк просрочек один
для всех.

"""


__author__ = 'Glukhenko A.V.'


from collections import defaultdict

import sbis
from loans.loanConsts import LC
from .plan_row import PlanRow
from .payment_row import PaymentRow
from .delay_row import DelayRow
from .correction_row import CorrectionRow


class RealPaymentSchedule(PlanRow, PaymentRow, DelayRow, CorrectionRow):
    """Класс для построения реального графика платежей"""
    def __init__(self, _filter, navigation):
        super().__init__(_filter, navigation)

    def build(self):
        """Построение графика платежей"""
        if not self._is_valid_filter():
            return self.result

        if self._is_registered():
            schedule = self.build_schedule()
        else:
            schedule = super().build_schedule()
        return schedule

    def build_schedule(self):
        """Строит график по займу"""
        self._build_schedule()
        self._post_processing(self.result)
        return self.result

    def _build_schedule(self):
        """
        Строит график по займу
        Принципы построения реального графика платежей следующие:
        1. График имеет плановые периоды оплаты (месяц).
        2. В течении планового периода может поступить N платежей и/или образовываться закрытая/открытая просрочки
        и/или образоваться корректирующая строка нетипового сценария. Все эти записи хранятся в period_rows
        (в рамках планового периода). Позже принимается решение, какие записи добавлять в график, какие нет.
        Важно: в процессе обработки платежа тип записи может иметь значение SCHEDULE_PAYMENT или SCHEDULE_PAYMENTS,
        в period_rows всегда храним с ключом SCHEDULE_PAYMENT, т.к. не важно запись с одним платежем или несколькими.
        3. Если поступает платеж и имеется факт просрочки платежа - то такая просрочка считается закрытой.
        (она построянная на все время).
        4. Если текущий день входит в плановый период и имеется факт просрочки - то такая просрочка считается открытой
        (она меняется изо дня в день, суммы просрочки увеличиваются если платежи не поступают).
        5. Закрытая просрочка формируется только платежом.
        6. Открытая просрочка формируется только текущим днем.
        7. При наличии корректирующей записи в плановом периоде, в график не должны попасть записи просрочек и плана.
        8. В плановом пероде сначала обрабатываются платежи, потом плановые записи, потом корректирующие записи.
        """
        debt = None if self._is_registered() else self._filter.Get('SizePayment')
        plan_dates = self._get_plan_dates(use_date_prolongation=True)
        last_plan_date = plan_dates[-1]
        period_rows = defaultdict(list)

        periods = self._get_periods(use_date_prolongation=True, hide_dublicate=True, check_disbursement=True)
        for date_begin, date_end in periods:
            is_last_sub_period = self.__check_is_last_sub_period(date_end, last_plan_date, debt)

            self.__check_date_delay(date_begin)
            self._processing_payments(date_begin, date_end, debt, period_rows)
            self._processing_plan(date_begin, date_end, debt, is_last_sub_period, period_rows)
            self._processing_correction(date_begin, period_rows)
            self.__post_processing_period(date_end, period_rows)

            row_debt = self.__get_row_debt(period_rows)
            if row_debt:
                debt = row_debt.Get('ОстатокДолга')

            if self.__is_need_interrupt_build(period_rows, debt, date_end):
                break
            period_rows.clear()

    def __is_need_interrupt_build(self, period_rows, debt, date_end):
        """
        Проверяет, требуется ли прервать построение графика. Прерываем при наступлении следующих случаев:
        - платежи все обработаны И
            - суммы по графику выплачены ИЛИ
            - сформирована корректирующая запись ИЛИ
            - остаток долга нулевой по будущей плановой записи
        :param period_rows: записи графика по плановому периоду, defaultdict
        :param debt: остаток долга
        :param date_end: дата окончания планового подпериода
        """
        is_paid_off = not debt and not self._is_underpayment_exist()
        correction_exist = bool(period_rows[LC.SCHEDULE_CORRECTION])
        is_last_future_plan = self.__check_last_future_plan(period_rows)
        return self.__payments_is_processed(date_end) and \
            any((is_paid_off, correction_exist, is_last_future_plan))

    def __check_is_last_sub_period(self, date_end, last_plan_date, debt):
        """
        Проверяет, идет ли обработка последнего планового периода
        :param date_end: окончание планового периода
        :param last_plan_date: дата последнего планового периода
        :param debt: остаток долга
        """
        already_paid_off = debt is not None and debt <= self.monthly_payment
        is_last_sub_period = (date_end == last_plan_date) or already_paid_off
        return is_last_sub_period

    def __payments_is_processed(self, date_end):
        """
        Проверяет, все ли платежи обработаны
        :param date_end: дата окончаия планового подпериода
        """
        date_last_payment = self._get_date_last_payment()
        return bool(date_last_payment and date_end >= date_last_payment)

    def __check_last_future_plan(self, period_rows):
        """
        Проверяет, создана ли последняя будущая плановая запись
        :param period_rows: записи графика по плановому периоду, defaultdict
        Примечание: остаток долга нулевой для такой плановой записи
        """
        plan = period_rows[LC.SCHEDULE_PLAN]
        return plan and plan[0].Get('Дата') > self.today and not plan[0].Get('ОстатокДолга')

    def __post_processing_period(self, date_end, period_rows):
        """
        Постобработка планового периода. Определяемся какие строки нужно добавить в график платежей.
        :param date_end: окончание планового периода, date
        :param period_rows: записи графика по плановому периоду, defaultdict
        Примечание: если требуется скрыть плановую запись, пересекающуюся с открытой просрочкой, то плановые суммы
        надо перекинуть в открытую просрочку.
        """
        processing_rows = [
            LC.SCHEDULE_PLAN,
            LC.SCHEDULE_PAYMENT,
            LC.SCHEDULE_OPEN_DELAY,
            LC.SCHEDULE_DELAY,
            LC.SCHEDULE_CORRECTION,
        ]

        # if period_rows[LC.SCHEDULE_CORRECTION]:
        #     processing_rows.remove(LC.SCHEDULE_PLAN)
        #     processing_rows.remove(LC.SCHEDULE_OPEN_DELAY)
        #     processing_rows.remove(LC.SCHEDULE_DELAY)

        plan_row = period_rows[LC.SCHEDULE_PLAN][0] if period_rows[LC.SCHEDULE_PLAN] else None
        open_delay_row = period_rows[LC.SCHEDULE_OPEN_DELAY][0] if period_rows[LC.SCHEDULE_OPEN_DELAY] else None
        if self.__need_hide_plan_row(date_end, plan_row, open_delay_row):
            open_delay_row = self.join_sum_rows(open_delay_row, plan_row)
            period_rows[LC.SCHEDULE_OPEN_DELAY] = [open_delay_row]
            if LC.SCHEDULE_PLAN in processing_rows:
                processing_rows.remove(LC.SCHEDULE_PLAN)

        for type_row in processing_rows:
            for row in period_rows[type_row]:
                self.result.AddRow(row)

    def __need_hide_plan_row(self, date_end, plan_row, open_delay_row):
        """
        Определяет, нужно ли скрыть плановую запись в плановом периоде. Если открытую просрочку уже создали и открытая
        просрочка пересекается с плановой записью, то такую плановую запись надо скрыть (граничные условия)
        :param date_end: окончание планового периода
        :param plan_row: плановая запись
        :param open_delay_row: запись открытой просрочки
        """
        return all((
            bool(open_delay_row),
            bool(plan_row),
            date_end == self.today,
        ))

    @staticmethod
    def __get_row_debt(period_rows):
        """
        Возвращает строку графика, от которой нужно взять остаток долга
        :param period_rows: записи графика по плановому периоду, defaultdict
        Примечание: поскольку плановая запись обрабатывается после открытой просрочки, то приоритет для записи
        plan_or_delay_row отдается плановой записи
        Примечание2: при наличии и плановой и фактической записи с одинаковой датой, предпочтение отдается плановой
        записи, поскольку она обрабатывается позже
        """
        if period_rows[LC.SCHEDULE_CORRECTION]:
            return period_rows[LC.SCHEDULE_CORRECTION][0]

        plan_row = period_rows[LC.SCHEDULE_PLAN][0] if period_rows[LC.SCHEDULE_PLAN] else None
        delay_row = period_rows[LC.SCHEDULE_OPEN_DELAY][0] if period_rows[LC.SCHEDULE_OPEN_DELAY] else None
        payment_row = period_rows[LC.SCHEDULE_PAYMENT][-1] if period_rows[LC.SCHEDULE_PAYMENT] else None
        plan_or_delay_row = plan_row or delay_row
        # найдем строку, от которой взять остаток долга
        if all((payment_row, plan_or_delay_row)):
            date_payment = payment_row.Get('Дата')
            date_plan = plan_or_delay_row.Get('Дата')
            if date_payment != date_plan:
                row = payment_row if date_payment > date_plan else plan_or_delay_row
            else:
                row = plan_or_delay_row or payment_row
        else:
            row = plan_or_delay_row or payment_row
        return row

    def __check_date_delay(self, date):
        """
        Проверяет не появилась ли просрочка по графику
        :param date: дата начала очередного планового периода
        """
        if self._is_underpayment_exist():
            self.delay_period['end'] = date
            if not self.delay_period.get('begin'):
                self.delay_period['begin'] = date

    def __correct_date_delay(self, date):
        """
        Корректирует дату просрочки
        :param date: дата окончания прошлого подпериода
        PS: если просросчка не погашена, то дата окончания прошлого подпериода - становится началом новой просрочки
        """
        self.delay_period['begin'] = date if self._is_underpayment_exist() else None
        self.delay_period['end'] = None

    def _processing_payments(self, date_begin, date_end, debt, period_rows):
        """
        Обработка платежей
        :param date_begin: начало периода платежа, date
        :param date_end: окончание периода платежа, date
        :param debt: остаток долга за прошлый период, Money
        :param period_rows: записи графика по плановому периоду, defaultdict
        """
        payments_dates_by_plan = self._get_payments_dates_by_plan()
        dates_payments = payments_dates_by_plan.get(date_end, [])
        for date_payment in dates_payments:
            self._processing_payment(date_payment, date_payment, debt, period_rows)
        self.__check_date_delay(date_begin)

    def _processing_payment(self, date_begin, date_end, debt, period_rows):
        """
        Обработка платежа
        :param date_begin: начало периода платежа, date
        :param date_end: окончание периода платежа, date
        :param debt: остаток долга за прошлый период, Money
        :param period_rows: записи графика по плановому периоду, defaultdict
        Примечание: если платеж поступил при наличии просрочки, то дополнительно сформируем закрытую запись просрочки
        """
        payment = self.payments.get(date_begin)
        delay_period_begin = self.delay_period.get('begin')
        if delay_period_begin:
            close_delay_row = self._create_close_delay_row(delay_period_begin, date_end, debt, is_last_row=False)
            period_rows[LC.SCHEDULE_DELAY].append(close_delay_row)
        payment_row = self._create_fact_row(date_begin, date_end, payment,
                                            is_delay_payment=bool(period_rows[LC.SCHEDULE_DELAY]))
        period_rows[LC.SCHEDULE_PAYMENT].append(payment_row)
        self.__correct_date_delay(date_end)

    def _processing_plan(self, date_begin, date_end, debt, is_last_sub_period, period_rows):
        """
        Обработка плановой записи
        :param date_begin: начало периода плановой записи
        :param date_end: окончание периода плановой записи
        :param debt: остаток долга за прошлый период, Money
        :param is_last_sub_period: признак расчета последнего планового периода, bool
        :param period_rows: записи графика по плановому периоду, defaultdict
        """
        if self.__check_need_skip_old_plan(date_end):
            self.delay_periods.append((date_begin, date_end))
            return None

        if self.__check_need_open_delay(date_begin, date_end):
            is_last_row = is_last_sub_period and self.today == date_end
            open_delay_row = self._create_open_delay_row(self.delay_period.get('begin'), self.today, debt, is_last_row)
            debt = open_delay_row.Get('ОстатокДолга')
            period_rows[LC.SCHEDULE_OPEN_DELAY].append(open_delay_row)
            self.delay_period.clear()

        if self.__check_need_plan(is_last_sub_period, period_rows, debt, date_end):
            plan_row = self._create_plan_row(date_begin, date_end, debt, is_last_sub_period)
            self.delay_periods.clear()
            period_rows[LC.SCHEDULE_PLAN].append(plan_row)
        return None

    def _processing_correction(self, date_begin, period_rows):
        """
        Обработка корректирующей записи (запись добавляемая для нетипового сценария)
        :param date_begin: дата начала периода
        :param period_rows: записи графика по плановому периоду, defaultdict
        """
        date_payment = self.__get_date_payment_sub_period(date_begin, period_rows)
        if self.__check_need_correction_row(date_payment):
            correction_row = self._create_correction_row(date_payment)
            period_rows[LC.SCHEDULE_CORRECTION].append(correction_row)

    @staticmethod
    def __get_date_payment_sub_period(date_begin, period_rows):
        """
        Возвращает дату последнего платежа по плановому периоду
        :param date_begin: дата начала периода
        :param period_rows: записи графика по плановому периоду, defaultdict
        Примечание: если последний платеж идет до начала текущего планового периода - он нам не интересен
        """
        if period_rows[LC.SCHEDULE_PAYMENT]:
            last_date_payment_sub_period = period_rows[LC.SCHEDULE_PAYMENT][-1].Get('Дата')
            if last_date_payment_sub_period <= date_begin:
                last_date_payment_sub_period = None
        else:
            last_date_payment_sub_period = None

        return last_date_payment_sub_period

    def __check_need_correction_row(self, date_payment):
        """
        Проверяет, нужно ли создавать корректирующую запись
        :param date_payment: дата платежа
        Примечание: корректирующаую запись формируем при наступлении нетипового сценария
        Примечание2: если платеж пришел сегодня, то дадим еще время на "доплату" и не будем
        показывать корректировку
        """
        return date_payment and all((
            date_payment != self.today,
            date_payment == self._get_date_last_payment(),
            self.is_not_typical_case(),
        ))

    def __check_need_skip_old_plan(self, date_end):
        """
        Проверяет нужно ли пропустить обработку 'старой' плановой записи при наличии просрочки
        :param date_end: конец обрабатываемого периода
        Примечание: если есть просрочка и то обрабатывать старую плановую запись не нужно, до тех пор пока не наступит
        текущий день или не поступит платеж
        """
        return date_end < self.today and self.delay_period.get('begin')

    def __check_need_open_delay(self, date_begin, date_end):
        """
        Проверяет, нужно ли создавать открытую просрочку для обрабатываемого периода
        :param date_begin: начало обрабатываемого периода
        :param date_end: конец обрабатываемого периода
        Примечание: если текущий день входит в период плана и есть просрочка, то дополнительно сформируем открытую
        просрочку (учитываем что открытой просрочки в один день не может быть)
        """
        delay_period_begin = self.delay_period.get('begin')
        return delay_period_begin and all((
            date_begin <= self.today <= date_end,
            delay_period_begin != self.today,
        ))

    def __check_need_plan(self, is_last_sub_period, period_rows, debt, date_end):
        """
        Проверяет, нужно ли создавать плановую запись
        :param is_last_sub_period: признак последней строки графика
        :param period_rows: записи графика по плановому периоду, defaultdict
        :param debt: остаток долга, sbis.Money
        :param date_end: окончание планового подпериода, дата
        Примечание:
        Плановую запись не требуется создавать если для последнего периода:
        - есть открытая просрочка
        - последний платеж оплатил всю задолжность (нулевой ОстатокДолга)
        """
        open_delay_exist = bool(period_rows[LC.SCHEDULE_OPEN_DELAY])
        is_old_plan = date_end <= self.today
        is_first_period = debt is None
        is_last_payment_paid_debt = self.__is_last_payment_paid_debt(period_rows)
        case_skip = any((open_delay_exist, is_last_payment_paid_debt))
        need_skip = all((
            is_old_plan,
            not is_first_period,
            is_last_sub_period,
            case_skip,
        ))
        return not need_skip

    def __is_last_payment_paid_debt(self, period_rows):
        """
        Проверяет, что последний платеж закрыл всю задолжность
        :param period_rows: записи графика по плановому периоду, defaultdict
        """
        is_paid_debt = False
        payments = period_rows[LC.SCHEDULE_PAYMENT]
        if payments:
            last_payment = payments[-1]
            is_paid_debt = not last_payment.Get('ОстокДолга')
        return is_paid_debt

    @staticmethod
    def join_sum_rows(base_row, *rows):
        """
        Перекидывает суммы вспомогательных строк rows в базовую строку base_row
        Объединяет строки отчета
        :param base_row: базовая строка отчета
        :param rows: вспомогательные строки
        :return: строка с объединеными суммами
        Примечание: учитываем что при увеличении суммы погашения основного долга, меняется и остаток долга
        """
        fields = (
            'ОсновнойДолг', 'ОсновнойДолгПлан',
            'НачисленныеПроценты', 'НачисленныеПроцентыПлан',
            'РазмерПлатежа', 'РазмерПлатежаПлан',
        )
        change_body = sbis.Money(0)
        for row in rows:
            for field in fields:
                base_row[field] = base_row.Get(field) + row.Get(field)
                if field in ('ОсновнойДолг', 'ОсновнойДолгПлан'):
                    change_body += row.Get(field)

        base_row['ОстатокДолга'] = base_row.Get('ОстатокДолга') - change_body
        return base_row

    @staticmethod
    def __get_name_case(id_case):
        """Возвращает название сценария, по которому была сформирована строка графика платежей"""
        name_cases = {
            LC.CASE_DEFAULT_DELAY: LC.CASE_DEFAULT_DELAY_NAME,
            LC.CASE_EARLY_FIRST_PAYMENT: LC.CASE_EARLY_FIRST_PAYMENT_NAME,
            LC.CASE_LAST_ROW_DELAY: LC.CASE_LAST_ROW_DELAY_NAME,
            LC.CASE_OVER_BODY_DEBT_DELAY: LC.CASE_OVER_BODY_DEBT_DELAY_NAME,
        }
        return name_cases.get(id_case)
