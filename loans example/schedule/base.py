"""
Базовый модуль для построения графика платежей по займам
"""


__author__ = 'Glukhenko A.V.'


import datetime
import math
from collections import defaultdict
from functools import lru_cache

import sbis
from loans.loanConsts import LC
from loans.loanDBConsts import LCDB
from loans.percentsCommon import LoansDates
from loans.percentsCommon import PercentsCalculator
from loans.utils.periods import DatePeriod
from loans.schedule_v3.payments import Payments
from loans.loanRemains import LoanRemains
from loans.percentsCommon import LoanPercentsCalculator
from loans.version_loans import get_date_build
from .mixins import FieldNamesMixin, PaymentsStorageMixin
from .helpers import swap_id_type_row


class BasePaymentsSchedule(FieldNamesMixin, PaymentsStorageMixin):
    """Базовый класс по построению графика платежей займа"""
    def __init__(self, _filter, navigation):
        FieldNamesMixin.__init__(self)
        PaymentsStorageMixin.__init__(self)

        self._filter = _filter
        self.navigation = navigation
        self.lcdb = LCDB()
        self.today = self._filter.Get('DateBuild') or get_date_build() or datetime.date.today()
        self.doc_type = sbis.Session.ObjectName()
        self.result_format = sbis.MethodResultFormat('{}.SchedulePayment'.format(self.doc_type), 4)

        self.total = defaultdict(sbis.Money)
        self.percents_calc = PercentsCalculator()
        self.percents_calc_by_accrual = self.__create_calc_by_accrual()
        self.disbursements, self.percents, self.payments = self.__get_payments()
        # суммы по идеальному плану (рассчитывается в дочернем классе IdealPaymentSchedule)
        self.ideal_plans = {}
        # рассчиитывается в дочерних классах
        self.monthly_payment = sbis.Money(0)

    @lru_cache(maxsize=1)
    def _is_registered(self):
        """
        Возвращает признак, что договор займа зарегестрирован, т.е. произошла выдача денег
        Примечание: в печатной форме показываем только идеальный график платежей
        """
        # return False
        return bool(self.disbursements and not self.__check_is_print_request())

    def _get_date_begin_schedule(self):
        """
        Возвращает дату начала построения графика платежей
        Примечание: если выдача денег произошла позже, то это никак не влияет на дату начала графика (ранее график
        начинался строится с даты первой выдачи денег, см. blame)
        """
        return self._filter.Get('DateBegin')

    @lru_cache(maxsize=1)
    def get_rate(self):
        """Возвращает ставку по договору"""
        return sbis.Money((self._filter.Get('Rate') or 0) / 100)

    @lru_cache(maxsize=1)
    def get_monthly_rate(self):
        """Возвращает месячную ставку по договору"""
        return sbis.Money(self.get_rate() / 12)

    def __create_calc_by_accrual(self):
        """
        Создает калькулятор процентов, который используется при начислении процентов
        Примечание: этот калькулятор на основе ЖО, выполняется очень долго, надо будет заменить
        """
        calc_by_accrual = None
        type_doc = self._filter.Get('TypeDoc')
        id_face_loan = self._filter.Get('IdFaceLoan')
        id_organization = self._filter.Get('IdOrganization')
        if all((id_face_loan, id_organization)):
            loan = sbis.Record({
                'ТипДокумента': type_doc,
                'Лицо': id_face_loan,
                'ДокументНашаОрганизация': id_organization,
            })
            calc_by_accrual = LoanPercentsCalculator(LoanRemains(loan, self.lcdb))
        return calc_by_accrual

    @staticmethod
    def __check_is_print_request():
        """Проверяет что запрос пришел на печать графика. В таком случае надо строить идеальный график"""
        return 'ПечатьДокументов' in sbis.Session.TaskMethodName()

    @lru_cache(maxsize=1)
    def _is_valid_filter(self):
        """Проверяет валидность фильтра, для построение графика платежей"""
        type_schedule = self._filter.Get('TypeSchedule')
        fields = [
            self._filter.Get('TypeDoc'),
            self._filter.Get('SizePayment'),
            self._filter.Get('DateBegin'),
            type_schedule is not None,
        ]
        if type_schedule != LC.REPAYMENT_ON_DEMAND:
            fields.append(self._filter.Get('DateEnd'))

        return all(fields)

    def __check_payment_possible(self):
        """Проверяет права на создание оплаты"""
        return self.__check_access() and self.doc_type == LC.RECEIVED_LOAN_DOC_TYPE

    def __check_access(self):
        """Проверяет права на зону, в зависимости от режима оплаты"""
        type_doc_payment = self.__get_type_doc_payment()
        method_name = '{}.Create'.format(type_doc_payment)
        access = sbis.CheckRights.MethodRestrictions(method_name, 2)
        return access.Get('Allow')

    def __get_type_doc_payment(self):
        """Возвращает название зоны: платеж или заявка на оплату"""
        return 'ЗаявкаНаОплату' if self.__is_use_pay_app() else 'РасходныйОрдер'

    @staticmethod
    def __is_use_pay_app():
        """Проверяет конфигурацию на предмет, стоит ли использовать заявку на оплату"""
        apps = sbis.ГлобальныеПараметрыКлиента.ПолучитьЗначение("doNotUsePayApps") or "True"
        return apps.lower() != "true"

    def __get_payments(self):
        """Возвращает информацию по платежам"""
        return Payments(self._filter, self.lcdb).get_list()

    @lru_cache(maxsize=1)
    def get_first_date_disbursement(self):
        """Возвращает дату первой выдачи займа"""
        first_date_disbursement = None
        if self.disbursements:
            first_date_disbursement = sorted(self.disbursements)[0]
        return first_date_disbursement

    @lru_cache(maxsize=1)
    def _get_total_sum_disbursement(self):
        """Возвращает итоговую сумму всех выдачей по займу"""
        total_sum_disbursement = None
        if self.disbursements:
            total_sum_disbursement = sum(
                map(lambda r: sbis.Money(r.Get(self.disb_dc_field_name)), self.disbursements.values()))
        return total_sum_disbursement

    @lru_cache(maxsize=1)
    def __get_plan_date_first_payment(self, check_disbursement=False):
        """
        Возвращает ожидаемую дату первого платежа
        :param check_disbursement: учитывать дату выдачи денег
        Примечание: если выдача денег прошла позже даты первого платежа, то ориентируемся на дату выдачи денег
        """
        date_begin = self._filter.Get('DateBegin')
        date_end = self._filter.Get('DateEnd')

        first_payment_date = self._filter.Get('FirstPaymentDate')
        if first_payment_date and date_begin <= first_payment_date <= date_end:
            first_date = first_payment_date
        else:
            first_date = min(LoansDates.monthDelta(date_begin, 1), date_end)
        if check_disbursement:
            first_date_disbursement = self.get_first_date_disbursement()
            if first_date_disbursement > first_date:
                first_date = first_date_disbursement
        return first_date

    @lru_cache(maxsize=1)
    def _get_date_last_payment(self):
        """Возвращает дату последнего платежа"""
        date_last_payment = None
        dates = list(self.payments.keys())
        if dates:
            dates.sort()
            last_payment = self.payments.get(dates[-1])
            date_last_payment = last_payment.Get('Дата')
        return date_last_payment

    def is_not_typical_case(self):
        """
        Проверяет график на наличие нетипового сценария в графике
        Примечание: сценарий считается нетиповым когда после последнего платежа получаем одну из ситуаций:
        - недоплата только по процентам
        - недоплата по процентам и переплата по основному долгу
        - переплата только по процентам
        - переплата по процентам и недоплата по основному долгу
        Примечание2: учитываем что
        """
        case1 = all((
                self._get_balance('percent') > 0,
                self._get_balance('body_debt') <= 0,
            ))
        case2 = all((
                self._get_balance('percent') < 0,
                self._get_balance('body_debt') >= 0,
            ))

        print(f'correction case1: {case1}, case2: {case2}')
        return any((
            all((
                self._get_balance('percent') > 0,
                self._get_balance('body_debt') <= 0,
            )),
            all((
                self._get_balance('percent') < 0,
                self._get_balance('body_debt') >= 0,
            )),
        ))

    def _check_need_prolongation(self):
        """
        Проверяет, нужно ли достраивать график, т.е. пролонгировать
        PS: учитываем что платежи могут поступать после окончания договора
        """
        return all((
            self._is_registered(),
            self._filter.Get('DateEnd') and self._filter.Get('DateEnd') < self._get_date_prolongation(),
        ))

    @lru_cache(maxsize=1)
    def _get_date_prolongation(self):
        """Возвращает дату пролонгации"""
        date_last_payment = self._get_date_last_payment()
        if date_last_payment:
            date_prolongation = max([self.today, date_last_payment])
        else:
            date_prolongation = self.today
        return date_prolongation

    @lru_cache(maxsize=2)
    def _get_plan_dates(self, use_date_prolongation):
        """
        Получение списка дат с учетом типа погашения
        :param use_date_prolongation: использовать дату пролонгации
        Примечание: в помесячном погашении ориентируемся на дату первого платежа если займ выдан, в противном случае
        берем начало графика
        """
        date_end = self._filter.Get('DateEnd')
        type_schedule = self._filter.Get('TypeSchedule')
        if type_schedule in (LC.ANNUITY_SCHEDULE, LC.DIFFERENTIATED_SCHEDULE):
            plan_date_first_payment = self.__get_plan_date_first_payment()
            date_begin = plan_date_first_payment or self._filter.Get('DateBegin')
            dates = LoansDates.getNextMonthForPeriod(
                date_begin,
                date_end,
                LoansDates.isEndOfMonth(plan_date_first_payment),
            )
            user_payments_count = self.__get_user_payments_count()
            if user_payments_count:
                dates = dates[:user_payments_count]
        elif type_schedule in (LC.REPAYMENT_DEBT_AND_PERCENTS_AT_THE_END,):
            dates = [date_end]
        elif type_schedule in (LC.REPAYMENT_ON_DEMAND,):
            dates = []

        if all((use_date_prolongation, self._check_need_prolongation())):
            dates.append(self._get_date_prolongation())
        dates.sort()
        return dates

    @lru_cache(maxsize=1)
    def _get_schedule_dates(self):
        """
        Возвращает список дат графика
        В список входят:
        - плановые даты начислений
        - фактические платежи
        - текущий день для формирования открытой просрочки
        :return: list of datetime.date
        """
        schedule_dates = self._get_plan_dates(use_date_prolongation=True) + list(self.payments.keys())
        if self.today not in schedule_dates:
            schedule_dates.append(self.today)
        schedule_dates.sort()
        return schedule_dates

    @lru_cache(maxsize=4)
    def _get_periods(self, use_date_prolongation, hide_dublicate=True, check_disbursement=False):
        """
        Вовзращет список периодов графика
        :param use_date_prolongation: использовать дату пролонгации
        :param hide_dublicate: скрывать дублирующие периоды
        :param check_disbursement: учитывать дату выдачи денег
        """
        if self._filter.Get('TypeSchedule') == LC.REPAYMENT_ON_DEMAND:
            schedule_periods = []
        else:
            if check_disbursement:
                date_begin = max(self._get_date_begin_schedule(), self.get_first_date_disbursement())
                dates = [date_begin] + self._get_plan_dates(
                    use_date_prolongation)
                dates = list(filter(lambda d: d >= date_begin, dates))
            else:
                dates = [self._get_date_begin_schedule()] + self._get_plan_dates(use_date_prolongation)
            schedule_periods = LoansDates.get_sub_periods(*dates, hide_dublicate=hide_dublicate)
        return schedule_periods

    @lru_cache(maxsize=1)
    def _get_agg_disbursements(self):
        """
        Аггрегирует выдачи займов по плановым датам платежа
        :return: словарь вида
            disbursements_by_month[plan_date] = {
                first_date_disbursement: first_sum_disbursement,
                second_date_disbursement: second_sum_disbursement,
                ...
            }
        PS: в один день может быть и выдача займа и плановый платеж, в приоритете выдача займа
        """
        plan_dates = self._get_plan_dates(use_date_prolongation=True)
        dates = plan_dates + list(self.disbursements.keys())
        dates.sort()
        disbursements_by_month = {}
        debts = {}

        for date in dates:
            disbursement = self.disbursements.get(date)
            if disbursement:
                debts[date] = disbursement.Get(self.disb_dc_field_name)
            if date in plan_dates:
                disbursements_by_month[date] = {
                    'debts': debts,
                }
                debts = {}

        return disbursements_by_month

    @lru_cache(maxsize=1)
    def _get_agg_payments(self):
        """
        Аггрегирует фактические платежи по плановым датам платежа
        :return: словарь вида
            payment_by_month[plan_date] = {
                'debts': {
                    first_date_payment: first_sum_payment,
                    second_date_payment: second_sum_payment,
                    ...
                },
                'total': {
                    'body_debt': total_body_debt,
                    'percent': total_percent,
                    'size_payment': total_size_payment,
                    'debt': last_debt,
                }
            }
        PS: в один день может быть и фактический платеж и плановый платеж, в приоритете фактический платеж
        """
        plan_dates = self._get_plan_dates(use_date_prolongation=True)
        dates = plan_dates + list(self.payments.keys())
        dates = list(set(dates))
        dates.sort()
        payment_by_month = {}
        debts = {}
        total = {
            'body_debt': sbis.Money(),
            'percent': sbis.Money(),
            'size_payment': sbis.Money(),
            'debt': None,
        }

        for date in dates:
            payment = self.payments.get(date)
            if payment:
                debts[payment.Get('Дата')] = payment.Get(self.payment_dc_field_name)
                total['body_debt'] += payment.Get(self.payment_dc_field_name)
                total['percent'] += payment.Get(self.payment_percent_dc_field_name)
                total['size_payment'] = total['body_debt'] + total['percent']
                total['debt'] = payment.Get('ОстатокДолга')

            payment_exist = any(total)
            if date in plan_dates and all((debts, payment_exist)):
                payment_by_month[date] = {
                    'debts': debts,
                    'total': total,
                }
                debts = {}
                total = {
                    'body_debt': sbis.Money(),
                    'percent': sbis.Money(),
                    'size_payment': sbis.Money(),
                    'debt': None,
                }

        return payment_by_month

    @lru_cache(maxsize=1)
    def _get_plan_date_by_payment(self):
        """
        Возвращает связь фактического платежа с его родительским плановым платежом
        :return: словарь вида
            plan_date_by_payment[payment_date] = plan_date
        """
        plan_dates = self._get_plan_dates(use_date_prolongation=True)
        dates = plan_dates + list(self.payments.keys())
        dates.sort()
        date_payments = []
        plan_date_by_payment = {}

        for date in dates:
            if date in self.payments:
                date_payments.append(date)
            if date in plan_dates:
                plan_date_by_payment.update(
                    {date_payment: date for date_payment in date_payments}
                )
                date_payments = []

        return plan_date_by_payment

    @lru_cache(maxsize=1)
    def _get_payments_dates_by_plan(self):
        """
        Возвращает набор платежей по плановой дате
        :return: {plan_date: [payment_dates_1, payment_dates_2, ...]}
        """
        payment_dates_by_plan = {}
        plan_dates = self._get_plan_dates(use_date_prolongation=True)
        dates = list(set(plan_dates) | set(self.payments.keys()))
        dates.sort()
        payment_dates = []

        for date in dates:
            if date in self.payments:
                payment_dates.append(date)
            if date in plan_dates:
                payment_dates_by_plan[date] = payment_dates
                payment_dates = []

        return payment_dates_by_plan

    @lru_cache(maxsize=1)
    def _get_plan_period_by_date(self):
        """
        Возвращает плановый период по дате записи графика
        где запись графика может быть:
        - фактический платеж
        - открытая просрочка (т.е. текущий день)
        :return: словарь вида
            plan_period_by_date[schedule_date] = (plan_date_begin, plan_date_end)
        PS: не понятно пока как сделать оптимально и без двух циклов, подумать позже
        """
        plan_period = {}
        schedule_dates = list(set(self._get_schedule_dates()) | {self.today})
        schedule_dates.sort()
        plan_periods = self._get_periods(use_date_prolongation=True)

        for schedule_date in schedule_dates:
            for plan_date_begin, plan_date_end in plan_periods:
                if plan_date_begin < schedule_date <= plan_date_end:
                    plan_period[schedule_date] = (plan_date_begin, plan_date_end)
        return plan_period

    def __get_user_payments_count(self):
        """Возвращает пользовательское количество платежей, расчитывающеся исходя из заданого размера платежа"""
        payments_count = None
        if self._filter.Get('MonthlyPayment'):
            if self._filter.Get('TypeSchedule') != LC.ANNUITY_SCHEDULE:
                payments_count = self.__simple_calc_payments_count()
            else:
                payments_count = self.__calc_payments_count()
        return payments_count

    def __simple_calc_payments_count(self):
        """
        Рассчитывает количество платежей по ежемесячному платежу, заданному пользователем
        """
        loan_sum = self._get_total_sum_disbursement() or self._filter.Get('SizePayment')
        payments_count = loan_sum / self._filter.Get('MonthlyPayment')
        return math.ceil(payments_count)

    def __calc_payments_count(self):
        """
        Рассчитывает количество платежей по ежемесячному платежу, заданному пользователем
        PS: не производим расчет в случае маленького ежемесячного платежа
        """
        monthly_rate = self.get_monthly_rate()
        user_monthly_payment = self._filter.Get('MonthlyPayment')
        loan_sum = self._filter.Get('SizePayment')
        if user_monthly_payment < loan_sum * monthly_rate:
            return None

        base = 1 + monthly_rate
        term = user_monthly_payment / (user_monthly_payment - loan_sum * monthly_rate)
        payments_count = math.log(term, base)
        return math.ceil(payments_count)

    @lru_cache(maxsize=128)
    def _calc_percent(self, debt, date_begin, date_end, limit_date_payment=None):
        """
        Рассчитывает сумму начислений процентов исходя из новых выдач и новых платежей в периоде date_begin - date_end
        :param debt: остаток долга на date_begin, по которому идет начисление процентов
        :param date_begin: начало периода
        :param date_end: окончание периода
        :param limit_date_payment: ограничитель платежа
        Примечание: если в месяце поступило 3 платежа: 5, 10 и 12 числа, то при передаче 10 числа в limit_date_payment
        то будет проигнорирован платеж поступивший позже, т.е. 12 числа
        :return: сумма начисленных процентов
        """
        debt = debt or sbis.Money()
        rate = self.get_rate()
        percent = sbis.Money()

        disbursements = self._get_agg_disbursements().get(date_end, {})
        payments = self._get_agg_payments().get(date_end, {})
        disbursement_dates = disbursements.get('debts', {}).keys()
        payment_dates = payments.get('debts', {}).keys()
        if limit_date_payment:
            payment_dates = [payment_date for payment_date in payment_dates if payment_date <= limit_date_payment]

        sub_periods = LoansDates.get_sub_periods(date_begin, date_end, *disbursement_dates, *payment_dates)
        for begin_sub_period, end_sub_period in sub_periods:
            debt += disbursements.get('debts', {}).get(begin_sub_period, sbis.Money())
            debt -= payments.get('debts', {}).get(begin_sub_period, sbis.Money())
            percent += self.percents_calc.calc(debt, rate, begin_sub_period, end_sub_period)

        return round(percent, 2)

    def _calc_percent_by_accrual(self, date_begin, date_end):
        """
        Рассчитывает сумму процентов исходя из логики начислений процентов
        :param date_begin: дата начала расчета
        :param date_end: дата окончания расчета
        :return:
        """
        result = self.percents_calc_by_accrual.calc(self.get_rate(), date_begin, date_end, use_cache=True)
        return result.get(LC.FLD_PERCENTS_BOOK_ACC)

    def _calc_near_payment(self, schedule):
        """Рассчитывает дату ближайшего платежа"""
        type_rows = (LC.SCHEDULE_PLAN, LC.SCHEDULE_OPEN_DELAY)
        future_plans = [rec for rec in schedule if
                        rec.Get('ТипЗаписи') in type_rows and rec.Get('Дата') >= self.today]
        if future_plans:
            future_plans[0]['БлижайшийПлатеж'] = True

    def _post_processing(self, schedule):
        """
        Постобработка графика платежей
        ВАЖНО:
        Перед вычислением просрочки необходимо:
        - отсортировать по дате
        - рассчитанное поле ближайшего платежа, __calc_near_payment
        - использовать устаревшие плановые записи, т.е. __remove_old_plan строго после расчета просрочки
        """
        schedule.sort(key=lambda rec: (rec.Get('Дата')))
        self._calc_near_payment(schedule)
        self._add_years(schedule)
        self._remove_incorrect_plan(schedule)
        self._calc_payment_button(schedule)
        self._mark_separator_line(schedule)
        self._calc_show_total(schedule)
        self._sort_result(schedule)
        self._calc_outcome(schedule)

    def _add_years(self, schedule):
        """
        Добавляет в набор графика платежей служебные строки дат
        """
        month, day = (1, 1) if self._filter.Get('OrderBy') == 'ASC' else (12, 31)
        for year in self.__calc_years(schedule):
            rec = sbis.Record(self.result_format)
            rec['@Документ'].From(sbis.ObjectId('ГодПлатежа', year))
            rec['Дата'] = datetime.date(year, month, day)
            rec['ОписаниеДата'] = str(year)
            rec['ТипЗаписи'] = LC.SCHEDULE_DATE
            schedule.AddRow(rec)

    def __calc_years(self, schedule):
        """Рассчитывает служебные строки дат, которые необходимо добавить в список займов"""
        if self._is_registered():
            years = {row.Get('Дата').year for row in schedule if row.Get('Дата') >= self.today}
        else:
            years = {row.Get('Дата').year for row in schedule}
        years.update(
            {date.year for date in self.payments}
        )

        if years:
            years = list(years)
            years.sort()
            index = 0 if self._filter.Get('OrderBy') == 'ASC' else len(years) - 1
            years.pop(index)
        return years

    def _get_description_delay(self, delay_begin, delay_end):
        """Возвращает описание строки просрочки"""
        name_period = DatePeriod.calc_name_period(delay_begin, delay_end)
        if delay_end == self.today and delay_end not in self.payments:
            description = 'Просрочка {name_period}'.format(name_period=name_period)
        else:
            description = 'Была просрочка {name_period}'.format(name_period=name_period)
        return description

    def _get_description_date_delay(self, delay_begin, delay_end):
        """
        Возвращает красивое описание даты строки просрочки
        PS: при наличии платежа на дату просрочки, текст зануляем
        """
        count_months_delay = self.__get_count_months_delay(delay_begin, delay_end)
        if delay_begin in self.payments:
            return None
        return DatePeriod.calc_beautiful_month_period(delay_begin, delay_end, count_months_delay)

    def __get_count_months_delay(self, delay_begin, delay_end):
        """
        Определяет количество просроченных месяцев
        :param delay_begin: начало просрочки, date
        :param delay_end: конец просрочки, date
        :return: количество месяцев просрочки, int
        """
        return len([date for date in self.ideal_plans if delay_begin <= date < delay_end])

    def _get_change_debts_by_month(self, date):
        """Возвращает на сколько изменился остаток долга для плановой записи date"""
        debt = sbis.Money()
        new_disbursements = self._get_agg_disbursements().get(date, {}).get('debts')
        new_payments = self._get_agg_payments().get(date, {}).get('debts')
        if new_disbursements:
            debt += sum(new_disbursements.values())
        if new_payments:
            debt -= sum(new_payments.values())
        return debt

    def _create_outcome(self):
        """Возвращает формат для строки итогов"""
        outcome = sbis.Record(self.result_format)
        outcome.CopyOwnFormat()
        outcome.AddString('order_by')
        outcome.AddMoney('monthly_payment')
        outcome.AddBool('payments_exist')
        return outcome

    @staticmethod
    def _calc_show_total(schedule):
        """Рассчитывает признак отображения шапки графика платежа"""
        show_total = schedule.Size() > 1
        for i in range(schedule.Size()):
            schedule.Set(i, 'show_total', show_total)

    def _calc_total_sum(self, schedule):
        """
        Рассчитывает итоговые суммы графика платежей
        - при наличии платежей, итоги содержат сумму всех платежей
        - при отсутствии платежей, итоги содержат сумму всех плановых записей в будущем
        """
        if not schedule:
            return

        is_registered = self._is_registered()
        use_payment = all((is_registered, self.payments))
        only_future_plan = all((is_registered, not self.payments))

        if use_payment:
            agg_type_row = (LC.SCHEDULE_PAYMENT, LC.SCHEDULE_PAYMENTS)
        else:
            agg_type_row = (LC.SCHEDULE_PLAN, LC.SCHEDULE_OPEN_DELAY)

        debts = []
        for i in range(schedule.Size()):
            type_row = schedule.Get(i, 'ТипЗаписи')
            date = schedule.Get(i, 'Дата')
            if type_row in agg_type_row:
                if only_future_plan and date < self.today:
                    continue
                for field in ('РазмерПлатежа', 'ОсновнойДолг', 'НачисленныеПроценты'):
                    source_field = field if use_payment else f'{field}План'
                    self.total[field] += schedule.Get(i, source_field)
                debts.append(schedule.Get(i, 'ОстатокДолга'))

        index = -1 if self._filter.Get('OrderBy') == 'ASC' else 0
        self.total['ОстатокДолга'] = debts[index] if debts else sbis.Money(0)

    def _mark_separator_line(self, schedule):
        """Добавляет разделительную линию, указывающую на текущий день"""
        if not self._is_registered():
            return

        type_rows = (
            LC.SCHEDULE_PLAN,
            LC.SCHEDULE_PAYMENT,
            LC.SCHEDULE_DELAY,
            LC.SCHEDULE_PAYMENTS,
        )
        rows_by_date = {row.Get('Дата'): row for row in schedule if row.Get('ТипЗаписи') in type_rows}
        if rows_by_date:
            dates = sorted(rows_by_date.keys())
            old_dates = list(filter(lambda d: d and d <= self.today, dates))
            if old_dates:
                last_date = dates[-1]
                separator_date = old_dates[-1]
                if separator_date != last_date:
                    row = rows_by_date.get(separator_date)
                    row['РазделительнаяЛиния'] = True

    def _remove_incorrect_plan(self, schedule):
        """
        Удаляет 'устаревшие' плановые записи графика платежей
        Плановая запись является устаревшей если:
        - она в прошлом
        - в будущем, но уже уплачена
        PS: Поскольку может быть удалена плановая запись с ближайшим платежем, то рассчитаем новую
        """
        near_payment_removed = False
        if self._is_registered():
            for i in reversed(range(schedule.Size())):
                if schedule.Get(i, 'ТипЗаписи') == LC.SCHEDULE_PLAN and self.__plan_is_incorrect(i, schedule):
                    near_payment_removed = near_payment_removed or bool(schedule.Get(i, 'БлижайшийПлатеж'))
                    print(f'REMOVE PLAN: {schedule[i]}')
                    # schedule.DelRow(i)

        if near_payment_removed:
            self._calc_near_payment(schedule)

    def __plan_is_incorrect(self, i, schedule):
        """
        Проверяет, является ли плановая запись графика 'устаревшей'
        :param i: порядковый номер записи графика
        :param schedule: график платежей
        """
        return schedule.Get(i, 'Дата') < self.today or schedule.Get(i, 'already_paid')

    def _calc_payment_button(self, schedule):
        """
        Рассчитывает поле can_payment, которое отвечает за отображение кнопки создания платежа "Уплатить"
        """
        can_payment = all((self._is_registered(), self.__check_payment_possible()))
        for i in range(schedule.Size()):
            schedule.Set(i, 'can_payment', can_payment)

    def _calc_balance(self, date, plan, fact, is_delay_payment=False):
        """
        Осуществляет расчет баланса после обработки записи плана или платежки
        :param date: дата обрабатываемой записи
        :param plan: суммы плановой записи
        :param fact: суммы платежки
        :param is_delay_payment: признак что данный платеж поступил в пользу оплаты просрочки, bool
        """
        is_payment = any(fact.values())

        if is_payment:
            storage = self.prepayment
            if date in self._get_plan_dates(use_date_prolongation=True):
                storage = self.timely_payment
            if is_delay_payment:
                storage = self.delay_payment

            storage['size_payment'] += fact['size_payment']
            storage['body_debt'] += fact['body_debt']
            storage['percent'] += fact['percent']
        else:
            self.underpayment['size_payment'] += plan['size_payment']
            self.underpayment['body_debt'] += plan['body_debt']
            self.underpayment['percent'] += plan['percent']

    def _correct_debt(self, debt, date, plan):
        """
        Корректирует остаток, по которому рассчитываются следующие плановые записи
        Корректируется сумма остатка на
            - сумму изменения остатка в течении месяца в зависимости от новых выдач или платежей
            - плановую сумму в графике, если он позже текущего дня и есть недоплата
        """
        debt = debt or sbis.Money()
        if self._is_registered():
            # учет выдачи/погашений
            change_debts = self._get_change_debts_by_month(date)
            if change_debts:
                debt += change_debts
            # учет плана
            if date >= self.today and self._is_underpayment_exist():
                debt -= plan.get('body_debt')
        else:
            debt = debt - plan.get('body_debt')
        return debt

    def _sort_result(self, schedule):
        """
        Сортировка графика платежей
        Данные сортируются по
        1. дате
        2. типу записи
        3. первичному ключу
        Примечание: есть особенность сортировки по типу записи, сначала всегда идут строки года, а потом данные этого
        года, т.е.
        1. Строка года должна быть всегда сверху, вне зависимости от сортировки (первая часть сотровки по типу записи)
        2. Строка данных (просрочка план или платеж), должен зависеть от клиентской сортировки (вторая часть сортировки
        по типу записи)
        """
        swap_id_type_row(schedule, to_new_const=True)

        order_by = self._filter.Get('OrderBy')
        direction = -1 if order_by == 'DESC' else 1
        is_reverse = order_by == 'DESC'
        # определим новый идентификатор записи года, учитывая манипулиции swap_id_type_row
        new_id_type_schedule_year = 0

        schedule.sort(key=lambda rec: (
            rec.Get('Дата') or rec.Get('ДатаНачалаПросрочки'),
            # первая часть сотровки по типу записи
            -1 * direction * int(rec.Get('ТипЗаписи') == new_id_type_schedule_year),
            # вторая часть сортировки по типу записи
            rec.Get('ТипЗаписи'),
            direction * rec.Get('@Документ'),
        ), reverse=is_reverse)

        swap_id_type_row(schedule, to_new_const=False)

    def _calc_outcome(self, schedule):
        """Добавляет строку итогов"""
        self._calc_total_sum(schedule)
        schedule.outcome = self._create_outcome()
        schedule.outcome['@Документ'].From(sbis.ObjectId(LC.SCHEDULE_OUTCOME_NAME, -1))
        schedule.outcome['РазмерПлатежа'] = self.total.get('РазмерПлатежа')
        schedule.outcome['РазмерПлатежаПлан'] = self.total.get('РазмерПлатежа')
        schedule.outcome['ОсновнойДолг'] = self.total.get('ОсновнойДолг')
        schedule.outcome['ОсновнойДолгПлан'] = self.total.get('ОсновнойДолг')
        schedule.outcome['НачисленныеПроценты'] = self.total.get('НачисленныеПроценты')
        schedule.outcome['НачисленныеПроцентыПлан'] = self.total.get('НачисленныеПроценты')
        schedule.outcome['ОстатокДолга'] = self.total.get('ОстатокДолга')
        schedule.outcome['ТипЗаписи'] = LC.SCHEDULE_OUTCOME
        schedule.outcome['monthly_payment'] = self.monthly_payment
        schedule.outcome['order_by'] = self._filter.Get('OrderBy')
        schedule.outcome['payments_exist'] = bool(self._is_valid_filter() and self.payments)
        schedule.outcome['show_total'] = schedule.Size() > 1
