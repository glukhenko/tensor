"""
Модуль отвечает за формирование строк платежей в графике.
"""


__author__ = 'Glukhenko A.V.'


from collections import defaultdict

import sbis
from loans.loanConsts import LC
from .ideal import IdealPaymentSchedule


class PaymentRow(IdealPaymentSchedule):
    """Класс хранящий обработчики по формированию строк платежей"""
    def __init__(self, _filter, navigation):
        super().__init__(_filter, navigation)

    def _create_fact_row(self, date_begin, date_end, payment, is_delay_payment):
        """
        Создает запись платежки графика платежей
        :param date_begin: начало периода платежки
        :param date_end: окончание периода платежки
        :param payment: запись платежа, Record
        :param is_delay_payment: признак что данный платеж поступил в пользу оплаты просрочки, bool
        :return: запись, Record
        """
        plan = defaultdict(sbis.Money)
        fact = self.__get_fact_sum(payment)

        self._calc_balance(date_end, plan, fact, is_delay_payment=is_delay_payment)

        row = sbis.Record(self.result_format)
        row['@Документ'].From(payment.Get('@Документ'))
        row['Дата'] = payment.Get('Дата')
        row['НачалоПериода'] = date_begin
        row['КонецПериода'] = date_end
        row['РазмерПлатежа'] = fact['size_payment']
        row['РазмерПлатежаПлан'] = sbis.Money()
        row['ОсновнойДолг'] = fact['body_debt']
        row['ОсновнойДолгПлан'] = sbis.Money()
        row['НачисленныеПроценты'] = fact['percent']
        row['НачисленныеПроцентыПлан'] = sbis.Money()
        row['ОстатокДолга'] = payment.Get('ОстатокДолга')
        row['ТипЗаписи'] = self.__get_type_row(payment)
        row['already_paid'] = self._is_already_paid()
        row['Детализация'] = self._get_detail(fact.get('detail'))
        return row

    def __get_fact_sum(self, payment):
        """
        Возвращает фактические суммы по платежу
        """
        is_issued = self.lcdb.isIssuedLoanTypeByID(self._filter.Get('TypeDoc'))
        debit_payment = payment.Get(self.payment_debit_field_name)
        credit_payment = payment.Get(self.payment_credit_field_name)
        debit_percent = payment.Get(self.percent_debit_field_name)
        credit_percent = payment.Get(self.percent_credit_field_name)
        if is_issued:
            body_debt = credit_payment - debit_payment
            percent = credit_percent - debit_percent
        else:
            body_debt = debit_payment - credit_payment
            percent = debit_percent - credit_percent
        size_payment = body_debt + percent

        return {
            'size_payment': size_payment,
            'percent': percent,
            'body_debt': body_debt,
        }

    def __get_type_row(self, payment):
        """
        Возвращает тип документа платежа
        :param payment: запись платежа, Record
        """
        if payment.Get('ТипДокумента') == 'НачальныйОстаток':
            type_row = LC.SCHEDULE_INITIAL_BALANCE
        elif len(payment.Get('id_docs')) > 1:
            type_row = LC.SCHEDULE_PAYMENTS
        else:
            type_row = LC.SCHEDULE_PAYMENT
        return type_row
