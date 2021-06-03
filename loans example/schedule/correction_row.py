"""
Модуль отвечает за формирование корректирующих строк при наступлении нетипового сценария.
"""


__author__ = 'Glukhenko A.V.'


import sbis
from loans.loanConsts import LC
from .ideal import IdealPaymentSchedule


class CorrectionRow(IdealPaymentSchedule):
    """Класс хранящий обработчики по формированию строк платежей"""
    def __init__(self, _filter, navigation):
        super().__init__(_filter, navigation)

    def _create_correction_row(self, date_payment):
        """
        Создает корректирующую запись
        :param date_payment: дата платежа
        """
        balance_percent = self._get_balance('percent')
        balance_body_debt = self._get_balance('body_debt')

        id_row = str(date_payment).replace('-', '')
        row = sbis.Record(self.result_format)
        row['@Документ'].From('{},{}'.format(id_row, LC.SCHEDULE_CORRECTION_NAME))
        row['Дата'] = date_payment
        row['НачалоПериода'] = date_payment
        row['КонецПериода'] = date_payment
        row['РазмерПлатежаПлан'] = sbis.Money()
        row['ОсновнойДолг'] = sbis.Money()
        row['ОсновнойДолгПлан'] = balance_body_debt
        row['НачисленныеПроценты'] = sbis.Money()
        row['НачисленныеПроцентыПлан'] = balance_percent
        row['ТипЗаписи'] = LC.SCHEDULE_CORRECTION
        row['already_paid'] = self._is_already_paid()
        row['Подсказка'] = self.__get_tooltip_not_typical_case(balance_percent, balance_body_debt)
        row['Детализация'] = self._get_detail()
        return row

    @staticmethod
    def __get_tooltip_not_typical_case(balance_percent, balance_body_debt):
        """
        Возвращает подсказку (тултип) для корректирующей строки нетипового сценария
        :param balance_percent: баланс по процентам
        :param balance_body_debt: баланс по основному долгу
        """
        descriptions = []
        if balance_percent:
            descriptions.append('недоплата процентов' if balance_percent > 0 else 'переплата процентов')
        if balance_body_debt:
            descriptions.append('недоплата основного долга' if balance_body_debt > 0 else 'переплата основного долга')
        return f'{", ".join(descriptions).capitalize()}. Скорректируйте платеж.'
