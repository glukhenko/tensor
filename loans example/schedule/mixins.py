"""
Модуль, содержающий миксины, помогающие в процессе построения графиков
Примечание: классы в данном модуле нельзя назвать официально миксинами (т.к. в основном наследуется только базовым
классом BasePaymentsSchedule). Они были реализованы с целью упрощения логики базового класса BasePaymentsSchedule.
"""


__author__ = 'Glukhenko A.V.'


from collections import defaultdict

import sbis
from loans.loanConsts import LC


class FieldNamesMixin:
    """Класс используется для хранения имен полей"""
    def __init__(self):
        self.disb_debit_field_name = 'ДебетДолг'
        self.disb_credit_field_name = 'КредитДолг'
        self.payment_debit_field_name = 'ДебетДолг'
        self.payment_credit_field_name = 'КредитДолг'
        self.percent_debit_field_name = 'ДебетПроценты'
        self.percent_credit_field_name = 'КредитПроценты'
        self.disb_dc_field_name, self.payment_dc_field_name, self.payment_percent_dc_field_name = self.__get_dc_field()

    def __get_dc_field(self):
        """Возвращает поля дебета кредита в зависимости от объекта"""
        obj = sbis.Session.ObjectName()
        is_issued = obj == LC.ISSUED_LOAN_DOC_TYPE
        if is_issued:
            disb_dc_field_name = self.disb_debit_field_name
            payment_dc_field_name = self.payment_credit_field_name
            payment_percent_dc_field_name = self.percent_credit_field_name
        else:
            disb_dc_field_name = self.disb_credit_field_name
            payment_dc_field_name = self.payment_debit_field_name
            payment_percent_dc_field_name = self.percent_debit_field_name
        return disb_dc_field_name, payment_dc_field_name, payment_percent_dc_field_name

class PaymentsStorageMixin:
    """
    Класс используется для хранения платежей направленных на различные цели
    Структура объекта хранения состоит из следующих полей
    size_payment - сумма платежа
    body_debt - сумма основного долга
    percent - сумма процентов
    debt - остаток долга
    """
    def __init__(self):
        self.storage_fields = ('size_payment', 'body_debt', 'percent')
        self.storage_debt_field = 'debt'
        # внесенные денежные средства, раньше планового срока
        self.prepayment = defaultdict(sbis.Money)
        # внесенные денежные средства, в дату планового платежа
        self.timely_payment = defaultdict(sbis.Money)
        # внесенные денежные средства, в пользу погашения просрочки
        self.delay_payment = defaultdict(sbis.Money)
        # недоплата по плановому графику платежа
        self.underpayment = defaultdict(sbis.Money)
        # переплата по плановому графику платежа
        self.overpayment = defaultdict(sbis.Money)

    def _get_balance(self, field):
        """
        Возвращает значение баланса
        :param field: поле по которому рассчитывается баланс
        :return: Сумма баланса, Money
        Примечание:
        - положительное значение баланса показывает что есть недоплата
        - отрицательное значение баланса показывает что есть переплата/предоплата/своевременная оплата
        """
        return self.underpayment[field] - self.overpayment[field] - \
            self.prepayment[field] - self.timely_payment[field] - self.delay_payment[field]

    def _is_underpayment_exist(self, field=None):
        """Проверяет наличие недоплаты"""
        if field in self.storage_fields:
            is_underpayment = self._get_balance(field) > 0
        else:
            is_underpayment = any(self._get_balance(field) > 0 for field in self.storage_fields)
        return is_underpayment

    def _is_overpayment_exist(self, field=None):
        """Проверяет наличие переплаты"""
        if field in self.storage_fields:
            is_overpayment = self._get_balance(field) < 0
        else:
            is_overpayment = any(self._get_balance(field) < 0 for field in self.storage_fields)
        return is_overpayment

    def _is_ideal_balance(self):
        """Проверяет является ли баланс погашения идеальным"""
        return all(self._get_balance(field) == 0 for field in self.storage_fields)

    def _is_already_paid(self):
        """
        Проверяет, что плановая запись графика оплачена в полном объеме
        PS: необходимо для скрытия плановых записей в графике платежей
        Примечание: по умолчанию скрываются "прошедшие" плановые записи. is_already_paid - говорит что надо скрыть
        будущую плановую запись, ибо она уже оплачена.
        """
        return all(self._get_balance(field) <= 0 for field in self.storage_fields)

    def _rebalance_by_overpayment(self):
        """
        Делает перебалансировку между хранилищами (при наличии переплаты)
        """
        for field in self.storage_fields:
            all_payment = self.prepayment[field] + self.timely_payment[field] + self.delay_payment[field]
            if all((
                    self.underpayment[field] > 0,
                    self.underpayment[field] < all_payment,
            )):
                self.overpayment[field] += all_payment - self.underpayment[field]
                self.underpayment[field] = sbis.Money(0)
                self.prepayment[field] = sbis.Money(0)
                self.timely_payment[field] = sbis.Money(0)
                self.delay_payment[field] = sbis.Money(0)

    def _correct_underpayment_by_delay(self, prev_plan, delay):
        """
        Корректирует суммы недоплаты, после формирования строки просрочки
        :param prev_plan: плановые суммы предыдущего планового периода
        :param delay: суммы просрочки
        Примечание: после формирования просрочки (открытой или закрытой) плановая сумма увеличилась, надо откатить
        насчитанные плановые суммы по предыщему периоду и зафиксировать рассчитанные суммы просрочки
        """
        for field in self.storage_fields:
            self.underpayment[field] += delay.get(field) - prev_plan.get(field)

    def _get_detail(self, row_detail=None):
        """
        Возвращает детальную информацию по созданию записи графика платежей
        :param row_detail: детальная информацию по созданию конкретной записи (плановая, платеж, просрочка,
        корректирующая)
        """
        detail = self._get_detail_store()
        if row_detail:
            detail = f'{row_detail}\n{detail}'
        return detail

    def _get_detail_store(self):
        """Возвращает информацию по хранили"""
        stores = (
            ('Недоплата', self.underpayment),
            ('Переплата', self.overpayment),
            ('Предоплата', self.prepayment),
            ('Своевременная плата', self.timely_payment),
            ('Оплата просрочки', self.delay_payment),
        )
        detail = '\n'.join(
            f"{name}: "\
            f"[Платеж: {str(store.get('size_payment') or 0)}, "\
            f"Основной долг: {str(store.get('body_debt') or 0)}, "\
            f"Проценты: {str(store.get('percent') or 0)}]"
            for name, store in stores if any(store.values())
        )
        if detail:
            detail = f'Содержимое хранилищ:\n{detail}\n{self._get_detail_balance()}'
        return detail

    def _get_detail_balance(self):
        """Возвращает информацию по балансу"""
        return f"Баланс: "\
               f"[Платеж: {str(self._get_balance('size_payment') or 0)}, "\
               f"Основной долг: {str(self._get_balance('body_debt') or 0)}, "\
               f"Проценты: {str(self._get_balance('percent') or 0)}]"
