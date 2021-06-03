"""
В договоре займа в графике платежей у ближайшего планового платежа требуется отобразить кнопку "Уплатить".
По кнопке уплатить должен создаваться документ исходящий платеж или заявка на оплату.
"""


__author__ = 'Glukhenko A.V.'

from collections import defaultdict

import sbis


PAYMENT_REQUEST_TYPE_DOC = 'ЗаявкаНаОплату'
OUTGOING_PAYMENT_TYPE_DOC = 'ИсходящийПлатеж'
PAYMENT_REQUEST_NAME_REGL = 'Заявка на оплату'
REPAYMENT_LOAN_NAME_REGL = 'Погашение займа и процентов'
REPAYMENT_LOAN_BODY_NAME_REGL = 'Погашение займа'
REPAYMENT_LOAN_PERCENT_NAME_REGL = 'Погашение процентов по займу'


class PaymentButton:
    """
    Базовый класс нового реестра займы и кредиты
    """
    def __init__(self, _filter):
        self._filter = _filter
        self.type_obj = sbis.Session.ObjectName()
        self.regls = self.__get_regls()
        self.id_type_docs = self.__get_id_type_docs_by_name()

    def payment_info(self):
        """
        Получение информации для создания платежа или заявки на оплату из графика платежей
        :return: Record
        Примечание:
        1. В случае создания платежа - возвращаются данные по типу документа "ИсходящийПлатеж"
        2. В случае создания заявки на оплату следует указать регламент операции
        """
        doc_regl = self.__get_doc_regl()
        if doc_regl:
            name_type_doc = doc_regl.Get('ТипДокумента').Get('Тип')
            values = {
                'ИдРегламент': doc_regl.Get('Идентификатор'),
                'НазваниеРегламента': doc_regl.Get('Название'),
                'ТипДокумента': name_type_doc,
                '@ТипДокумента': self.id_type_docs.get(name_type_doc),
                'ВидСвязи': 1,
            }
            regl_by_operation = self.__get_id_regl_by_operation()
            if regl_by_operation:
                values['ПлатежныеДокументы.Regulation'] = regl_by_operation
            return sbis.Record(values)

    def __get_regls(self):
        """
        Возвращает регламенты по типам документов
        :return: словарь вида
        {(type_doc, name_regl): [reglament1, reglament2, ...]}
        """
        type_docs = [PAYMENT_REQUEST_TYPE_DOC, OUTGOING_PAYMENT_TYPE_DOC]
        regl_names = [PAYMENT_REQUEST_NAME_REGL, REPAYMENT_LOAN_NAME_REGL, REPAYMENT_LOAN_BODY_NAME_REGL,
                      REPAYMENT_LOAN_PERCENT_NAME_REGL]

        result = defaultdict(list)

        format_type_doc = sbis.RecordFormat()
        format_type_doc.AddString('Тип')
        format_type_doc.AddString('ПодТип')
        rs_type_docs = sbis.RecordSet(format_type_doc)
        for type_doc in type_docs:
            rec = sbis.Record({
                'Тип': type_doc,
                'ПодТип': None,
            })
            rs_type_docs.AddRow(rec)

        _filter = sbis.Record({
            'Действующий': True,
            'ТипДокумента': rs_type_docs,
        })
        regulations = sbis.Regulation.List(_filter).Get('Регламент')
        for i, regl in enumerate(regulations):
            type_doc = regl.Get('ТипДокумента').Get('Тип')
            name_regl = regl.Get('Название')
            if name_regl in regl_names:
                key = (type_doc, name_regl)
                result[key].append(regl)
        return result

    def __get_id_type_docs_by_name(self):
        """Возвращает словарь вида {ТипДокумента: @ТипДокумента}"""
        type_docs = [PAYMENT_REQUEST_TYPE_DOC, OUTGOING_PAYMENT_TYPE_DOC]
        sql = '''
            SELECT
                "ТипДокумента", "@ТипДокумента"
            FROM
                "ТипДокумента"
            WHERE
                "ТипДокумента" = ANY($1::text[])
        '''
        return {row.Get('ТипДокумента'): row.Get('@ТипДокумента') for row in sbis.SqlQuery(sql, type_docs)}

    def __get_doc_regl(self):
        """Возвращает регламент по создаваемому документу (заявка на оплату или исходящий платеж)"""
        doc_regl = None
        name_type_doc = self.__get_name_type_doc()
        name_regl = self.__get_name_regl()
        regls = self.regls.get((name_type_doc, name_regl))
        if regls:
            doc_regl = regls[0]
        return doc_regl

    def __get_id_regl_by_operation(self):
        """Возвращает идентификтор регламента по запрашивамой операции"""
        type_doc = OUTGOING_PAYMENT_TYPE_DOC
        name_regl = self.__get_name_regl_operation()
        regls = self.regls.get((type_doc, name_regl))
        if regls:
            return regls[0].Get('@Регламент')

    def __get_name_type_doc(self):
        """Возвращает название типа документа (по создаваемому документу)"""
        return PAYMENT_REQUEST_TYPE_DOC if self.__get_use_pay_app() else OUTGOING_PAYMENT_TYPE_DOC

    def __get_name_regl(self):
        """Возвращает название регламента (по создаваемому документу)"""
        if self.__get_use_pay_app():
            name_regl = PAYMENT_REQUEST_NAME_REGL
        else:
            name_regl = self.__get_name_regl_operation()
        return name_regl

    def __get_name_regl_operation(self):
        """Возвращает название регламента операции"""
        if self._filter.Get('IsPercentage'):
            name_regl_operation = REPAYMENT_LOAN_NAME_REGL
        else:
            name_regl_operation = REPAYMENT_LOAN_BODY_NAME_REGL
        return name_regl_operation

    @staticmethod
    def __get_use_pay_app():
        """Проверяет конфигурацию на предмет, стоит ли использовать заявку на оплату"""
        apps = sbis.ГлобальныеПараметрыКлиента.ПолучитьЗначение("doNotUsePayApps") or "True"
        return apps.lower() != "true"
