"""
Вспомогательный модуль хелпер
"""


__author__ = 'Glukhenko A.V.'

import calendar

import sbis
from .const import FORWARD, BACKWARD


def cut_by_navigation(rows, navigation):
    """
    Обрезает результат согласно навигации
    :param rows: набор данных, RecordSet
    :param navigation: объект навигации
    """
    direction = navigation.Direction()
    limit = navigation.Limit()
    id_docs = rows.ToList('@Документ')

    id_docs_for_remove = []
    if direction == FORWARD:
        id_docs_for_remove = id_docs[limit:]
    if direction == BACKWARD:
        id_docs_for_remove = id_docs[:len(id_docs) - limit]

    for rec in reversed(rows):
        if rec.Get('@Документ') in id_docs_for_remove:
            rows.DelRow(rec)


def get_date_update(rec):
    """
    Рассчитывает дату изменения документа
    :param rec: запись
    :return:
    """
    params = rec.Get('ДокументРасширение.Параметры')
    try:
        params = sbis.ParseHstore(params) if params else dict()
    except Exception as err:
        sbis.WarningMsg('some problem with ParseHstore: {}'.format(err))
        params = {}
    return params.get('date_update') or rec.Get('ДР.ДатаВремяСоздания').date()


def is_last_month_day(date):
    """
    Проверяем что документ начисления процентов зафиксирован последним днем месяца
    :param date: дата документа
    """
    return calendar.monthrange(date.year, date.month)[1] == date.day
