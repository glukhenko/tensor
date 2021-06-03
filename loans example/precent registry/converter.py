"""
Модуль предназначен для инициализации кеша по договорам займов.
Данный кеш используется для формирования плановый начислений процентов.
PS: ранее чтобы выяснить основной долг по договору и плановый период
начисления процентов, выполнялся тяжеловесный запрос в таблицу
ДебетКредит, причем дважды. Закешировав эту информацию мы можем быстро
получить список плановых процентов.

Кешируемая информация:
Основной долг по договору
Начальная дата планового периода
Конечная дата планового периода
"""


__author__ = 'Glukhenko A.V.'


import sbis
from loans.loanConsts import LC
from loans.cache.base import BaseCacheLoan
from .plan_docs import PlanPercentsList


KEY_CACHE_PLAN_CONTRACTORS = 'plan_docs'


class CachePlanPercent(BaseCacheLoan):
    """Класс отчечает за кеширования договоров для построения планового реестра процентов"""
    def __init__(self):
        pass

    def recalc_cache(self):
        """Пересчитывает кеш по всем договорам"""
        issues_contractors = self.__get_contractors(LC.PERCENTS_ON_ISSUED_LOANS)
        self.save_cache_contracts(issues_contractors)
        received_contractors = self.__get_contractors(LC.PERCENTS_ON_RECEIVED_LOANS)
        self.save_cache_contracts(received_contractors)

    def __get_contractors(self, type_obj):
        """Возвращает список договоров"""
        _filter = sbis.Record({
            'ФильтрДокументНашаОрганизация': -2
        })
        navigation = sbis.Navigation(999999, 0, True)

        return PlanPercentsList(_filter, navigation, type_obj=type_obj).get_documents(only_contractors=True)

    def save_cache_contracts(self, contracts):
        """
        Сохраняет расчитанные договора в кэш
        :param contracts: список договоров, RecorsSet
        """
        cache_contracts = []

        for contract in contracts:
            params = ';'.join([
                str(contract.Get('ДатаС')),
                str(contract.Get('date_last_payment') or ''),
                str(contract.Get('debt')),
            ])
            cache_contracts.append(
                {
                    'id_doc': contract.Get('id_contract'),
                    'params': sbis.CreateHstore({KEY_CACHE_PLAN_CONTRACTORS: params}),
                },
            )

        sbis.LogMsg(f'Write cache for {len(cache_contracts)} contractors')
        self.mass_update(key_cache=KEY_CACHE_PLAN_CONTRACTORS, values=cache_contracts)
