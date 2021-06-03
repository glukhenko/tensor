"""
Модуль содержит константы для реестра процентов
"""


__author__ = 'Glukhenko A.V.'

import sbis

# Типы записией в реестре
LIST_PERCENT_FACT = 0
LIST_PERCENT_PLAN = 1
LIST_TODAY_SEPARATOR = 2
LIST_YEAR_SEPARATOR = 3

# Количество плановых записей на первой странице
COUNT_PLAN_ROWS = 2

# Направления навигации по курсору
BOTHWAYS = sbis.NavigationDirection.ndBOTHWAYS
FORWARD = sbis.NavigationDirection.ndFORWARD
BACKWARD = sbis.NavigationDirection.ndBACKWARD

# Зоны
PERCENT_ZONE = 'Начисление процентов'

# Флаги прав
ACCESS_EXCLUDE = 0
ACCESS_READ = 2
ACCESS_WRITE = 4
ACCESS_ADMIN = 8
