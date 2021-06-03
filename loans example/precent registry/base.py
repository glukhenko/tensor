"""
Модуль для построения реестра документов начислений процентов с использованием навигации по курсору.

При построении реестра используется аггрегирующий класс PercentsListAggregator, который запрашивает данные из разных
источниов.

Источники
Класс PercentsListNew - класс отвечает за построение фактических документов начислений процентов.
Класс PlanPercentsList - класс отвечает за построение плановых документов начислений процентов.

Примечание
- фактическое начисление могут создать будущей датой (документ находится выше плановых) и/или
- плановые начисления пропустить по каким-то организациям (плановая запись находится ниже фактических)
то нельзя говорить говорить что документы находящиеся ниже текущей даты всегда являются фактическими начислениями.
В связи с этим принято делить область страницы на FORWARD и BACKWARD области.
Важно: любая область может содержать в себе (1..M) записей как плановых так и фактических записей.

Построение первой страницы.
По умолчанию требуется вернуть 2 плановые записи выше курсора. Но может быть случай когда ниже курсора вернулось
недостаточное количество записей, поэтому мы 'растягиваем' BACKWARD область от COUNT_PLAN_ROWS до нужного количества
записей.
FORWARD область: LIMIT - COUNT_PLAN_ROWS
BACKWARD область: LIMIT - len(forward_docs)
https://online.sbis.ru/opendoc.html?guid=d1453f48-6e6e-4ec0-aead-788efc8da4a6

Построение 2..N страницы. Страница как правило содержит либо только BACKWARD область (скролирование вверх), либо только
FORWARD область (скролирование вниз).

Структура курсора: поскольку на каждой области сливаются 2 списка, то курсор навигации является состовным и состоит из
1. id_doc - идентификатор фактического документа
2. date_doc - дата фактического документа
3. id_plan_doc - идентификатор плановой записи
4. date_plan_doc - дата плановой записи
Важно: значения прошлого курсора надо прокидывать в текущий, если текущий не определен (подробнее в примере)

Пример: создали 2 документа начисления на будущие даты. Первый документ на 1 BACKWARD странице, а второй документ на
7 BACKWARD странице. Получается если не прокидывать курсор первого документа с первой страницы на последующие, то в
момент загрузки 7 страницы мы получим дублирующий первый документ.

Страница может содержать служебные записи: линия текущего дня, разделители года. Соотвтественно сокращаем количество
документов начислений в зависимости от количества служебеных данных.

TODO:
1. Перенести методы по работе с навигацией (в частности с курсором) в отдельный класс.
Не забыть про helpers.py метод cut_by_navigation
"""


__author__ = 'Glukhenko A.V.'

import datetime

import sbis
from loans.utils.periods import BeautifulDateName
from loans.percents import get_percent_data
from loans.loanDBConsts import LCDB
from loans.version_loans import get_date_build
from .const import COUNT_PLAN_ROWS
from .const import BOTHWAYS, FORWARD, BACKWARD
from .const import LIST_PERCENT_FACT, LIST_PERCENT_PLAN, LIST_TODAY_SEPARATOR, LIST_YEAR_SEPARATOR
from .const import ACCESS_WRITE, ACCESS_ADMIN
from .const import PERCENT_ZONE
from .docs import PercentsListNew
from .plan_docs import PlanPercentsList
from .helpers import cut_by_navigation, get_date_update, is_last_month_day

FORMAT_DATE_CURSOR = '%Y-%m-%d'


class PercentsListAggregator:
    """Аггрегирующий класс для реестра процентов"""
    def __init__(self, _filter, navigation):
        self._filter = _filter
        self.navigation = navigation
        self._lcdb = LCDB()
        self.today = get_date_build() or datetime.date.today()

        self.doc_type = sbis.Session.ObjectName()
        self.result_format = sbis.MethodResultFormat(f'{self.doc_type}.СписокЛесенка', 4)
        self.result_format.AddRecord('РП.Документ')

        self.id_organization = self._filter.Get('ФильтрДокументНашаОрганизация')
        self.sources = self.__get_sources()
        self.is_first_page = self.__check_first_page()

    def __get_organization_selected(self):
        """Возвращает признак, что выбрана организация"""
        return bool(self.id_organization and int(self.id_organization) not in (-1, -2))

    def __get_sources(self):
        """Возвращает список требуемых источников"""
        source = self._filter.Get('ТипЗаписи')
        if source is not None:
            sources = {source} & {LIST_PERCENT_FACT, LIST_PERCENT_PLAN}
        else:
            sources = {LIST_PERCENT_FACT, LIST_PERCENT_PLAN}
        return sources

    def __get_date_doc_cursor(self, field):
        """
        Возвращает дату документа из курсора
        :param field: поле курсора
        """
        date_doc = None
        position = self.navigation.Position()
        if position:
            value = position.Get(field)
            if value is not None:
                if not isinstance(value, datetime.date):
                    date_doc = datetime.datetime.strptime(value, FORMAT_DATE_CURSOR).date()
                else:
                    date_doc = value
        return date_doc

    def __get_id_doc_cursor(self, field):
        """
        Возвращает идентификатор документа из курсора
        :param field: поле курсора
        """
        id_doc = None
        position = self.navigation.Position()
        if position:
            value = position.Get(field)
            if value is not None:
                if not isinstance(value, int):
                    id_doc = int(value)
                else:
                    id_doc = value
        return id_doc

    def __get_border_date_cursor(self):
        """
        Возвращает ближайшую граничную дату документа из курсора
        Примечание: при добавлении служебных строк года или текущего дня, необходимо учитывать дату граничного документа
        из курсора. В зависимости от направления скролирования необходимо брать минимальное или максимальное значение.
        """
        border_date = None
        cursor_dates = list(filter(None, [
            self.__get_date_doc_cursor('date_doc'),
            self.__get_date_doc_cursor('date_plan_doc'),
        ]))
        if cursor_dates:
            date_by_direction = {
                BACKWARD: max(cursor_dates),
                BOTHWAYS: self.today,
                FORWARD: min(cursor_dates),
            }
            border_date = date_by_direction.get(self.navigation.Direction())
        return border_date

    def __get_need_today_cursor(self):
        """Возвращает признак need_today из курсора"""
        need_today = True
        position = self.navigation.Position()
        if position:
            value = position.Get('need_today')
            if value is not None:
                need_today = bool(value)
        return need_today

    def __get_count_create_buttons(self):
        """Возвращает количество кнопок создания, которые надо отобразить на клиенте"""
        count_create_buttons = 2
        position = self.navigation.Position()
        if position:
            value = position.Get('count_create_buttons')
            if value is not None:
                count_create_buttons = int(value)
        return count_create_buttons

    def __check_first_page(self):
        """Проверяет, что запрашивается первая страница реестра"""
        is_first_page = any((
                self.navigation.Direction() == BOTHWAYS,
                self._filter.Get('СписокИдО'),
        ))
        self._filter.AddBool('is_first_page', is_first_page)
        return is_first_page

    def __check_need_fact(self):
        """Проверяет, в каких случаях требуется формировать фактические записи"""
        return LIST_PERCENT_FACT in self.sources

    def __check_need_plan(self):
        """Проверяет, в каких случаях требуется формировать плановые записи"""
        return all((
            not self._filter.Get('ФильтрПроведенные'),
            not self._filter.Get('ФильтрОтветственноеЛицо'),
            not self._filter.Get('ФильтрУдаленные'),
            not self._filter.Get('ФильтрПоМаске'),
            not self._filter.Get('СписокИдО'),
            self._filter.Get('ФильтрПроведенные') is not False,
            LIST_PERCENT_PLAN in self.sources,
        ))

    def __check_need_service_row(self):
        """Проверяет, требуется ли возвращать служебные строки (запись текущего дня и записи годов)"""
        return not self._filter.Get('СписокИдО')

    def __check_need_row_today(self, docs):
        """
        Проверяет нужна ли разделительная линия
        :param docs: набор документов
        """
        begin_period, end_period = self.__get_page_period(docs)
        return all((
            self.__check_need_service_row(),
            self.__get_need_today_cursor(),
            all((begin_period, end_period)) and begin_period <= self.today < end_period,
        ))

    def __get_prev_position(self):
        """Возвращает значения текущих курсоров"""
        if self.is_first_page:
            position = {
                LIST_PERCENT_FACT: {
                    'id_doc': None,
                    'date_doc': None,
                },
                LIST_PERCENT_PLAN: {
                    'id_doc': None,
                    'date_doc': None,
                },
            }
        else:
            data = {
                'id_doc': self.__get_id_doc_cursor('id_doc'),
                'date_doc': self.__get_date_doc_cursor('date_doc'),
                'id_plan_doc': self.__get_id_doc_cursor('id_plan_doc'),
                'date_plan_doc': self.__get_date_doc_cursor('date_plan_doc'),
            }
            data = {key: str(value) if value is not None else None for key, value in data.items()}
            position = {
                LIST_PERCENT_FACT: {
                    'id_doc': data.get('id_doc'),
                    'date_doc': data.get('date_doc'),
                },
                LIST_PERCENT_PLAN: {
                    'id_doc': data.get('id_plan_doc'),
                    'date_doc': data.get('date_plan_doc'),
                },
            }
        return position

    def get_documents(self):
        """Возвращает список документов со всех источниокв"""
        backward_docs = sbis.RecordSet(self.result_format)
        forward_docs = sbis.RecordSet(self.result_format)
        if self.is_first_page:
            forward_docs = self.__request_forward_documents(count_on_page=self.navigation.Limit() - COUNT_PLAN_ROWS)
            backward_docs = self.__request_backward_documents(count_on_page=self.navigation.Limit() - len(forward_docs))
        else:
            if self.navigation.Direction() == FORWARD:
                forward_docs = self.__request_forward_documents(count_on_page=self.navigation.Limit())
            if self.navigation.Direction() == BACKWARD:
                backward_docs = self.__request_backward_documents(count_on_page=self.navigation.Limit())

        docs = self.__merge_result(backward_docs, forward_docs)
        self.__post_processing(docs)
        self.__sort_rs(docs)
        return docs

    def __request_forward_documents(self, count_on_page):
        """
        Загрузка forward (нижних) документов
        :param count_on_page: количество запрашиваемых записей
        """
        navigation = sbis.Navigation(
            sbis.NavigationPositionTag(),
            self.navigation.Position() or sbis.Record(),
            self.navigation.Limit() if self._filter.Get('СписокИдО') else count_on_page,
            FORWARD,
            True,
        )
        doc_list = self.__request_fact_documents(navigation)
        plan_list = self.__request_plan_documents(navigation)
        return self.__merge_documents(doc_list, plan_list, navigation)

    def __request_backward_documents(self, count_on_page):
        """
        Загрузка backward (верхних) документов
        :param count_on_page: количество запрашиваемых записей
        """
        navigation = sbis.Navigation(
            sbis.NavigationPositionTag(),
            self.navigation.Position() or sbis.Record(),
            self.navigation.Limit() if self._filter.Get('СписокИдО') else count_on_page,
            BACKWARD,
            True,
        )
        doc_list = self.__request_fact_documents(navigation)
        plan_list = self.__request_plan_documents(navigation)
        return self.__merge_documents(doc_list, plan_list, navigation)

    def __request_fact_documents(self, navigation):
        """
        Запрашивает список существующих документов начисления процентов
        :param navigation: навигация запроса
        """
        if self.__check_need_fact():
            doc_list = PercentsListNew(self._filter, navigation, self.doc_type, self.result_format).get_documents()
            doc_list.rsPtr.Migrate(self.result_format)
        else:
            doc_list = sbis.RecordSet(self.result_format)
            doc_list.nav_result = sbis.NavigationResult(False)
        return doc_list

    def __request_plan_documents(self, navigation):
        """
        Запрашивает список плановых документов начисления процентов
        :param navigation: навигация запроса
        """
        if self.__check_need_plan():
            plan_list = PlanPercentsList(self._filter, navigation).get_documents()
            plan_list.Migrate(self.result_format)
        else:
            plan_list = sbis.RecordSet(self.result_format)
            plan_list.nav_result = sbis.NavigationResult(False)
        return plan_list

    def __merge_documents(self, doc_list, plan_list, navigation):
        """
        Объединяет фактические и плановые документы
        Примечание: ориентирован только на построение forward_result и backward_result и настройке навигации в
        зависиомости от источников
        :param doc_list: объект доклиста с фактическими начислениями
        :param plan_list: список плановых начислений
        :param navigation: объект навигации
        :param merged_result: объект, куда помещается результат объединения списков
        """
        merged_result = sbis.RecordSet(self.result_format)
        if self.__check_need_fact():
            for doc in doc_list.rsPtr:
                merged_result.AddRow(doc)
        if self.__check_need_plan():
            for plan_doc in plan_list:
                merged_result.AddRow(plan_doc)
        self.__sort_rs(merged_result)
        self.__calc_navigation(doc_list, plan_list, navigation, merged_result)
        return merged_result

    def __calc_navigation(self, doc_list, plan_list, navigation, merged_result):
        """
        Расчитывает навигацию после объединения фактических и плановых документов
        :param doc_list: объект доклиста с фактическими начислениями
        :param plan_list: список плановых начислений
        :param navigation: объект навигации
        :param merged_result: объект, куда помещается результат объединения навигации
        """
        fact_more_exist = self.__is_more_exist_fact(doc_list, navigation)
        plan_more_exist = self.__is_more_exist_plan(plan_list)

        result_more_exist = any((
            any((fact_more_exist, plan_more_exist)),
            self.__get_count_result(doc_list, plan_list) > navigation.Limit(),
        ))

        merged_result.nav_result = sbis.NavigationResult(result_more_exist)
        if result_more_exist:
            cut_by_navigation(merged_result, navigation)

    def __is_more_exist_fact(self, doc_list, navigation):
        """Проверяет наличие еще фактических записей"""
        if self.__check_need_fact():
            if navigation.Direction() == BACKWARD:
                fact_more_exist = doc_list.result.nav_result.HaveDataBefore()
            else:
                fact_more_exist = doc_list.result.nav_result.GetIsNext()
        else:
            fact_more_exist = False
        return fact_more_exist

    def __is_more_exist_plan(self, plan_list):
        """Проверяет наличие еще плановых записей"""
        if self.__check_need_plan():
            plan_more_exist = plan_list.nav_result.GetIsNext()
        else:
            plan_more_exist = False
        return plan_more_exist

    def __get_count_result(self, doc_list, plan_list):
        """Возвращает количество записей"""
        count_result = 0
        if self.__check_need_fact():
            count_result += doc_list.rsPtr.Size()
        if self.__check_need_plan():
            count_result += plan_list.Size()
        return count_result

    def __merge_result(self, backward_docs, forward_docs):
        """
        Формирует итоговый результат
        :param backward_docs: список документов в BACKWARD зоне
        :param forward_docs: список документов в FORWARD зоне
        """
        docs = sbis.RecordSet(self.result_format)
        for rec in backward_docs:
            docs.AddRow(rec)
        for rec in forward_docs:
            docs.AddRow(rec)

        self.__sort_rs(docs)
        self.__calc_result_navigation(docs, backward_docs.nav_result, forward_docs.nav_result)
        return docs

    def __post_processing(self, docs):
        """
        Постобработка результата
        - рассчитывает необходимые поля
        - добавляет служебные строки дат и текущего дня
        :param docs: список документов
        """
        docs_data = self.__request_docs_data(docs)
        organization_selected = self.__get_organization_selected()
        for doc in docs:
            id_doc = doc.Get('@Документ')
            type_row = doc.Get('ТипЗаписи')
            if type_row == LIST_PERCENT_FACT:
                doc['РП.Лицо1.СписокНазваний'] = docs_data.get(id_doc, {}).get('РП.Лицо1.СписокНазваний')
            doc['РП.Начислено'] = docs_data.get(id_doc, {}).get('РП.Начислено')
            doc['РП.Остаток'] = docs_data.get(id_doc, {}).get('РП.Остаток')
            doc['ДатаКрасивоеНазвание'] = self.__get_beautiful_date_name(doc)
            doc['ДатаИзменения'] = get_date_update(doc)
            doc['show_organization'] = organization_selected

        self.__add_row_today(docs)
        self.__add_row_year(docs)
        self.__set_calc_button(docs)
        self.__calc_position(docs)
        self.__create_outcome(docs)

    def __request_docs_data(self, docs):
        """
        Загружает данные по документам (наименования и суммы)
        :param docs: список документов
        """
        docs_data = {}
        id_docs = [id_doc for id_doc in docs.ToList('@Документ') if id_doc > 0]
        for id_doc, rec in get_percent_data(self._lcdb, id_docs).items():
            docs_data[id_doc] = {
                'РП.Лицо1.СписокНазваний': rec.Get('РП.Лицо1.СписокНазваний'),
                'РП.Начислено': rec.Get('РП.Начислено'),
                'РП.Остаток': rec.Get('РП.Остаток'),
            }
        return docs_data

    def __add_row_today(self, docs):
        """
        Добавляет разделяющую линию текущего дня
        :param docs: список документов
        """
        if self.__check_need_row_today(docs):
            today_text = sbis.rk('Сегодня')
            date_text = BeautifulDateName.get_beautiful_date(self.today)
            row = sbis.Record(self.result_format)
            row['@Документ'] = 0
            row['Дата'] = self.today
            row['ТипЗаписи'] = LIST_TODAY_SEPARATOR
            row['ДатаКрасивоеНазвание'] = f'''{today_text} {date_text}'''
            docs.AddRow(row)

    def __get_page_period(self, docs):
        """
        Возвращает период формируемой страницы (т.е. даты первого и последнего документа на странице)
        Примечание: в случае если на странице один документ или документы с одинаковыми датами, то используем даты
        курсора для формирования периода
        :param docs: набор документов на странице, RecordSet
        :return: begin_period, end_period
        """
        dates = list(set(docs.ToList('Дата')))
        begin_period, end_period = None, None

        if dates:
            if len(dates) == 1:
                border_date = self.__get_border_date_cursor()
                if border_date:
                    begin_period = border_date
                else:
                    begin_period = self.today
                end_period = dates[0]
            else:
                dates.sort()
                begin_period = dates[0]
                end_period = dates[-1]

        return begin_period, end_period

    def __add_row_year(self, docs):
        """
        Добавляет служебную строку даты
        :param docs: список документов
        """
        if not self.__check_need_service_row():
            return

        for year in self.__calc_years_on_page(docs):
            service_date_row = sbis.Record(self.result_format)
            date = datetime.date(year, 12, 31)
            id_row = int(str(date).replace('-', ''))
            service_date_row['@Документ'] = -1 * id_row
            service_date_row['Дата'] = date
            service_date_row['ТипЗаписи'] = LIST_YEAR_SEPARATOR
            service_date_row['ДатаКрасивоеНазвание'] = str(year)
            docs.AddRow(service_date_row)

    def __calc_years_on_page(self, docs):
        """
        Рассчитывает количество строк года на странице.
        :param docs: набор документов
        Примечание: учитываются данные курсора - даты документов на предыдущей странице
        """
        years = {doc.Get('Дата').year for doc in docs}

        if not self.is_first_page:
            border_date = self.__get_border_date_cursor()
            if border_date:
                years.add(border_date.year)

        if years:
            years = list(years)
            years.sort()
            years.pop(len(years) - 1)

        return years

    @staticmethod
    def __sort_rs(rows):
        """Сортирует результат выборки"""
        rows.sort(key=lambda rec: (
            rec.Get('Дата'),
            rec.Get('ТипЗаписи') or -1,
            rec.Get('@Документ'),
        ), reverse=True)

    def __calc_result_navigation(self, docs, backward_nav_result, forward_nav_result):
        """
        Рассчитывает результат навигации
        :param docs: список документов
        :param backward_nav_result: результат навигации BACKWARD зоны
        :param forward_nav_result: результат навигации FORWARD зоны
        Примечание: если найдены служебные строки, то проставим признак наличия след.страниц (обрезанные записи
        нужно будет зачитать следующей страницей)
        """
        count_service_rows = self.__calc_count_service_rows(docs)
        need_cut_result = all((
            docs.Size() == self.navigation.Limit(),
            count_service_rows,
        ))

        if self.is_first_page:
            docs.nav_result = sbis.NavigationResult(
                backward_nav_result.GetIsNext() or need_cut_result,
                forward_nav_result.GetIsNext() or need_cut_result,
            )
        else:
            navigations = {
                BACKWARD: backward_nav_result,
                FORWARD: forward_nav_result,
            }
            navigation = navigations.get(self.navigation.Direction())
            docs.nav_result = sbis.NavigationResult(
                navigation.GetIsNext() or need_cut_result,
            )

        if need_cut_result:
            self.__cut_result_by_service_rows(docs, count_service_rows)

    def __calc_count_service_rows(self, docs):
        """Рассчитывает количество служебных строк на странице"""
        years = {doc.Get('Дата').year for doc in docs}
        count_service_rows = len(years) - 1
        if self.__check_need_row_today(docs):
            count_service_rows += 1
        return count_service_rows

    def __set_calc_button(self, docs):
        """
        Устанавливает признак кнопки Рассчитать для плановой записи
        :param docs: список документов
        """
        count_create_buttons = self.__get_count_create_buttons()
        if count_create_buttons and self.__check_zone(PERCENT_ZONE, (ACCESS_WRITE, ACCESS_ADMIN)):
            button_created = 0
            for i in reversed(range(docs.Size())):
                if docs.Get(i, 'ТипЗаписи') == LIST_PERCENT_PLAN:
                    show_create_button = False
                    if docs.Get(i, 'Дата') < self.today:
                        show_create_button = True
                    elif count_create_buttons - button_created > 0:
                        show_create_button = True
                        button_created += 1
                    docs.Set(i, 'show_create_button', show_create_button)

    @staticmethod
    def __check_zone(zone, actions):
        """
        Проверяет доступность действий на зоне для пользователя
        :param zone: зона
        :param actions: действия пользователя
        """
        rights = sbis.CheckRights.AccessAreaRestrictions(zone)
        return any(bool(rights.Get('Access') & action) for action in actions)

    def __create_outcome(self, docs):
        """
        Создает строку итогов
        :param docs: список документов
        """
        outcome = sbis.Record(self.result_format)
        outcome.CopyOwnFormat()
        outcome.AddInt16('count_create_button')
        outcome.AddBool('need_row_today')
        docs.outcome = outcome

    def __calc_need_row_today(self, docs):
        """
        Рассчитывает, потребуется ли добавлять линию текущего дня для следующих страниц
        :param docs: набор документов на странице
        :return: bool
        """
        need_today = self.__get_need_today_cursor()
        if need_today:
            today_row_on_the_page = any(filter(lambda row: row.Get('ТипЗаписи') == LIST_TODAY_SEPARATOR, docs))
            need_today = not today_row_on_the_page
        return need_today

    def __cut_result_by_service_rows(self, docs, count_service_rows):
        """
        Обрезает список документов на количество служебных строк
        :param docs: список документов
        :param count_service_rows: количество служебных записей
        Примечание: после удаления документов, количество служебных строк может уменьшится. Принимаем тот факт что на
        странице может быть меньше LIMIT записей, т.к. курсоры выручат.
        """
        direction = self.navigation.Direction()
        limit = self.navigation.Limit() - count_service_rows
        id_docs = [doc.Get('@Документ') for doc in docs if
                   doc.Get('ТипЗаписи') in (LIST_PERCENT_PLAN, LIST_PERCENT_FACT)]

        if direction in (BOTHWAYS, FORWARD):
            id_docs_for_remove = id_docs[limit:]
        else:
            id_docs_for_remove = id_docs[:len(id_docs) - limit]

        for doc in reversed(docs):
            if doc.Get('@Документ') in id_docs_for_remove:
                docs.DelRow(doc)

    def __calc_position(self, docs):
        """
        Расчитывает значение курсора
        Формат курсора: [id_doc, date_doc, id_plan_doc, date_plan_doc]
        :param docs: список документов
        Примечание: важно прокидывать значения текущего курсора, если на текущей странице отсутствуют записи
        соответствующего типа
        """
        first_row, last_row = self.__calc_boundary_rows(docs)
        first_row, last_row = self.__join_positions(first_row, last_row)
        count_create_button = self.__calc_count_create_buttons(docs)
        need_row_today = self.__calc_need_row_today(docs)
        backward = [
            first_row[LIST_PERCENT_FACT]['id_doc'], first_row[LIST_PERCENT_FACT]['date_doc'],
            first_row[LIST_PERCENT_PLAN]['id_doc'], first_row[LIST_PERCENT_PLAN]['date_doc'],
            str(count_create_button),
            str(need_row_today),
        ]
        forward = [
            last_row[LIST_PERCENT_FACT]['id_doc'], last_row[LIST_PERCENT_FACT]['date_doc'],
            last_row[LIST_PERCENT_PLAN]['id_doc'], last_row[LIST_PERCENT_PLAN]['date_doc'],
            str(count_create_button),
            str(need_row_today),
        ]
        if self.is_first_page:
            next_position = {'backward': backward, 'forward': forward}
            docs.SetMetadataHashTable("nextPosition", next_position)
        else:
            next_positions = {
                BACKWARD: backward,
                FORWARD: forward,
            }
            docs.Metadata().AddJson('nextPosition', next_positions.get(self.navigation.Direction()))

    @staticmethod
    def __calc_boundary_rows(docs):
        """
        Рассчитывает первые и последние строки в результате (результат содержит записи двух типов: план и факт)
        :param docs: список документов
        Примечание: учитываем что в наборе могут быть записи двух типов, плановые и фактические начисления. Требуется
        найти граничные строки для этих двух типов
        """
        template = {'id_doc': None, 'date_doc': None}
        first_row = {LIST_PERCENT_FACT: dict(template), LIST_PERCENT_PLAN: dict(template)}
        last_row = {LIST_PERCENT_FACT: dict(template), LIST_PERCENT_PLAN: dict(template)}

        for i in range(docs.Size()):
            type_row = docs.Get(i, "ТипЗаписи")
            if type_row not in (LIST_PERCENT_FACT, LIST_PERCENT_PLAN):
                continue
            for field, position_field in (
                    ('@Документ', 'id_doc'),
                    ('Дата', 'date_doc'),
            ):
                value = docs.Get(i, field)
                if first_row.get(type_row).get(position_field) is None:
                    first_row[type_row][position_field] = str(value) if value is not None else None
                last_row[type_row][position_field] = str(value) if value is not None else None

        return first_row, last_row

    def __calc_count_create_buttons(self, docs):
        """
        Рассчитывает сколько кнопок осталось создать (на следующих страницах)
        :param docs: список документов
        """
        count_create_buttons = self.__get_count_create_buttons()
        count_created_buttons = len(
            [1 for row in docs if row.Get('show_create_button') and row.Get('Дата') >= self.today]
        )
        return max(0, count_create_buttons - count_created_buttons)

    def __join_positions(self, first_row, last_row):
        """
        Объединяет старый и текущий курсоры
        :param first_row: backward курсор
        :param last_row: forward курсор
        """
        prev_position = self.__get_prev_position()
        for field in ('id_doc', 'date_doc'):
            for type_source in (LIST_PERCENT_FACT, LIST_PERCENT_PLAN):
                if not first_row[type_source][field]:
                    first_row[type_source][field] = prev_position[type_source][field]
                if not last_row[type_source][field]:
                    last_row[type_source][field] = prev_position[type_source][field]
        return first_row, last_row

    @staticmethod
    def __get_beautiful_date_name(row):
        """
        Формирует красивое название поля даты
        Примечание: для единичных документов в виде "25 фев", для групповых в виде "Март"
        """
        date = row.Get('Дата')
        if is_last_month_day(date):
            beautiful_date = BeautifulDateName.get_beautiful_month(date)
        else:
            beautiful_date = BeautifulDateName.get_beautiful_date(date)
        return beautiful_date


def get_percents_list(_filter, navigation):
    """
    Возвращает реестр процентов
    :param _filter: фильтр запроса
    :param navigation: навигация запроса
    """
    return PercentsListAggregator(_filter, navigation).get_documents()
