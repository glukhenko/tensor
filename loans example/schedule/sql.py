"""
Модуль содержащий SQL запросы по графикам платежей
"""


__author__ = 'Glukhenko A.V.'

LIST_PAYMENTS = '''
    WITH payments AS (
        SELECT
            "Документ",
            MAX("Дата") AS "Дата",
            SUM(CASE WHEN "Лицо3" = $4::integer AND "Тип" = 1 THEN "Сумма" ELSE 0 END) AS "ДебетДолг",
            SUM(CASE WHEN "Лицо3" = $4::integer AND "Тип" = 2 THEN "Сумма" ELSE 0 END) AS "КредитДолг",
            SUM(CASE WHEN "Лицо3" = $5::integer AND "Тип" = 1 THEN "Сумма" ELSE 0 END) AS "ДебетПроценты",
            SUM(CASE WHEN "Лицо3" = $5::integer AND "Тип" = 2 THEN "Сумма" ELSE 0 END) AS "КредитПроценты",
            SUM(CASE
                WHEN
                    ($8::boolean IS TRUE AND "Тип" = 2) OR
                    ($8::boolean IS NOT TRUE AND "Тип" = 1)
                THEN
                    "Сумма"
                ELSE
                    0
            END) AS "Платеж"
        FROM
            "ДебетКредит"
        WHERE
            -- фильтруем старые начальные остатки
            "Документ" IS NOT NULL AND
            "Тип" in (1,2) AND
            "Счет" = any($1::integer[]) AND
            "НашаОрганизация" = $2::integer AND
            "Лицо2" = $3::integer AND
            "Лицо3" = any(array[$4::integer, $5::integer]) AND
            "Сумма" <> 0
            -- временно отключаем фильтр по дате, если выдача произошла до даты начала графика - учтем это
            -- TODO: вернуть фильтр после решения задач
            -- https://online.sbis.ru/opendoc.html?guid=4fa3b49a-9063-48dd-8f23-73565a56ec39
            -- https://online.sbis.ru/opendoc.html?guid=81b93abf-88be-43ba-818b-2b21036c017b
            -- "Дата" BETWEEN $6::date AND $7::date
        GROUP BY
            "Документ"
    )
    SELECT
        pp."Дата",
        CASE
            WHEN COUNT(DISTINCT "@Документ") > 1
            THEN (-1 * row_number() OVER () )::text || ',Документы'
            ELSE MAX("Документ") || ',Документ'
        END "@Документ",
        ARRAY_AGG(DISTINCT pp."Документ") "id_docs",
        SUM("ДебетДолг") "ДебетДолг",
        SUM("КредитДолг") "КредитДолг",
        SUM("ДебетПроценты") "ДебетПроценты",
        SUM("КредитПроценты") "КредитПроценты",
        SUM(pp."Платеж") "Платеж",
        SUM(
            CASE
                WHEN
                    $8::boolean IS TRUE
                THEN
                    SUM(pp."ДебетДолг") - SUM(pp."КредитДолг")
                ELSE
                    SUM(pp."КредитДолг") - SUM(pp."ДебетДолг")
            END
        ) OVER (order by pp."Дата") AS "ОстатокДолга",
        MIN(td."ТипДокумента") "ТипДокумента"
    FROM
        payments pp
    LEFT JOIN
        "Документ" d
    ON
        d."@Документ" = pp."Документ"
    LEFT JOIN
        "ТипДокумента" td
    ON
        d."ТипДокумента" = td."@ТипДокумента"
    GROUP BY
        pp."Дата", "Платеж" <> 0
'''

PAYMENTS_BY_DATE = '''
    WITH payments AS (
        SELECT
            "Документ",
            SUM("Сумма") "Сумма"
        FROM
            "ДебетКредит"
        WHERE
            "Лицо2" = (
                SELECT "Лицо" FROM "Документ" WHERE "@Документ" = $1
            )
            AND "Тип" = $3::int
            AND "Дата" = $2::date
        GROUP BY
            "Документ"
    )
    SELECT
        pp."Документ" "@Документ",
        doc."Дата",
        doc."Регламент",
        doc."Номер",
        pp."Сумма",
        face."Название" "НазваниеОрганизации"
    FROM
        payments pp
    LEFT JOIN
        "Документ" doc
        ON pp."Документ" = doc."@Документ"
    LEFT JOIN "ДокументРасширение" doc_ext
        ON doc."@Документ" = doc_ext."@Документ"
    LEFT JOIN "Лицо" face
        ON doc."ДокументНашаОрганизация" = face."@Лицо"      
    LEFT JOIN "ТипДокумента"  type_doc
        ON doc."ТипДокумента" = type_doc."@ТипДокумента"
    WHERE
        type_doc."ТипДокумента" = ANY($4::text[])
    ORDER BY
        pp."Документ"
'''

FILTER_FOR_SCHEDULE = '''
    SELECT
        doc."@Документ",
        diff_doc."ДатаНач" AS "{date_start}",
        diff_doc."ДатаКнц" AS "{date_stop}",
        doc."@Документ" AS "{loan}",
        doc."Лицо" AS "{face_loan}",
        diff_doc."Коэффициент" AS "{rate}",
        diff_doc."Состав" AS "{monthly_payment}",
        doc."ДокументНашаОрганизация" AS "{id_organization}",
        diff_doc."Срок" AS "{first_payment_date}",
        ext_doc."Сумма" AS "{size_payment}",
        diff_doc."ТипГрафикаПогашения" AS "{type_schedule}",
        doc."ТипДокумента" AS "{type_doc}",
        -- данный параметр исключительно для конвертора ежемесячного платежа
        type_doc."ТипДокумента" AS "{name_type_doc}"
    FROM
        "Документ" doc
    LEFT JOIN
        "ДокументРасширение" ext_doc
        USING("@Документ")
    LEFT JOIN
        "РазличныеДокументы" diff_doc
        USING("@Документ")
    LEFT JOIN
        "ТипДокумента" type_doc
        ON doc."ТипДокумента" = type_doc."@ТипДокумента"
    WHERE
        {filter_by_docs}
        {filter_by_type_schedules}
        {filter_without_rate}
'''
