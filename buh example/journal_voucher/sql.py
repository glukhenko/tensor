"""
Глухенко А.В.
Модуль, хранящий базовые конструкции запроса по отчету Журнал Ордер, в разных режимах
"""

CORR_WITH_ANALYTICS_SQL = '''
    WITH {base_cte_account}
    SELECT
        STRING_AGG(a, ',') AS corr_arr
    FROM
        (
            SELECT
                '{{' ||
                (CASE WHEN dk."Тип" = 1 THEN 2 ELSE 1 END)::TEXT ||
                '|' ||
                COALESCE(dk."Счет", 0) ||
                '|' ||
                COALESCE(SUM(dk."Сумма"), 0) ||
                '|' ||
                COALESCE(SUM(dk."СуммаН"), 0) ||
                '|' ||
                COALESCE(dk."Валюта", 'RUB') ||
                '|' ||
                COALESCE(MAX(dk."Сумма"), 0) ||
                '|' ||
                COALESCE(MAX(dk."СуммаН"), 0) ||
                '}}' AS a
            FROM
                (
                    SELECT
                        dk."Тип",
                        dk2."Счет",
                        dk."Сумма",
                        dk."СуммаН",
                        dk."Валюта"
                    FROM
                        "ДебетКредит" dk
                    {type_join} JOIN
                        "ДебетКредит" dk2
                        ON
                            dk."Проводка" = dk2."Проводка" AND
                            dk."Тип" <> dk2."Тип" AND
                            -- тут нужна одна любая строчка. бухгалтерская сумма есть почти всегда, и она одна
                            dk2."Валюта" IS NULL AND
                            {dc_corresponds_cond}
                    WHERE
                        dk."Тип" IN (1, 2) AND
                        dk."Дата" BETWEEN $2::date AND $3::date AND
                        dk."Счет" = ANY($1::int[])
                        {ab_corresponds_cond}
                        {filter_by_internal_turnover}
                ) dk
            GROUP BY
                dk."Тип",
                dk."Счет",
                dk."Валюта"
        ) d
'''

CORR_WITHOUT_ANALYTICS_SQL = '''
    WITH {base_cte_account}
    SELECT
        STRING_AGG(a, ',') AS corr_arr
    FROM
        (
            SELECT
                '{{' ||
                dc_type::TEXT ||
                '|' ||
                acc ||
                '|' ||
                COALESCE(SUM(suma), 0) ||
                '|' ||
                COALESCE(SUM(sumt), 0) ||
                '|' ||
                COALESCE(curr, 'RUB') ||
                '|' ||
                COALESCE(MAX(suma), 0) ||
                '|' ||
                COALESCE(MAX(sumt), 0) ||
                '}}' AS a
            FROM
                (
                    {base_acc_turnover}
                ) corr
            GROUP BY
                dc_type,
                acc,
                curr
        ) d
'''

CORR_WITH_ANALYTICS_PERIOD_SQL = '''
    (
        SELECT
            {calc_corr_arr}
        FROM (
            SELECT
                dk."Тип",
                dk2."Счет",
                dk."Сумма",
                dk."СуммаН",
                dk."Валюта"
            FROM
                "ДебетКредит" dk
                LEFT JOIN "ДебетКредит" dk2 ON (
                    dk."Проводка" = dk2."Проводка" AND dk."Тип" <> dk2."Тип"
                )
            WHERE
                dk."Тип" IN (1,2) AND
                dk."Дата" BETWEEN '{date_st}' AND '{date_end}' AND
                dk."Дата" BETWEEN {period_st} AND {period_end} AND
                dk."Счет" = ANY(ARRAY[{acc_list}]) AND
                {other_conditions}
                {filter_by_internal_turnover}
        ) corr
    ) AS corr_arr
'''

CORR_WITHOUT_ANALYTICS_PERIOD_SQL = '''
    (
        SELECT
            {calc_corr_arr_ab}
        FROM (
            SELECT
                1 AS dc_type,
                adb AS acc,
                {sum_field},
                sumt,
                curr
            FROM acc_turnover
            WHERE
                acr = ANY(ARRAY[{acc_list}]) AND
                dt BETWEEN '{date_st}' AND '{date_end}' AND
                dt BETWEEN {period_st} AND {period_end} AND
                {org_flt}
            UNION ALL
            SELECT
                2 AS dc_type,
                acr AS acc,
                {sum_field},
                sumt,
                curr
            FROM acc_turnover
            WHERE
                adb = ANY(ARRAY[{acc_list}]) AND
                dt BETWEEN '{date_st}' AND '{date_end}' AND
                dt BETWEEN {period_st} AND {period_end} AND
                {org_flt}
        ) corr
    ) AS corr_arr
'''

CORR_AB_SQL = ''',
    (
        SELECT
            {calc_corr_arr}
        FROM
            (
                SELECT
                    "Тип",
                    "Счет",
                    SUM("Сумма") AS "Сумма",
                    SUM("СуммаН") AS "СуммаН",
                    "Валюта"
                FROM
                    (
                        {base_corr_query_amount}
                        UNION ALL
                        {base_corr_query}
                    ) d
                GROUP BY
                    "Валюта",
                    "Счет",
                    "Тип"
            ) d
    ) AS corr_arr
'''

CORR_AB_ACCOUNT_WITHOUT_ANALYTICS_SQL = ''',
    (
        SELECT
            {calc_corr_arr_ab}
        FROM
            (
            SELECT
                dc_type,
                acc,
                SUM(suma) suma,
                SUM(sumt) sumt,
                curr
            FROM
                (
                    {base_acc_turnover}
                ) d
                GROUP BY
                    acc,
                    dc_type,
                    curr
            ) corr
    ) AS corr_arr
'''

CORR_AB_ACCOUNT_WITH_ANALYTICS_SQL = ''',
    (
        SELECT
            {calc_corr_arr}
        FROM
        (
            SELECT
                dk."Тип",
                dk2."Счет",
                SUM(dk."Сумма") AS "Сумма",
                SUM(dk."СуммаН") AS "СуммаН",
                dk."Валюта"
            FROM
                "ДебетКредит" dk
                LEFT JOIN "ДебетКредит" dk2 ON (
                    dk."Проводка" = dk2."Проводка" AND
                    dk."Тип" <> dk2."Тип"
                )
            WHERE
                dk."Тип" IN (1,2) AND
                dk."Дата" BETWEEN '{date_st}' AND '{date_end}' AND
                dk."Счет" = ANY(ARRAY(
                    SELECT child
                    FROM ps
                    WHERE top = ABS(real_id)
                )) AND
                dk."Счет" = ANY(ARRAY[{acc_list}]) AND
                {other_conditions}
                {filter_by_internal_turnover}
            GROUP BY
                dk."Валюта",
                dk."Тип",
                dk2."Счет"
        ) d
    ) AS corr_arr
'''

CURR_ORDER_SQL = '''
    (
        SELECT
            HSTORE(
                ARRAY_AGG(key),
                ARRAY_AGG(
                    TO_CHAR(
                        ({field}->>key)::NUMERIC(32,2),
                        'S000000000D00'
                    )
                )
            )
        FROM
            JSONB_OBJECT_KEYS({field}) AS d(key)
    )
'''

CALC_CORR_ARR = '''
    STRING_AGG(
        (
            '{{' ||
            CASE WHEN "Тип" = 1 THEN 2 ELSE 1 END ||
            '|' ||
            COALESCE("Счет", 0) ||
            '|' ||
            COALESCE("Сумма", 0) ||
            '|' ||
            COALESCE("СуммаН", 0) ||
            '|' ||
            COALESCE("Валюта", 'RUB') ||
            '}}'
        ),
        ','
    )
'''

CALC_CORR_ARR_AB = '''
    STRING_AGG(
        (
            '{{' ||
            dc_type ||
            '|' ||
            acc ||
            '|' ||
            COALESCE(suma, 0) ||
            '|' ||
            COALESCE(sumt, 0) ||
            '|' ||
            COALESCE(curr, 'RUB') ||
            '}}'
        ),
        ','
    )
'''

BASE_CORR_QUERY = '''
    SELECT
        dk."Тип",
        (
            SELECT
                dk2."Счет"
            FROM
                "ДебетКредит" dk2
            WHERE
                dk."Проводка" = dk2."Проводка" AND
                dk."Тип" <> dk2."Тип"
            LIMIT 1
        ) AS "Счет",
        dk."Сумма",
        dk."СуммаН",
        dk."Валюта"
    FROM
        "ДебетКредит" dk
    WHERE
        dk."Тип" IN (1, 2) AND
        dk."Дата" BETWEEN '{date_st}' AND '{date_end}' AND
        dk."Счет" = ANY(ARRAY[{acc_list}]) AND
        {filter_by_amount}
        {other_conditions}
        {filter_by_internal_turnover}
'''

BASE_CTE_ACCOUNT = '''
    ps AS (
        SELECT
            "@ПланСчетов" AS acc,
            "Признаки"[8] AS curr_flag,
            "Признаки"[7] AS tax_flag
        FROM "ПланСчетов" ps
        WHERE "@ПланСчетов" = ANY($1::int[])
    )
'''


BASE_ACC_TURNOVER = '''
    SELECT
        1 AS dc_type,
        adb AS acc,
        {sum_field},
        CASE WHEN tax_flag THEN sumt END AS sumt,
        curr
    FROM
        acc_turnover
    JOIN ps ON acr = {join_acc_field}
    WHERE
        acr = ANY(ARRAY[{acc_list}]) AND
        (
            curr_flag IS TRUE OR
            curr IS NULL
        ) AND
        {filter_acc_debit}
        dt BETWEEN '{date_st}' AND '{date_end}' AND
        {org_flt}
    UNION ALL
    SELECT
        2 AS dc_type,
        acr AS acc,
        {sum_field},
        CASE WHEN tax_flag THEN sumt END AS sumt,
        curr
    FROM
        acc_turnover
    JOIN ps ON adb = {join_acc_field} 
    WHERE
        adb = ANY(ARRAY[{acc_list}]) AND
        (
            curr_flag IS TRUE OR
            curr IS NULL
        ) AND
        {filter_acc_credit}
        dt BETWEEN '{date_st}' AND '{date_end}' AND
        {org_flt}
'''
