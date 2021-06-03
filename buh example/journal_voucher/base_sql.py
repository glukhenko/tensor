"""
Глухенко А.В.
Модуль, хранящий базовые конструкции запроса по отчету Журнал Ордер, в разных режимах
"""

AB_BASE_SQL = '''
    {cte_with_accounts_hier}
    SELECT *
    FROM
    (
        SELECT
            limited_data.*,
            real_id::text correct_real_id,
            NULL::int "Документ",
            NULL::int "НашаОрганизация",
            NULL::date "Дата",
            NULL::int "doc_id", 
            {additional_columns}
            {corresponds}
        FROM
            (
                SELECT
                    md.*
                    {field_for_name_order}
                FROM
                    (
                        SELECT
                            real_id,
                            {result_sum_cols}
                        FROM
                            (
                                SELECT
                                    *
                                    {curr_aggregate}
                                FROM
                                    (
                                        SELECT
                                            *,
                                            {window_agg_cols}
                                        FROM
                                            (
                                                SELECT
                                                    {main_data_cols},
                                                    curr
                                                FROM
                                                    acc_balance
                                                WHERE
                                                    {where_flt} AND
                                                    {dates_filter}
                                                GROUP BY
                                                    {group_col},
                                                    curr
                                                HAVING
                                                    {zero_sum_filters}
                                            ) d
                                        WHERE
                                            {saldo_filters}
                                        WINDOW w AS (PARTITION BY curr)
                                    ) d
                            ) d
                        GROUP BY
                            real_id
                    ) md
                {join_tables_for_sort}
                {join_tables_for_search}
                WHERE
                    {filter_by_search}
                ORDER BY
                    {order_by}
                LIMIT
                    $3::int
                OFFSET
                    $4::int
            ) AS limited_data
            {join_extended_tables}
        ORDER BY
            {order_by}
    ) d
    WHERE
        {filter_by_ido}
'''

AB_WITH_ANALYTICS_HIERARCHY_SQL = '''
    WITH RECURSIVE raw_data AS (
        SELECT
            {main_data_cols},
            curr
            {corresponds}
        FROM
            acc_balance
        WHERE
            {where_flt} AND
            {dates_filter}
        GROUP BY
            {group_col},
            curr
        HAVING
            {zero_sum_filters}
    )
    {cte_hierarchy}
    , data_with_parents AS (
        -- прокидываем суммы детей к родителям, позже родителей прогруппируем и проссумируем
        SELECT
            "real_id",
            curr,
            ec."Раздел" "parent",
            ec."Раздел@",
            {sum_fields}
            {correspondence_field}
        FROM
            raw_data
        LEFT JOIN
            entity_tree ec
                ON ec."@Лицо" = real_id
        UNION ALL
        SELECT
            data_with_parents.parent "real_id",
            curr,
            ec."Раздел" "parent",
            ec."Раздел@",
            {sum_fields}
            {correspondence_field}
        FROM
            data_with_parents
        JOIN
            entity_tree ec
                ON data_with_parents.parent = ec."@Лицо"
    )
    SELECT *
    FROM
        (
            SELECT
                limited_data.*,
                real_id::text correct_real_id,
                NULL::int "Документ",
                NULL::int "НашаОрганизация",
                NULL::date "Дата",
                NULL::int "doc_id", 
                {additional_columns}
            FROM
                (
                    SELECT
                        md.*
                        {field_for_name_order}
                    FROM
                        (
                            SELECT
                                real_id,
                                "Раздел@",
                                {result_sum_cols}
                                {grouped_correspondence_field}
                            FROM
                                (
                                    SELECT
                                        *
                                        {curr_aggregate}
                                    FROM
                                        (
                                            SELECT
                                                *,
                                                {window_agg_cols}
                                            FROM
                                                (
                                                    SELECT 
                                                        *
                                                    FROM 
                                                        data_with_parents
                                                    WHERE
                                                        {filter_by_parent}
                                                ) d
                                            WHERE
                                                {saldo_filters}
                                            WINDOW w AS (PARTITION BY curr)
                                        ) d
                                ) d
                            GROUP BY
                                real_id, "Раздел@"
                        ) md
                    {join_tables_for_sort}
                    {join_tables_for_search}
                    WHERE
                        {filter_by_search}
                    ORDER BY
                        {order_by}
                    LIMIT
                        $2::int
                    OFFSET
                        $3::int
                ) AS limited_data
                {join_extended_tables}
        ) d
    WHERE
        {filter_by_ido}
    ORDER BY
        {order_by}
'''

AB_WITH_PERIOD_BASE_SQL = '''
    WITH data_by_start_date AS (
        SELECT
            *
        FROM acc_balance
        WHERE
            {where_flt} AND
            $2::date - 1 BETWEEN dt AND dte
    ), data_by_start_date_hstore AS (
        SELECT
            jsonb_object(
                array_agg(coalesce(org::text, '') || ',' || acc::text || ',' || coalesce(curr, 'RUB')),
                array_agg((data_by_start_date)::TEXT)
            ) AS data
        FROM
            data_by_start_date
    )
    , raw_data AS (
        SELECT
            {main_fields_ab_period},
            curr
        FROM
            acc_balance
        WHERE
            {where_flt} AND
            dt BETWEEN $2::date AND $3::date
    )
    , data_with_delta_balance AS (
        SELECT
            {main_fields_ab_period},
            COALESCE(bdb, 0) - COALESCE(
                LAG(
                    bdb,
                    1,
                    (
                        SELECT SUM(
                            (
                                (
                                    data->>(coalesce(org::text, '') || ',' || acc::text || ',' || coalesce(curr, 'RUB'))
                                )::acc_balance
                            ).bdb
                        )
                        FROM data_by_start_date_hstore
                    )
                ) OVER w,
                0
            ) bdb_oa,
            COALESCE(bcr, 0) - COALESCE(
                LAG(
                    bcr,
                    1,
                    (
                        SELECT SUM(
                            (
                                (
                                    data->>(coalesce(org::text, '') || ',' || acc::text || ',' || coalesce(curr, 'RUB'))
                                )::acc_balance
                            ).bcr
                        )
                        FROM data_by_start_date_hstore
                    )
                ) OVER w,
                0
            ) bcr_oa,
            COALESCE(bdbt, 0) - COALESCE(
                LAG(
                    bdbt,
                    1,
                    (
                        SELECT SUM(
                            (
                                (
                                    data->>(coalesce(org::text, '') || ',' || acc::text || ',' || coalesce(curr, 'RUB'))
                                )::acc_balance
                            ).bdbt
                        )
                        FROM data_by_start_date_hstore
                    )
                ) OVER w,
                0
            ) bdbt_oa,
            COALESCE(bcrt, 0) - COALESCE(
                LAG(
                    bcrt,
                    1,
                    (
                        SELECT SUM(
                            (
                                (
                                    data->>(coalesce(org::text, '') || ',' || acc::text || ',' || coalesce(curr, 'RUB'))
                                )::acc_balance
                            ).bcrt
                        )
                        FROM data_by_start_date_hstore
                    )
                ) OVER w,
                0
            ) bcrt_oa,
            curr
        FROM
            raw_data
        WINDOW w AS (
            PARTITION BY org, acc, curr
            ORDER BY dt
        )
    )
    , group_by_id_curr AS (
        SELECT
            {grp_dt_col} real_id,
            SUM(vdb) vdb,
            SUM(vcr) vcr,
            SUM(vdbt) vdbt,
            SUM(vcrt) vcrt,
            (
                SELECT COALESCE(SUM(bdb), 0)
                FROM data_by_start_date dbsd
                WHERE COALESCE(dbsd.curr, 'RUB') = COALESCE(dwdb.curr, 'RUB')
            ) + SUM(SUM(bdb_oa)) OVER (PARTITION BY curr ORDER BY {grp_dt_col}) bdbe,
            (
                SELECT COALESCE(SUM(bcr), 0)
                FROM data_by_start_date dbsd
                WHERE COALESCE(dbsd.curr, 'RUB') = COALESCE(dwdb.curr, 'RUB')
            ) + SUM(SUM(bcr_oa)) OVER (PARTITION BY curr ORDER BY {grp_dt_col}) bcre,
            (
                SELECT COALESCE(SUM(bdbt), 0)
                FROM data_by_start_date
                WHERE curr IS NULL
            ) + SUM(SUM(bdbt_oa)) OVER (PARTITION BY curr ORDER BY {grp_dt_col}) bdbet,
            (
                SELECT COALESCE(SUM(bcrt), 0)
                FROM data_by_start_date
                WHERE curr IS NULL
            ) + SUM(SUM(bcrt_oa)) OVER (PARTITION BY curr ORDER BY {grp_dt_col}) bcret,
            curr
        FROM
            data_with_delta_balance dwdb
        GROUP BY
            {grp_dt_col},
            curr
    )
    , calc_turnovers AS (
        SELECT
            real_id,
            SUM(COALESCE(vdb, 0)) FILTER(WHERE curr IS NULL) vdb,
            SUM(COALESCE(vcr, 0)) FILTER(WHERE curr IS NULL) vcr,
            SUM(COALESCE(vdbt, 0)) FILTER(WHERE curr IS NULL) vdbt,
            SUM(COALESCE(vcrt, 0)) FILTER(WHERE curr IS NULL) vcrt,

            {curr_turnover_fields} -- vdbc/vcrc/vdbc_s/vcrc_s

            SUM(bdbe) FILTER(WHERE curr IS NULL) bdbe,
            SUM(bcre) FILTER(WHERE curr IS NULL) bcre,
            SUM(bdbet) FILTER(WHERE curr IS NULL) bdbet,
            SUM(bcret) FILTER(WHERE curr IS NULL) bcret,

            jsonb_object_agg(curr, bdbe) FILTER(WHERE curr IS NOT NULL AND COALESCE(bdbe, 0) <> 0) bdbec,
            jsonb_object_agg(curr, bcre) FILTER(WHERE curr IS NOT NULL AND COALESCE(bcre, 0) <> 0) bcrec

        FROM
            group_by_id_curr
        GROUP BY
            real_id
        ORDER BY
            real_id
    )
    , calc_start_balance AS (
        SELECT
            "real_id",
            LAG(bdbe, 1, (SELECT SUM(bdb) FROM data_by_start_date WHERE curr IS NULL)) OVER w_real_id bdbs,
            LAG(bcre, 1, (SELECT SUM(bcr) FROM data_by_start_date WHERE curr IS NULL)) OVER w_real_id bcrs,
            LAG(bdbet, 1, (SELECT SUM(bdbt) FROM data_by_start_date WHERE curr IS NULL)) OVER w_real_id bdbst,
            LAG(bcret, 1, (SELECT SUM(bcrt) FROM data_by_start_date WHERE curr IS NULL)) OVER w_real_id bcrst,
            
            LAG(bdbec, 1, (
                SELECT jsonb_object_agg(curr, bdb)
                FROM data_by_start_date
                WHERE curr IS NOT NULL AND bdb <> 0
            )) OVER w_real_id bdbsc,
            
            LAG(bcrec, 1, (
                SELECT jsonb_object_agg(curr, bcr)
                FROM data_by_start_date
                WHERE curr IS NOT NULL AND bcr <> 0
            )) OVER w_real_id bcrsc
            FROM
                calc_turnovers
            WINDOW w_real_id AS (
                ORDER BY real_id
            )
    )
    , prepare_result AS (
        SELECT
            -- расчет сумм по балансу начала
            COALESCE($1::text || ',', '') || ct."real_id" || '@periodicity' id,
            SUBSTRING(ct."real_id" FROM 2) "Название",
            $1::TEXT parent,
            
            ct."real_id",
            ct."vdb",
            ct."vcr",
            ct."vdbt",
            ct."vcrt",
            ct."vdbc",
            ct."vcrc",
            ct."vdbc_s",
            ct."vcrc_s",
            ct."bdbe",
            ct."bcre",
            ct."bdbet",
            ct."bcret",
            
            csb."bdbs",
            csb."bcrs",
            csb."bdbst",
            csb."bcrst",
            csb."bdbsc",
            csb."bcrsc",
            
            ct.bdbec,
            ct.bcrec,
            
            -- расчет сумм по оборотам
            SUM(vdb) over() vdb_s,
            SUM(vcr) over() vcr_s,
            SUM(vdbt) over() vdbt_s,
            SUM(vcrt) over() vcrt_s,
            -- расчет сумм по балансу начала
            FIRST_VALUE(bdbs) OVER() bdbs_s,
            FIRST_VALUE(bcrs) OVER() bcrs_s,
            FIRST_VALUE(bdbst) OVER() bdbst_s,
            FIRST_VALUE(bcrst) OVER() bcrst_s,
            -- расчет сумм по балансу конца
            LAST_VALUE(bdbe) OVER() bdbe_s,
            LAST_VALUE(bcre) OVER() bcre_s,
            LAST_VALUE(bdbet) OVER() bdbet_s,
            LAST_VALUE(bcret) OVER() bcret_s,
            -- расчет валютных сумм по балансу начала
            FIRST_VALUE(bdbsc) OVER() bdbsc_s,    
            FIRST_VALUE(bcrsc) OVER() bcrsc_s,
            -- расчет валютных сумм по балансу конца
            LAST_VALUE(ct.bdbec) OVER() bdbec_s,
            LAST_VALUE(ct.bcrec) OVER() bcrec_s
        FROM
            calc_turnovers ct
        LEFT JOIN calc_start_balance csb
            ON ct."real_id" = csb."real_id"
    )
    SELECT
        real_id,
        id,
        NULL::int "Документ",
        NULL::date "Дата",
        NULL::int "doc_id",
        NULL::int "НашаОрганизация",
        {name_col} "Название",
        parent,
        {is_node} "parent@",
        -- balance start
        bdbs_s,
        bcrs_s,
        bdbst_s,
        bcrst_s,
        bdbsc_s,
        bcrsc_s,
        -- balance end
        bdbe_s,
        bcre_s,
        bdbet_s,
        bcret_s,
        bdbec_s,
        bcrec_s,
        -- turnover
        vdb_s,
        vcr_s,
        vdbt_s,
        vcrt_s,
        vdbc_s,
        vcrc_s,
        {result_select_fields}
    FROM
        prepare_result
    WHERE
        ({filter_by_empty}) AND
        ({filter_by_saldo}) AND
        ({filter_by_ido})
    ORDER BY real_id DESC
    LIMIT
        $4::int
    OFFSET
        $5::int
'''

DC_BASE_SQL = '''
    WITH group_by_curr AS (
        SELECT
            {raw_fields}
        FROM
            "ДебетКредит" dk
            {join_corresponds}
        WHERE
            {dc_filter}
            AND dk."Тип" IN (1,2)
            AND dk."Дата" BETWEEN $3::date AND $4::date
            {filter_by_internal_turnover}
        GROUP BY
            {group_fields_by_curr},
            dk."Валюта"
        HAVING
            NOT (
                {having_fields}
            ) IS NULL
    )
    , group_by_doc_date AS (
        SELECT
            {fields_group_by_doc}
        FROM
            group_by_curr gbc
        GROUP BY
            {group_fields}
    )
    , prepare_result AS (
        SELECT
            $1::text parent,
            $2::bool "parent@",
            gbdd.*
            {fields_for_sort}
        FROM
            group_by_doc_date gbdd
            {tables_for_sort}
        WHERE
            {search_filter}
        ORDER BY
            {sort_fields}
        LIMIT
            $5::int OFFSET $6::int
    )
    SELECT
        pr.*,
        {result_fields}
    FROM
        prepare_result pr
        {addition_tables}
    WHERE
        {filter_by_ido}
    ORDER BY
        {sort_result}
'''

DC_WITH_HIERARCHY_BASE_SQL = '''
    WITH RECURSIVE raw_data AS (
        SELECT
            {raw_fields}
        FROM
            "ДебетКредит" dk
            {corresponds_tables}
        WHERE
            {where_cond}
            AND dk."Тип" IN (1,2)
            {filter_by_internal_turnover}
        GROUP BY
            dk."Счет",
            dk."Валюта"
    )
    , data_with_parents AS (
        -- прокидываем суммы детей к родителям, позже родителей прогруппируем и проссумируем
        SELECT
            "Валюта",
            "real_id",
            {amount_fields}
            {correspond_array_field}

            ps."Номер",
            ps."Признаки",
            ps."Название",
            ps."Раздел" "parent",
            ps."Раздел@"
        FROM
            raw_data
        LEFT JOIN
            "ПланСчетов" ps
                ON ps."@ПланСчетов" = real_id

        UNION ALL

        SELECT
            "Валюта",
            data_with_parents.parent "real_id",
            {amount_fields}
            {correspond_array_field}

            ps."Номер",
            ps."Признаки",
            ps."Название",
            ps."Раздел" "parent",
            ps."Раздел@"
        FROM
            data_with_parents,
            "ПланСчетов" ps
        WHERE
            data_with_parents.parent = ps."@ПланСчетов"
    )
    {total_curr_cte}
    SELECT *
    FROM
    (
        SELECT *
        FROM
        (
            SELECT
                dwp_external."real_id"::TEXT,
                COALESCE($1::text || ',', '') || real_id || '@account' id,
                NULL::int "НашаОрганизация",
                $1::text parent,
                -- В отчете не может быть развернуто узла (False)
                MIN("Раздел@"::int)::bool OR {is_node}::bool "parent@",
        
                MIN("Номер") "Номер",
                MIN("Название") "Название",
                MIN("Признаки") AS "Признаки",
                 
                {result_amount_fields}
                {group_correspond_array_field}
        
                -- расчет валюты для строки отчета
                {result_curr_amount_fields}
                -- расчет валюты для всего отчета
                {result_curr_total_fields}
            FROM
                data_with_parents dwp_external
            WHERE
                {filter_by_parent_acc}
                {search_filter}
            GROUP BY
                "real_id"
            HAVING NOT (
                {having_fields}
            ) IS NULL
        ) ps
        ORDER BY 
            {sort_result}
        LIMIT
            $3::int
        OFFSET
            $4::int
    ) d
    WHERE
        {filter_by_ido}
'''

DC_WITH_ANALYTICS_HIERARCHY_SQL = '''
    WITH RECURSIVE raw_data AS (
        SELECT
            {raw_fields}
        FROM
            "ДебетКредит" dk
            {corresponds_tables}
        WHERE
            {where_cond}
            AND dk."Тип" IN (1,2)
            {filter_by_internal_turnover}
        GROUP BY
            dk."Лицо{analytics_num}",
            dk."Валюта"
    )
    {cte_hierarchy}
    , data_with_parents AS (
        -- прокидываем суммы детей к родителям, позже родителей прогруппируем и проссумируем
        SELECT
            "Валюта",
            "real_id",
            {amount_fields}
            {correspond_array_field}
            COALESCE(ec.analytic_name, 'Без названия') "Название",
            ec."Раздел" "parent",
            ec."Раздел@"
        FROM
            raw_data
        JOIN
            entity_tree ec
                ON ec."@Лицо" = real_id
        UNION ALL
        SELECT
            "Валюта",
            data_with_parents.parent "real_id",
            {amount_fields}
            {correspond_array_field}
            COALESCE(ec.analytic_name, 'Без названия') "Название",
            ec."Раздел" "parent",
            ec."Раздел@"
        FROM
            data_with_parents
        JOIN
            entity_tree ec
                ON data_with_parents.parent = ec."@Лицо"
    )
    {total_curr_cte}
    SELECT *
    FROM
        (
            SELECT
                dwp_external."real_id",                
                COALESCE($1::text || ',', '') || real_id || '@face{analytics_num}' id,
                NULL::int "НашаОрганизация",
                $1::text parent,
                -- В отчете не может быть развернуто узла (False)
                MIN("Раздел@"::int)::bool OR {is_node}::bool "parent@",
                
                MIN("Название") "Название",
        
                {result_amount_fields}
                {group_correspond_array_field}
        
                -- расчет валюты для строки отчета
                {result_curr_amount_fields}
                -- расчет валюты для всего отчета
                {result_curr_total_fields}
            FROM
                data_with_parents dwp_external
            WHERE
                {filter_by_parent}
            GROUP BY
                "real_id"
            HAVING NOT (
                {having_fields}
            ) IS NULL
            LIMIT
                $2::int
            OFFSET
                $3::int
        ) d
    WHERE
        {filter_by_ido}
'''
