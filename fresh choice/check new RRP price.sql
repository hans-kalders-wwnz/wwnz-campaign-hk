WITH SourceA AS (
    -- Original Query (Final Output)
    SELECT
        article_id,
        pack_size,
        unit_of_measure,
        cd_retail_price,
        cnt
    FROM
        (
            SELECT
                article_id,
                pack_size,
                unit_of_measure,
                cd_retail_price,
                cnt,
                ROW_NUMBER() OVER(PARTITION BY article_id ORDER BY cnt DESC) row_num 
            FROM
                (
                    SELECT
                        article_id,
                        pack_size,
                        unit_of_measure,
                        p.value AS cd_retail_price,
                        p.count AS cnt
                    FROM
                        (
                            SELECT
                                article_id,
                                u.pack_size,
                                u.unit_of_measure,
                                APPROX_TOP_COUNT(IFNULL(standard_price, retail_price), 1) AS most_freq_price
                            FROM
                                `gcp-wow-food-de-pel-prod.datamart.store_article_detail` AS sa
                            INNER JOIN
                                `gcp-wow-food-de-pel-prod.datamart.store` AS s
                                USING(store_id)
                            LEFT JOIN
                                `gcp-wow-food-de-pel-prod.datamart.article` AS a
                                USING(article_id), UNNEST(a.uom) u 
                            WHERE
                                (sa.is_article_life_cycle_new IS TRUE OR sa.is_article_life_cycle_ranged IS TRUE)
                                AND s.is_countdown_metro_store IS FALSE
                                AND s.is_countdown_neighbourhood_store IS FALSE
                                AND sa.is_store_active IS TRUE
                                AND sa.financial_week_end_date = IF(
                                    "Mon" = FORMAT_DATE("%a", CURRENT_DATE("NZ")), 
                                    DATE_ADD(DATE_TRUNC(CURRENT_DATE("NZ"), week), INTERVAL 0 week),
                                    DATE_ADD(DATE_TRUNC(CURRENT_DATE("NZ"), week), INTERVAL 1 week)
                                )
                                AND sa.uom = u.unit_of_measure
                                AND sa.category_description NOT IN (
                                    'CIGARETTES', 'FRONT OF STORE OTHER', 'SEASONAL FOODS', 'NEWSAGENCY',
                                    'ELECTRONIC CIGARETTES', 'SMOKING ACCESSORIES',
                                    'STATIONERY-CARDS & WRAP', 'STATIONERY-NZ POST', 'STATIONERY-SEASONAL STATIONERY'
                                )
                                AND sa.district_id IN ("UNI", "LNI")
                                -- AND sa.article_id = "350115"  -- Keep the filter for a quick test
                            GROUP BY 1, 2, 3
                        )
                        CROSS JOIN UNNEST(most_freq_price) AS p
                )

        )
                    WHERE
                row_num = 1
),

SourceB AS (
SELECT
    article_id,
    pack_size,
    unit_of_measure,
    cd_retail_price,
    cnt
FROM
    (
        SELECT
            article_id,
            pack_size,
            unit_of_measure,
            cd_retail_price,
            cnt,
            -- REVISED ROW_NUMBER LOGIC:
            -- 1. Order by count (frequency) DESC
            -- 2. As a tie-breaker, order by the most recent DateSellPriceStart DESC
            ROW_NUMBER() OVER(
                PARTITION BY article_id, unit_of_measure 
                ORDER BY 
                    -- cnt DESC,                            -- Most frequent price
                    DateSellPriceStart DESC ,               -- Tie-breaker: Most recent price start date
                    cnt DESC                                -- Most frequent price
            ) AS row_num
        FROM
            (
                SELECT
                    a.Article AS article_id,
                    u.pack_size,
                    a.UoM AS unit_of_measure,
                    a.CurrentSellPrice AS cd_retail_price,
                    a.DateSellPriceStart,                 -- Include the date field
                    COUNT(a.CurrentSellPrice) AS cnt
                FROM
                    `gcp-wow-ent-im-tbl-prod.adp_dm_masterdata_view.dim_article_site_uom_v` AS a
                INNER JOIN
                    `gcp-wow-cd-data-engine-prod.consume_store.dim__store` AS b
                    ON a.site = b.store_id
                INNER JOIN
                    `gcp-wow-cd-data-engine-prod.consume_product_and_pricing.dim__article_hierarchy` AS c
                    ON a.article = c.article_id
                INNER JOIN
                    `gcp-wow-cd-data-engine-prod.consume_product_and_pricing.dim__article_uom` AS u
                    ON a.article = u.article_id AND a.UoM = u.unit_of_measure_id
                WHERE
                    1=1
                    AND a.SalesOrganisation = '2010'
                    AND a.PriceSpecificationElementTypeCode = 'VKP0'
                    AND a.article = '221149' -- Retaining filter for testing
                    AND b.is_countdown_metro_store IS FALSE
                    AND b.is_countdown_neighbourhood_store IS FALSE
                    AND b.district_id IN ('UNI', 'LNI')
                    AND b._is_current = TRUE AND c._is_current = TRUE AND u._is_current = TRUE
                    AND c.category_description NOT IN (
                        'CIGARETTES', 'FRONT OF STORE OTHER', 'SEASONAL FOODS', 'NEWSAGENCY',
                        'ELECTRONIC CIGARETTES', 'SMOKING ACCESSORIES',
                        'STATIONERY-CARDS & WRAP', 'STATIONERY-NZ POST', 'STATIONERY-SEASONAL STATIONERY'
                    )
                GROUP BY 1, 2, 3, 4, 5  -- Group by all non-aggregated columns, including DateSellPriceStart
            )
    )
WHERE
    row_num = 1
)

-- Final Comparison Query
SELECT
    COALESCE(A.article_id, B.article_id) AS article_id,
    COALESCE(A.unit_of_measure, B.unit_of_measure) AS unit_of_measure,
    COALESCE(A.pack_size, B.pack_size) AS pack_size,
    A.cd_retail_price AS price_source_a,
    B.cd_retail_price AS price_source_b,
    A.cnt AS count_source_a,
    B.cnt AS count_source_b,
    CASE
        WHEN A.article_id IS NULL THEN 'Missing in Source A (Old)'
        WHEN B.article_id IS NULL THEN 'Missing in Source B (New)'
        WHEN cast(A.cd_retail_price as float64) <> cast(B.cd_retail_price as float64) THEN 'Price Mismatch'
        WHEN A.cnt <> B.cnt THEN 'Count Mismatch'
        ELSE 'Unexpected Difference'
    END AS difference_type
FROM
    SourceA AS A
FULL OUTER JOIN
    SourceB AS B
    ON A.article_id = B.article_id
    AND A.unit_of_measure = B.unit_of_measure
-- WHERE
--     A.article_id IS NULL                     -- Rows only in New Source (B)
--     OR B.article_id IS NULL                  -- Rows only in Old Source (A)
--     OR 
--     CAST(A.cd_retail_price as FLOAT64) <> CAST(B.cd_retail_price as float64)  -- Price value mismatch
    -- OR A.cnt <> B.cnt                        -- Price count mismatchORDER BY 1, 2.
-- this is a new line