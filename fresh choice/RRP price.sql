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