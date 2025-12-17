/*

s.store_name LIKE("%FreshChoice%")

HK  | 2024-01-31 | In the cte_retail_rrp changed the WHERE condition statement from LEFT(s.store_name,11) ="FreshChoice" to s.store_name LIKE("%FreshChoice%")
                   this is to allow for test stores to be pulled through in the data set

HK  | 2024-03-13 | In the cte_retail_rrp added in the WHERE condition statement AND s.store_name NOT LIKE ("%Test%") 
                   to remove test stores from the output

*/


CREATE OR REPLACE TABLE `gcp-wow-wwnz-wdl-ae-dev.adhoc.fc_rrp`
AS

-- Latest Price from 

WITH cte_retail_rrp AS
(

SELECT
            Article,
            Site,
            SalesOrganisation,
            PriceList,
            CurPurchaseCost,
            CurSellPriceInclTax,
            CurSellPriceExclTax,
            SalesUnit,
            DateValidFrom,
            DateValidTo,
            ALCStatus,
            ALCStatusDate,
            ROW_NUMBER() over(PARTITION BY SalesOrganisation,Site,Article ORDER BY DateValidTo DESC) row_num
        FROM
            `gcp-wow-wwnz-wdl-ae-prod.wdl_src_staging.dim_article_sell_cost_price_hist_v_upd` AS p
        LEFT JOIN `gcp-wow-wwnz-wdl-ae-prod.wdl_prod_datamart.store` AS s
            ON p.Site=s.store_id                        
        WHERE
            CurPurchaseCost IS NOT NULL
            AND DateValidTo IS NOT NULL
            AND DateValidFrom IS NOT NULL
            AND (SalesOrganisation = '9050'
            OR SalesOrganisation = '9060')
            -- AND Article IN ("265384","489992")
            -- AND site="9301"
            -- only return stores prefix with FreshChoice and SuperValue
            AND IF(
            s.store_name LIKE("%FreshChoice%")
            OR
            LEFT(s.store_name,10) ="SuperValue"
            ,TRUE,FALSE)
            AND s.store_name NOT LIKE ("%Test%")
            -- AND  ALCStatus IN ("RA","NA") -- Ranged and New Articles
)


,
-- Most frequent price Nationally in stores

cte_cd_current_shelf AS
(
  SELECT
  article_id,
  pack_size,
  unit_of_measure,
  financial_week_end_date,
  is_clearance,
  is_fastpath_offer,
  is_multibuy_offer,
  is_onecard_offer,
  is_great_price,
  is_great_price_multibuy,       
  cd_retail_price as cd_retail_price_current,  
  cnt
  
    
  FROM
    (
      SELECT
      article_id,
      pack_size,
      unit_of_measure,
      financial_week_end_date,
      is_clearance,
      is_fastpath_offer,
      is_multibuy_offer,
      is_onecard_offer,
      is_great_price,
      is_great_price_multibuy,                     
      cd_retail_price,  
      cnt,
      ROW_NUMBER() OVER(PARTITION BY article_id ORDER BY cnt DESC) row_num   -- Need to partition by most frequent promotion

      FROM
          (
              SELECT
                                    article_id,
                                    pack_size,
                                    unit_of_measure,
                                    financial_week_end_date,
                                    is_clearance,
                                    is_fastpath_offer,
                                    is_multibuy_offer,
                                    is_onecard_offer,
                                    is_great_price,
                                    is_great_price_multibuy,              
              p.value     as        cd_retail_price,
              p.count as            cnt,
              
              FROM
              (
                    SELECT                                                  article_id,
                                                                          u.pack_size,
                                                                          u.unit_of_measure,
                                                                        sa.financial_week_end_date,   
                                                                        sa.is_clearance,
                                                                        sa.is_fastpath_offer,
                                                                        sa.is_multibuy_offer,
                                                                        sa.is_onecard_offer,
                                                                        sa.is_great_price,
                                                                        sa.is_great_price_multibuy,       
                        APPROX_TOP_COUNT(shelf_price, 1)                    most_freq_price,                              -- removes occasional store specific pricing

                    FROM `gcp-wow-food-de-pel-prod.datamart.store_article_detail` AS sa
                      INNER JOIN `gcp-wow-food-de-pel-prod.datamart.store` AS s
                        USING(store_id)
                      LEFT JOIN `gcp-wow-food-de-pel-prod.datamart.article` as a
                        USING(article_id), UNNEST(a.uom) u          
                      WHERE (sa.is_article_life_cycle_new IS TRUE OR sa.is_article_life_cycle_ranged IS TRUE)
                        AND s.is_countdown_metro_store IS FALSE                                                          -- exclude metro stores
                        AND s.is_countdown_neighbourhood_store IS FALSE                                                  -- exclude neighborhood stores   
                        AND sa.is_store_active IS TRUE                                                                   -- Removes stores prefeix with 'New - '
                        AND sa.financial_week_end_date= IF(
                                          "Mon"=FORMAT_DATE("%a",CURRENT_DATE("NZ") ),                        -- Today is Monday Last Week Price
                                            DATE_ADD(DATE_TRUNC(CURRENT_DATE("NZ"), week) , INTERVAL 0 week)  -- Last Week
                                            ,DATE_ADD(DATE_TRUNC(CURRENT_DATE("NZ"), week) , INTERVAL 1 week) -- Current Week
                                                          )
                        AND sa.uom=u.unit_of_measure                                                                     -- Selling Unit of Measure 

                        AND sa.category_description NOT IN (
                                  'CIGARETTES',
                                  'FRONT OF STORE OTHER',
                                  'SEASONAL FOODS',
                                  'NEWSAGENCY',
                                  'ELECTRONIC  CIGARETTES',
                                  'SMOKING ACCESSORIES',
                                  'STATIONERY-CARDS & WRAP',
                                  'STATIONERY-NZ POST',
                                  'STATIONERY-SEASONAL STATIONERY')
                        -- AND sa.article_id="266966"
                    GROUP BY 1,2,3,4,5,6,7,8,9,10
              )

              CROSS JOIN UNNEST(most_freq_price) AS p
          )
    )

    WHERE row_num=1
      
)
,
-- cte_cd_shelf as
-- (
-- SELECT
--             p.Article,
--             a.article_description,
--             p.SalesUnit,
--             p.Site,
--             s.store_name,
--             p.DateValidFrom,
--             DateValidTo,
--             CurSellPriceInclTax,
--             LastReceiptDate,
--             ROW_NUMBER() over(PARTITION BY SalesOrganisation,Site,Article ORDER BY LastReceiptDate DESC,DateValidFrom DESC) row_num_receipt, -- Last receipt date
--             -- p.*    
--         FROM
--             `gcp-wow-ent-im-tbl-prod.adp_dm_masterdata_view.dim_article_sell_cost_price_hist_v` AS p
--         LEFT JOIN `gcp-wow-food-de-pel-prod.datamart.store` AS s
--             ON p.Site=s.store_id
--         INNER JOIN `gcp-wow-food-de-pel-prod.datamart.article` AS a
--             on p.Article=a.article_id

--         WHERE
--             CurPurchaseCost IS NOT NULL
--             AND DateValidTo IS NOT NULL
--             AND p.DateValidFrom IS NOT NULL
--             AND LastReceiptDate IS NOT NULL
--             AND (SalesOrganisation = '2010')
--             AND PriceList<>""    
--             -- AND p.Article IN ("62562")
--             -- AND p.site="9301"
--             AND is_online_store is false                       -- Exclude Online Price 
--             AND is_countdown_metro_store is false              -- Exclude Metro 
--             AND is_countdown_neighbourhood_store is false      -- Exclude Neighbourhood
--             -- AND ALCStatus IN ("RA","NA")                       -- Only ranged and new articles             
--             AND s.store_close_date < CURRENT_DATE("NZ")        -- Exclude Closed stores 
--             AND s.store_name NOT LIKE "%New%"                  -- Exclude stores with New 
--             AND DateValidTo="9999-12-31"                       -- Latest Price in SAP 
--             )
-- ,
-- cte_cd_current_recommended_retail AS
-- (

--   SELECT                   article             as article_id,
--                            SalesUnit           as unit_of_measure,
--   MAX(CurSellPriceInclTax)                     as cd_retail_price
--   FROM cte_cd_shelf c
--   WHERE row_num_receipt=1
--   GROUP BY 1,2 
-- )
---
-- deprecated 16/06/23
-- for instances when products are on great price this is returning the great price instead of the shelf price
-- 
-- 2025-12-17 Updated from gcp-wow-food-de-pel-prod.datamart to New data sources
cte_cd_current_recommended_retail AS
(
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
                    DateSellPriceStart DESC    ,            -- Tie-breaker: Most recent price start date
                    cnt DESC                                -- Most frequent price by store count


            ) AS row_num
        FROM
            (
                SELECT
                    a.Article AS article_id,
                    u.pack_size,
                    a.UoM AS unit_of_measure,
                    IF(a.PriceSpecificationElementTypeCode = 'VKP0', a.CurrentSellPrice,a.CurrentSellPrice) AS cd_retail_price, -- VKP0 Is standard price, when standard doesn't exist return non-standard off promo price
                    a.DateSellPriceStart,                 -- Include the date field
                    -- a.start_dttm     ,                   
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
                    AND a.SalesOrganisation = '2010'  -- Woolworths NZ
                    -- AND a.article = '503482' -- Retaining filter for testing                    
                    AND b.is_countdown_metro_store IS FALSE -- Exclude Metro Pricing
                    AND b.is_countdown_neighbourhood_store IS FALSE -- Exclude Neighbourhood Pricing
                    AND b.is_online_store IS FALSE -- Exclude Online Pricing
                    AND b.store_id != '9927' -- exclude Favona Cafe WWNZ pricing
                    AND b.district_id IN ('UNI', 'LNI') -- North Island pricing only
                    AND b.store_name NOT LIKE '%Closed%' -- Exclude closed store prcing
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
,
-- Latest Price from Wholesale

cte_wholesale AS 
(
SELECT 
                                                              a.article,
                                                              a.article_description,
                                                              u.unit_of_measure,
                                                                financial_week_end_date,
                                                                invoice_date,
                                                                wdl_cost_charge,
                                                                store_id,
                                                                sourcing_dc,
                                                                sourcing_dc_name,

  wdl_cost_charge/order_multiple_despatched/order_multiple      cost_price,                                                              
  franchise_base_cost/order_multiple_despatched/order_multiple  unit_price,
 FROM `gcp-wow-wwnz-wdl-ae-prod.wdl_prod_financial.dc_franchise_store_purchase_transaction` AS a
  LEFT JOIN `gcp-wow-food-de-pel-prod.datamart.dim_date` AS b
    ON a.invoice_date=b.date
  LEFT JOIN `gcp-wow-food-de-pel-prod.datamart.article` AS c
    ON a.article=c.article_id,UNNEST(uom) u
 WHERE franchise_base_cost!=0 
)
,
cte_lastest_price AS
(
SELECT 
                                                                                article
                  ,                                                             article_description
                  ,                                                             unit_of_measure
                  ,                                                             financial_week_end_date
                  ,                                                             unit_price
                  ,                                                             cost_price
                  ,                                                             store_id
                  ,                                                             sourcing_dc
                  ,                                                             sourcing_dc_name


,row_number() OVER(PARTITION BY article ORDER BY invoice_date DESC,wdl_cost_charge ASC ) row_num -- take latest invoice price and cost

FROM cte_wholesale
)
,
cte_wholesale_unit_price AS
(
SELECT
article                    article_id
,                          article_description
,                          unit_of_measure
,                          financial_week_end_date
-- ,                          store_id
,                          row_num
,                          sourcing_dc
,                          sourcing_dc_name

,ROUND(AVG(cost_price)*1.15,2)  wholesale_cost_price
,ROUND(AVG(unit_price)*1.15,2)  wholesale_unit_price

FROM cte_lastest_price
WHERE row_num=1

GROUP BY 1,2,3,4,5,6,7
-- filter latest week unit price
)

,
-- Latest Price from POS (Smart Retail)
cte_latest_week_retail_price AS
(
SELECT
  rl.article_id
, rl.article_description
, rl.store_id
, rl.store_name
-- ,  s.price_list
-- ,  s.price_list_description
, rl.unit_of_measure
,  u.pack_size
, financial_week_end_date
, receipt_date 
, rl.unit_price_paid
, rl.retail_price
, SUM(total_discount)  OVER(PARTITION BY rl.article_id,rl.store_id ORDER BY financial_week_end_date DESC)       total_discount
, SUM(gst_excluded_amount)  OVER(PARTITION BY rl.article_id,rl.store_id ORDER BY financial_week_end_date DESC)  gst_excluded_amount
, SUM(quantity)  OVER(PARTITION BY rl.article_id,rl.store_id ORDER BY financial_week_end_date DESC)             quantity
,row_number() OVER(PARTITION BY rl.article_id,rl.store_id ORDER BY receipt_date DESC,time_receipt_generated DESC )               row_num
FROM  `gcp-wow-wwnz-wdl-ae-prod.wdl_prod_datamart.sales_receipt_line` AS rl
  -- INNER JOIN `gcp-wow-wwnz-wdl-ae-prod.wdl_prod_datamart.store` AS s
  --   ON rl.store_id=s.store_id
  LEFT JOIN `gcp-wow-wwnz-wdl-ae-prod.wdl_prod_datamart.article` a
    ON rl.article_id=a.article_id
  ,UNNEST(uom) as u
WHERE rl.unit_of_measure=u.unit_of_measure 
AND rl.is_online_order IS FALSE   -- only instore pricing 
AND rl.return_reason_id =""       -- exclude returned products
AND rl.financial_week_end_date= 
IF(
  "Mon"=FORMAT_DATE("%a",CURRENT_DATE("NZ") ),                        -- Today is Monday Last Week Price
    DATE_ADD(DATE_TRUNC(CURRENT_DATE("NZ"), week) , INTERVAL 0 week)  -- Last Week
    ,DATE_ADD(DATE_TRUNC(CURRENT_DATE("NZ"), week) , INTERVAL 1 week) -- Current Week
)
AND rl.category_description NOT IN (
                                  'CIGARETTES',
                                  'FRONT OF STORE OTHER',
                                  'SEASONAL FOODS',
                                  'NEWSAGENCY',
                                  'ELECTRONIC  CIGARETTES',
                                  'SMOKING ACCESSORIES',
                                  'STATIONERY-CARDS & WRAP',
                                  'STATIONERY-NZ POST',
                                  'STATIONERY-SEASONAL STATIONERY')

)

,
cte_rrp_price_list AS
(
    SELECT
        Article,
        Site,
        PriceList as price_list,
        CurSellPriceInclTax as rrp,
        SalesUnit,
        DateValidFrom,
        DateValidTo,
        ALCStatus,
        ALCStatusDate,

    FROM cte_retail_rrp
    WHERE row_num=1
    -- removed active stores as per conversation with Felicity 06/06
    -- to return all stores prefixed with SuperValue and FreshChoice
    --  AND Site IN (SELECT store_id FROM cte_latest_week_retail_price UNION ALL SELECT "9642" ) -- only retail stores and price list 3A
)

,
cte_retail_date AS 
(
SELECT
   article_id
  ,article_description
  ,store_id
  ,row_num
  ,store_name
  ,receipt_date
  ,financial_week_end_date
  ,unit_of_measure
  ,pack_size
  ,unit_price_paid
  ,retail_price
  ,gst_excluded_amount
  ,quantity
  ,total_discount
FROM cte_latest_week_retail_price
  WHERE
  row_num=1 
  -- and store_id="9301"
  -- and article_id="58970"

) -- select * from cte_retail_date
,

cte_latest_week_13wk_sales AS
(
SELECT
  rl.article_id
, rl.article_description
, rl.store_id
, rl.store_name
-- ,  s.price_list
-- ,  s.price_list_description
, rl.unit_of_measure
, SUM(gst_excluded_amount)  gst_excluded_amount_13wk_sales

FROM  `gcp-wow-wwnz-wdl-ae-prod.wdl_prod_datamart.sales_receipt_line` AS rl
  -- INNER JOIN `gcp-wow-wwnz-wdl-ae-prod.wdl_prod_datamart.store` AS s
  --   ON rl.store_id=s.store_id
  LEFT JOIN `gcp-wow-wwnz-wdl-ae-prod.wdl_prod_datamart.article` a
    ON rl.article_id=a.article_id
  ,UNNEST(uom) as u
WHERE rl.unit_of_measure=u.unit_of_measure 

-- Last 13 weeks
AND rl.financial_week_end_date BETWEEN  DATE_SUB(DATE_TRUNC(CURRENT_DATE("NZ"), week), INTERVAL 12 week)
                                    AND DATE_SUB(DATE_TRUNC(CURRENT_DATE("NZ"), week), INTERVAL 0 week)

GROUP BY 1,2,3,4,5
)

,
-- top 300 
-- maintained by fresh choice 
-- https://docs.google.com/spreadsheets/d/1IrFu5JJw6Q11ZiRzsn_haHxFTRP309NkR0USxWO8-3E/edit#gid=0


cte_top_300 AS
(
  SELECT 
                            a.article_id
    ,                       p.price_famly
    ,IFNULL(p.primary,"N") as primary_line
  FROM `gcp-wow-wwnz-wdl-ae-prod.wdl_prod_datamart.article` AS a
    LEFT JOIN `gcp-wow-food-de-pel-prod.master.fresh_choice_primary_top_300` AS p
      ON a.price_family_id=p.price_famly
)
,
-- price promise
-- maintained by fresh choice 
-- https://docs.google.com/spreadsheets/d/1TxTZFzSWmNHz2if4pFTHs9chTTHIhC5pXXdbV5QGtfo/edit#gid=1625977871

cte_price_promise AS
(
  SELECT DISTINCT article_number,price_promise

  FROM `gcp-wow-food-de-pel-prod.master.fresh_choice_price_promise`

)

SELECT

     a.article_description
    ,a.article_id
    ,rrp.Site as store_id
    ,s.store_name
    ,rrp.SalesUnit as unit_of_measure
    ,r.pack_size
    ,IFNULL(t.primary_line,"N") as   primary_line
    ,IFNULL(pp.price_promise,"N") as price_promise
    ,rrp.price_list
    ,rrp.rrp
    ,r.retail_price
    ,r.receipt_date
    ,r.financial_week_end_date as financial_week_end_date_fc
    ,cd.cd_retail_price_current
    ,cd_rrp.cd_retail_price as cd_recommended_retail_price
    ,cd.is_clearance
    ,cd.is_fastpath_offer
    ,cd.is_multibuy_offer
    ,cd.is_onecard_offer    
    ,cd.is_great_price
    ,cd.is_great_price_multibuy
    ,cd.financial_week_end_date as financial_week_end_date_cd
    ,wholesale_cost_price
    ,wholesale_unit_price
    ,sourcing_dc
    ,sourcing_dc_name
    ,w13.gst_excluded_amount_13wk_sales
    ,a.price_family_id
    ,a.price_family_name
    ,a.department_description
    ,a.category_description
    ,a.sub_category_description
    ,a.segment_description
    ,a.category_manager_name
    ,a.vendor_description
    ,a.nz_own_brand
    ,s.store_brand
    ,rrp.DateValidFrom
    ,rrp.DateValidTo
    ,rrp.ALCStatus
    ,rrp.ALCStatusDate

FROM cte_rrp_price_list AS rrp
    LEFT JOIN cte_retail_date AS r
        ON r.store_id=rrp.Site AND r.article_id=rrp.Article AND r.unit_of_measure=rrp.SalesUnit -- only return products that have a price_list
    LEFT JOIN cte_cd_current_shelf AS cd
        ON rrp.Article=cd.article_id AND rrp.SalesUnit=cd.unit_of_measure  --AND r.financial_week_end_date=cd.financial_week_end_date
    LEFT JOIN cte_wholesale_unit_price AS w
        ON rrp.Article=w.article_id AND rrp.SalesUnit=w.unit_of_measure     -- rrp.Site=w.store_id AND -- average gp % by dc
    LEFT JOIN cte_top_300 AS t
        ON rrp.Article=t.article_id
    LEFT JOIN `gcp-wow-wwnz-wdl-ae-prod.wdl_prod_datamart.article` AS a
        ON rrp.Article=a.article_id
    LEFT JOIN cte_cd_current_recommended_retail AS cd_rrp
        ON rrp.Article=cd_rrp.article_id AND rrp.SalesUnit =cd_rrp.unit_of_measure
    LEFT JOIN cte_price_promise AS pp
        ON rrp.Article=pp.article_number
    LEFT JOIN `gcp-wow-wwnz-wdl-ae-prod.wdl_prod_datamart.store` AS s
        ON rrp.Site=s.store_id        
    LEFT JOIN cte_latest_week_13wk_sales AS w13
        ON w13.store_id=rrp.Site AND w13.article_id=rrp.Article AND w13.unit_of_measure=rrp.SalesUnit

WHERE 
  a.category_description NOT IN (
  'CIGARETTES',
  'FRONT OF STORE OTHER',
  'SEASONAL FOODS',
  'NEWSAGENCY',
  'ELECTRONIC  CIGARETTES',
  
  'STATIONERY',
  'ELECTRONIC SERVICES'
  )
  AND
  a.sub_category_description
  NOT IN (
    'CARDS & WRAP - VENDOR REFILL',
    'NZ POST',
    'SMOKING ACCESSORIES'

  )