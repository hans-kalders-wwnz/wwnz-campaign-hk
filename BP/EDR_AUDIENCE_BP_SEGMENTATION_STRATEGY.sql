-- CREATE OR REPLACE TABLE offers_working.EDR_AUDIENCE_BP_SEGMENTATION_STRATEGY AS

WITH sales_fuel_partner AS (
  SELECT
    scv_id,
    partner_id,
    SUM(COALESCE(base_points_earn, 0)) AS base_points,
    SUM(COALESCE(bonus_points_earn, 0)) AS bonus_points,
    COUNT(DISTINCT receipt_id) AS transactions,
    MIN(transaction_start_date_nzt) AS first_trx_date,
    MAX(transaction_start_date_nzt) AS last_trx_date
  FROM
    `gcp-wow-cd-data-engine-prod.consume_loyalty.trn__fuel_redemption_detail` AS frv
  WHERE
    LOWER(Redemption_Type) IN ('base', 'bonus')
    --AND date(transaction_start_date_nzt) > date_add(current_date('Pacific/Auckland'), INTERVAL -24 week)
  GROUP BY
    1,2
),

non_fuel_partners_sales AS (
  SELECT
    scv_id,
    partner_id,
    SUM(COALESCE(base_points_earn, 0)) AS base_points,
    SUM(COALESCE(bonus_points_earn, 0)) AS bonus_points,
    COUNT(DISTINCT ee_reference_number) AS transactions,
    MIN(transaction_start_date_nzt) AS first_trx_date,
    MAX(transaction_start_date_nzt) AS last_trx_date
  FROM
    `gcp-wow-cd-data-engine-prod.consume_marketing.trn__campaign_sales_reward_detail` AS csrt
  WHERE
    LOWER(redeem_type) IN ('base', 'bonus')
    AND behaviour_offer_flag IS FALSE
    --AND date(transaction_start_date_nzt) > date_add(current_date('Pacific/Auckland'), INTERVAL -24 week)
  GROUP BY
    1,2
),

BP_points AS (
  WITH aux AS (
    SELECT
      *
    FROM
      sales_fuel_partner
    UNION ALL
    SELECT
      *
    FROM
      non_fuel_partners_sales
  )
  SELECT
    scv_id,
    SUM(base_points) AS BP_base_points,
    SUM(bonus_points) AS BP_bonus_points
  FROM
    aux
  WHERE
    partner_id = '2011'
  GROUP BY
    1
),

points AS (
  WITH aux AS (
    SELECT
      *
    FROM
      sales_fuel_partner
    UNION ALL
    SELECT
      *
    FROM
      non_fuel_partners_sales
  )
  SELECT
    scv_id,
    SUM(base_points) AS total_base_points,
    SUM(bonus_points) AS total_bonus_points,
    SUM(transactions) AS transactions,
    MIN(first_trx_date) AS first_trx_date,
    MAX(last_trx_date) AS last_trx_date
  FROM
    aux
  GROUP BY
    1
),

partners AS (
  SELECT DISTINCT
    scv_id,
    partner_id
  FROM
    sales_fuel_partner
  UNION DISTINCT
  SELECT DISTINCT
    scv_id,
    partner_id
  FROM
    non_fuel_partners_sales
),

partners_transactions AS (
  SELECT DISTINCT
    partners.scv_id,
    REPLACE(p.partner_name_current, '_NZ', '') AS partner_name
  FROM
    partners
  INNER JOIN `gcp-wow-cd-data-engine-prod.consume_loyalty.dim__partner` AS p
    ON partners.partner_id = p.partner_id
),

partners_nums AS (
  SELECT
    scv_id,
    SUM(CASE
      WHEN partner_name = 'BP' THEN 1
      ELSE 0
    END) AS BP_CUSTOMER,
    COUNT(DISTINCT partner_name) AS Different_partners
  FROM
    partners_transactions
  GROUP BY
    1
),

fuel_sales_and_litres AS (
  SELECT DISTINCT
    partner_id,
    transaction_start_date_nzt,
    IF(redemption_type = 'cpl discount', transaction_start_date_nzt, NULL) AS transaction_start_date_nzt_FUEL,
    receipt_id,
    scv_id,
    IF(LOWER(redemption_type) IN ('base', 'bonus'), total_item_sales_amount, NULL) AS sales_amount_total,
    IF(redemption_type = 'cpl discount', total_item_sales_amount, NULL) AS edr_sales_fuel_nzd,
    IF(redemption_type = 'cpl discount', article_quantity, NULL) AS fuel_sold_litres,
    IF(redemption_type = 'cpl discount', receipt_id, NULL) AS fuel_receipt_id,
    IF(redemption_type = 'cpl discount', total_discount_amount / article_quantity, NULL) AS cpl_discount
  FROM
    `gcp-wow-cd-data-engine-prod.consume_loyalty.trn__fuel_redemption_detail` AS frv
  WHERE
    partner_id = '2011'
    --date(transaction_start_date_nzt) > date_add(current_date('Pacific/Auckland'), INTERVAL -24 week)
),

sum_fuel_sales_and_litres AS (
  SELECT
    scv_id,
    SUM(sales_amount_total) AS total_sales_BP,
    SUM(edr_sales_fuel_nzd) AS total_sales_fuel_BP,
    SUM(sales_amount_total) - SUM(edr_sales_fuel_nzd) AS total_sales_shop_BP,
    COUNT(DISTINCT receipt_id) AS transactions_BP,
    COUNT(DISTINCT fuel_receipt_id) AS fuel_transactions_BP,
    COUNT(DISTINCT receipt_id) - COUNT(DISTINCT fuel_receipt_id) AS shop_transactions_BP,
    SUM(fuel_sold_litres) AS total_fuel_litres_BP,
    SUM(fuel_sold_litres) / COUNT(DISTINCT fuel_receipt_id) AS Average_fuel_litres_BP,
    SAFE_DIVIDE(DATE_DIFF(CURRENT_DATE('Pacific/Auckland'), MIN(transaction_start_date_nzt_FUEL), DAY), COUNT(DISTINCT fuel_receipt_id)) AS days_to_fuel_BP,
    SAFE_DIVIDE(DATE_DIFF(CURRENT_DATE('Pacific/Auckland'), MIN(transaction_start_date_nzt), DAY), COUNT(DISTINCT receipt_id) - COUNT(DISTINCT fuel_receipt_id)) AS days_to_shop_BP,
    SUM(cpl_discount) AS cpl_discount_total,
    SUM(CASE
      WHEN DATE(transaction_start_date_nzt) >= DATE_ADD(CURRENT_DATE('Pacific/Auckland'), INTERVAL -8 WEEK) THEN 1
      ELSE 0
    END) AS Last_2M,
    SUM(CASE
      WHEN DATE(transaction_start_date_nzt) >= DATE_ADD(CURRENT_DATE('Pacific/Auckland'), INTERVAL -16 WEEK)
      AND (transaction_start_date_nzt) < DATE_ADD(CURRENT_DATE('Pacific/Auckland'), INTERVAL -8 WEEK) THEN 1
      ELSE 0
    END) AS Previous2M_to_last_2M,
    SUM(CASE
      WHEN (transaction_start_date_nzt) < DATE_ADD(CURRENT_DATE('Pacific/Auckland'), INTERVAL -16 WEEK) THEN 1
      ELSE 0
    END) AS Before4M,
    MIN(transaction_start_date_nzt) AS first_BP_transaction_date,
    MAX(transaction_start_date_nzt) AS last_BP_transaction_date
  FROM
    fuel_sales_and_litres
  GROUP BY
    1
),

BP AS (
  SELECT
    Site_Type,
    Region___Area,
    Site_Name,
    LATITUDE AS BP_LAT,
    LONGITUDE AS BP_LON,
    CONCAT(LATITUDE, ',', LONGITUDE) AS BP_COORD
  FROM
    `gcp-wow-food-de-pel-prod.dev_online.BP_GAS_LOCATION`
  WHERE
    LOWER(Site_Name) LIKE '%bp %'
),

WWNZ AS (
  SELECT
    store_id,
    supermarket_name,
    store_latitude AS WWNZ_LAT,
    store_longitude AS WWNZ_LON,
    CONCAT(store_latitude, ',', store_longitude) AS CD_COORD
  FROM
    `gcp-wow-food-de-pel-prod.spatial.supermarket_tracker`
  WHERE
    LOWER(supermarket_name) LIKE '%countdown%'
    OR LOWER(supermarket_name) LIKE '%wool%'
),

DISTANCES AS (
  SELECT
    WWNZ.*,
    BP.*,
    ST_DISTANCE(ST_GEOGPOINT(WWNZ_LON, WWNZ_LAT), ST_GEOGPOINT(BP_LON, BP_LAT)) / 1000 AS distance_in_km
  FROM
    WWNZ
  FULL JOIN BP
    ON BP.Site_Type = 'RETAIL - BP'
),

ROW_NUMBER_DIST AS (
  SELECT
    DISTANCES.*,
    ROW_NUMBER() OVER(PARTITION BY store_id ORDER BY distance_in_km ASC) AS rn
  FROM
    DISTANCES
),

CLOSEST_BP AS (
  SELECT
    *
  FROM
    ROW_NUMBER_DIST
  WHERE
    rn = 1
),

non_fuel_sales AS (
  SELECT
    scv_id,
    ee_reference_number,
    total_item_sales_amount,
    ROW_NUMBER() OVER(PARTITION BY ee_reference_number) AS rn
  FROM
    `gcp-wow-cd-data-engine-prod.consume_marketing.trn__campaign_sales_reward_detail`
),

non_fuel_sales2 AS (
  SELECT
    scv_id,
    SUM(total_item_sales_amount) AS non_fuel_sales
  FROM
    non_fuel_sales
  WHERE
    rn = 1
  GROUP BY
    1
),

base_cust AS (
  SELECT DISTINCT
    scv_id
  FROM
    `gcp-wow-cd-data-engine-prod.consume_customer.dim__retail_customer`
  WHERE
    scv_id IS NOT NULL
    AND customer_status_description = 'Active'
    AND _is_current = TRUE
    AND is_customer_deleted = FALSE
    AND is_valid_email = TRUE
),

orange_prefs AS (
  SELECT
    scv_id AS scv_id,
    edr_allow_contact_all,
    edr_allow_partner_comms_bp_email
  FROM
    `gcp-wow-cd-data-engine-prod.consume_customer.trn__customer_preference_pivot_curr`
),

bp_contactable AS (
  SELECT DISTINCT
    b.scv_id
  FROM
    base_cust AS b
  INNER JOIN orange_prefs AS p
    ON p.scv_id = b.scv_id
  WHERE
    1 = 1
    AND edr_allow_contact_all = TRUE #Needed for all commerical EDR comms
    AND edr_allow_partner_comms_bp_email = TRUE #Change this to which ever pref required for your audience
),

aux_20_CPL AS (
  SELECT
    scv_id,
    transaction_start_datetime_nzt,
    receipt_id,
    IF(redemption_type = 'cpl discount', total_discount_amount / article_quantity, NULL) AS cpl_discount,
    ROW_NUMBER() OVER(PARTITION BY scv_id ORDER BY DATE(transaction_start_datetime_nzt) ASC) AS rn
  FROM
    `gcp-wow-cd-data-engine-prod.consume_loyalty.trn__fuel_redemption_detail` AS frv
  WHERE
    partner_id = '2011'
    AND DATE(transaction_start_date_nzt) BETWEEN '2024-02-01' AND '2024-02-17'
    AND redemption_type = 'cpl discount'
    --and scv_id = '2dac8178-d414-47df-a6f9-258a153bf471'
),

aux_20_CPL_2 AS (
  SELECT DISTINCT
    scv_id
  FROM
    aux_20_CPL
  WHERE
    rn = 1
),

bp_emails AS (
  SELECT DISTINCT
    email_name,
    job_id
  FROM
    `gcp-wow-cd-data-engine-prod.consume_customer_contact.trn__customer_campaign_communication` AS c
  LEFT JOIN (
    SELECT DISTINCT
      email_name,
      job_id
    FROM
      gcp-wow-food-de-pel-prod.offers.sfmc_edr_send_logs
  ) AS sl
    ON sl.job_id = c.sfmc_send_id
),

bp_unsubs AS (
  SELECT DISTINCT
    scv_id
  FROM
    offers.sfmc_clicks AS c #All the click actions for all emails
  INNER JOIN bp_emails AS b
    ON b.job_id = c.send_id
  WHERE
    CONTAINS_SUBSTR(alias, 'unsubscribe') = TRUE
)

SELECT
  partners_nums.scv_id,
  CASE
    WHEN (partners_nums.BP_CUSTOMER = 1) THEN 'BP customer'
    ELSE 'Non BP customer'
  END AS bp_customer,
  CASE
    WHEN partners_nums.BP_CUSTOMER = 1
      AND Last_2M = 0
      AND fcc.cvm_strategy_segment_current IN ('PROTECT') THEN 'Lapsed BP customer - Loyal WWNZ customer'
    WHEN partners_nums.BP_CUSTOMER = 1
      AND Last_2M = 0
      AND fcc.cvm_strategy_segment_current IN ('GROW') THEN 'Lapsed BP customer - Cross-shopper WWNZ customer'
    WHEN partners_nums.BP_CUSTOMER <> 1
      AND fcc.cvm_strategy_segment_current IN ('PROTECT') THEN 'Non BP customer - Loyal WWNZ customer'
    WHEN partners_nums.BP_CUSTOMER <> 1
      AND fcc.cvm_strategy_segment_current IN ('GROW') THEN 'Non BP customer - Cross-shopper WWNZ customer'
    WHEN (SAFE_DIVIDE(COALESCE(non_fuel_sales2.non_fuel_sales, 0) + COALESCE(sum_fuel_sales_and_litres.total_sales_BP, 0), DATE_DIFF(points.last_trx_date, points.first_trx_date, DAY) / 30) >= 300)
      AND (partners_nums.BP_CUSTOMER <> 1) THEN 'Non BP customer - High EDR volume other segments'
    WHEN (SAFE_DIVIDE(sum_fuel_sales_and_litres.transactions_BP, DATE_DIFF(sum_fuel_sales_and_litres.last_BP_transaction_date, sum_fuel_sales_and_litres.first_BP_transaction_date, DAY) / 30) >= 1
      AND DATE_DIFF(CURRENT_DATE('Pacific/Auckland'), sum_fuel_sales_and_litres.last_BP_transaction_date, DAY) <= 30
      AND sum_fuel_sales_and_litres.transactions_BP >= 10)
      OR (partners_nums.Different_partners >= 4)
      OR (SAFE_DIVIDE(sum_fuel_sales_and_litres.transactions_BP, DATE_DIFF(sum_fuel_sales_and_litres.last_BP_transaction_date, sum_fuel_sales_and_litres.first_BP_transaction_date, DAY) / 30) >= 3
      OR SAFE_DIVIDE(sum_fuel_sales_and_litres.total_sales_BP, DATE_DIFF(sum_fuel_sales_and_litres.last_BP_transaction_date, sum_fuel_sales_and_litres.first_BP_transaction_date, DAY) / 30) >= 200) THEN 'Others - Already engaged BP customer'
    WHEN risk.Segmentacion = 'Risk'
      AND (partners_nums.BP_CUSTOMER = 1) THEN 'Infrequent BP customer - AASF risk segmentation'
    WHEN risk.Segmentacion = 'Risk'
      AND (partners_nums.BP_CUSTOMER <> 1) THEN 'Non BP customer - AASF risk segmentation'
    WHEN SAFE_DIVIDE(sum_fuel_sales_and_litres.transactions_BP, DATE_DIFF(sum_fuel_sales_and_litres.last_BP_transaction_date, sum_fuel_sales_and_litres.first_BP_transaction_date, DAY) / 30) >= 0.5
      OR SAFE_DIVIDE(sum_fuel_sales_and_litres.total_sales_BP, DATE_DIFF(sum_fuel_sales_and_litres.last_BP_transaction_date, sum_fuel_sales_and_litres.first_BP_transaction_date, DAY) / 30) >= 30 THEN 'Others - Infrequent BP customer - Mid BP volume'
    WHEN (SAFE_DIVIDE(points.transactions, DATE_DIFF(points.last_trx_date, points.first_trx_date, DAY) / 30) >= 1
      OR SAFE_DIVIDE(COALESCE(non_fuel_sales2.non_fuel_sales, 0) + COALESCE(sum_fuel_sales_and_litres.total_sales_BP, 0), DATE_DIFF(points.last_trx_date, points.first_trx_date, DAY) / 30) > 100)
      AND (partners_nums.BP_CUSTOMER <> 1) THEN 'Others - Non BP customer - Mid EDR volume'
    WHEN partners_nums.BP_CUSTOMER = 1 THEN 'Others - Infrequent BP customer - Low BP engagement'
    WHEN (partners_nums.BP_CUSTOMER <> 1) THEN 'Others - Non BP customer - Low EDR engagement'
    ELSE 'Others'
  END AS Targeting,
  CASE
    WHEN partners_nums.BP_CUSTOMER = 1
      AND Last_2M = 0
      AND fcc.cvm_strategy_segment_current IN ('PROTECT') THEN 'Re-engage'
    WHEN partners_nums.BP_CUSTOMER = 1
      AND Last_2M = 0
      AND fcc.cvm_strategy_segment_current IN ('GROW') THEN 'Re-engage'
    WHEN partners_nums.BP_CUSTOMER <> 1
      AND fcc.cvm_strategy_segment_current IN ('PROTECT') THEN 'Win'
    WHEN partners_nums.BP_CUSTOMER <> 1
      AND fcc.cvm_strategy_segment_current IN ('GROW') THEN 'Win'
    WHEN (SAFE_DIVIDE(COALESCE(non_fuel_sales2.non_fuel_sales, 0) + COALESCE(sum_fuel_sales_and_litres.total_sales_BP, 0), DATE_DIFF(points.last_trx_date, points.first_trx_date, DAY) / 30) >= 300)
      AND (partners_nums.BP_CUSTOMER <> 1) THEN 'Win'
    WHEN (SAFE_DIVIDE(sum_fuel_sales_and_litres.transactions_BP, DATE_DIFF(sum_fuel_sales_and_litres.last_BP_transaction_date, sum_fuel_sales_and_litres.first_BP_transaction_date, DAY) / 30) >= 1
      AND DATE_DIFF(CURRENT_DATE('Pacific/Auckland'), sum_fuel_sales_and_litres.last_BP_transaction_date, DAY) <= 30
      AND sum_fuel_sales_and_litres.transactions_BP >= 10)
      OR (partners_nums.Different_partners >= 4)
      OR (SAFE_DIVIDE(sum_fuel_sales_and_litres.transactions_BP, DATE_DIFF(sum_fuel_sales_and_litres.last_BP_transaction_date, sum_fuel_sales_and_litres.first_BP_transaction_date, DAY) / 30) >= 3
      OR SAFE_DIVIDE(sum_fuel_sales_and_litres.total_sales_BP, DATE_DIFF(sum_fuel_sales_and_litres.last_BP_transaction_date, sum_fuel_sales_and_litres.first_BP_transaction_date, DAY) / 30) >= 200) THEN 'Protect'
    WHEN risk.Segmentacion = 'Risk'
      AND (partners_nums.BP_CUSTOMER = 1) THEN 'Grow'
    WHEN risk.Segmentacion = 'Risk'
      AND (partners_nums.BP_CUSTOMER <> 1) THEN 'Re-engage'
    WHEN SAFE_DIVIDE(sum_fuel_sales_and_litres.transactions_BP, DATE_DIFF(sum_fuel_sales_and_litres.last_BP_transaction_date, sum_fuel_sales_and_litres.first_BP_transaction_date, DAY) / 30) >= 0.5
      OR SAFE_DIVIDE(sum_fuel_sales_and_litres.total_sales_BP, DATE_DIFF(sum_fuel_sales_and_litres.last_BP_transaction_date, sum_fuel_sales_and_litres.first_BP_transaction_date, DAY) / 30) >= 30 THEN 'Grow'
    WHEN (SAFE_DIVIDE(points.transactions, DATE_DIFF(points.last_trx_date, points.first_trx_date, DAY) / 30) >= 1
      OR SAFE_DIVIDE(COALESCE(non_fuel_sales2.non_fuel_sales, 0) + COALESCE(sum_fuel_sales_and_litres.total_sales_BP, 0), DATE_DIFF(points.last_trx_date, points.first_trx_date, DAY) / 30) > 100)
      AND (partners_nums.BP_CUSTOMER <> 1) THEN 'Win'
    WHEN partners_nums.BP_CUSTOMER = 1 THEN 'Grow'
    WHEN (partners_nums.BP_CUSTOMER <> 1) THEN 'Win'
    ELSE 'Others'
  END AS Targeting_type,
  CASE
    WHEN bp_contactable.scv_id IS NULL THEN 'Non contactable for BP'
    ELSE 'Contactable for BP'
  END AS BP_CONTACTABLE,
  CASE
    WHEN bp_unsubs.scv_id IS NULL THEN 0
    ELSE 1
  END AS BP_UNSUBSCRIBED,
  DATETIME(CURRENT_TIMESTAMP(), 'Pacific/Auckland') AS model_run_date_time_NZT
FROM
  partners_nums
LEFT JOIN sum_fuel_sales_and_litres
  ON sum_fuel_sales_and_litres.scv_id = partners_nums.scv_id
LEFT JOIN points
  ON points.scv_id = partners_nums.scv_id
LEFT JOIN BP_POINTS
  ON BP_POINTS.scv_id = partners_nums.scv_id
LEFT JOIN `gcp-wow-cd-data-engine-prod.consume_feature_library.features_customer_current` AS fcc
  ON partners_nums.scv_id = fcc.scv_id
LEFT JOIN `gcp-wow-food-de-pel-prod.adhoc.FUEL_COHORTS_FINAL` AS risk
  ON risk.single_customer_view_id = partners_nums.scv_id
LEFT JOIN CLOSEST_BP
  ON CLOSEST_BP.store_id = (CASE
    WHEN fcc.customer_preferred_store_id = '9128'
    AND customer_main_store_id_no_default IS NULL THEN NULL
    ELSE fcc.customer_preferred_store_id
  END)
LEFT JOIN non_fuel_sales2
  ON non_fuel_sales2.scv_id = partners_nums.scv_id
LEFT JOIN bp_contactable
  ON bp_contactable.scv_id = partners_nums.scv_id
LEFT JOIN aux_20_CPL_2
  ON aux_20_CPL_2.scv_id = partners_nums.scv_id
LEFT JOIN bp_unsubs
  ON bp_unsubs.scv_id = partners_nums.scv_id
--where partners_nums.scv_id  = '2dac8178-d414-47df-a6f9-258a153bf471'
--where (partners_nums.BP_CUSTOMER = 1 OR sum_fuel_sales_and_litres.total_fuel_litres_BP > 0)
