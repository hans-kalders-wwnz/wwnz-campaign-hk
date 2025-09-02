-- This is the final, production-ready master query for brand-level BYB campaign reporting.
-- It automatically identifies all BYB campaigns and calculates a comprehensive set of metrics for each brand, for each campaign.


-- Dynamically identifies all BYB campaigns from the offer master table.
WITH byb_campaigns AS (
  SELECT
  offer_id,
  offer_name,
  "BYB_" || (DENSE_RANK() OVER (ORDER BY offer_start_date_nzt ASC)) as byb_name
  FROM`gcp-wow-cd-data-engine-prod.consume_marketing.dim__offer_header`
  WHERE _is_current = TRUE
  AND campaign_code = "CTG-0020" -- Dedicated code for BYB campaigns.
  AND LOWER(offer_status_description) IN ('active', 'expired')
  AND LOWER(offer_name) NOT LIKE "%test%"
  )

-- Remove the BYB 1 campaigns
, byb_campaigns_cleaned as (
  SELECT
  offer_id,
  offer_name,
  byb_name
  FROM byb_campaigns
WHERE offer_id NOT IN ("90066239","90066219")
  )


  -- Determines the precise start and end dates for each campaign from allocation data.
, coupon_dates AS (
  SELECT
  b.offer_id,
  b.offer_name,
  b.byb_name,
  MIN(DATE(a.offer_allocation_start_datetime_nzt)) AS campaign_start_date,
  MAX(DATE(a.offer_allocation_end_datetime_nzt)) AS campaign_end_date
  FROM `gcp-wow-cd-data-engine-prod.consume_marketing.trn__campaign_customer_offer_allocation` a
  INNER JOIN byb_campaigns_cleaned b ON a.offer_id = b.offer_id
  WHERE UPPER(allocation_status) = 'ACTIVE'
  GROUP BY ALL
  )

  -- Creates the final config table with all dynamic date ranges for each campaign.
, campaign_config AS (
  SELECT
  offer_id,
  offer_name,
  byb_name,
  campaign_start_date,
  campaign_end_date,
  DATE_DIFF(campaign_end_date, campaign_start_date, WEEK) AS campaign_duration_weeks,
  DATE_SUB(campaign_start_date, INTERVAL DATE_DIFF(campaign_end_date, campaign_start_date, WEEK) WEEK) AS pre_campaign_start_date,
  DATE_SUB(campaign_start_date, INTERVAL 1 DAY) AS pre_campaign_end_date,
  DATE_ADD(campaign_end_date, INTERVAL 1 DAY) AS post_campaign_start_date,
  DATE_ADD(campaign_end_date, INTERVAL DATE_DIFF(campaign_end_date, campaign_start_date, WEEK) WEEK) AS post_campaign_end_date
  FROM coupon_dates
  )

, dates_offer as (
  -- Join campaign_config with the dim__date table to get all dates within the campaign periods
  SELECT
    d.financial_week_end_date,
    d.financial_week_start_date,
    d.date,
    d.date_key,
    d.pel_week,
    -- Use the dynamic dates from campaign_config in the CASE statement
    CASE
      WHEN d.date BETWEEN cc.pre_campaign_start_date AND cc.pre_campaign_end_date THEN 'Pre Campaign'
      WHEN d.date BETWEEN cc.campaign_start_date AND cc.campaign_end_date THEN 'Campaign Period'
      WHEN d.date BETWEEN cc.post_campaign_start_date AND cc.post_campaign_end_date THEN 'Post Campaign'
    END AS campaign_period,
    cc.offer_id
  FROM campaign_config AS cc
  LEFT JOIN `gcp-wow-cd-data-engine-prod.consume_common.dim__date` AS d ON d.date BETWEEN cc.pre_campaign_start_date AND cc.post_campaign_end_date
)

,byb_articles as (
  SELECT distinct offer_id,
                  brand,
                  article_id
  FROM `gcp-wow-cd-bse-prod.campaign_excellence_operational_tables.BYB_brand_article_master_table`
)

-- Get one big grab of data from srl for use in many later steps.
, big_srl_snip as (
  SELECT
    srl.receipt_id,
    srl.commercial_id,
    srl.article_id,
    SUM(srl.gst_included_amount) as gst_included_amount,
    MAX(srl.receipt_date) as receipt_date,
    MAX(IFNULL(srl.single_customer_view_id,"")) as scv_id
  FROM `gcp-wow-cd-data-engine-prod.consume_sales.obt__sales_receipt_line` srl
  WHERE
    srl.receipt_date BETWEEN (SELECT MIN(pre_campaign_start_date) FROM campaign_config) AND (SELECT MAX(post_campaign_end_date) FROM campaign_config)
  GROUP BY 1, 2, 3
)

,srl_joined_brand_dates as (
  SELECT
    srl.receipt_date,
    srl.scv_id,
    srl.gst_included_amount,
    d.financial_week_start_date,
    d.campaign_period,
    d.pel_week,
    d.offer_id,
    b.brand,
    IF(srl.commercial_id='',srl.receipt_id,srl.commercial_id) as unique_txns,
    CASE WHEN srl.scv_id <>"" THEN IF(srl.commercial_id='',srl.receipt_id,srl.commercial_id) END as swiped_txns,
    CASE WHEN srl.scv_id <>"" THEN srl.gst_included_amount ELSE 0 END as swiped_sales
  FROM big_srl_snip srl
  INNER JOIN dates_offer d ON srl.receipt_date = d.date
  INNER JOIN byb_articles b ON b.article_id = srl.article_id AND b.offer_id = d.offer_id
  WHERE d.campaign_period IS NOT NULL
)

,scan_tag_rates as (
SELECT
  offer_id,
  financial_week_start_date,
  pel_week,
  campaign_period,
  brand,
  SAFE_DIVIDE(COUNT(DISTINCT swiped_txns),COUNT(DISTINCT unique_txns)) as scan_rate,
  SAFE_DIVIDE(SUM(swiped_sales),SUM(gst_included_amount)) as tag_rate,
FROM srl_joined_brand_dates srl
GROUP BY
  offer_id,
  financial_week_start_date,
  pel_week,
  campaign_period,
  brand
)

SELECT
  offer_id,
  financial_week_start_date,
  pel_week,
  campaign_period,
  brand,
  scan_rate,
  tag_rate,
  AVG(scan_rate) OVER (PARTITION BY brand, campaign_period) as avg_scan_rate,
  AVG(tag_rate) OVER (PARTITION BY brand, campaign_period) as avg_tag_rate
FROM
  scan_tag_rates
ORDER BY
  financial_week_start_date ASC, brand ASC, offer_id ASC