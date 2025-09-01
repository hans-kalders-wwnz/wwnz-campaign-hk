WITH
byb_campaigns AS (
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
WHERE offer_id NOT IN ("90066239","90066219") #Removing BYB1 for two reasons: 1) This campaign used a separate clone campaigns, so results are split over two ids. 2) Redemption figures are wrong for online orders due to data issue which was fixed before BYB
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
  DATE_SUB(campaign_start_date, INTERVAL (DATE_DIFF(campaign_end_date, campaign_start_date, WEEK)) WEEK) AS pre_campaign_start_date,
  DATE_SUB(campaign_start_date, INTERVAL 1 DAY) AS pre_campaign_end_date,
  DATE_ADD(campaign_end_date, INTERVAL 1 DAY) AS post_campaign_start_date,
  DATE_ADD(campaign_end_date, INTERVAL (DATE_DIFF(campaign_end_date, campaign_start_date, WEEK)) WEEK) AS post_campaign_end_date,
  DATE_SUB(campaign_start_date, INTERVAL 26 WEEK) AS pre_campaign_26w_start_date,
  DATE_SUB(campaign_start_date, INTERVAL 52 WEEK) AS pre_campaign_52w_start_date
  FROM coupon_dates
  ) -- select * from campaign_config

, dates_offer as (
  -- Join campaign_config with the dim__date table to get all dates within the campaign periods
  SELECT
    d.financial_week_end_date,
    d.financial_week_start_date, 
    d.date,
    d.date_key,
    d.month_short_description,
    d.pel_week,
    -- Use the dynamic dates from campaign_config in the CASE statement
    CASE
      WHEN d.date BETWEEN cc.pre_campaign_start_date AND cc.pre_campaign_end_date THEN 'Pre Campaign'
      WHEN d.date BETWEEN cc.campaign_start_date AND cc.campaign_end_date THEN 'Campaign Period'
      WHEN d.date BETWEEN cc.post_campaign_start_date AND cc.post_campaign_end_date THEN 'Post Campaign'
    END AS campaign_period,
    cc.offer_id,
    cc.offer_name,
    cc.byb_name,
    cc.campaign_start_date,
    cc.campaign_end_date,
    cc.campaign_duration_weeks,
    cc.pre_campaign_start_date,
    cc.pre_campaign_end_date,
    cc.post_campaign_start_date,
    cc.post_campaign_end_date,
    cc.pre_campaign_26w_start_date,
    cc.pre_campaign_52w_start_date
  FROM campaign_config AS cc
  CROSS JOIN `gcp-wow-cd-data-engine-prod.consume_common.dim__date` AS d
  WHERE d.date BETWEEN cc.pre_campaign_52w_start_date AND cc.post_campaign_end_date -- Filter to a relevant date range
) -- select * from dates_offer

,byb_articles as (
  SELECT distinct offer_id,
                  brand,
                  article_id
  FROM `gcp-wow-cd-bse-prod.campaign_excellence_operational_tables.BYB_brand_article_master_table`
)
,

cte_article_offer_day AS (
SELECT
      a.offer_id,
      a.brand,
      a.article_id,
      dof.date,
      dof.pel_week,
      dof.campaign_period,
      dof.byb_name,
FROM byb_articles a
INNER JOIN dates_offer dof
ON a.offer_id = dof.offer_id

WHERE campaign_period IS NOT NULL

)

,sr_snip as (
  SELECT 
  aod.campaign_period,
  aod.offer_id,
  receipt_date,
  single_customer_view_id as scv_id,
  case when is_online_order then commercial_id else receipt_id end as unique_txn_id,
  gst_included_amount,
  quantity,
FROM dates_offer aod
  INNER JOIN `gcp-wow-cd-data-engine-prod.consume_sales.obt__sales_receipt_header` srh
    ON aod.date=srh.receipt_date
WHERE 1=1
AND campaign_period IS NOT NULL -- Only relevant campaign periods
AND single_customer_view_id <> "" AND single_customer_view_id IS NOT NULL
) 

,offer_id_redeemers AS (
  SELECT distinct scv_id,offer_id
  FROM `gcp-wow-cd-data-engine-prod.consume_marketing.trn__campaign_sales_reward_detail` 
  WHERE offer_id IN (SELECT  offer_id FROM byb_articles)
)

,baskets as (
  SELECT scv_id,
  unique_txn_id,
  campaign_period,
  offer_id,
  max(receipt_date) as receipt_date,
  sum(gst_included_amount) as gst_included_amount,
  sum(quantity) as quantity
  FROM sr_snip
  GROUP BY scv_id, unique_txn_id,campaign_period,offer_id
)


,basket_dates as (
  SELECT 
  d.pel_week,
  b.scv_id,
  b.unique_txn_id,
  b.campaign_period,
  b.offer_id,
  b.receipt_date,
  b.gst_included_amount,
  b.quantity,
  case when r.scv_id IS NULL then "non_redeemer" when r.scv_id IS NOT NULL then "redeemer" end as byb_redeemer 
  FROM baskets b
  LEFT JOIN dates_offer d ON d.date = b. receipt_date
  LEFT JOIN offer_id_redeemers r ON r.scv_id = b.scv_id AND b.offer_id=r.offer_id

)

SELECT campaign_period,
byb_redeemer,
offer_id,
count(distinct scv_id) as unique_customers,
safe_divide(count(distinct unique_txn_id), count(distinct pel_week)) as baskets_per_week,
safe_divide(sum(gst_included_amount), count(distinct unique_txn_id)) as avg_basket_size,
safe_divide(sum(quantity), count(distinct unique_txn_id)) as avg_basket_qty
FROM basket_dates 
GROUP BY 
campaign_period,
byb_redeemer,
offer_id

ORDER BY offer_id DESC,campaign_period