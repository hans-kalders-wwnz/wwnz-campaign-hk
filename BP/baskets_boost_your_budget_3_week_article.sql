

WITH
-- ===============================================
-- DYNAMIC CAMPAIGN & DATE CONFIGURATION
-- ===============================================
byb_campaigns AS (
SELECT
  offer_id,
  offer_name,
  "BYB_" || (DENSE_RANK() OVER (ORDER BY offer_start_date_nzt ASC)) as byb_name
FROM `gcp-wow-cd-data-engine-prod.consume_marketing.dim__offer_header`
WHERE _is_current = TRUE
AND campaign_code = "CTG-0020"
AND LOWER(offer_status_description) IN ('active', 'expired')
AND LOWER(offer_name) NOT LIKE "%test%"
),

byb_campaigns_cleaned AS (
SELECT
offer_id,
offer_name,
byb_name
FROM byb_campaigns
WHERE offer_id NOT IN ("90066239","90066219")
),

coupon_dates AS (
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
),

campaign_config AS (
SELECT
offer_id,
offer_name,
byb_name,
campaign_start_date,
campaign_end_date,
DATE_DIFF(campaign_end_date, campaign_start_date, WEEK) AS campaign_duration_weeks,
DATE_SUB(campaign_start_date, INTERVAL 4 WEEK) AS pre_campaign_start_date,
DATE_SUB(campaign_start_date, INTERVAL 1 DAY) AS pre_campaign_end_date,
DATE_ADD(campaign_end_date, INTERVAL 1 DAY) AS post_campaign_start_date,
DATE_ADD(campaign_end_date, INTERVAL 4 WEEK) AS post_campaign_end_date
FROM coupon_dates
),

dates_offer AS (
SELECT
d.financial_week_end_date,
d.financial_week_start_date,
d.date,
d.date_key,
d.month_short_description,
d.pel_week,
CASE
WHEN d.date BETWEEN cc.pre_campaign_start_date AND cc.pre_campaign_end_date THEN 'Pre Campaign'
WHEN d.date BETWEEN cc.campaign_start_date AND cc.campaign_end_date THEN 'Campaign Period'
WHEN d.date BETWEEN cc.post_campaign_start_date AND cc.post_campaign_end_date THEN 'Post Campaign'
END AS campaign_period,
cc.offer_id,
cc.offer_name,
cc.byb_name
FROM campaign_config AS cc
CROSS JOIN `gcp-wow-cd-data-engine-prod.consume_common.dim__date` AS d
WHERE d.date BETWEEN cc.pre_campaign_start_date AND cc.post_campaign_end_date
),

byb_articles AS (
SELECT
CAST(article_id AS STRING) AS article_id,
article_description,
brand,
offer_id
FROM `gcp-wow-cd-bse-prod.campaign_excellence_operational_tables.BYB_brand_article_master_table`
WHERE offer_id IN (SELECT offer_id FROM byb_campaigns_cleaned)
),

-- ===============================================
-- SALES & REDEMPTION DATA SNIPPETS WITH CHANNEL
-- ===============================================
sr_snip_swipes AS (
SELECT
  receipt_id,
  is_online_order,
  single_customer_view_id,
  CASE WHEN single_customer_view_id <> '' AND single_customer_view_id IS NOT NULL THEN 1 ELSE 0 END AS is_swiped
FROM
  `gcp-wow-cd-data-engine-prod.consume_sales.obt__sales_receipt_header`
WHERE
  receipt_date IN (SELECT date FROM dates_offer WHERE campaign_period IS NOT NULL)
),

srl_snip_unit_price AS (
SELECT
srl.receipt_id,
srl.article_id,
srl.article_description,
srl.receipt_date,
ifnull(srl.single_customer_view_id,"") AS scv_id,
CASE
WHEN srh.is_online_order = TRUE THEN 'Online'
ELSE 'In Store'
END AS channel,
srh.is_swiped,
sum(srl.gst_included_amount) AS sales,
sum(srl.quantity) AS units,
avg(srl.unit_price_paid) AS unit_price_paid
FROM
`gcp-wow-cd-data-engine-prod.consume_sales.obt__sales_receipt_line` AS srl
INNER JOIN
sr_snip_swipes AS srh ON srl.receipt_id = srh.receipt_id AND srl.single_customer_view_id = srh.single_customer_view_id
WHERE
srl.receipt_date IN (SELECT date FROM dates_offer WHERE campaign_period IS NOT NULL)
GROUP BY
srl.receipt_id,
srl.article_id,
srl.article_description,
srl.receipt_date,
scv_id,
channel,
is_swiped
),

offer_redemptions AS (
SELECT
  scv_id,
  receipt_id,
  article_id,
  offer_id,
  SUM(quantity) AS redemption_quantity,
  SUM(bonus_points_earn) AS bonus_points_earn
FROM
  `gcp-wow-cd-data-engine-prod.consume_marketing.trn__campaign_sales_reward_detail`
WHERE
  offer_id IN (SELECT offer_id FROM byb_campaigns_cleaned)
GROUP BY 1, 2, 3, 4
),

-- ===============================================
-- MAIN LOGIC: JOINING AND CALCULATING
-- ===============================================
cte_agg AS (
SELECT
  d.financial_week_start_date,
  d.campaign_period,
  d.pel_week,
  d.financial_week_end_date,
  d.offer_id,
  d.byb_name,
  b.brand,
  s.article_id,
  s.article_description,
  s.channel,
  s.sales,
  s.units,
  s.receipt_id,
  s.is_swiped,
  o.redemption_quantity,
  o.bonus_points_earn,
  case when o.scv_id IS NOT NULL then s.units else 0 end as boosted_units,
  case when o.scv_id IS NOT NULL then s.sales else 0 end as boosted_sales,
  s.unit_price_paid
FROM srl_snip_unit_price s
INNER JOIN dates_offer d ON s.receipt_date = d.date
INNER JOIN byb_articles b ON b.article_id = s.article_id AND b.offer_id = d.offer_id
LEFT JOIN offer_redemptions o ON s.scv_id = o.scv_id AND s.receipt_id = o.receipt_id AND s.article_id = o.article_id AND d.offer_id = o.offer_id
WHERE d.campaign_period IS NOT NULL
)

-- ===============================================
-- FINAL AGGREGATION & OUTPUT
-- ===============================================
SELECT
  byb_name,
  offer_id,
  campaign_period,
  pel_week,
  financial_week_end_date,
  financial_week_start_date,
  channel,
  article_id,
  article_description,
  brand,
  count(distinct 
    case 
      when campaign_period='Pre Campaign' then pel_week
      when campaign_period='Campaign Period' then pel_week 
      when campaign_period='Post Campaign' then pel_week
    end) cnt_weeks_campaign_period,
  sum(boosted_units) AS boosted_units,
  sum(boosted_sales) AS boosted_sales,
  sum(sales) AS total_sales,
  sum(units) AS total_units,
  sum(redemption_quantity) AS total_redemptions,
  sum(bonus_points_earn) * 0.0091 AS points_cost,
  avg(unit_price_paid) AS avg_unit_price_paid,
  -- Raw values for dashboard calculation
  count(distinct receipt_id) as total_transactions,
  sum(is_swiped) as swiped_transactions,
  sum(case when is_swiped = 1 then sales else 0 end) as swiped_sales
 -- Tag Rate - Swiped Sales / Sales
 -- Scan Rate - Swpied Txn / Total Txn
FROM cte_agg a
    
GROUP BY ALL
ORDER BY byb_name, pel_week, channel, brand, article_id