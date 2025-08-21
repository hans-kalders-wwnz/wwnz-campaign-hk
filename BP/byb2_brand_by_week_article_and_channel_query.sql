CREATE OR REPLACE TABLE gcp-wow-cd-email-app-test.campaign_excellence.byb2_brand_by_week_article_and_channel_query as (
-- Brand  by week, article and channel query

#Get the dates and weeks of the BYB campaign
with dates as (
SELECT distinct financial_week_end_date,
financial_week_start_date,
date,
date_key,
month_short_description,
pel_week,
CASE
WHEN date BETWEEN '2025-03-03' AND '2025-03-30' THEN 'Pre Campaign'
WHEN date BETWEEN "2025-03-31" AND '2025-04-27' THEN 'Campaign Period'
WHEN date BETWEEN '2025-04-28' AND '2025-05-25' THEN 'Post Campaign'
END AS campaign_period
FROM `gcp-wow-cd-data-engine-prod.consume_common.dim__date`
WHERE date BETWEEN '2025-03-03' AND '2025-05-25'
)

#Brand logic held in separate query: https://console.cloud.google.com/bigquery?ws=!1m7!1m6!12m5!1m3!1sgcp-wow-food-de-pel-prod!2sus-central1!3s22ba4556-d1e3-45ab-ba3b-31e09cbb7e3e!2e1
,byb_brand_article as (
SELECT
CAST(article AS STRING) AS article_id,
article_description,
brand
FROM
`gcp-wow-food-de-pel-prod.offers_working.byb_fy2025_q4_pca_table_brand`
)

#Take a snip of sales_receipt to speed up later joins
,sr_snip as (
SELECT single_customer_view_id,
receipt_id,
commercial_id,
is_online_order
FROM datamart.sales_receipt
WHERE financial_week_start_date BETWEEN "2025-03-31" AND "2025-05-25" #during campaign and following period
AND single_customer_view_id <> "" AND single_customer_view_id IS NOT NULL
)

#Take a snip of sales_receipt_line to speed up later joins
,srl_snip as (
SELECT single_customer_view_id as scv_id,
receipt_id,
article_id,
sum(gst_included_amount) as gst_included_amount
FROM `datamart.sales_receipt_line`
WHERE financial_week_start_date BETWEEN "2025-03-31" AND "2025-05-25" #during campaign and following period
AND single_customer_view_id <> "" AND single_customer_view_id IS NOT NULL
GROUP BY scv_id,
receipt_id,
article_id
)


#unique redemptions by date
,offer_redemptions AS (
SELECT
scv_id,
transaction_start_date_nzt AS redemption_date,
pel_week,
a.receipt_id,
CASE
WHEN is_online_order = TRUE THEN 'Online'
ELSE 'In Store'
END AS channel,
a.article_id,
b.brand AS brand,
b.article_description,
a.quantity as redemptions,
a.store_id,
total_item_sales_amount AS basket_redemption_total,
sum(base_points_earn) as base_points_earn,
sum(bonus_points_earn) as bonus_points_earn,
sum(reward_value) as reward_value

FROM
`gcp-wow-cd-data-engine-prod.consume_marketing.trn__campaign_sales_reward_detail` AS a
LEFT JOIN
byb_brand_article AS b
ON a.article_id = b.article_id
LEFT JOIN
sr_snip AS c
ON a.scv_id = c.single_customer_view_id
AND a.receipt_id = c.receipt_id
LEFT JOIN
dates AS dt
ON a.transaction_start_date_nzt = dt.date
WHERE
offer_id IN ('90078999') -- prod BYB 2 offer
GROUP BY scv_id,
redemption_date,
pel_week,
receipt_id,
channel,
article_id,
brand,
article_description,
redemptions,
store_id,
basket_redemption_total
)


##Brand by week, article and channel
#Start

SELECT brand,
article_description,
pel_week,
channel,
sum(redemptions) as redemptions,
sum(gst_included_amount) as gst_included_amount,
sum(bonus_points_earn)*0.0091 as points_cost
FROM offer_redemptions o
LEFT JOIN srl_snip s ON s.scv_id = o.scv_id AND s.receipt_id = o.receipt_id AND s.article_id = o.article_id
GROUP BY
brand,
article_description,
pel_week,
channel

#End
)