CREATE OR REPLACE TABLE gcp-wow-cd-email-app-test.campaign_excellence.byb2_scan_tag_rates as (

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
,byb_brand_article AS (
SELECT
CAST(article AS STRING) AS article_id,
article_description,
brand
FROM
`gcp-wow-food-de-pel-prod.offers_working.byb_fy2025_q4_pca_table_brand`
)

#Scan and Tag Rate Section - Don't need the brand summary info
#Start
,big_srl_snip as (
  SELECT receipt_id,
  commercial_id,
  article_id,
  sum(gst_included_amount) as gst_included_amount,
  max(receipt_date) as receipt_date,
  max(ifnull(single_customer_view_id,"")) as scv_id
  FROM datamart.sales_receipt_line
  WHERE financial_week_start_date >= "2025-03-03"
  GROUP BY receipt_id,
  commercial_id,
  article_id
)

,srl_joined_brand_dates as (
  SELECT srl.*,
  financial_week_start_date,
  campaign_period,
  pel_week,
  brand,
  if(commercial_id='',receipt_id,commercial_id) as unique_txns,
  case when scv_id <>"" then if(commercial_id='',receipt_id,commercial_id) end as swiped_txns,
  case when scv_id <>"" then gst_included_amount else 0 end as swiped_sales,
  FROM big_srl_snip srl
  INNER JOIN dates d ON srl.receipt_date = d.date
  INNER JOIN byb_brand_article b ON b.article_id = srl.article_id
)

,scan_tag_rates as (
SELECT financial_week_start_date,
pel_week,
campaign_period,
brand,
safe_divide(count(distinct swiped_txns),count(distinct unique_txns)) as scan_rate,
safe_divide(sum(swiped_sales),sum(gst_included_amount)) as tag_rate,

FROM srl_joined_brand_dates srl
GROUP BY financial_week_start_date, pel_week, campaign_period, brand
ORDER BY financial_week_start_date ASC, brand ASC
)

SELECT *,
avg(scan_rate) OVER (PARTITION BY brand, campaign_period) as avg_scan_rate,
avg(tag_rate) OVER (PARTITION BY brand, campaign_period) as avg_tag_rate,
FROM scan_tag_rates
)


select * from gcp-wow-cd-email-app-test.campaign_excellence.byb2_scan_tag_rates


