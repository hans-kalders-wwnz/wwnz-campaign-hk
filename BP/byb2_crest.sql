CREATE OR REPLACE TABLE gcp-wow-cd-email-app-test.campaign_excellence.byb2_crest as (
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
CAST(article AS STRING) AS article,
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
b.brand,
b.article article_description,
a.quantity as redemptions,
a.store_id,
total_item_sales_amount AS basket_redemption_total,
sum(base_points_earn) as base_points_earn,
sum(bonus_points_earn) as bonus_points_earn,
sum(reward_value) as reward_value

FROM
`gcp-wow-cd-data-engine-prod.consume_marketing.trn__campaign_sales_reward_detail` AS a
LEFT JOIN
byb_brand_article b
ON a.article_id = b.article
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



##Brand Summary
#Start

#All the customers who shopped in the last year
,existing_shoppers_12m as (
SELECT distinct single_customer_view_id as scv_id,
article_id
FROM datamart.sales_receipt_line
WHERE financial_week_start_date BETWEEN DATE_SUB("2025-03-31", INTERVAL 52 WEEK) AND "2025-03-30" #1 year prior to the campaign start
AND single_customer_view_id <> "" AND single_customer_view_id IS NOT NULL
AND gst_included_amount >0
)

#All the customers who shopped in the last 6 months
,existing_shoppers_6m as (
SELECT distinct single_customer_view_id as scv_id,
article_id
FROM datamart.sales_receipt_line
WHERE financial_week_start_date BETWEEN DATE_SUB("2025-03-31", INTERVAL 26 WEEK) AND "2025-03-30" #6 months prior to the campaign start
AND single_customer_view_id <> "" AND single_customer_view_id IS NOT NULL
AND gst_included_amount >0
)


#All the customers who shopped in the target BRANDs in the last year
,existing_brand_shoppers_12m as (
SELECT distinct scv_id,
b.brand
FROM existing_shoppers_12m e
INNER JOIN `gcp-wow-food-de-pel-prod.offers_working.byb_fy2025_q4_pca_table_brand` b ON CAST(b.article AS STRING)  = e.article_id
)

#All the customers who shopped in the target BRANDs in the last 6 month
,existing_brand_shoppers_6m as (
SELECT distinct scv_id,
b.brand
FROM existing_shoppers_6m e
INNER JOIN `gcp-wow-food-de-pel-prod.offers_working.byb_fy2025_q4_pca_table_brand` b ON CAST(b.article AS STRING)  = e.article_id
)

#Customers who redeemed on this brand, who have not shopped that brand in the last year
,new_to_brand_raw as (
SELECT distinct o.brand,
o.scv_id
FROM offer_redemptions o
LEFT JOIN existing_brand_shoppers_12m e12 ON o.scv_id = e12.scv_id AND o.brand = e12.brand
WHERE e12.brand IS NULL AND e12.scv_id IS NULL # Not in the existing 12 m group

)

#Count of new to brand shoppers
,new_to_brand_shoppers as (
SELECT brand,
count(distinct scv_id) as new_to_brand_redeemers
FROM new_to_brand_raw
GROUP BY brand
)

#Customers who redeemed on this brand, who have shopped the brand in the last year, but not in the last 6 months
,reactivated_raw as (
SELECT distinct o.brand,
o.scv_id
FROM offer_redemptions o
LEFT JOIN existing_brand_shoppers_12m e12 ON o.scv_id = e12.scv_id AND o.brand = e12.brand
LEFT JOIN existing_brand_shoppers_6m e6 ON o.scv_id = e6.scv_id AND o.brand = e6.brand
WHERE e6.brand IS NULL AND e6.scv_id IS NULL # Not in the existing 6m group AND
AND e12.brand IS NOT NULL AND e12.scv_id IS NOT NULL #Is in the existing 12m group
)


#Count of reactivated shoppers
,reactivated_shoppers as (
SELECT brand,
count(distinct scv_id) as reactivated_redeemers
FROM reactivated_raw
GROUP BY brand
)

#Customers who redeemed on this brand, who have shopped the brand in the last6 months
,repeat_raw as (
SELECT distinct o.brand,
o.scv_id
FROM offer_redemptions o
LEFT JOIN existing_brand_shoppers_6m e6 ON o.scv_id = e6.scv_id AND o.brand = e6.brand
WHERE e6.brand IS NOT NULL AND e6.scv_id IS NOT NULL# Is in the existing 6m group
)


#Count of repeat shoppers
,repeat_shoppers as (
SELECT brand,
count(distinct scv_id) as repeat_redeemers
FROM repeat_raw
GROUP BY brand
)


#Customers who shopped each brand during the campaign (may or may not have redeemed the offer)
,srl_snip_campaign as (
SELECT distinct single_customer_view_id as scv_id,
article_id
FROM datamart.sales_receipt_line
WHERE financial_week_start_date BETWEEN "2025-03-31" AND '2025-04-27'
AND single_customer_view_id <> "" AND single_customer_view_id IS NOT NULL
AND gst_included_amount >0
)

,campaign_shoppers as (
SELECT distinct brand,
scv_id
FROM srl_snip_campaign a
INNER JOIN `gcp-wow-food-de-pel-prod.offers_working.byb_fy2025_q4_pca_table_brand` b ON CAST(b.article AS STRING)  = a.article_id
)

#Count of customers who shopped this brand
,shopped_brand as (
SELECT brand,
count(distinct scv_id) as brand_shoppers
FROM campaign_shoppers
GROUP BY brand
)


#Find those who shopped but didn't boost!
#Customers who actviated the offer
,activated AS (
SELECT DISTINCT scv_id
FROM `gcp-wow-cd-data-engine-prod.consume_marketing.trn__customer_activated_offer`
WHERE offer_id IN ('90078999') -- prod BYB 2 offer
)

#Customers who shopped that brand, but never activated (and therefore will not have redeemed for this purchase)
,did_not_boost as (
SELECT brand,
count(distinct c.scv_id) as non_boosters
FROM campaign_shoppers c
LEFT JOIN activated a ON a.scv_id = c.scv_id
WHERE a.scv_id IS NULL #Did not activate
GROUP BY brand
)



#Retained shoppers in each group

,post_campaign_shoppers as (
SELECT distinct single_customer_view_id as scv_id,
article_id
FROM datamart.sales_receipt_line
WHERE financial_week_start_date BETWEEN '2025-04-28' AND '2025-05-25' #4 weeks following the campaign
AND single_customer_view_id <> "" AND single_customer_view_id IS NOT NULL
AND gst_included_amount >0
)


#All the customers who shopped in the target BRANDs in the last year
,post_campaign_brand_shoppers as (
SELECT distinct scv_id,
b.brand
FROM post_campaign_shoppers p
INNER JOIN `gcp-wow-food-de-pel-prod.offers_working.byb_fy2025_q4_pca_table_brand` b ON CAST(b.article AS STRING)  = p.article_id
)


#Repeat & retained

,retained_repeat as (
SELECT p.brand,
count(distinct p.scv_id) as retained_repeat_shoppers
FROM post_campaign_brand_shoppers p
INNER JOIN repeat_raw r ON r.scv_id = p.scv_id AND r.brand = p.brand
GROUP BY p.brand
)

,retained_reactivated as (
SELECT p.brand,
count(distinct p.scv_id) as retained_reactivated_shoppers
FROM post_campaign_brand_shoppers p
INNER JOIN reactivated_raw r ON r.scv_id = p.scv_id AND r.brand = p.brand
GROUP BY p.brand
)


,retained_new_to_brand as (
SELECT p.brand,
count(distinct p.scv_id) as retained_new_to_brand_shoppers
FROM post_campaign_brand_shoppers p
INNER JOIN new_to_brand_raw r ON r.scv_id = p.scv_id AND r.brand = p.brand
GROUP BY p.brand
)


#Summarise the key values for this brand
,brand as (
SELECT brand,
sum(redemptions) as redemptions,
count(distinct o.scv_id) as unique_redeemers,
sum(gst_included_amount) as boosted_sales,
sum(bonus_points_earn)*0.0091 as points_cost
FROM offer_redemptions o
LEFT JOIN srl_snip s ON s.scv_id = o.scv_id AND s.receipt_id = o.receipt_id AND s.article_id = o.article_id
GROUP BY
brand
)



#CREST Section
#Start - Need to use all the same code as for Brand Summary, except the change the final CTE


,crest_groups as (
SELECT customer_crest_segment_group as crest,
scv_id
FROM `gcp-wow-cd-data-engine-prod.consume_feature_library.features_customer_current`
WHERE customer_crest_segment_group IS NOT NULL
)

,repeat_crest as (
SELECT a.brand,
crest,
count(a.scv_id) as repeat_crest_customers
FROM repeat_raw a
LEFT JOIN crest_groups c ON a.scv_id = c.scv_id
GROUP BY brand,crest
)

,reactivated_crest as (
SELECT a.brand,
crest,
count(a.scv_id) as reactivated_crest_customers
FROM reactivated_raw a
LEFT JOIN crest_groups c ON a.scv_id = c.scv_id
GROUP BY brand,crest
)

,new_to_brand_crest as (
SELECT a.brand,
crest,
count(a.scv_id) as new_to_brand_crest_customers
FROM new_to_brand_raw a
LEFT JOIN crest_groups c ON a.scv_id = c.scv_id
GROUP BY brand,crest
)

,total_crest as (
SELECT distinct crest
FROM crest_groups
)

,total_brands as (
SELECT distinct brand
FROM offer_redemptions
)

,crest_brand_base as (
SELECT brand,
crest
FROM total_brands b
LEFT JOIN total_crest c ON TRUE
)

,crest_per_brand_numerator as (
SELECT brand,
crest,
ifnull(repeat_crest_customers,0) as repeat_crest_customers,
ifnull(reactivated_crest_customers,0) as reactivated_crest_customers,
ifnull(new_to_brand_crest_customers,0) as new_to_brand_crest_customers
FROM crest_brand_base
LEFT JOIN repeat_crest using(brand,crest)
LEFT JOIN reactivated_crest using(brand,crest)
LEFT JOIN new_to_brand_crest using(brand,crest)
)

,crest_per_brand_denominator as (
SELECT *,
sum(repeat_crest_customers) OVER (PARTITION BY brand) as total_repeat,
sum(reactivated_crest_customers) OVER (PARTITION BY brand) as total_reactivated,
sum(new_to_brand_crest_customers) OVER (PARTITION BY brand) as total_new_to_brand
FROM crest_per_brand_numerator
)

SELECT *,
safe_divide(repeat_crest_customers,total_repeat) as repeat_crest_perc,
safe_divide(reactivated_crest_customers,total_reactivated) as reactivated_crest_perc,
safe_divide(new_to_brand_crest_customers,total_new_to_brand) as new_to_brand_crest_perc
FROM crest_per_brand_denominator
)