create or replace table  gcp-wow-food-de-pel-prod.adhoc.boosting_invoicing_Boost_Invoicing_Model_Driven_connected AS (

/**We introduce vendor data.**/
with  boost_offer_campaings as  (select offer_id 
from `gcp-wow-cd-data-engine-prod`.consume_marketing.dim__offer_campaign_master cm
WHERE ( campaign_id  like '%ITM%')
group by 1 )

, redeemed_offer_article as (
select OFFER_ID, article_id FROM `gcp-wow-cd-data-engine-prod`.consume_marketing.trn__campaign_sales_reward_detail
WHERE OFFER_ID IN (SELECT OFFER_ID FROM boost_offer_campaings) 
group by 1, 2) 

, vendors_0 as (
select a.offer_id, a.article_id, b.vendor_id,	b.vendor_description

from redeemed_offer_article a 
left join `gcp-wow-food-de-pel-prod.datamart.article_vendor` b on a.article_id = b.article_id
group by 1, 2, 3, 4 )

, offer_id_own_brand as (
select offer_id,  nz_own_brand

from redeemed_offer_article a 
left join `gcp-wow-food-de-pel-prod.datamart.article_vendor` b on a.article_id = b.article_id
 group by 1, 2 )

, offer_id_vendors_1 as (
select offer_id,  vendor_id,
CASE WHEN vendor_description IS NULL THEN '0' ELSE vendor_description END vendor_description,
from vendors_0 group by 1, 2 , 3  )

, offer_id_vendors_2 as (
select offer_id, count(distinct vendor_description) as Q_Vendors,  ARRAY_AGG(distinct vendor_id)  AS vendor_id_list,
ARRAY_AGG(distinct vendor_description)  AS vendor_description_list
from offer_id_vendors_1 group by 1 )

, offer_id_article_1 as (
select offer_id,  article_id 
from vendors_0 group by 1, 2)

, offer_id_article_2 as (
select offer_id, ARRAY_AGG(distinct article_id) AS articles_list 
from offer_id_article_1 group by 1)

, all_boost_offers as (
  select offer_id from redeemed_offer_article group by 1
)

, offer_vendor_table as (
select a.offer_id, b.articles_list, c.vendor_id_list, c.vendor_description_list, c.Q_Vendors
from all_boost_offers a
left join offer_id_article_2 b on a.offer_id = b.offer_id 
left join offer_id_vendors_2 c on a.offer_id = c.offer_id  )

/** Here finish the fixed in vendor data**/
, raw_data_by_offer_id as (
select 
offer_id,
offer_name,
offer_type,
offer_target_type,
product_offer_type,
pel_week,
#count( distinct a.scv_id) as total_coupon_allocated,
#count( distinct b.scv_id) as total_activations,
#count( distinct c.scv_id) as unique_redeemers,
sum(offer_redeemed_count) as total_offer_redeemed_count,
sum(bonus_points_earn) as total_bonus_points_earn

from  gcp-wow-food-de-pel-prod.adhoc.kpi_metrics_campaignexcellence


where offer_id is not null and offer_start_date < current_date('Pacific/Auckland')   
and scv_id_coupon_allocated not in ('Invalid')
and number_week is not null
and lower(offer_name) not like '%pinned%'
group by 1, 2, 3, 4, 5, 6)



select
a.offer_id,
a.pel_week,
SPLIT(a.offer_name,'|')[1] as offer_name,
a.offer_type, 
a.offer_target_type, 
case when a.offer_type = 'product offer' then product_offer_type else null end as product_offer_type,
i.nz_own_brand,
g.Q_Vendors,
TO_JSON_STRING(g.articles_list) as articles_list,
TO_JSON_STRING(g.vendor_id_list) as vendor_id_list,
TO_JSON_STRING(g.vendor_description_list) as vendor_description_list,
total_offer_redeemed_count,
total_bonus_points_earn
#,
#total_coupon_allocated,
#total_activations,
#unique_redeemers,
#unique_redeemers / total_coupon_allocated as redeemers_over_allocated,
#safe_divide(unique_redeemers,total_activations) as redeemers_over_activated

from raw_data_by_offer_id a
left join offer_vendor_table g on a.offer_id = g.offer_id
left join offer_id_own_brand i on a.offer_id = i.offer_id 
where offer_type = 'product offer'
order by pel_week desc, offer_type desc, Q_Vendors )
