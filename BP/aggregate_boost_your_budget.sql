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
  SELECT *
  FROM byb_campaigns
WHERE offer_id NOT IN ("90066239","90066219") #Removing BYB1 for two reasons: 1) This campaign used a separate clone campaigns, so results are split over two ids. 2) Redemption figures are wrong for online orders due to data issue which was fixed before BYB
  )


  -- Determines the precise start and end dates for each campaign from allocation data.
, coupon_dates AS (
  SELECT
  b.*,
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
  *,
  DATE_DIFF(campaign_end_date, campaign_start_date, WEEK) AS campaign_duration_weeks,
  DATE_SUB(campaign_start_date, INTERVAL DATE_DIFF(campaign_end_date, campaign_start_date, WEEK) WEEK) AS pre_campaign_start_date,
  DATE_SUB(campaign_start_date, INTERVAL 1 DAY) AS pre_campaign_end_date,
  DATE_ADD(campaign_end_date, INTERVAL 1 DAY) AS post_campaign_start_date,
  DATE_ADD(campaign_end_date, INTERVAL DATE_DIFF(campaign_end_date, campaign_start_date, WEEK) WEEK) AS post_campaign_end_date,
  DATE_SUB(campaign_start_date, INTERVAL 26 WEEK) AS pre_campaign_26w_start_date,
  DATE_SUB(campaign_start_date, INTERVAL 52 WEEK) AS pre_campaign_52w_start_date
  FROM coupon_dates
  )


-- Get the master list of articles and brands used in BYB campaigns (maintained manually by Campaign Excellence)
,byb_articles as (
  SELECT distinct offer_id,
  brand,
  article_id
  FROM gcp-wow-cd-bse-prod.campaign_excellence_operational_tables.BYB_brand_article_master_table
)

-- Get a list of absolute min,max dates for all BYB ever to use to filter srl before querying, as the table is so big
, campaign_date_boundaries AS (
  SELECT
    MIN(pre_campaign_52w_start_date) as min_date,
    MAX(DATE_ADD(post_campaign_end_date, INTERVAL 2 WEEK)) as max_date
  FROM campaign_config
)


-- Get one big grab of data from srl for use in many later steps. Grouping by individual receipt and article, so there is only one row per article for a  given receipt. Time differences between when online orders are placed and invoiced is handled by use if the unique_txn_date. Don't need to grab the commercial ID as only using the final online receipt

, all_sales_raw AS (
  SELECT
  cc.*,
  case when srl.single_customer_view_id IS NULL or srl.single_customer_view_id = "" then NULL else srl.single_customer_view_id end AS scv_id, #Allow customers who didn't swipe, so can use later for scan/tag rates, but clean the value
  srl.receipt_id,
  CASE WHEN srl.is_online_order = TRUE THEN srl.online_order_date ELSE srl.receipt_date END AS unique_txn_date,
  a.brand,
  srl.article_id,
  sum(gst_included_amount) as gst_included_amount,
  sum(quantity) as quantity
  FROM `gcp-wow-cd-data-engine-prod.consume_sales.obt__sales_receipt_line` srl
  INNER JOIN campaign_config cc ON srl.receipt_date BETWEEN cc.pre_campaign_52w_start_date AND DATE_ADD(cc.post_campaign_end_date, INTERVAL 2 WEEK) #Joining all the way to end of post campaign to capture online orders invoiced after campaign end
  INNER JOIN byb_articles a ON cc.offer_id = a.offer_id AND srl.article_id = a.article_id
  WHERE srl.receipt_date BETWEEN (SELECT min_date FROM campaign_date_boundaries) AND (SELECT max_date FROM campaign_date_boundaries)
  AND ((is_online_order = FALSE AND receipt_date BETWEEN cc.pre_campaign_52w_start_date AND cc.post_campaign_end_date) #Instore orders
    OR (is_online_order_completed = TRUE AND online_order_date BETWEEN cc.pre_campaign_52w_start_date AND cc.post_campaign_end_date)) #Or a completed online order that was placed during pre to post campaign
  GROUP BY ALL
)


  -- Get the total unique activations for each BYB offer.
, total_activations AS (
    SELECT
      a.offer_id,
      COUNT(DISTINCT a.scv_id) AS total_activations
    FROM `gcp-wow-cd-data-engine-prod.consume_marketing.trn__customer_activated_offer` a
    INNER JOIN campaign_config cc ON a.offer_id = cc.offer_id
    GROUP BY a.offer_id
  )

  -- Gathers all redemption records for each BYB offer.
,   offer_redemptions AS (
    SELECT
      cc.offer_id,
      r.scv_id,
      r.receipt_id,
      r.article_id,
      r.quantity AS redemptions,
      r.bonus_points_earn
    FROM `gcp-wow-cd-data-engine-prod.consume_marketing.trn__campaign_sales_reward_detail` r
    INNER JOIN campaign_config cc ON r.offer_id = cc.offer_id
  )
  
  -- Defines the base of BYB redeemers for each campaign.
, byb_redeemers_by_brand AS (
    SELECT DISTINCT o.offer_id,
    b.brand,
    o.scv_id
    FROM offer_redemptions o
    INNER JOIN byb_articles b ON o.article_id = b.article_id AND o.offer_id = b.offer_id
  )


  -- Identifies historical shoppers for each brand to define redeemer segments. The customers may not have redeemed BYB
, historic_brand_shoppers AS (
    SELECT
      offer_id,
      brand,
      scv_id,
      MAX(CASE WHEN unique_txn_date BETWEEN pre_campaign_26w_start_date AND pre_campaign_end_date THEN 1 ELSE 0 END) AS shopped_last_6m,
      MAX(CASE WHEN unique_txn_date BETWEEN pre_campaign_52w_start_date AND pre_campaign_end_date THEN 1 ELSE 0 END) AS shopped_last_12m,
    FROM all_sales_raw
    WHERE scv_id IS NOT NULL
    GROUP BY ALL
  )


  -- Classifies redeemers as New, Reactivated, or Repeat. Customers who shopped in the last 12 months are repeat. Shopped in 12m but not last 6 are reactivated and never shopped in last 52 are new
, redeemer_segments_raw AS (
  SELECT
  r.offer_id,
  r.brand,
  r.scv_id,
  CASE WHEN h.shopped_last_6m = 1 THEN 'Repeat' WHEN h.shopped_last_12m = 1 THEN 'Reactivated' ELSE 'New' END AS redeemer_type
  FROM byb_redeemers_by_brand r
  LEFT JOIN historic_brand_shoppers h ON r.scv_id = h.scv_id AND r.brand = h.brand AND r.offer_id = h.offer_id
  )



  -- Aggregates core performance and redeemer segments by brand
, performance_and_segments_agg AS (
    SELECT
      r.offer_id,
      b.brand,
      SUM(r.redemptions) AS redemptions,
      COUNT(DISTINCT r.scv_id) AS unique_redeemers,
      SUM(s.gst_included_amount) AS byb_sales,
      SUM(r.bonus_points_earn) as bonus_points,
      SUM(r.bonus_points_earn) * 0.0091 AS points_cost,
      SAFE_DIVIDE(SUM(s.gst_included_amount), SUM(r.bonus_points_earn) * 0.0091) AS scr,
      COUNT(DISTINCT CASE WHEN rsr.redeemer_type = 'New' THEN r.scv_id END) AS new_to_brand_redeemers,
      COUNT(DISTINCT CASE WHEN rsr.redeemer_type = 'Reactivated' THEN r.scv_id END) AS reactivated_redeemers,
      COUNT(DISTINCT CASE WHEN rsr.redeemer_type = 'Repeat' THEN r.scv_id END) AS repeat_redeemers
    FROM offer_redemptions AS r
    INNER JOIN byb_articles AS b ON r.article_id = b.article_id AND r.offer_id = b.offer_id
    LEFT JOIN all_sales_raw s ON r.receipt_id = s.receipt_id AND r.article_id = s.article_id AND r.offer_id = s.offer_id
    LEFT JOIN redeemer_segments_raw AS rsr ON r.scv_id = rsr.scv_id AND b.brand = rsr.brand AND r.offer_id = rsr.offer_id
    GROUP BY offer_id, brand
  )


-- Find all the customers who shoppe each brand in the 4 weeks after campaigns
,post_campaign_shoppers as (
  SELECT DISTINCT offer_id,
  brand,
  scv_id
  FROM all_sales_raw
  WHERE unique_txn_date BETWEEN post_campaign_start_date AND post_campaign_end_date
  AND scv_id IS NOT NULL
)


-- List of customers who shopped during campaign, even if didn't redeem
,campaign_shoppers as (
  SELECT DISTINCT offer_id,
  brand,
  scv_id
  FROM all_sales_raw
  WHERE unique_txn_date BETWEEN campaign_start_date AND campaign_end_date
  AND scv_id IS NOT NULL
)


  -- Calculates shopper and retention metrics.
, shopper_and_retention_agg AS (
    SELECT
      cs.offer_id,
      cs.brand,
      
      COUNT(DISTINCT cs.scv_id) as brand_shoppers,
      COUNT(DISTINCT CASE WHEN a.scv_id IS NULL THEN cs.scv_id END) as non_boosters,
      COUNT(DISTINCT CASE WHEN rsr.redeemer_type = 'New' AND post.scv_id IS NOT NULL THEN rsr.scv_id END) AS retained_new_to_brand_shoppers,
      COUNT(DISTINCT CASE WHEN rsr.redeemer_type = 'Reactivated' AND post.scv_id IS NOT NULL THEN rsr.scv_id END) AS retained_reactivated_shoppers,
      COUNT(DISTINCT CASE WHEN rsr.redeemer_type = 'Repeat' AND post.scv_id IS NOT NULL THEN rsr.scv_id END) AS retained_repeat_shoppers
    FROM campaign_shoppers cs
    LEFT JOIN byb_redeemers_by_brand AS a ON cs.scv_id = a.scv_id AND cs.offer_id = a.offer_id AND cs.brand = a.brand
    LEFT JOIN redeemer_segments_raw rsr ON cs.scv_id = rsr.scv_id AND cs.brand = rsr.brand AND cs.offer_id = rsr.offer_id
    LEFT JOIN post_campaign_shoppers post ON cs.scv_id = post.scv_id AND cs.brand = post.brand AND cs.offer_id = post.offer_id
    GROUP BY offer_id, brand
  )


-- Brand specific scan and tag rates
-- Start with a list of all the unique transaction pre and during campaign, both for non swiped and swiped
,pre_during_campaign_txns as (
  SELECT offer_id,
  pre_campaign_start_date,
  pre_campaign_end_date,
  campaign_start_date,
  campaign_end_date,
  campaign_duration_weeks,
  brand,
  receipt_id,
  scv_id,
  unique_txn_date,
  sum(gst_included_amount) as sales, #Sum across all the articles
  sum(quantity) as units
  FROM all_sales_raw s
  WHERE unique_txn_date BETWEEN pre_campaign_start_date AND campaign_end_date
  GROUP BY ALL
)


  -- Defines the base data for calculating scan/tag rates & overall sales and unit uplift
, scan_tag_base AS (
  SELECT
  offer_id,
  brand,
  campaign_duration_weeks,
  COUNT(DISTINCT CASE WHEN unique_txn_date BETWEEN pre_campaign_start_date AND pre_campaign_end_date THEN receipt_id END) as pre_campaign_brand_baskets,
  COUNT(DISTINCT CASE WHEN unique_txn_date BETWEEN pre_campaign_start_date AND pre_campaign_end_date AND scv_id IS NOT NULL THEN receipt_id END) as pre_campaign_swiped_brand_baskets,
  COUNT(DISTINCT CASE WHEN unique_txn_date BETWEEN campaign_start_date AND campaign_end_date THEN receipt_id END) as campaign_brand_baskets,
  COUNT(DISTINCT CASE WHEN unique_txn_date BETWEEN campaign_start_date AND campaign_end_date AND scv_id IS NOT NULL THEN receipt_id END) as campaign_swiped_brand_baskets,
  
  SUM(CASE WHEN unique_txn_date BETWEEN pre_campaign_start_date AND pre_campaign_end_date THEN sales END) as pre_campaign_brand_sales,
  SUM(CASE WHEN unique_txn_date BETWEEN pre_campaign_start_date AND pre_campaign_end_date AND scv_id IS NOT NULL THEN sales END) as pre_campaign_swiped_brand_sales,
  SUM(CASE WHEN unique_txn_date BETWEEN campaign_start_date AND campaign_end_date THEN sales END) as campaign_brand_sales,
  SUM(CASE WHEN unique_txn_date BETWEEN campaign_start_date AND campaign_end_date AND scv_id IS NOT NULL THEN sales END) as campaign_swiped_brand_sales,

  SUM(CASE WHEN unique_txn_date BETWEEN pre_campaign_start_date AND pre_campaign_end_date THEN units END) as pre_campaign_brand_units,
  SUM(CASE WHEN unique_txn_date BETWEEN campaign_start_date AND campaign_end_date THEN units END) as campaign_brand_units,
  FROM pre_during_campaign_txns
  GROUP BY offer_id, brand, campaign_duration_weeks
)


-- Calculates uplift metrics by comparing pre vs campaign periods.
, uplift_agg AS (
    SELECT
      offer_id,
      brand,
      SAFE_DIVIDE((campaign_brand_sales/campaign_duration_weeks) - (pre_campaign_brand_sales/4), (pre_campaign_brand_sales/4)) AS sales_uplift,
      SAFE_DIVIDE((campaign_brand_units/campaign_duration_weeks) - (pre_campaign_brand_units/4), (pre_campaign_brand_units/4)) AS units_uplift,
      SAFE_DIVIDE((campaign_swiped_brand_baskets/campaign_brand_baskets) - (pre_campaign_swiped_brand_baskets/pre_campaign_brand_baskets), (pre_campaign_swiped_brand_baskets/pre_campaign_brand_baskets)) AS scan_rate_uplift,
      SAFE_DIVIDE((campaign_swiped_brand_sales/campaign_brand_sales) - (pre_campaign_swiped_brand_sales/pre_campaign_brand_sales), (pre_campaign_swiped_brand_sales/pre_campaign_brand_sales)) AS tag_rate_uplift,
    FROM scan_tag_base
  )
  

  -- Attaches CREST segment information to each redeemer.
-- Gets the cleaned segment for every customer who shopped during the campaign period.
, redeemer_profiles AS (
  SELECT
    rsr.offer_id,
    rsr.brand,
    rsr.scv_id,
    rsr.redeemer_type,
    LOWER(IFNULL(fch.customer_crest_segment_group, 'no_segment')) AS crest
  FROM redeemer_segments_raw rsr
  LEFT JOIN campaign_config cc ON rsr.offer_id = cc.offer_id
  LEFT JOIN `gcp-wow-cd-data-engine-prod.consume_feature_library.features_customer_history` fch
    ON fch.scv_id = rsr.scv_id AND fch.at_date = cc.campaign_start_date
)

  -- Get the counts for each crest segment within the redeemer type
, crest_summary AS (
  SELECT
  offer_id,
  brand,
  COUNT(DISTINCT CASE WHEN redeemer_type = 'Repeat' AND crest = 'conscious' THEN scv_id END) AS repeat_crest_conscious_customers,
  COUNT(DISTINCT CASE WHEN redeemer_type = 'Reactivated' AND crest = 'conscious' THEN scv_id END) AS reactivated_crest_conscious_customers,
  COUNT(DISTINCT CASE WHEN redeemer_type = 'New' AND crest = 'conscious' THEN scv_id END) AS new_to_brand_crest_conscious_customers,
  COUNT(DISTINCT CASE WHEN redeemer_type = 'Repeat' AND crest = 'refined' THEN scv_id END) AS repeat_crest_refined_customers,
  COUNT(DISTINCT CASE WHEN redeemer_type = 'Reactivated' AND crest = 'refined' THEN scv_id END) AS reactivated_crest_refined_customers,
  COUNT(DISTINCT CASE WHEN redeemer_type = 'New' AND crest = 'refined' THEN scv_id END) AS new_to_brand_crest_refined_customers,
  COUNT(DISTINCT CASE WHEN redeemer_type = 'Repeat' AND crest = 'essentials' THEN scv_id END) AS repeat_crest_essentials_customers,
  COUNT(DISTINCT CASE WHEN redeemer_type = 'Reactivated' AND crest = 'essentials' THEN scv_id END) AS reactivated_crest_essentials_customers,
  COUNT(DISTINCT CASE WHEN redeemer_type = 'New' AND crest = 'essentials' THEN scv_id END) AS new_to_brand_crest_essentials_customers,
  COUNT(DISTINCT CASE WHEN redeemer_type = 'Repeat' AND crest = 'savers' THEN scv_id END) AS repeat_crest_savers_customers,
  COUNT(DISTINCT CASE WHEN redeemer_type = 'Reactivated' AND crest = 'savers' THEN scv_id END) AS reactivated_crest_savers_customers,
  COUNT(DISTINCT CASE WHEN redeemer_type = 'New' AND crest = 'savers' THEN scv_id END) AS new_to_brand_crest_savers_customers,
  COUNT(DISTINCT CASE WHEN redeemer_type = 'Repeat' AND crest = 'traditional' THEN scv_id END) AS repeat_crest_traditional_customers,
  COUNT(DISTINCT CASE WHEN redeemer_type = 'Reactivated' AND crest = 'traditional' THEN scv_id END) AS reactivated_crest_traditional_customers,
  COUNT(DISTINCT CASE WHEN redeemer_type = 'New' AND crest = 'traditional' THEN scv_id END) AS new_to_brand_crest_traditional_customers,
  COUNT(DISTINCT CASE WHEN redeemer_type = 'Repeat' AND crest = 'no_segment' THEN scv_id END) AS repeat_crest_no_segment_customers,
  COUNT(DISTINCT CASE WHEN redeemer_type = 'Reactivated' AND crest = 'no_segment' THEN scv_id END) AS reactivated_crest_no_segment_customers,
  COUNT(DISTINCT CASE WHEN redeemer_type = 'New' AND crest = 'no_segment' THEN scv_id END) AS new_to_brand_crest_no_segment_customers
  FROM redeemer_profiles
  GROUP BY offer_id, brand
  )


-- Combine all aggregated metrics into the final master table structure.
SELECT
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
cc.pre_campaign_52w_start_date,
psa.brand,
psa.redemptions,
psa.unique_redeemers,
psa.byb_sales,
psa.bonus_points,
psa.points_cost,
psa.scr,
psa.new_to_brand_redeemers,
psa.reactivated_redeemers,
psa.repeat_redeemers,
ta.total_activations, -- NEW: Added total activations
sra.brand_shoppers,
sra.non_boosters,
sra.retained_new_to_brand_shoppers,
sra.retained_reactivated_shoppers,
sra.retained_repeat_shoppers,
ua.sales_uplift,
ua.units_uplift,
ua.scan_rate_uplift,
ua.tag_rate_uplift,
c.repeat_crest_conscious_customers,
c.reactivated_crest_conscious_customers,
c.new_to_brand_crest_conscious_customers,
c.repeat_crest_refined_customers,
c.reactivated_crest_refined_customers,
c.new_to_brand_crest_refined_customers,
c.repeat_crest_essentials_customers,
c.reactivated_crest_essentials_customers,
c.new_to_brand_crest_essentials_customers,
c.repeat_crest_savers_customers,
c.reactivated_crest_savers_customers,
c.new_to_brand_crest_savers_customers,
c.repeat_crest_traditional_customers,
c.reactivated_crest_traditional_customers,
c.new_to_brand_crest_traditional_customers,
c.repeat_crest_no_segment_customers,
c.reactivated_crest_no_segment_customers,
c.new_to_brand_crest_no_segment_customers
FROM campaign_config cc
LEFT JOIN performance_and_segments_agg psa ON cc.offer_id = psa.offer_id
LEFT JOIN total_activations ta ON cc.offer_id = ta.offer_id -- NEW: Joined total_activations CTE
LEFT JOIN shopper_and_retention_agg sra ON psa.offer_id = sra.offer_id AND psa.brand = sra.brand
LEFT JOIN uplift_agg ua ON psa.offer_id = ua.offer_id AND psa.brand = ua.brand
LEFT JOIN crest_summary c ON psa.offer_id = c.offer_id AND psa.brand = c.brand
ORDER BY psa.offer_id, psa.brand