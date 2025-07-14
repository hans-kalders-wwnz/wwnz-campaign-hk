--CREATE OR REPLACE TABLE `gcp-wow-cd-email-app-test.campaign_excellence.starfish_email_audience_FY2603_GAS_CPLdiscountwk3_15CPL_16_07_2025_90085848_FY2603_GAS_CPLdiscountwk3_15CPL_90085848` AS  
(


WITH base_customer as
(SELECT DISTINCT scv_id, 
DATE(TIMESTAMP(datetime_created), 'Pacific/Auckland') as datetime_created
FROM gcp-wow-cd-data-engine-prod.consume_customer.dim__retail_customer
WHERE scv_id IS NOT NULL
AND customer_status_description = 'Active'
AND _is_current = TRUE
AND is_customer_deleted = FALSE
AND is_valid_email=TRUE
)

,orange_prefs as (
  SELECT distinct scv_id
  FROM `gcp-wow-cd-data-engine-prod.consume_customer.trn__customer_preference_pivot_curr`
  where edr_allow_contact_all=TRUE
  and edr_allow_partner_comms_gas_email=TRUE
)



,bp_customer as (
SELECT DISTINCT scv_id 
FROM `gcp-wow-cd-data-engine-prod.consume_loyalty.trn__fuel_redemption_detail`
WHERE scv_id IS NOT NULL
and partner_id='2011' --bp partner ID
)


,edr_2_months_never_scanned_bp as (
SELECT DISTINCT scv_id
FROM base_customer
WHERE scv_id NOT IN (SELECT scv_id from bp_customer)
AND datetime_created <= DATETIME_ADD(CURRENT_DATE('Pacific/Auckland'), INTERVAL - 2 MONTH))


, final_audience as (
select distinct scv_id
from edr_2_months_never_scanned_bp
where
scv_id in (select scv_id from orange_prefs)

  UNION DISTINCT -- adding seedlists

  SELECT
    DISTINCT scv_id
  FROM
    `gcp-wow-cd-data-engine-prod.consume_marketing.trn__campaign_customer_partner_list`
  WHERE
    _source = 'Partner Seed List'
    AND partner_id = '2016'

)


select
distinct scv_id,
'FY2603_GAS_CPLdiscountwk3_15CPL_16_07_2025_90085848' as campaign_id,
'FY2603_GAS_CPLdiscountwk3_15CPL_90085848' as execution_id,
' ' as segment
from final_audience
)

;