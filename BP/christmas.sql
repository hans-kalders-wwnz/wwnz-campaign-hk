WITH cte_event_input AS (
SELECT 
  t.Article_code, 
  t.Article_name, 
  -- convert to '0x'when joining to weeks that have 01 in front
  CASE WHEN LENGTH(CAST(t.Report_start_week AS STRING))=1
      THEN CONCAT('0',CAST(t.Report_start_week AS STRING))
      ELSE CAST(t.Report_start_week AS STRING)
      END Report_start_week , 
  CASE WHEN LENGTH(CAST(t.Report_finish_week AS STRING))=1
      THEN CONCAT('0',CAST(t.Report_finish_week AS STRING))
      ELSE CAST(t.Report_finish_week AS STRING)
      END Report_finish_week ,     
  t.TY_purchase, 
  t.LY_Purchase
  , week 
FROM `gcp-wow-cd-data-engine-prod.landing_sheets.landing_sheets__seasonal_purchase_christmas`  t, UNNEST(GENERATE_ARRAY( Report_start_week , Report_finish_week )) AS week)
  ,
cte_purchase_by_week AS (
SELECT 
                            Article_code,
      CASE WHEN LENGTH(CAST(week AS STRING))=1
            THEN CONCAT('0',CAST(week AS STRING))
            ELSE CAST(week AS STRING) END as week, 
SUM(TY_purchase)         as TY_purchase,
SUM(LY_Purchase)         as LY_Purchase
FROM cte_event_input

GROUP BY 1,2)
-- select  * from cte_purchase_by_week where Article_code='194964'
,
-- find current and last year christmas day
cte_seasonal_date AS
(
SELECT 

MAX(CASE WHEN financial_year = (SELECT financial_year FROM `gcp-wow-cd-data-engine-prod.consume_common.dim__date` WHERE current_date('NZ')=date ) THEN
          date
     END) ty_event_date
,MAX(CASE WHEN financial_year = (SELECT financial_year FROM `gcp-wow-cd-data-engine-prod.consume_common.dim__date` WHERE (current_date('NZ')-365)=date ) THEN
          date
     END) ly_event_date     

  FROM `gcp-wow-cd-data-engine-prod.consume_common.dim__date`
  WHERE holiday_name='Christmas Day'
  AND (
  financial_year = (SELECT financial_year FROM `gcp-wow-cd-data-engine-prod.consume_common.dim__date` WHERE current_date('NZ')=date ) OR         
  financial_year = (SELECT financial_year FROM `gcp-wow-cd-data-engine-prod.consume_common.dim__date` WHERE (current_date('NZ')-365)=date ) 
  )
)

,
cte_site_day_article AS
(

SELECT
                    sm.Article,
                    sm.Calendar_Day,

                   dd.financial_year,
SUM(Interim_GP)        Interim_GP,
SUM(Sales_Qty_SUoM)    Sales_Qty_SUoM,
SUM(Sales_ExclTax)     Sales_ExclTax

FROM `gcp-wow-ent-im-tbl-prod.gs_nz_fin_data.fin_smktnz_profit_v` AS sm
  INNER JOIN `gcp-wow-cd-data-engine-prod.consume_common.dim__date` AS dd
    ON sm.Calendar_Day=dd.date

  WHERE sm.SalesOrg='2010'
    -- filter event articles
    AND CAST(Article AS STRING) IN (SELECT  CAST(Article_code AS STRING) FROM cte_event_input)
    -- filter event start and end week
    AND CAST(substr(week_of_financial_period, 5 , 2) AS STRING) 
      BETWEEN 
            
              (SELECT CAST(MIN(Report_start_week) AS STRING) FROM cte_event_input)
          AND (SELECT CAST(MAX(Report_finish_week) AS STRING) FROM cte_event_input)
    -- filter last 54 weeks
    AND dd.date > DATE_SUB(current_date('Pacific/Auckland'), INTERVAL 106 week)

    -- AND sm.Site='9107'
    AND sm.Article !='UNK'
    -- and sm.Article= '65269' 
    -- AND sm.Department_Description='MEAT'
    --  AND Article='286428'

GROUP BY 1,2,3
-- ,4,5
)

-- select * 
-- from cte_site_day_article

-- select financial_year,sum(Sales_Qty_SUoM)

-- from cte_site_day_article
-- where article='286428' and financial_year=2022
-- group by 1

,
cte_seasonal_days_out AS
(
SELECT 
 a.                                                         Article
,a.                                                         financial_year
,a.                                                         Interim_GP
,a.                                                         Sales_Qty_SUoM
,a.                                                         Sales_ExclTax
,a.                                                         Calendar_Day
,DATE_DIFF(Calendar_Day,CAST(sd.ty_event_date AS DATE),day) DaysOut_TY
,DATE_DIFF(Calendar_Day,CAST(sd.ly_event_date AS DATE),day) DaysOut_LY

FROM cte_site_day_article AS a
  CROSS JOIN cte_seasonal_date As sd  

)
,
cte_all_seasonal AS

(
  select Article_code,date,b.financial_year

,DATE_DIFF(date,(SELECT date FROM `gcp-wow-cd-data-engine-prod.consume_common.dim__date` WHERE  holiday_name='Christmas Day'
and financial_year =(select financial_year from `gcp-wow-cd-data-engine-prod.consume_common.dim__date` where date=current_date('NZ') )
),day) DaysOut_TY

,DATE_DIFF(date,(SELECT date FROM `gcp-wow-cd-data-engine-prod.consume_common.dim__date` WHERE  holiday_name='Christmas Day'
and financial_year =(select financial_year from `gcp-wow-cd-data-engine-prod.consume_common.dim__date` where date=(current_date('NZ')-365) )
),day) DaysOut_LY



 from cte_purchase_by_week as a
     inner join `gcp-wow-cd-data-engine-prod.consume_common.dim__date` as b
      on a.week=CAST(substr(week_of_financial_period, 5 , 2) AS STRING)

WHERE 
 (
  financial_year = (SELECT financial_year FROM `gcp-wow-cd-data-engine-prod.consume_common.dim__date` WHERE current_date('NZ')=date ) OR         
  financial_year = (SELECT financial_year FROM `gcp-wow-cd-data-engine-prod.consume_common.dim__date` WHERE (current_date('NZ')-365)=date ) 
  )     

  -- and Article_code='65269' order by 2 desc
)
,
cte_new_seasonal_days
AS
(
select 
  a.Article_code Article
  ,a.date as Calendar_Day
 ,a.financial_year
 ,a.DaysOut_TY
 ,a.DaysOut_LY
 ,Interim_GP
 ,Sales_Qty_SUoM
 ,Sales_ExclTax 

 from  cte_all_seasonal as a
   left join cte_seasonal_days_out as b
    on a.Article_code=b.Article and a.date=b.Calendar_Day
)
,
cte_ty_ly AS
(
SELECT 
                                                          soh.financial_year,
CAST(substr(dd.week_of_financial_period, 5 , 2) AS STRING) as week,
                                                          soh.Article,
                                                          soh.Calendar_Day,
                                                          soh.DaysOut_TY,
                                                          soh.DaysOut_LY
                                                          

-- this year
,SUM(CASE WHEN soh.financial_year=(SELECT financial_year FROM `datamart.dim_date` WHERE current_date('NZ')=date )
          THEN 
            Sales_ExclTax
            END  ) as ty_sales
,SUM(CASE WHEN soh.financial_year=(SELECT financial_year FROM `datamart.dim_date` WHERE current_date('NZ')=date )
          THEN
            Sales_Qty_SUoM             
            END  ) as ty_Sales_Qty_SUoM
,SUM(CASE WHEN soh.financial_year=(SELECT financial_year FROM `datamart.dim_date` WHERE current_date('NZ')=date )
          THEN
            Interim_GP
            END ) as ty_Interim_GP

--last year
,SUM(CASE WHEN soh.financial_year=(SELECT financial_year FROM `datamart.dim_date` WHERE (current_date('NZ')-365)=date )
          THEN 
            Sales_ExclTax
            END  ) as ly_sales
,SUM(CASE WHEN soh.financial_year=(SELECT financial_year FROM `datamart.dim_date` WHERE (current_date('NZ')-365)=date )
          THEN
            Sales_Qty_SUoM             
            END  ) as ly_Sales_Qty_SUoM
,SUM(CASE WHEN soh.financial_year=(SELECT financial_year FROM `datamart.dim_date` WHERE (current_date('NZ')-365)=date )
          THEN
            Interim_GP
            END ) as ly_Interim_GP

FROM cte_new_seasonal_days AS soh
  INNER JOIN `gcp-wow-cd-data-engine-prod.consume_common.dim__date` AS dd
    ON soh.Calendar_Day=dd.date

GROUP BY 1,2,3,4,5,6
)

-- select sum(ly_Sales_Qty_SUoM)
-- from cte_ty_ly
-- where Article='286428'
,
cte_ty_days_out AS
(
SELECT 
                         Article
,                        Calendar_Day
,                        DaysOut_TY
,                        week
,                        financial_year
,SUM(ty_sales)           ty_sales
,SUM(ty_Sales_Qty_SUoM)  ty_Sales_Qty_SUoM
,SUM(ty_Interim_GP)      ty_Interim_GP
 
FROM cte_ty_ly
  WHERE financial_year=(SELECT financial_year FROM `gcp-wow-cd-data-engine-prod.consume_common.dim__date` WHERE current_date('NZ')=date )
AND DaysOut_TY IS NOT NULL
GROUP BY 1,2,3,4,5
)

-- SELECT *
-- select sum(ty_Sales_Qty_SUoM)
-- FROM cte_ty_days_out
-- where Article='286428'

-- order by Calendar_Day DESC
,
cte_ly_days_out AS
(
SELECT 
                         Article
,                        Calendar_Day
,                        DaysOut_LY
,                        week
,                        financial_year
,SUM(ly_sales)           ly_sales
 ,SUM(ly_Sales_Qty_SUoM) ly_Sales_Qty_SUoM
 ,SUM(ly_Interim_GP)     ly_Interim_GP
 
--  ,SUM(LY_purchase)       LY_purchase

FROM cte_ty_ly
  WHERE financial_year=(SELECT financial_year FROM `gcp-wow-cd-data-engine-prod.consume_common.dim__date` WHERE (current_date('NZ')-365)=date )
 AND DaysOut_LY IS NOT NULL
GROUP BY 1,2,3,4,5
)

-- select sum(ly_Sales_Qty_SUoM)
-- FROM cte_ly_days_out
-- where Article='286428'

,
cte_ty_ly_days_out AS (
SELECT
 ifnull(ty.Article,ly.Article)                  Article
,ifnull(ty.Calendar_Day,ly.Calendar_Day) AS     Calendar_Day
,ifnull(ty.DaysOut_TY,ly.DaysOut_LY)     AS     days_out
,ifnull(ty.week,ly.week)                AS      week
,ifnull(ty.financial_year,ly.financial_year) AS financial_year
,IFNULL(ty.DaysOut_TY,ly.DaysOut_LY)  AS DaysOut_TY
,ly.DaysOut_LY
  ,SUM(ty.ty_sales)                             ty_sales
  ,SUM(ty.ty_Sales_Qty_SUoM)                    ty_Sales_Qty_SUoM
  ,SUM(ty.ty_Interim_GP)                        ty_Interim_GP
  
  -- ,SUM(TY_purchase)                         TY_purchase 
  ,SUM(ly.ly_sales)                            ly_sales
  ,SUM(ly.ly_Sales_Qty_SUoM)                   ly_Sales_Qty_SUoM
  ,SUM(ly.ly_Interim_GP)                       ly_Interim_GP
  
  -- ,SUM(ly.LY_purchase)                         LY_purchase

FROM cte_ty_days_out AS ty
  FULL OUTER JOIN cte_ly_days_out AS ly
    ON ty.DaysOut_TY=ly.DaysOut_LY AND ty.Article=ly.Article  

GROUP BY 1,2,3,4,5,6,7
)
,
-- Used to calculate days out that are this year only
cte_get_max_days_out

AS 
(
  SELECT MAX(days_out) days_out_max
  FROM cte_ty_ly_days_out

  WHERE Calendar_Day=(CURRENT_DATE('NZ')-1)
) -- select * from cte_get_max_days_out
,
-- NULL vendors being returned in datamart.article finding a unique vendors from group view and returning vendor ID
cte_xmas_null
as
(

SELECT    
                              Article,
CAST(NationalVendor AS INT64) NationalVendor,
                              End_dttm,
                              row_number() over(partition by article order by End_dttm desc ) row_num -- group view returning vendor number Walkers 

FROM `gcp-wow-ent-im-tbl-prod.adp_dm_masterdata_view.dim_article_hierarchy_v_hist`

-- Only Xmas seasonal articles
WHERE Article IN (SELECT Article_code from `gcp-wow-cd-data-engine-prod.landing_sheets.landing_sheets__seasonal_purchase_christmas`)
-- CD Sales Org
AND SalesOrg='2010' 


)
,
-- used to return vendor description to avoid null values from datamart.article
cte_vendor_description AS
  (
SELECT
  Article,
  VNVendor,
  VNVendorDescription vendor_description 
FROM 
(
  SELECT

    Article,VNVendor, VNVendorDescription, row_number() over(partition by Article order by VNVendor) row_num

  FROM `gcp-wow-ent-im-tbl-prod.adp_dm_masterdata_view.marc_master_article_site_v`

  WHERE SalesOrganisation='2010'
)

WHERE row_num=1

  )
SELECT 
   a.department_description AS  Department_Description
  ,a.category_description       Category_Description   
  ,a.sub_category_description   SubCategory_Description	
  ,a.segment_description        Segment_Description
  ,a.price_family_name          Price_Family_Description	
  ,a.general_manager_name       general_manager_name
  ,y.Article                    Article
  ,a.article_description        Article_Description 
  ,v.vendor_description         single_vendor_description 
  ,y.                           DaysOut_TY 
  ,y.                           DaysOut_LY 
  ,y.days_out                   DaysOut
  ,y.                           financial_year
  ,y.ty_sales                   ty_Sales_ExclTax
  ,y.ty_Sales_Qty_SUoM          ty_Items_Sold
  ,y.ty_Interim_GP              ty_Interim_GP
  ,p.TY_purchase                TY_purchase
  ,y.ly_sales                   ly_Sales_ExclTax
  ,y.ly_Sales_Qty_SUoM          ly_Items_Sold
  ,y.ly_Interim_GP              ly_Interim_GP
  ,p.LY_Purchase                LY_Purchase
  ,y.Calendar_Day            as date
  ,                           d.pel_week
  ,sum(CASE WHEN y.financial_year = (SELECT financial_year FROM `gcp-wow-cd-data-engine-prod.consume_common.dim__date` WHERE current_date('NZ')=date ) THEN y.ty_Sales_Qty_SUoM END) 
      OVER(PARTITION BY y.Article,y.financial_year ORDER BY y.Calendar_Day ) 
                                cumulative_ty
    ,sum( y.ly_Sales_Qty_SUoM ) 
      OVER(PARTITION BY y.Article ORDER BY y.days_out ) 
                                cumulative_ly
  ,CASE WHEN MAX(days_out_max) OVER() >= days_out AND y.Calendar_Day < CURRENT_DATE('NZ') THEN TRUE ELSE FALSE END is_ty                                

FROM cte_ty_ly_days_out AS y
   INNER JOIN cte_purchase_by_week AS p
    ON CAST(y.week AS STRING)=CAST(p.week AS STRING) AND y.Article=p.Article_code
   INNER JOIN `gcp-wow-food-de-pel-prod.datamart.article` AS a
    ON y.Article=a.article_id
   INNER JOIN `gcp-wow-cd-data-engine-prod.consume_common.dim__date` AS d
    ON y.Calendar_Day=d.date
   LEFT JOIN cte_vendor_description AS v
    ON v.Article=y.Article
   CROSS JOIN  cte_get_max_days_out