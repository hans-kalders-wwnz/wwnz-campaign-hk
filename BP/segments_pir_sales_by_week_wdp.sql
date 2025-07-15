/*-------------------------------------------------------
Table inputs:
  gcp-wow-cd-data-engine-prod.consume_common.dim__date
  gcp-wow-ent-im-tbl-prod.gs_nz_fin_data.fin_smktnz_profit_v
  gcp-wow-ent-im-tbl-prod.adp_dm_marketshare_view.circana_marketshare_v


Purpose:
  Brings in 'offer' information as well as shelf prices (national) (?)


Changes:
  2022-03-07 [Matt Whittaker]: Updated from outputting tables in adhoc to be self contained so it can be embedded in Tableau dashboard.  
  2023-02-28 [Matt Whittaker]: Added trading gp in final output.
  2025-07-15 [Hans Kalders]:  Removed duplicated IRI product ID line 122 
--------------------------------------------------------*/


WITH
  --pull date data
  cte_dates AS (
    SELECT
      DENSE_RANK() OVER (ORDER BY financial_week_start_date desc) AS LatestWeekOrder,
      pel_week AS PelWeek,
      date AS JoinDate,
      financial_week_end_date AS WeekEndDate,
      financial_year AS FinYear,
      calendar_year as CalYear,
      financial_week_start_date
    FROM `gcp-wow-cd-data-engine-prod.consume_common.dim__date`
    WHERE financial_week_start_date BETWEEN DATE_SUB(CURRENT_DATE("Pacific/Auckland"), INTERVAL 104 Week) AND DATE_SUB(CURRENT_DATE("Pacific/Auckland"), INTERVAL 1 Week)),
 
  --pull article details
  cte_products AS (
    SELECT
      article_id,
      -- article_Description,
      Price_Family_id,
      Price_Family_name,
      Segment_description,
      Sub_Category_Description,
      Category_Description,
      Category_Manager_Name,
      Department_Description,
      Merchandise_Manager_Name,
      -- nz_own_brand,
      category_id,
      department_id
    -- FROM gcp-wow-food-de-pel-prod.datamart.article
    FROM `gcp-wow-cd-data-engine-prod.consume_product_and_pricing.dim__article_hierarchy`
    WHERE _is_current
    
    ),


  --pull sales & profit for relevant categories
  cte_profit AS (
    SELECT
      Article,
      Department_Description as DeptDesc,
      Category_Description as CatDesc,
      SubCategory_Description as SCDesc,
      Segment_Description as SegDesc,
      DATE_TRUNC(calendar_day, WEEK(MONDAY)) AS JoinDate,
      SUM(Sales_ExclTax) AS Sales,
      SUM(Interim_GP) AS IntGP,
      SUM(Trading_GP) AS TradingGP,
      SUM(Dumps_Cost) AS Dumps,
      SUM(Total_stock_loss) AS StockLoss,
      SUM(Total_Markdown) AS Markdowns,
      SUM(Manual_Claims) AS ManualClaims,  
      SUM(Interim_GP-Manual_Claims) AS IntGPm,
      SUM(COGS) AS COGS
    FROM `gcp-wow-ent-im-tbl-prod.gs_nz_fin_data.fin_smktnz_profit_v` P
    WHERE (upper(department_description ) IN ('GENERAL MERCHANDISE','GROCERIES','PERISHABLES','LIQUOR','CIGARETTES')
      OR upper(subcategory_description ) in ('PETFOOD','CHEESE PRE-PACKED ENTERTAINING', 'CHEESE PRE-PACKED COOKING', 'RESALE INTERNATIONAL BREAD', 'RESALE BAKERY SNACKS', 'RESALE CAKE', 'RESALE BREAD','MEALS - PREPACKED','SALADS PREPACKED','PREPARED FOOD - PREPACKED','HAM/BACON/SMALLGOODS PREPACKED'))
      AND category <> 'C10577'
      AND SalesOrg='2010'
      AND DATE_TRUNC(calendar_day, WEEK(MONDAY)) BETWEEN DATE_SUB(CURRENT_DATE("Pacific/Auckland"),INTERVAL 104 Week) AND DATE_SUB(CURRENT_DATE("Pacific/Auckland"),INTERVAL 1 Week)
    GROUP BY 1,2,3,4,5,6),
 
  --pull IRI sales figures for CD, Competitors & TKA's
  cte_iri_base_data AS (
    SELECT
      CAST(startdate AS DATE) AS startdate,
      ProgressiveNZReference AS ProductID,
      segment AS IRISeg,
      subcategory AS IRISubCat,
      category AS IRICat,
      department AS IRIDepartment,
      ProductDescription AS IRIProdDesc,
      SUM(CASE WHEN MarketDescription='Total Key Accounts' THEN Value_Excl_Tax ELSE 0 END) AS TKASales,
      SUM(CASE WHEN MarketDescription='Total Key Accounts' THEN units ELSE 0 END) AS TKAUnits,
      SUM(CASE WHEN MarketDescription='Total Competitor' THEN Value_Excl_Tax ELSE 0 END) AS CompSales,
      SUM(CASE WHEN MarketDescription='Total Competitor' THEN units ELSE 0 END) AS CompUnits,
      SUM(CASE WHEN MarketDescription='Total Supervalue/Fresh Choice' THEN Value_Excl_Tax ELSE 0 END) AS SVFCSales,
      SUM(CASE WHEN MarketDescription='Total Supervalue/Fresh Choice' THEN units ELSE 0 END) AS SVFCUnits,
      SUM(CASE WHEN MarketDescription='Total NZ Woolworths (Excl SVFC)' THEN Value_Excl_Tax ELSE 0 END) AS CDSales,
      SUM(CASE WHEN MarketDescription='Total NZ Woolworths (Excl SVFC)' THEN units ELSE 0 END) AS CDUnits,
    FROM `gcp-wow-ent-im-tbl-prod.adp_dm_marketshare_view.circana_marketshare_v`
    WHERE
      MarketDescription IN ('Total Key Accounts','Total Competitor','Total NZ Woolworths (Excl SVFC)','Total Supervalue/Fresh Choice')
      AND (upper(department) IN ('GENERAL MERCHANDISE','GROCERIES','PERISHABLES','LIQUOR','CIGARETTES')
        OR upper(subcategory) IN ('PETFOOD','CHEESE PRE-PACKED ENTERTAINING', 'CHEESE PRE-PACKED COOKING', 'RESALE INTERNATIONAL BREAD', 'RESALE BAKERY SNACKS', 'RESALE CAKE', 'RESALE BREAD','MEALS - PREPACKED','SALADS PREPACKED','PREPARED FOOD - PREPACKED','HAM/BACON/SMALLGOODS PREPACKED'))
      AND CAST(startdate AS DATE) BETWEEN DATE_SUB(CURRENT_DATE("Pacific/Auckland"), INTERVAL 104 Week) AND DATE_SUB(CURRENT_DATE("Pacific/Auckland"), INTERVAL 1 Week)
    GROUP BY 1,2,3,4,5,6,7),


  --pull article join key (progressive_nz_reference) from IRI data
  cte_iri_unique_rec AS (


SELECT
  ProductID,
  IRIDesc
FROM (
  SELECT
    ProgressiveNZReference AS ProductID,
    ProductDescription AS IRIDesc,
    SUM(Value_Excl_Tax) AS CDSales,
    -- RANK() OVER (PARTITION BY progressive_nz_reference ORDER BY SUM(value_excl_gst) DESC) AS Largest, -- Original line
    ROW_NUMBER() OVER (PARTITION BY ProgressiveNZReference ORDER BY SUM(Value_Excl_Tax) DESC) AS RowNum -- Use ROW_NUMBER instead of RANK for true uniqueness
  FROM
    `gcp-wow-ent-im-tbl-prod.adp_dm_marketshare_view.circana_marketshare_v`
  WHERE
    (upper(department) IN ('GENERAL MERCHANDISE', 'GROCERIES', 'PERISHABLES', 'LIQUOR', 'CIGARETTES')
      OR upper(subcategory) IN ('PETFOOD', 'CHEESE PRE-PACKED ENTERTAINING', 'CHEESE PRE-PACKED COOKING', 'RESALE INTERNATIONAL BREAD', 'RESALE BAKERY SNACKS', 'RESALE CAKE', 'RESALE BREAD', 'MEALS - PREPACKED', 'SALADS PREPACKED', 'PREPARED FOOD - PREPACKED', 'HAM/BACON/SMALLGOODS PREPACKED'))
    AND CAST(startdate AS DATE) BETWEEN DATE_SUB(CURRENT_DATE("Pacific/Auckland"), INTERVAL 105 Week) AND DATE_SUB(CURRENT_DATE("Pacific/Auckland"), INTERVAL 1 Week)
    AND ProgressiveNZReference IS NOT NULL
  GROUP BY ALL
    -- Only group by progressive_nz_reference (ProductID)
    -- ,2 -- Remove product_description from GROUP BY
)
WHERE
  RowNum = 1 -- Filter for the top row per ProductID
 
    UNION ALL  
   
    SELECT
      DISTINCT
      CAST(NULL AS STRING) AS ProductID,
      ProductDescription AS IRIDesc,
    FROM `gcp-wow-ent-im-tbl-prod.adp_dm_marketshare_view.circana_marketshare_v`
    WHERE
      (UPPER(department) IN ('GENERAL MERCHANDISE','GROCERIES','PERISHABLES','LIQUOR','CIGARETTES')
        OR UPPER(subcategory) IN ('PETFOOD','CHEESE PRE-PACKED ENTERTAINING', 'CHEESE PRE-PACKED COOKING', 'RESALE INTERNATIONAL BREAD', 'RESALE BAKERY SNACKS', 'RESALE CAKE', 'RESALE BREAD','MEALS - PREPACKED','SALADS PREPACKED','PREPARED FOOD - PREPACKED','HAM/BACON/SMALLGOODS PREPACKED'))
      AND CAST(startdate AS DATE) BETWEEN DATE_SUB(CURRENT_DATE("Pacific/Auckland"), INTERVAL 105 Week) AND DATE_SUB(CURRENT_DATE("Pacific/Auckland"), INTERVAL 1 Week)
      AND ProgressiveNZReference is null)
      
      ,
 
  --join IRI sales data to cte_iri_unique_rec to get article join key along with sales
  cte_iri_comp_data_final AS (
    SELECT
      startdate,
      ProductID,
      IRISeg,
      IRISubCat,
      IRICat,
      IRIDepartment,
      IRIDesc,
      SUM(TKASales) AS TKASales,
      SUM(TKAUnits) AS TKAUnits,
      SUM(CompSales) AS CompSales,
      SUM(CompUnits) AS CompUnits,
      SUM(SVFCSales) AS SVFCSales,
      SUM(SVFCUnits) AS SVFCUnits,
      SUM(CDSales) AS CDSales,
      SUM(CDUnits) AS CDUnits,
    FROM (
      SELECT
        b.* EXCEPT(IRIProdDesc),
        a.IRIDesc
      FROM cte_iri_base_data b
      INNER JOIN cte_iri_unique_rec a
        ON b.productid = a.productid
      WHERE a.productid is not null
     
      UNION ALL
     
      SELECT
        c.* EXCEPT(IRIProdDesc),
        d.IRIDesc
      FROM cte_iri_base_data c
      INNER JOIN cte_iri_unique_rec d
        ON c.IRIProdDesc = d.IRIDesc
      WHERE d.productid IS NULL
      )
    GROUP BY 1,2,3,4,5,6,7),


  --join profit data (cte_profit) with IRI data (cte_iri_comp_data_final), date data (cte_dates) & product data (cte_products)
  pir_base AS (
    SELECT  
      profit.Article,
      profit.DeptDesc,
      profit.CatDesc,
      profit.SCDesc,
      profit.SegDesc,
      profit.JoinDate,
      profit.Sales,
      profit.IntGP,
      profit.TradingGP,
      profit.Dumps,
      profit.StockLoss,
      profit.Markdowns,
      profit.ManualClaims,
      profit.IntGPm,
      profit.COGS,
      iri.startdate,
      iri.ProductID,
      iri.IRISeg,
      iri.IRISubCat,
      iri.IRICat,
      iri.IRIDepartment,
      iri.IRIDesc,
      iri.TKASales,
      iri.TKAUnits,
      iri.CompSales,
      iri.CompUnits,
      iri.SVFCSales,
      iri.SVFCUnits,
      iri.CDSales,
      iri.CDUnits,
      dt.pelweek,
      dt.weekenddate,
      dt.LatestWeekOrder,
      dt.FinYear,
      dt.CalYear,
      prd.Price_Family_id,
      prd.Price_Family_name,
      prd.Category_Manager_Name,
      prd.Merchandise_Manager_Name,
    FROM cte_profit profit
    FULL OUTER JOIN cte_iri_comp_data_final iri
      ON Profit.Article = IRI.ProductID
        AND Profit.JoinDate = IRI.startdate
    LEFT OUTER JOIN cte_dates dt
      ON dt.JoinDate = coalesce(iri.startdate,profit.joindate)
    LEFT OUTER JOIN cte_products prd
      ON prd.article_id = coalesce(profit.article,iri.productid)),




cd_hierarchy as (      
--aggregate data to segment level  
    SELECT
      UPPER(Department_Description) as DeptCombined,
      UPPER(Category_Description) as CatCombined,
      UPPER(SubCategory_Description) as SubCatCombined,
      UPPER(Segment_Description) as SegCombined,
      d.WeekEndDate AS WeekEndDate,
      d.PelWeek,
      d.LatestWeekOrder,
      SUM(Sales_ExclTax) AS Sales,
      SUM(Interim_GP) AS IntGP,
      SUM(Trading_GP) AS TradingGP,
      SUM(Dumps_Cost) AS Dumps,
      SUM(Total_stock_loss) AS StockLoss,
      SUM(Total_Markdown) AS Markdowns,
      SUM(Manual_Claims) AS ManualClaims,  
      SUM(Interim_GP-Manual_Claims) AS IntGPm,
      SUM(COGS) AS COGS
    FROM `gcp-wow-ent-im-tbl-prod.gs_nz_fin_data.fin_smktnz_profit_v`  p
    LEFT JOIN  cte_dates d
    on p.Calendar_Day = d.JoinDate
    WHERE
       category <> 'C10577'
      AND SalesOrg='2010'
      AND p.article not like 'Z%' --dummy article
      AND DATE_TRUNC(calendar_day, WEEK(MONDAY)) BETWEEN DATE_SUB(CURRENT_DATE("Pacific/Auckland"),INTERVAL 104 Week) AND DATE_SUB(CURRENT_DATE("Pacific/Auckland"),INTERVAL 1 Week)
    GROUP BY 1,2,3,4,5,6,7),


iri_hierarchy as
(
SELECT
  PelWeek,
  WeekEndDate,
  UPPER( COALESCE(IRIDepartment, DeptDesc)) DeptCombined,
  UPPER( COALESCE(IRICat, CatDesc)) CatCombined,
  UPPER( COALESCE(IRISubCat,SCDesc)) SubCatCombined,
  UPPER( COALESCE(IRISeg, SegDesc)) SegCombined,
  LatestWeekOrder,
  SUM(Sales) sales,
  SUM(IntGP) IntGP,
  SUM(IntGP-ManualClaims) IntGPm,
  SUM(TradingGP) TradingGP,
  SUM(CDSales) CDSales,
  SUM(CompSales) CompSales,
  SUM(SVFCSales) SVFCSales,
  SUM( TKASales) TKASales,
  SUM(StockLoss) StockLoss,
  SUM(CDUnits) CDUnits,
  '' as CatMgr  ,
  '' as MerchMgr,  
  '' as DeptDesc,
  '' as CatDesc,
  '' as SCDesc,
  '' as SegDesc,
  CURRENT_DATE("Pacific/Auckland") refresh_date,
FROM pir_base p
GROUP BY 1,2,3,4,5,6,7
)


SELECT
  COALESCE(i.PelWeek,c.PelWeek)PelWeek,
  COALESCE(i.WeekEndDate,c.WeekEndDate)WeekEndDate,
  COALESCE(i.DeptCombined,c.DeptCombined)DeptCombined,
  COALESCE(i.CatCombined,c.CatCombined)CatCombined,
  COALESCE(i.SubCatCombined,c.SubCatCombined)SubCatCombined,
  COALESCE(i.SegCombined,c.SegCombined)SegCombined,
  COALESCE(i.LatestWeekOrder,c.LatestWeekOrder)LatestWeekOrder,
  c.sales,
  c.IntGP,
  c.IntGPm,
  c.TradingGP,
  i.CDSales,
  i.CompSales,
  i.SVFCSales,
  i.TKASales,
  c.StockLoss,
  i.CDUnits,
  '' as CatMgr  ,
  '' as MerchMgr,  
  '' as DeptDesc,
  '' as CatDesc,
  '' as SCDesc,
  '' as SegDesc,
  CURRENT_DATE("Pacific/Auckland") refresh_date,
FROM iri_hierarchy i
full outer join cd_hierarchy c
using(WeekEndDate,SegCombined,SubCatCombined,CatCombined,DeptCombined)
-- where upper(SubCatCombined ) in ('BABY - CARE')
-- AND WeekEndDate >= '2023-11-05' AND WeekEndDate <= '2024-01-28'
-- and WeekEndDate = '2024-01-28'
order by 1,2