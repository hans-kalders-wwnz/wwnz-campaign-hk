

DECLARE financial_periods_list STRING;
DECLARE dynamic_sql STRING;
DECLARE current_date_nz DATE DEFAULT CURRENT_DATE('NZ'); -- Define current date once

-- Step 1: Generate the dynamic list of financial periods for the PIVOT clause
-- This subquery generates the 'YYYYWW' format for the last 52 weeks
SET financial_periods_list = (
  SELECT
    STRING_AGG(formatted_week ORDER BY week_of_financial_period DESC) -- Now orders by the original week, which is available from the subquery
  FROM (
    SELECT DISTINCT -- Apply DISTINCT here first
      week_of_financial_period,
      FORMAT("'%s'", week_of_financial_period) AS formatted_week -- Format the week
    FROM
      `gcp-wow-food-de-pel-prod.datamart.dim_date`
    WHERE
      financial_week_end_date BETWEEN DATE_SUB(DATE_TRUNC(current_date_nz, WEEK), INTERVAL 51 WEEK)
      AND DATE_TRUNC(current_date_nz, WEEK)
  )
);

-- Step 2: Construct the full dynamic SQL query
SET dynamic_sql = FORMAT("""
  SELECT *
  FROM (
    SELECT
      week_of_financial_period,
      article_id,
      SUM(sa.store_weekly_gst_excluded_sales) AS store_weekly_gst_excluded_sales
    FROM
      `gcp-wow-food-de-pel-prod.datamart.store_article_detail` AS sa
    INNER JOIN (
      SELECT DISTINCT financial_week_end_date, week_of_financial_period
      FROM `gcp-wow-food-de-pel-prod.datamart.dim_date`
    ) AS d
      ON sa.financial_week_end_date = d.financial_week_end_date
    WHERE
      sa.category_description = 'NEWSAGENCY'
      AND d.financial_week_end_date BETWEEN
        DATE_SUB(DATE_TRUNC(CURRENT_DATE('NZ'), WEEK), INTERVAL 51 WEEK)
        AND DATE_TRUNC(CURRENT_DATE('NZ'), WEEK)
    GROUP BY 1, 2
  )
  PIVOT (
    SUM(store_weekly_gst_excluded_sales)
    FOR week_of_financial_period IN (%s)
  )
""", financial_periods_list);

-- Step 3: Execute the dynamic SQL query
EXECUTE IMMEDIATE dynamic_sql;