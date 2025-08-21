CREATE OR REPLACE TABLE adhoc.byb2024_pca_table_brand as (

WITH sku_full_list AS (

-- Angel Bay
SELECT
  DISTINCT online_brand,
  'Angel Bay' AS brand_cleaned,
  a.article_id,
  a.article_name_current
FROM
  `gcp-wow-cd-data-engine-prod.consume_product_and_pricing.dim__article` AS a
WHERE
  UPPER(article_name_current) LIKE '%ANGEL BAY%' OR UPPER(article_name_current) LIKE '%ANGELBAY%'
  OR UPPER(online_brand) LIKE '%ANGEL BAY%' AND UPPER(article_name_current) NOT LIKE '%ANGEL BAY%'

UNION DISTINCT

-- Auntie Dai's
SELECT
  DISTINCT online_brand,
  "Auntie Dai's" AS brand_cleaned,
  a.article_id,
  a.article_name_current
FROM
  `gcp-wow-cd-data-engine-prod.consume_product_and_pricing.dim__article` AS a
WHERE
  UPPER(article_name_current) LIKE '%AUNTIE DAI%'

UNION DISTINCT

-- Avalanche
SELECT
  DISTINCT a.online_brand,
  "Avalanche" AS brand_cleaned,
  a.article_id,
  a.article_name_current
FROM
  `gcp-wow-cd-data-engine-prod.consume_product_and_pricing.dim__article` AS a
WHERE
  (UPPER(a.online_brand) LIKE '%AVALANC%' AND UPPER(a.online_brand) NOT LIKE '%LOADED%')
  AND a.article_id NOT IN ('476882','706745','8244078','266120','20328')

UNION DISTINCT

-- Bell
SELECT
  DISTINCT online_brand,
  'Bell' AS brand_cleaned,
  a.article_id,
  a.article_name_current
FROM
  `gcp-wow-cd-data-engine-prod.consume_product_and_pricing.dim__article` AS a
WHERE
  (UPPER(online_brand) IN ('BELL','BELL FOR KIDS','BELL KENYA','BELL TEA','BELL ZESTY') OR a.article_id IN ('637131','637141','636998'))

UNION DISTINCT

-- Better Eggs
SELECT
  DISTINCT online_brand,
  'Better Eggs' AS brand_cleaned,
  a.article_id,
  a.article_name_current
FROM
  `gcp-wow-cd-data-engine-prod.consume_product_and_pricing.dim__article` AS a
WHERE
  (UPPER(article_name_current) LIKE '%BETTER EGG%' OR a.article_id IN ('177104','277316'))

UNION DISTINCT

-- Colgate
SELECT
  DISTINCT online_brand,
  CASE
    WHEN article_id IN ('759714','759725','779571','759743','759735','758994','759702') THEN 'DISPUTED'
    ELSE 'Colgate' 
  END AS brand_cleaned,
  a.article_id,
  a.article_name_current
FROM
  `gcp-wow-cd-data-engine-prod.consume_product_and_pricing.dim__article` AS a
WHERE
  (UPPER(article_name_current) LIKE '%COLGAT%' AND UPPER(article_name_current) NOT LIKE '%PALLET%' AND UPPER(article_name_current) NOT LIKE '%TRAY%')
  OR (UPPER(online_brand) LIKE '%COLGAT%' AND UPPER(article_name_current) NOT LIKE '%COLGAT%')

UNION DISTINCT

-- Copper Kettle
SELECT
  DISTINCT online_brand,
  'Copper Kettle' AS brand_cleaned,
  a.article_id,
  a.article_name_current
FROM
  `gcp-wow-cd-data-engine-prod.consume_product_and_pricing.dim__article` AS a
WHERE
  (UPPER(online_brand) LIKE '%COPPER KETTLE%' AND UPPER(article_name_current) NOT LIKE '%MASTER%')
    OR article_id IN ('479472')

UNION DISTINCT

-- Cyclops
SELECT
  DISTINCT online_brand,
  'Cyclops' AS brand_cleaned,
  a.article_id,
  a.article_name_current
FROM
  `gcp-wow-cd-data-engine-prod.consume_product_and_pricing.dim__article` AS a
WHERE
  (UPPER(online_brand) LIKE '%YCLOP%' OR a.article_id IN ('509922','509908'))

UNION DISTINCT

-- Dettol
SELECT
  DISTINCT online_brand,
  'Dettol' AS brand_cleaned,
  a.article_id,
  a.article_name_current
FROM
  `gcp-wow-cd-data-engine-prod.consume_product_and_pricing.dim__article` AS a
WHERE
  (UPPER(article_name_current) LIKE '%DETTO%' AND article_id NOT IN ('724246'))
  OR (UPPER(online_brand) LIKE '%DETTO%' AND UPPER(article_name_current) NOT LIKE '%DETTO%')
  OR (UPPER(article_name_current) LIKE '%GLEN 20%' OR UPPER(article_name_current) LIKE '%GLEN20%')

UNION DISTINCT

-- Energizer
SELECT
  DISTINCT online_brand,
  'Energizer' AS brand_cleaned,
  a.article_id,
  a.article_name_current
FROM
  `gcp-wow-cd-data-engine-prod.consume_product_and_pricing.dim__article` AS a
WHERE
 ((UPPER(article_name_current) LIKE '%ENERGIZER%')
  OR (UPPER(online_brand) LIKE '%ENERGIZER%' AND UPPER(article_name_current) NOT LIKE '%ENERGIZER%'))
  AND a.article_id NOT IN ('390947','388439')

UNION DISTINCT

-- Farmland
SELECT
  DISTINCT online_brand,
  'Farmland' AS brand_cleaned,
  a.article_id,
  a.article_name_current
FROM
  `gcp-wow-cd-data-engine-prod.consume_product_and_pricing.dim__article` AS a
WHERE
 (UPPER(online_brand) LIKE '%FARMLAND%' AND UPPER(online_brand) NOT LIKE '%MATHIAS%' OR UPPER(online_brand) LIKE '%LUNCH CLUB%' OR UPPER(article_name_current) LIKE '%JUST CUT%')   

UNION DISTINCT

-- Finish
SELECT
  DISTINCT online_brand,
  'Finish' AS brand_cleaned,
  a.article_id,
  a.article_name_current
FROM
  `gcp-wow-cd-data-engine-prod.consume_product_and_pricing.dim__article` AS a
WHERE article_id <> "139508" #this product is listed as both dettol and finish - should be in dettol
  AND (UPPER(online_brand) LIKE '%FINISH%' 
  AND UPPER(online_brand) NOT LIKE '%LASTING%' AND UPPER(online_brand) NOT LIKE '%SCHICK%' 
  AND UPPER(online_brand) NOT LIKE '%HERB%' AND UPPER(online_brand) NOT LIKE '%TOUCH%' AND UPPER(online_brand) NOT LIKE '%ARMOR%')
  OR article_id IN ('360283','618609','214262','8310955','746328','336785','418466','336781','147758','571965','674712','408344','280172','655398','55662','199029','579237','8310954',
  '336786','8291935','8291933','618643','717055','618638','717112','7810893','8383331','618647','449459','418443','8042194','417603','8339637','336783','885933','655216','7301582','655421',
  '754543','921079','763114','763106','732642','783270','783268','783257','783283','783281','783269','783282')

UNION DISTINCT

-- Henergy
SELECT
  DISTINCT online_brand,
  'Henergy' AS brand_cleaned,
  a.article_id,
  a.article_name_current
FROM
  `gcp-wow-cd-data-engine-prod.consume_product_and_pricing.dim__article` AS a
WHERE
  UPPER(online_brand) LIKE '%HENERGY%'

UNION DISTINCT

-- McCain
SELECT
  DISTINCT online_brand,
  'McCain' AS brand_cleaned,
  a.article_id,
  a.article_name_current
FROM
  `gcp-wow-cd-data-engine-prod.consume_product_and_pricing.dim__article` AS a
WHERE
  (UPPER(article_name_current) LIKE '%CCAIN%' OR UPPER(article_name_current) LIKE '%C CAIN%')
  AND ((UPPER(online_brand) LIKE '%CCAIN%' OR UPPER(online_brand) LIKE '%C CAIN%')
    AND (UPPER(article_name_current) NOT LIKE '%CCAIN%') OR UPPER(article_name_current) NOT LIKE '%C CAIN%')

UNION DISTINCT

-- Mono
SELECT
  DISTINCT online_brand,
  'Mono' AS brand_cleaned,
  a.article_id,
  a.article_name_current
FROM
  `gcp-wow-cd-data-engine-prod.consume_product_and_pricing.dim__article` AS a
WHERE
  (UPPER(online_brand) LIKE '%MONO%' AND UPPER(online_brand) NOT LIKE '%ANIS%' AND UPPER(online_brand) NOT LIKE '%POLE%'
        AND UPPER(online_brand) NOT LIKE '%GRAP%' AND UPPER(online_brand) NOT LIKE '%BLUS%' AND UPPER(online_brand) NOT LIKE '%HEADS%'
        AND UPPER(online_brand) NOT LIKE '%POLY%' AND UPPER(online_brand) NOT LIKE '%WAI%'  AND a.article_id NOT IN ('764378'))

UNION DISTINCT

-- Nutrigrain
SELECT
  DISTINCT online_brand,
  'Nutrigrain' AS brand_cleaned,
  a.article_id,
  a.article_name_current
FROM
  `gcp-wow-cd-data-engine-prod.consume_product_and_pricing.dim__article` AS a
WHERE
  (UPPER(article_name_current) LIKE '%RIGRAI%' OR UPPER(article_name_current) LIKE '%RI GRAI%' OR UPPER(article_name_current) LIKE '%RI-GRAI%') 
    AND UPPER(article_name_current) NOT LIKE '%OAK%'

UNION DISTINCT

-- Palmolive
SELECT
  DISTINCT online_brand,
  CASE
    WHEN article_id IN ('392795') THEN 'DISPUTED'
    ELSE 'Palmolive' 
  END AS brand_cleaned,
  a.article_id,
  a.article_name_current
FROM
  `gcp-wow-cd-data-engine-prod.consume_product_and_pricing.dim__article` AS a
WHERE ((UPPER(article_name_current) LIKE '%LMOLI%')
    OR UPPER(online_brand) LIKE '%LMOLI%' AND UPPER(article_name_current) NOT LIKE '%LMOLI%')
  AND a.article_id NOT IN ('759714','759725','779571','759743','759735','758994')

UNION DISTINCT

-- Purex
SELECT
  DISTINCT online_brand,
  'Purex' AS brand_cleaned,
  a.article_id,
  a.article_name_current
FROM
  `gcp-wow-cd-data-engine-prod.consume_product_and_pricing.dim__article` AS a
WHERE
  UPPER(article_name_current) LIKE '%PUREX%'

UNION DISTINCT

-- Smitten
SELECT
  DISTINCT online_brand,
  'Smitten' AS brand_cleaned,
  a.article_id,
  a.article_name_current
FROM
  `gcp-wow-cd-data-engine-prod.consume_product_and_pricing.dim__article` AS a
WHERE
  UPPER(article_name_current) LIKE '%SMITTEN%' AND UPPER(article_name_current) NOT LIKE '%APPLE%' AND UPPER(article_name_current) NOT LIKE '%BK%'
    AND article_id NOT IN ('765391','751127','8328745','8304364','436468')

UNION DISTINCT

-- Treasures
SELECT
  DISTINCT online_brand,
  'Treasures' AS brand_cleaned,
  a.article_id,
  a.article_name_current
FROM
  `gcp-wow-cd-data-engine-prod.consume_product_and_pricing.dim__article` AS a
WHERE
  (UPPER(article_name_current) LIKE '%TREASURES%' AND UPPER(article_name_current) NOT LIKE '%BK%' AND UPPER(online_brand) NOT LIKE '%FRIS%' AND UPPER(online_brand) NOT LIKE '%LITTLE%' 
          AND UPPER(online_brand) NOT LIKE '%VAU%' AND UPPER(online_brand) NOT LIKE '%WOW%'
          AND article_id NOT IN ('276475','140834','409849'))
    OR (article_id IN ('730841','686264','162381','162682','162451'))
    OR (UPPER(article_name_current) LIKE '%TREAS%' AND UPPER(article_name_current) LIKE '%NAPP%')

UNION DISTINCT

-- Tux
SELECT
  DISTINCT online_brand,
  'Tux' AS brand_cleaned,
  a.article_id,
  a.article_name_current
FROM
  `gcp-wow-cd-data-engine-prod.consume_product_and_pricing.dim__article` AS a
WHERE
  UPPER(online_brand) LIKE '%TUX%' AND UPPER(online_brand) NOT LIKE '%FORD%'
    
UNION DISTINCT

-- Vanish
SELECT
  DISTINCT online_brand,
  'Vanish' AS brand_cleaned,
  a.article_id,
  a.article_name_current
FROM
  `gcp-wow-cd-data-engine-prod.consume_product_and_pricing.dim__article` AS a
WHERE
  (UPPER(online_brand) LIKE '%VANISH%' OR UPPER(online_brand) LIKE '%NAPISA%' OR UPPER(online_brand) LIKE '%PREEN%' 
          OR a.article_id IN ('8042255','618646','215334','89896','215814','679358','215258','215334','618645','215258','679358','215814','89896','8042255','618646','299552','747296','327012'))
    
UNION DISTINCT

-- Watties
SELECT
  DISTINCT online_brand,
  'Watties' AS brand_cleaned,
  a.article_id,
  a.article_name_current
FROM
  `gcp-wow-cd-data-engine-prod.consume_product_and_pricing.dim__article` AS a
WHERE
  (UPPER(article_name_current) LIKE '%WATTI%' OR UPPER(article_name_current) LIKE '%WATI%' OR UPPER(online_brand) LIKE '%WATTI%')
   AND UPPER(online_brand) NOT LIKE '%BLUE%' AND UPPER(online_brand) NOT LIKE '%INGR%'
)



-- SKUs from BYB brands that have been ranged atleast once

SELECT
DISTINCT a.article_id,
brand_cleaned,
article_name_current AS article_name
FROM sku_full_list a
INNER JOIN `gcp-wow-food-de-pel-prod.datamart.store_article_detail` b 
ON a.article_id = b.article_id

)


#Come back and sort out the Colgate Palmolive products !!



-- SELECT article_id,
-- count(*) as _count
-- FROM byb_brand_article
-- GROUP BY article_id
-- HAVING _count >1
