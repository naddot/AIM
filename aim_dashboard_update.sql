INSERT INTO `bqsqltesting.AIM.AIM-override-file`

WITH Map AS (
  SELECT
    REGEXP_REPLACE(vehicle, r'\s+', '') AS vehicle_key,
    CarMake,
    CarModel,
    Pod,
    Category,
    Segment
  FROM `bqsqltesting.AIM.aim-vehiclemapping`
),

-- Map HB1..HB4 + SKU1..SKU16 onto positions 1..20
SourceCast AS (
  SELECT
    REGEXP_REPLACE(Vehicle, r'\s+', '') AS vehicle_key,
    Vehicle,
    Size,
    SAFE_CAST(HB1   AS INT64)  AS t1,
    SAFE_CAST(HB2   AS INT64)  AS t2,
    SAFE_CAST(HB3   AS INT64)  AS t3,
    SAFE_CAST(HB4   AS INT64)  AS t4,
    SAFE_CAST(SKU1  AS INT64)  AS t5,
    SAFE_CAST(SKU2  AS INT64)  AS t6,
    SAFE_CAST(SKU3  AS INT64)  AS t7,
    SAFE_CAST(SKU4  AS INT64)  AS t8,
    SAFE_CAST(SKU5  AS INT64)  AS t9,
    SAFE_CAST(SKU6  AS INT64)  AS t10,
    SAFE_CAST(SKU7  AS INT64)  AS t11,
    SAFE_CAST(SKU8  AS INT64)  AS t12,
    SAFE_CAST(SKU9  AS INT64)  AS t13,
    SAFE_CAST(SKU10 AS INT64)  AS t14,
    SAFE_CAST(SKU11 AS INT64)  AS t15,
    SAFE_CAST(SKU12 AS INT64)  AS t16,
    SAFE_CAST(SKU13 AS INT64)  AS t17,
    SAFE_CAST(SKU14 AS INT64)  AS t18,
    SAFE_CAST(SKU15 AS INT64)  AS t19,
    SAFE_CAST(SKU16 AS INT64)  AS t20
  FROM `bqsqltesting.AIM.AIMData`
  WHERE HB1 != '0'
),

Unpivoted AS (
  SELECT
    vehicle_key,
    vehicle,
    size,
    token_position,
    product_id
  FROM SourceCast
  UNPIVOT (
    product_id FOR token_position IN (
      t1  AS 1,  t2  AS 2,  t3  AS 3,  t4  AS 4,
      t5  AS 5,  t6  AS 6,  t7  AS 7,  t8  AS 8,
      t9  AS 9,  t10 AS 10, t11 AS 11, t12 AS 12,
      t13 AS 13, t14 AS 14, t15 AS 15, t16 AS 16,
      t17 AS 17, t18 AS 18, t19 AS 19, t20 AS 20
    )
  )
),

Enriched AS (
  SELECT
    m.CarMake    AS Make,
    m.CarModel   AS Model,
    u.vehicle    AS Vehicle,
    u.size       AS Size,
    REGEXP_EXTRACT(u.size, r'^(\d+)')   AS Width,
    REGEXP_EXTRACT(u.size, r'/(\d+)\s') AS Profile,
    REGEXP_EXTRACT(u.size, r'R(\d+)$')  AS Rim,
    LOWER(CONCAT(
      'aim-',
      REGEXP_REPLACE(m.CarMake,  r'\s+', ''), '-',
      REGEXP_REPLACE(m.CarModel, r'\s+', ''), '-',
      REGEXP_EXTRACT(u.size, r'^(\d+)'), '-',
      REGEXP_EXTRACT(u.size, r'/(\d+)\s'), '-',
      REGEXP_EXTRACT(u.size, r'R(\d+)$')
    )) AS TakeoverName,
    m.Segment,
    m.Category,
    u.token_position,
    u.product_id,
    c.Manufacturer,
    c.Model_Name,
    c.OE
  FROM Unpivoted u
  LEFT JOIN Map m
    ON u.vehicle_key = m.vehicle_key
  LEFT JOIN `bqsqltesting.AIM.aim-catalogue` c
    ON u.product_id = SAFE_CAST(c.Product_ID AS INT64)
  WHERE u.product_id IS NOT NULL
),

Aggregated AS (
  SELECT
    CURRENT_DATE('Europe/London') AS Today,
    Make,
    Model,
    Width,
    Profile,
    Rim,
    MAX(IF(token_position = 1,  product_id, NULL))  AS SKU,
    TakeoverName,
    '' AS RibbonName,
    MAX(IF(token_position = 2,  product_id, NULL))  AS SKU2,
    '' AS RibbonName2,
    MAX(IF(token_position = 3,  product_id, NULL))  AS SKU3,
    '' AS RibbonName3,
    MAX(IF(token_position = 4,  product_id, NULL))  AS SKU4,
    '' AS RibbonName4,
    'UK' AS Country,
    ""  AS Year,
    4   AS CAMUSlot,
    MAX(IF(token_position = 5,  product_id, NULL))  AS SKU5,
    '' AS RibbonName5,
    MAX(IF(token_position = 6,  product_id, NULL))  AS SKU6,
    '' AS RibbonName6,
    MAX(IF(token_position = 7,  product_id, NULL))  AS SKU7,
    '' AS RibbonName7,
    MAX(IF(token_position = 8,  product_id, NULL))  AS SKU8,
    '' AS RibbonName8,
    MAX(IF(token_position = 9,  product_id, NULL))  AS SKU9,
    '' AS RibbonName9,
    MAX(IF(token_position = 10, product_id, NULL))  AS SKU10,
    '' AS RibbonName10,
    MAX(IF(token_position = 11, product_id, NULL))  AS SKU11,
    '' AS RibbonName11,
    MAX(IF(token_position = 12, product_id, NULL))  AS SKU12,
    '' AS RibbonName12,
    MAX(IF(token_position = 13, product_id, NULL))  AS SKU13,
    '' AS RibbonName13,
    MAX(IF(token_position = 14, product_id, NULL))  AS SKU14,
    '' AS RibbonName14,
    MAX(IF(token_position = 15, product_id, NULL))  AS SKU15,
    '' AS RibbonName15,
    MAX(IF(token_position = 16, product_id, NULL))  AS SKU16,
    '' AS RibbonName16,
    MAX(IF(token_position = 17, product_id, NULL))  AS SKU17,
    '' AS RibbonName17,
    MAX(IF(token_position = 18, product_id, NULL))  AS SKU18,
    '' AS RibbonName18,
    MAX(IF(token_position = 19, product_id, NULL))  AS SKU19,
    '' AS RibbonName19,
    MAX(IF(token_position = 20, product_id, NULL))  AS SKU20,
    '' AS RibbonName20,
    ANY_VALUE(Manufacturer) AS Manufacturer,
    ANY_VALUE(Model_Name)   AS ModelName,
    ANY_VALUE(OE)           AS OE,
    Segment,
    Category,
    Size,
    Vehicle,
    UPPER(
      REGEXP_REPLACE(
        TRIM(CONCAT(
          CAST(Make   AS STRING),
          CAST(Model  AS STRING),
          CAST(Width  AS STRING),
          CAST(Profile AS STRING),
          CAST(Rim    AS STRING)
        )),
        r'\s+', ''
      )
    ) AS CAM
  FROM Enriched
  GROUP BY
    Make, Model, Width, Profile, Rim, TakeoverName, Segment, Category, Size, Vehicle
),

DistinctCount AS (
  SELECT
    Make, Model, Width, Profile, Rim,
    COUNT(DISTINCT product_id) AS distinct_sku_count
  FROM Enriched
  GROUP BY Make, Model, Width, Profile, Rim
)

SELECT
  a.*,
  d.distinct_sku_count,
  CASE WHEN mc.CAM IS NOT NULL THEN 'manual' ELSE 'AIM' END AS CAMSource
FROM Aggregated a
LEFT JOIN (
  SELECT DISTINCT
    UPPER(REGEXP_REPLACE(CAM, r'\s+', '')) AS CAM
  FROM `bqsqltesting.AIM.aim-manual-cams`
) mc
  ON mc.CAM = a.CAM
LEFT JOIN DistinctCount d
  USING (Make, Model, Width, Profile, Rim)
ORDER BY
  Make, Model, CAST(Rim AS INT64), CAST(Width AS INT64), CAST(Profile AS INT64);
