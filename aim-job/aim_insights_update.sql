INSERT INTO `bqsqltesting.AIM.aim_token_fact`

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

-- Cast new columns (HB1..HB4, SKU1..SKU16) and align to positions 1..20
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
    m.Segment,
    m.Category,
    u.token_position,
    u.product_id,  -- INT64
    c.Manufacturer,
    c.Model_Name,
    c.OE
  FROM Unpivoted u
  LEFT JOIN Map m
    ON u.vehicle_key = m.vehicle_key
  LEFT JOIN `bqsqltesting.AIM.aim-catalogue` c
    ON u.product_id = SAFE_CAST(c.Product_ID AS INT64)
  WHERE u.product_id IS NOT NULL
)

SELECT
  CURRENT_DATE('Europe/London') AS snapshot_date,
  e.Manufacturer,
  e.Make,
  e.Model,
  e.Vehicle,
  e.Size,
  e.Segment,
  e.Category,
  e.Model_Name,
  e.OE,
  (e.OE IS NOT NULL AND CAST(e.OE AS STRING) <> '') AS has_oe,
  e.product_id AS sku,
  e.token_position,
  CASE
    WHEN e.token_position BETWEEN 1 AND 4 THEN CONCAT('Hotbox ', CAST(e.token_position AS STRING))
    ELSE 'Waves'
  END AS token_bucket,
  CASE WHEN e.token_position BETWEEN 1 AND 4 THEN TRUE ELSE FALSE END AS is_hotbox,
  CASE WHEN e.token_position BETWEEN 1 AND 4 THEN e.token_position END AS hotbox_n,

  -- Build CAM key (remove all spaces)
  UPPER(
    REGEXP_REPLACE(
      TRIM(CONCAT(
        CAST(e.Make AS STRING),
        CAST(e.Model AS STRING),
        CAST(REGEXP_EXTRACT(e.Size, r'^(\d+)')   AS STRING), -- Width
        CAST(REGEXP_EXTRACT(e.Size, r'/(\d+)\s') AS STRING), -- Profile
        CAST(REGEXP_EXTRACT(e.Size, r'R(\d+)$')  AS STRING)  -- Rim
      )),
      r'\s+', ''
    )
  ) AS CAM,

  -- Identify if CAM is in manual list
  CASE WHEN mc.CAM IS NOT NULL THEN 'manual' ELSE 'AIM' END AS CAMSource,

  -- NEW: distinct number of unique SKUs for this Make/Model/Width/Profile/Rim
  COUNT(DISTINCT e.product_id) OVER (
    PARTITION BY
      e.Make,
      e.Model,
      REGEXP_EXTRACT(e.Size, r'^(\d+)'),      -- Width
      REGEXP_EXTRACT(e.Size, r'/(\d+)\s'),    -- Profile
      REGEXP_EXTRACT(e.Size, r'R(\d+)$')      -- Rim
  ) AS distinct_sku_count

FROM Enriched e
LEFT JOIN (
  SELECT DISTINCT
    UPPER(REGEXP_REPLACE(CAM, r'\s+', '')) AS CAM
  FROM `bqsqltesting.AIM.aim-manual-cams`
) mc
  ON mc.CAM = UPPER(
                REGEXP_REPLACE(
                  TRIM(CONCAT(
                    CAST(e.Make AS STRING),
                    CAST(e.Model AS STRING),
                    CAST(REGEXP_EXTRACT(e.Size, r'^(\d+)')   AS STRING),
                    CAST(REGEXP_EXTRACT(e.Size, r'/(\d+)\s') AS STRING),
                    CAST(REGEXP_EXTRACT(e.Size, r'R(\d+)$')  AS STRING)
                  )),
                  r'\s+', ''
                )
              );
