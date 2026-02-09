MERGE `{cam_table_id}` T
USING (
  WITH Map AS (
    SELECT
      REGEXP_REPLACE(LOWER(TRIM(vehicle)), r'[^a-z0-9]', '') AS vehicle_key,
      CarMake,
      CarModel
    FROM `bqsqltesting.AIM.aim-vehiclemapping`
  ),

  Stage AS (
    SELECT
      -- Vehicle mapping key
      REGEXP_REPLACE(LOWER(TRIM(S.Vehicle)), r'[^a-z0-9]', '') AS vehicle_key,

      -- Parse W/P/R from Size (e.g. "225/40 R18", "225/40ZR18")
      trim(REGEXP_EXTRACT(UPPER(S.Size), r'^\s*(\d{3})')) AS Width,
      trim(REGEXP_EXTRACT(UPPER(S.Size), r'^\s*\d{3}\s*/\s*(\d{2})')) AS Profile,
      trim(REGEXP_EXTRACT(UPPER(S.Size), r'R\s*(\d{2})\s*$')) AS Rim,

      -- Canonical make/model from mapping
      m.CarMake AS Make,
      m.CarModel AS Model,

      -- HB1–HB4 → SKU1–SKU4
      NULLIF(TRIM(SAFE_CAST(S.HB1 AS STRING)), '') AS SKU1,
      NULLIF(TRIM(SAFE_CAST(S.HB2 AS STRING)), '') AS SKU2,
      NULLIF(TRIM(SAFE_CAST(S.HB3 AS STRING)), '') AS SKU3,
      NULLIF(TRIM(SAFE_CAST(S.HB4 AS STRING)), '') AS SKU4,

      -- SKU1–20 → SKU5–24
      NULLIF(TRIM(SAFE_CAST(S.SKU1  AS STRING)), '') AS SKU5,
      NULLIF(TRIM(SAFE_CAST(S.SKU2  AS STRING)), '') AS SKU6,
      NULLIF(TRIM(SAFE_CAST(S.SKU3  AS STRING)), '') AS SKU7,
      NULLIF(TRIM(SAFE_CAST(S.SKU4  AS STRING)), '') AS SKU8,
      NULLIF(TRIM(SAFE_CAST(S.SKU5  AS STRING)), '') AS SKU9,
      NULLIF(TRIM(SAFE_CAST(S.SKU6  AS STRING)), '') AS SKU10,
      NULLIF(TRIM(SAFE_CAST(S.SKU7  AS STRING)), '') AS SKU11,
      NULLIF(TRIM(SAFE_CAST(S.SKU8  AS STRING)), '') AS SKU12,
      NULLIF(TRIM(SAFE_CAST(S.SKU9  AS STRING)), '') AS SKU13,
      NULLIF(TRIM(SAFE_CAST(S.SKU10 AS STRING)), '') AS SKU14,
      NULLIF(TRIM(SAFE_CAST(S.SKU11 AS STRING)), '') AS SKU15,
      NULLIF(TRIM(SAFE_CAST(S.SKU12 AS STRING)), '') AS SKU16,
      NULLIF(TRIM(SAFE_CAST(S.SKU13 AS STRING)), '') AS SKU17,
      NULLIF(TRIM(SAFE_CAST(S.SKU14 AS STRING)), '') AS SKU18,
      NULLIF(TRIM(SAFE_CAST(S.SKU15 AS STRING)), '') AS SKU19,
      NULLIF(TRIM(SAFE_CAST(S.SKU16 AS STRING)), '') AS SKU20,
      NULLIF(TRIM(SAFE_CAST(S.SKU17 AS STRING)), '') AS SKU21,
      NULLIF(TRIM(SAFE_CAST(S.SKU18 AS STRING)), '') AS SKU22,
      NULLIF(TRIM(SAFE_CAST(S.SKU19 AS STRING)), '') AS SKU23,
      NULLIF(TRIM(SAFE_CAST(S.SKU20 AS STRING)), '') AS SKU24,


      SAFE_CAST(S.last_modified AS TIMESTAMP) AS last_modified
    FROM `{staging_table_id}` S
    LEFT JOIN Map m
      ON REGEXP_REPLACE(LOWER(TRIM(S.Vehicle)), r'[^a-z0-9]', '') = m.vehicle_key
    WHERE
      m.CarMake IS NOT NULL
      AND m.CarModel IS NOT NULL
      -- only accept rows where Size parses cleanly
      AND REGEXP_CONTAINS(UPPER(S.Size), r'^\s*\d{3}\s*/\s*\d{2}\s*[A-Z]*\s*R\s*\d{2}\s*$')
  ),

  -- one row per unique key from staging (newest wins)
  StageDeduped AS (
    SELECT * EXCEPT(rn)
    FROM (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY UPPER(TRIM(Make)), UPPER(TRIM(Model)), Width, Profile, Rim
          ORDER BY last_modified DESC
        ) AS rn
      FROM Stage
    )
    WHERE rn = 1
  )

  SELECT * FROM StageDeduped
) S

ON
  UPPER(TRIM(T.Make))  = UPPER(TRIM(S.Make))
  AND UPPER(TRIM(T.Model)) = UPPER(TRIM(S.Model))
  AND TRIM(T.Width)   = S.Width
  AND TRIM(T.Profile) = S.Profile
  AND TRIM(T.Rim)     = S.Rim

WHEN MATCHED AND S.last_modified >= T.last_modified THEN
  UPDATE SET
    -- keep canonical mapped make/model
    Make  = S.Make,
    Model = S.Model,

    -- only overwrite if incoming not NULL/blank and not '-'
    SKU1  = COALESCE(NULLIF(S.SKU1,  '-'), T.SKU1),
    SKU2  = COALESCE(NULLIF(S.SKU2,  '-'), T.SKU2),
    SKU3  = COALESCE(NULLIF(S.SKU3,  '-'), T.SKU3),
    SKU4  = COALESCE(NULLIF(S.SKU4,  '-'), T.SKU4),
    SKU5  = COALESCE(NULLIF(S.SKU5,  '-'), T.SKU5),
    SKU6  = COALESCE(NULLIF(S.SKU6,  '-'), T.SKU6),
    SKU7  = COALESCE(NULLIF(S.SKU7,  '-'), T.SKU7),
    SKU8  = COALESCE(NULLIF(S.SKU8,  '-'), T.SKU8),
    SKU9  = COALESCE(NULLIF(S.SKU9,  '-'), T.SKU9),
    SKU10 = COALESCE(NULLIF(S.SKU10, '-'), T.SKU10),
    SKU11 = COALESCE(NULLIF(S.SKU11, '-'), T.SKU11),
    SKU12 = COALESCE(NULLIF(S.SKU12, '-'), T.SKU12),
    SKU13 = COALESCE(NULLIF(S.SKU13, '-'), T.SKU13),
    SKU14 = COALESCE(NULLIF(S.SKU14, '-'), T.SKU14),
    SKU15 = COALESCE(NULLIF(S.SKU15, '-'), T.SKU15),
    SKU16 = COALESCE(NULLIF(S.SKU16, '-'), T.SKU16),
    SKU17 = COALESCE(NULLIF(S.SKU17, '-'), T.SKU17),
    SKU18 = COALESCE(NULLIF(S.SKU18, '-'), T.SKU18),
    SKU19 = COALESCE(NULLIF(S.SKU19, '-'), T.SKU19),
    SKU20 = COALESCE(NULLIF(S.SKU20, '-'), T.SKU20),
    SKU21 = COALESCE(NULLIF(S.SKU21, '-'), T.SKU21),
    SKU22 = COALESCE(NULLIF(S.SKU22, '-'), T.SKU22),
    SKU23 = COALESCE(NULLIF(S.SKU23, '-'), T.SKU23),
    SKU24 = COALESCE(NULLIF(S.SKU24, '-'), T.SKU24),

    last_modified = S.last_modified

WHEN NOT MATCHED THEN
  INSERT (
    Make, Model, Width, Profile, Rim,
    SKU1, SKU2, SKU3, SKU4, SKU5, SKU6, SKU7, SKU8, SKU9, SKU10,
    SKU11, SKU12, SKU13, SKU14, SKU15, SKU16, SKU17, SKU18, SKU19, SKU20,
    SKU21, SKU22, SKU23, SKU24,
    last_modified
  )
  VALUES (
    S.Make, S.Model, S.Width, S.Profile, S.Rim,
    NULLIF(S.SKU1,  '-'), NULLIF(S.SKU2,  '-'), NULLIF(S.SKU3,  '-'), NULLIF(S.SKU4,  '-'),
    NULLIF(S.SKU5,  '-'), NULLIF(S.SKU6,  '-'), NULLIF(S.SKU7,  '-'), NULLIF(S.SKU8,  '-'),
    NULLIF(S.SKU9,  '-'), NULLIF(S.SKU10, '-'),
    NULLIF(S.SKU11, '-'), NULLIF(S.SKU12, '-'), NULLIF(S.SKU13, '-'), NULLIF(S.SKU14, '-'),
    NULLIF(S.SKU15, '-'), NULLIF(S.SKU16, '-'), NULLIF(S.SKU17, '-'), NULLIF(S.SKU18, '-'),
    NULLIF(S.SKU19, '-'), NULLIF(S.SKU20, '-'),
    NULLIF(S.SKU21, '-'), NULLIF(S.SKU22, '-'), NULLIF(S.SKU23, '-'), NULLIF(S.SKU24, '-'),
    S.last_modified
  );
