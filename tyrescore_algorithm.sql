CREATE OR REPLACE TABLE `bqsqltesting.nexus_tyrescore.TyreScore_algorithm_output` AS
WITH
  -- CLICKSTREAM DATA
  CLICKSTREAM AS (
    SELECT
      items.item_id AS SKU,
      CONCAT(REGEXP_REPLACE(items.item_list_id, r'-', '/'), ' R', REGEXP_EXTRACT(items.item_list_id, r'(\d+)$')) AS Size,
      CASE
        WHEN (UPPER(REPLACE(items.item_category5, ' ', ''))) IS NULL THEN 'NO SALES YET'
        ELSE (UPPER(REPLACE(items.item_category5, ' ', '')))
      END AS Vehicle,
      COUNTIF(event_name = 'add_to_cart') AS ADDTOCART,
      COUNTIF(event_name = 'view_item_list') AS PRODUCTLISTVIEWS
    FROM
      `bqsqltesting.analytics_249515915.events_*`,
      UNNEST(items) AS items
    WHERE
      _TABLE_SUFFIX BETWEEN FORMAT_TIMESTAMP('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH))
      AND FORMAT_TIMESTAMP('%Y%m%d', CURRENT_DATE())
      AND REGEXP_CONTAINS(items.item_id, r'^\d+$')
    GROUP BY
      1,
      2,
      3
  ),
  -- TYRESCORE BASE
  TyreData AS (
    SELECT
      ts.ProductId,
      ts.Grade AS GRADE,
      ts.Size AS SIZE,
      REGEXP_EXTRACT(ts.Size, r'^(\d+/\d+\s*R\d+)') AS StrippedSize,
      ts.Manufacturer AS BRAND,
      ts.Model AS Model,
      ts.WetGrip AS WET_GRIP,
      ts.Fuel AS FUEL,
      ts.Noise AS NOISE_REDUCTION,
      AVG(ts.Price) AS PRICE,
      ts.Season AS SEASONAL_PERFORMANCE,
      ts.Offer AS OFFER,
      ts.OE AS OE,
      ts.AwardScore AS AWARD_SCORE,
      AVG(ts.Price) / NULLIF((AVG(ts.PrevPrice7) + AVG(ts.PrevPrice28)) / 2, 0) AS PRICEFLUCTUATION,
      CASE
        WHEN LOWER(pc.run_flat) = 'runflat' THEN 'Runflat'
        ELSE 'Non-Runflat'
      END AS RunflatStatus
    FROM
      bqsqltesting.nexus_tyrescore.TyreScore ts
    LEFT JOIN
      `bqsqltesting.Reference_Tables.Product_catalogue` pc ON CAST(ts.ProductId AS STRING) = pc.SKU -- Changed: Cast ts.ProductId to STRING to match pc.SKU (assuming SKU is STRING)
    GROUP BY
      1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14, 16
  ),
  ScoredTyres AS (
    SELECT
      TyreData.*,
      CASE
        WHEN AWARD_SCORE = 'Award' THEN 10
        ELSE 0
      END AS AwardScore,
      CASE
        WHEN FUEL = 'A' THEN 10
        WHEN FUEL = 'B' THEN 9
        WHEN FUEL = 'C' THEN 5
        WHEN FUEL = 'D' THEN 2.5
        WHEN FUEL = 'E' THEN 1
        ELSE 0
      END AS FuelScore,
      CASE
        WHEN NOISE_REDUCTION LIKE '%-A' THEN 10
        WHEN NOISE_REDUCTION LIKE '%-B' THEN 5
        WHEN NOISE_REDUCTION LIKE '%-C' THEN 1
        ELSE 0
      END AS NoiseScore,
      CASE
        WHEN OE = 'Yes' THEN 10
        ELSE 0
      END AS OEScore,
      0 AS ReviewScore,
      CASE
        WHEN WET_GRIP = 'A' THEN 10
        WHEN WET_GRIP = 'B' THEN 9
        WHEN WET_GRIP = 'C' THEN 5
        WHEN WET_GRIP = 'D' THEN 2.5
        WHEN WET_GRIP = 'E' THEN 1
        ELSE 0
      END AS WetGripScore
    FROM
      TyreData
  ),
  WeightedScores AS (
    SELECT
      ScoredTyres.*,
      AwardScore * 1.00 AS WeightedAwardScore,
      FuelScore * 1.00 AS WeightedFuelScore,
      NoiseScore * 0.10 AS WeightedNoiseScore,
      OEScore * 1.00 AS WeightedOEScore,
      ReviewScore * 0.50 AS WeightedReviewScore,
      WetGripScore * 0.50 AS WeightedWetGripScore
    FROM
      ScoredTyres
  ),
  FinalScores AS (
    SELECT
      WeightedScores.*,
      (
        WeightedAwardScore + WeightedFuelScore + WeightedNoiseScore + WeightedOEScore + WeightedReviewScore + WeightedWetGripScore
      ) AS TyreRank
    FROM
      WeightedScores
  ),
  RankedScores AS (
    SELECT
      FinalScores.*,
      NTILE(100) OVER (
        ORDER BY
          TyreRank DESC
      ) AS Percentile
    FROM
      FinalScores
  ),
  ScoredWithTier AS (
    SELECT
      RankedScores.*,
      CASE
        WHEN Percentile <= 25 THEN '1.BEST TYRE SCORE'
        WHEN Percentile <= 50 THEN '2.BETTER TYRE SCORE'
        WHEN Percentile <= 75 THEN '3.GOOD TYRE SCORE'
        WHEN Percentile <= 90 THEN '4.FAIR TYRE SCORE'
        ELSE 'Unclassified'
      END AS TyreScore
    FROM
      RankedScores
  ),
  -- VEHICLE ASSIGNMENT
  TyreSalesVolume AS (
    SELECT
      ProductId,
      TRIM(UPPER(CONCAT(CarMake, CarModel))) AS Vehicle,
      Orders,
      Units
    FROM
      `bqsqltesting.nexus_tyrescore.CarMakeModelSales`
  ),
  TYRESCORE AS (
    SELECT
      TS.*,
      TSV.Vehicle,
      TSV.Orders,
      TSV.Units
    FROM
      ScoredWithTier TS
    LEFT JOIN
      TyreSalesVolume TSV ON CAST(TS.ProductId AS STRING) = TSV.ProductId -- Changed: Cast TS.ProductId to STRING to match TSV.ProductId (assuming TSV.ProductId is STRING from CarMakeModelSales)
    WHERE TSV.ProductId IS NOT NULL
  ),
  -- GOLDILOCKS PRICES
  AVG_PRICES AS (
    SELECT
      TRIM(UPPER(CONCAT(CarMake, CarModel))) AS Vehicle,
      REGEXP_EXTRACT(Size, r'^\d+/\d+\s*[A-Z]?\d+') AS SizePart,
      AVG(CAST(AvgPrice AS FLOAT64)) AS AvgPricePerUnit -- Corrected: Cast AvgPrice to FLOAT64
    FROM
      `bqsqltesting.nexus_tyrescore.CarMakeModelSales` AS sales
    JOIN
      `bqsqltesting.nexus_tyrescore.TyreScore` AS ts ON CAST(sales.ProductId AS INT64) = ts.ProductId -- Kept as INT64 comparison, assuming ts.ProductId is INT64
    WHERE
      CAST(sales.Units AS INT64) > 0
    GROUP BY
      Vehicle,
      SizePart
  ),
  OverallAvgPrices AS (
    SELECT
      REGEXP_EXTRACT(Size, r'^\d+/\d+\s*[A-Z]?\d+') AS SizePart,
      AVG(CAST(AvgPrice AS FLOAT64)) AS AvgPricePerUnit -- Corrected: Cast AvgPrice to FLOAT64
    FROM
      `bqsqltesting.nexus_tyrescore.CarMakeModelSales` AS sales
    JOIN
      `bqsqltesting.nexus_tyrescore.TyreScore` AS ts ON CAST(sales.ProductId AS INT64) = ts.ProductId -- Kept as INT64 comparison
    WHERE
      CAST(sales.Units AS INT64) > 0
    GROUP BY
      SizePart
  ),
  -- MICROSEGMENT: GRADE
  GradeShare AS (
    SELECT
      TRIM(UPPER(CONCAT(CarMake, CarModel))) AS Vehicle,
      REGEXP_EXTRACT(ts.Size, r'^\d+/\d+\s*[A-Z]?\d+') AS SizePart,
      ts.Grade,
      SUM(CAST(sales.Units AS INT64)) AS UnitsByGrade
    FROM
      `bqsqltesting.nexus_tyrescore.CarMakeModelSales` AS sales
    JOIN
      `bqsqltesting.nexus_tyrescore.TyreScore` AS ts ON CAST(sales.ProductId AS INT64) = ts.ProductId -- Kept as INT64 comparison
    WHERE
      CAST(sales.Units AS INT64) > 0
    GROUP BY
      Vehicle,
      SizePart,
      ts.Grade
  ),
  TotalUnitsByCombo AS (
    SELECT
      Vehicle,
      SizePart,
      SUM(UnitsByGrade) AS TotalUnits
    FROM
      GradeShare
    GROUP BY
      Vehicle,
      SizePart
  ),
  GradeShareFinal AS (
    SELECT
      gs.Vehicle,
      gs.SizePart,
      gs.Grade,
      ROUND((gs.UnitsByGrade / NULLIF(tuc.TotalUnits, 0)) * 100, 2) AS SharePercent
    FROM
      GradeShare gs
    JOIN
      TotalUnitsByCombo tuc ON gs.Vehicle = tuc.Vehicle
      AND gs.SizePart = tuc.SizePart
  ),
  GradePivot AS (
    SELECT
      Vehicle,
      SizePart,
      MAX(IF(GRADE = 'Premium', SharePercent, 0)) AS PremiumShare,
      MAX(IF(GRADE = 'MidRange', SharePercent, 0)) AS MidRangeShare,
      MAX(IF(GRADE = 'Budget', SharePercent, 0)) AS BudgetShare
    FROM
      GradeShareFinal
    GROUP BY
      Vehicle,
      SizePart
  ),
  -- RUNFLAT SHARE
  RunflatShare AS (
    SELECT
      TRIM(UPPER(CONCAT(CarMake, CarModel))) AS Vehicle,
      REGEXP_EXTRACT(ts.Size, r'^\d+/\d+\s*[A-Z]?\d+') AS SizePart,
      SUM(
        CASE
          WHEN LOWER(pc.run_flat) = 'runflat' THEN CAST(sales.Units AS INT64)
          ELSE 0
        END
      ) AS RunflatUnits,
      SUM(CAST(sales.Units AS INT64)) AS TotalUnits
    FROM
      `bqsqltesting.nexus_tyrescore.CarMakeModelSales` AS sales
    JOIN
      `bqsqltesting.nexus_tyrescore.TyreScore` AS ts ON CAST(sales.ProductId AS INT64) = ts.ProductId -- Kept as INT64 comparison
    LEFT JOIN
      `bqsqltesting.Reference_Tables.Product_catalogue` AS pc ON CAST(ts.ProductId AS STRING) = pc.SKU -- Changed: Cast ts.ProductId to STRING to match pc.SKU
    WHERE
      CAST(sales.Units AS INT64) > 0
    GROUP BY
      Vehicle,
      SizePart
  ),
  RunflatPercent AS (
    SELECT
      Vehicle,
      SizePart,
      ROUND((RunflatUnits / NULLIF(TotalUnits, 0)) * 100, 2) AS RunflatShare
    FROM
      RunflatShare
  )
-- FINAL SELECT
SELECT
  -- PRODUCT + TYRESCORE INFO
  TYRESCORE.TyreScore,
  TYRESCORE.ProductId,
  TYRESCORE.GRADE,
  TYRESCORE.BRAND,
  TYRESCORE.Model,
  TYRESCORE.WET_GRIP,
  TYRESCORE.FUEL,
  TYRESCORE.NOISE_REDUCTION,
  TYRESCORE.SEASONAL_PERFORMANCE,
  TYRESCORE.OE,
  TYRESCORE.AWARD_SCORE,
  TYRESCORE.RunflatStatus,
  -- <<< INSERTED FIELD HERE
  CASE
    WHEN TI.Segment IS NULL THEN 'NO SALES YET'
    ELSE CAST(TI.Segment AS STRING) -- Cast to STRING for consistency
  END AS Segment,
  CASE
    WHEN TI.PRICE_pct IS NULL THEN 'NO SALES YET'
    ELSE CAST(TI.PRICE_pct AS STRING) -- Cast to STRING for consistency
  END AS PRICE_pct,
  CASE
    WHEN TI.GRADE_pct IS NULL THEN 'NO SALES YET'
    ELSE CAST(TI.GRADE_pct AS STRING) -- Cast to STRING for consistency
  END AS GRADE_pct,
  CASE
    WHEN TI.FUEL_pct IS NULL THEN 'NO SALES YET'
    ELSE CAST(TI.FUEL_pct AS STRING) -- Cast to STRING for consistency
  END AS FUEL_pct,
  CASE
    WHEN TI.WET_GRIP_pct IS NULL THEN 'NO SALES YET'
    ELSE CAST(TI.WET_GRIP_pct AS STRING) -- Cast to STRING for consistency
  END AS WET_GRIP_pct,
  CASE
    WHEN TI.AWARD_SCORE_pct IS NULL THEN 'NO SALES YET'
    ELSE CAST(TI.AWARD_SCORE_pct AS STRING) -- Cast to STRING for consistency
  END AS AWARD_SCORE_pct,

  -- VEHICLE AND SIZE
  CASE
    WHEN TYRESCORE.Vehicle IS NULL THEN 'NO SALES YET'
    ELSE TYRESCORE.Vehicle
  END AS Vehicle,
  TYRESCORE.SIZE,

  -- PRICES
  TYRESCORE.PRICE,
  TYRESCORE.OFFER,
  ROUND(COALESCE(TYRESCORE.PRICEFLUCTUATION, 1), 2) AS PRICEFLUCTUATION,
  IFNULL(CAST(TYRESCORE.Orders AS INT64), 0) AS Orders, -- Ensure Orders is INT64
  IFNULL(CAST(TYRESCORE.Units AS INT64), 0) AS Units, -- Ensure Units is INT64
  IFNULL(
    NULLIF(ROUND(CAST(AP.AvgPricePerUnit AS FLOAT64), 2), CAST(0.0 AS FLOAT64)), -- Ensure AP.AvgPricePerUnit is FLOAT64 and literal 0.0 is FLOAT64
    ROUND(CAST(OAP.AvgPricePerUnit AS FLOAT64), 2) -- Ensure OAP.AvgPricePerUnit is FLOAT64
  ) AS GoldilocksZone,

  -- MICROSEGMENT BEHAVIOUR
  IFNULL(GP.PremiumShare, 0.0) AS PremiumShare,
  IFNULL(GP.MidRangeShare, 0.0) AS MidRangeShare,
  IFNULL(GP.BudgetShare, 0.0) AS BudgetShare,
  IFNULL(RP.RunflatShare, 0.0) AS RunflatShare,

  -- FUTURE SUPERSTAR POTENTIAL
  CASE
    WHEN TYRESCORE.Vehicle IS NULL THEN 'Potential'
    ELSE 'Active'
  END AS SalesStatus,

  -- CLICKSTREAM METRICS
  COALESCE(CAST(CS.PRODUCTLISTVIEWS AS INT64), 0) AS PRODUCTLISTVIEWS, -- Explicitly cast CS.PRODUCTLISTVIEWS to INT64
  ROUND(
    CASE
      WHEN SAFE_DIVIDE(IFNULL(CAST(TYRESCORE.Orders AS INT64), 0), COALESCE(CAST(CS.PRODUCTLISTVIEWS AS INT64), 0)) IS NULL THEN 0
      WHEN SAFE_DIVIDE(IFNULL(CAST(TYRESCORE.Orders AS INT64), 0), COALESCE(CAST(CS.PRODUCTLISTVIEWS AS INT64), 0)) * 100 >= 1 THEN 0.14
      ELSE SAFE_DIVIDE(IFNULL(CAST(TYRESCORE.Orders AS INT64), 0), COALESCE(CAST(CS.PRODUCTLISTVIEWS AS INT64), 0)) * 100
    END,
    3
  ) AS CLICKSTREAMRATE
FROM
  TYRESCORE
LEFT JOIN
  CLICKSTREAM CS ON CAST(CS.SKU AS INT64) = TYRESCORE.ProductId
  AND CS.Vehicle = TYRESCORE.Vehicle
LEFT JOIN
  AVG_PRICES AP ON TYRESCORE.Vehicle = AP.Vehicle
  AND REGEXP_EXTRACT(TYRESCORE.SIZE, r'^\d+/\d+\s*[A-Z]?\d+') = AP.SizePart
LEFT JOIN
  OverallAvgPrices OAP ON REGEXP_EXTRACT(TYRESCORE.SIZE, r'^\d+/\d+\s*[A-Z]?\d+') = OAP.SizePart
LEFT JOIN
  GradePivot GP ON TYRESCORE.Vehicle = GP.Vehicle
  AND REGEXP_EXTRACT(TYRESCORE.SIZE, r'^\d+/\d+\s*[A-Z]?\d+') = GP.SizePart
LEFT JOIN
  RunflatPercent RP ON TYRESCORE.Vehicle = RP.Vehicle
  AND REGEXP_EXTRACT(TYRESCORE.SIZE, r'^\d+/\d+\s*[A-Z]?\d+') = RP.SizePart
LEFT JOIN
  `bqsqltesting.nexus_tyrescore.tyrescore-influence` TI ON UPPER(REGEXP_REPLACE(TRIM(TYRESCORE.Vehicle), r'\s+', '')) = UPPER(TI.Vehicle) --Join the influence table and cast to uppercase
WHERE
  TYRESCORE.ProductId IS NOT NULL
  AND TYRESCORE.WET_GRIP IS NOT NULL;
