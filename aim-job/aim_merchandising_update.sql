DECLARE start_output_date DATE DEFAULT DATE_SUB(CURRENT_DATE('Europe/London'), INTERVAL 21 DAY);
DECLARE end_output_date   DATE DEFAULT DATE_SUB(CURRENT_DATE('Europe/London'), INTERVAL 1  DAY);
DECLARE start_scan_date   DATE DEFAULT DATE_SUB(start_output_date, INTERVAL 90 DAY);  -- scan farther back to find prior views

-- >>> SET THESE TO YOUR TRACKING-ISSUE DATES <<<
DECLARE bad_start_date DATE DEFAULT DATE '2025-10-27';
DECLARE bad_end_date   DATE DEFAULT DATE '2025-11-09';

BEGIN TRANSACTION;

-- 1) Remove existing rows in the rolling window (partition-pruned)
DELETE FROM `bqsqltesting.AIM.aim-merchandising-report`
WHERE Date BETWEEN start_output_date AND end_output_date;

-- -------------------------------------------------------------------------------------------------
-- OPTIMIZATION: Create a temporary table to scan the raw events ONCE.
-- Refined per review:
--   - Inner subquery extracts raw fields (user_id, page_location, item_category8) strictly ONCE.
--   - Outer query filters NULL user_id and computes derived fields (UPPER, REGEXP).
--   - Filters: Time window (90 days), Event types (view_item_list, purchase), United Kingdom.
-- -------------------------------------------------------------------------------------------------
CREATE TEMP TABLE base_events AS
SELECT
  event_date,
  event_timestamp,
  user_id,
  event_name,
  
  -- Item fields
  item_list_id,
  item_list_index,
  item_category5,
  item_brand,
  item_category,
  item_variant,
  item_id,
  price,
  quantity,

  -- Computed fields (CPU Saver)
  item_category8,
  UPPER(item_category8) AS item_category8_u,
  page_location_full,
  REGEXP_EXTRACT(page_location_full, r'=([^&?]*)') AS page_location_cam,
  transaction_id

FROM (
  SELECT
    PARSE_DATE('%Y%m%d', e.event_date) AS event_date,
    TIMESTAMP_MICROS(e.event_timestamp) AS event_timestamp,
    bqsqltesting.functions.BC_UID(e.user_pseudo_id, e.user_id) AS user_id,
    e.event_name,
    
    it.item_list_id,
    it.item_list_index,
    it.item_category5,
    it.item_brand,
    it.item_category,
    it.item_variant,
    it.item_id,
    it.price,
    it.quantity,

    -- Extract ONCE in inner query
    (SELECT ip.value.string_value FROM UNNEST(it.item_params) ip WHERE ip.key = 'item_category8') AS item_category8,
    (SELECT ep.value.string_value FROM UNNEST(e.event_params) ep WHERE ep.key = 'page_location') AS page_location_full,
    (SELECT ep.value.string_value FROM UNNEST(e.event_params) ep WHERE ep.key = 'transaction_id') AS transaction_id

  FROM `bqsqltesting.analytics_249515915.events_*` e, UNNEST(e.items) it
  WHERE
    e._TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', start_scan_date) AND FORMAT_DATE('%Y%m%d', end_output_date)
    AND e.event_name IN ('view_item_list', 'purchase')
    AND e.geo.country = 'United Kingdom'
)
WHERE user_id IS NOT NULL;


-- 2) Recompute the rolling window using the TEMP TABLE
INSERT INTO `bqsqltesting.AIM.aim-merchandising-report`
WITH
visibility_raw AS (
  SELECT
    event_date AS snapshot_date,
    event_timestamp AS visit_ts,
    user_id,
    'United Kingdom' AS country,
    event_name AS event,
    page_location_cam,
    -- item_category8_u is already available

    CASE
    --AIM
      WHEN item_category8_u LIKE 'PSUCMM-AIM%'    THEN 'AIM U - SWITCH'
      WHEN item_category8_u LIKE 'SUCMM-AIM%'     THEN 'AIM U'
      WHEN item_category8_u LIKE 'PXSUCMM-AIM%'   THEN 'AIM U - SWITCH'
      WHEN item_category8_u LIKE 'XSUCMM-AIM%'    THEN 'AIM U'
      WHEN item_category8_u LIKE '%-AIM%'         THEN 'AIM'
      WHEN item_category8_u LIKE 'PSCMM-AIM%'     THEN 'AIM - SWITCH'

    --CAM WAVES U
      WHEN item_category8_u LIKE 'SUCMM%'         THEN 'CAM WAVES U'
      WHEN item_category8_u LIKE 'SUCMM-MAN%'     THEN 'CAM WAVES U'
      WHEN item_category8_u LIKE 'XSUCMM%'        THEN 'CAM WAVES U'
      WHEN item_category8_u LIKE 'SUCMM-NONCAM%'  THEN 'CAM WAVES U'
      WHEN item_category8_u LIKE 'PSUCMM%'        THEN 'CAM WAVES U - SWITCH'
      WHEN item_category8_u LIKE 'PSUCMM-MAN%'    THEN 'CAM WAVES U - SWITCH'
      WHEN item_category8_u LIKE 'PXSUCMM%'       THEN 'CAM WAVES U - SWITCH'
      WHEN item_category8_u LIKE 'PSUCMM-NONCAM%' THEN 'CAM WAVES U - SWITCH'

    --NON MERCH U
      WHEN item_category8_u LIKE 'XSUCMM-NONCAM%' THEN 'CAM U'
      WHEN item_category8_u LIKE 'SUCMM-NONCAM%'  THEN 'CAM U'
      WHEN item_category8_u LIKE 'XUCMM-NONCAM%'  THEN 'CAM U'
      WHEN item_category8_u LIKE 'PXSUCMM-NONCAM%' THEN 'CAM U - SWITCH'
      WHEN item_category8_u LIKE 'PSUCMM-NONCAM%' THEN 'CAM U - SWITCH'
      WHEN item_category8_u LIKE 'PXUCMM-NONCAM%' THEN 'CAM U - SWITCH'

    --CAM U
      WHEN item_category8_u LIKE 'UCMM%'          THEN 'CAM U'
      WHEN item_category8_u LIKE 'XUCMM%'         THEN 'CAM U'
      WHEN item_category8_u LIKE 'UCMM-NONCAM%'   THEN 'CAM U'
      WHEN item_category8_u LIKE 'PUCMM%'         THEN 'CAM U - SWITCH'
      WHEN item_category8_u LIKE 'PXUCMM%'        THEN 'CAM U - SWITCH'
      WHEN item_category8_u LIKE 'PUCMM-NONCAM%'  THEN 'CAM U - SWITCH'

    --CAM WAVES
      WHEN item_category8_u LIKE 'SCMM%'          THEN 'CAM WAVES'
      WHEN item_category8_u LIKE 'PSCMM%'         THEN 'CAM WAVES - SWITCH'

    --CAM
      WHEN item_category8_u LIKE 'CMM%'           THEN 'CAM'
      WHEN item_category8_u LIKE 'PCMM%'          THEN 'CAM - SWITCH'

    --SIZE
      WHEN UPPER(TRIM(item_category5)) = 'NO VRM SEARCH' THEN 'SIZE'

    --NO VRM
      WHEN item_category8_u = 'NONCAM'            THEN 'NON MERCH VRM'

      ELSE 'NON MERCH VRM'
    END AS journey_category,
    REGEXP_REPLACE(LOWER(TRIM(item_category5)), r'[^a-z0-9]', '') AS vehicle,

    CASE
      WHEN item_list_index = '(not set)' THEN NULL
      WHEN SAFE_CAST(item_list_index AS INT64) = 200 THEN 4
      ELSE SAFE_CAST(item_list_index AS INT64)
    END AS SlotPosition,

    item_list_id AS Size,
    item_brand    AS Manufacturer,
    item_category AS ModelName,
    item_variant  AS Variant,
    item_id       AS SKU,

    CASE
      WHEN SAFE_CAST(item_list_index AS INT64) = 1   THEN 'Hotbox 1'
      WHEN SAFE_CAST(item_list_index AS INT64) = 2   THEN 'Hotbox 2'
      WHEN SAFE_CAST(item_list_index AS INT64) = 3   THEN 'Hotbox 3'
      WHEN SAFE_CAST(item_list_index AS INT64) IN (4,200) THEN 'Hotbox 4'
      WHEN SAFE_CAST(item_list_index AS INT64) = 99  THEN 'BrandSelector'
      ELSE 'Waves'
    END AS MerchandisingPosition
  FROM base_events
  WHERE
    event_name = 'view_item_list'
    AND NOT REGEXP_CONTAINS(
      page_location_full,
      'fitfast|truck|manufacturer|seasonal|runflat|reinforced|loadratings|carmanufacturer|speedratings|staging|inforgen'
    )
),

visibility_best AS (
  SELECT *
  FROM (
    SELECT
      v.*,
      ROW_NUMBER() OVER (
        PARTITION BY user_id, SKU, visit_ts
        ORDER BY
          COALESCE(SAFE_CAST(REGEXP_EXTRACT(MerchandisingPosition, r'Hotbox (\d+)') AS INT64),
                   CASE WHEN MerchandisingPosition = 'BrandSelector' THEN 99 ELSE 100 END),
          SlotPosition
      ) AS rn
    FROM visibility_raw v
  )
  WHERE rn = 1
),

visibility_best_flagged AS (
  SELECT
    vb.*,
    ROW_NUMBER() OVER (PARTITION BY vb.snapshot_date, vb.user_id ORDER BY vb.visit_ts) AS rn_user_day,
    ROW_NUMBER() OVER (PARTITION BY vb.snapshot_date, vb.user_id, vb.journey_category ORDER BY vb.visit_ts) AS rn_user_day_journey
  FROM visibility_best vb
),

purchases_raw AS (
  SELECT
    event_date AS purchase_date,
    event_timestamp AS purchase_ts,
    user_id,
    item_id AS SKU,
    COALESCE(quantity, 1) AS Units,
    COALESCE(price, 0) * COALESCE(quantity, 1) AS ItemRevenue,
    transaction_id
  FROM base_events
  WHERE
    event_name = 'purchase'
),

purchase_attribution AS (
  SELECT
    p.transaction_id, p.user_id, p.SKU, p.Units, p.ItemRevenue,
    v.visit_ts
  FROM purchases_raw p
  JOIN visibility_best v
    ON v.user_id = p.user_id
   AND v.SKU    = p.SKU
   AND v.visit_ts <= p.purchase_ts
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY p.transaction_id, p.user_id, p.SKU
    ORDER BY v.visit_ts DESC
  ) = 1
),

purchases_by_visit AS (
  SELECT
    user_id,
    SKU,
    visit_ts,
    COUNT(DISTINCT transaction_id) AS Orders,
    COUNT(DISTINCT user_id)        AS Purchasers,
    SUM(Units)                    AS PurchasedUnits,
    SUM(ItemRevenue)              AS PurchasedRevenue
  FROM purchase_attribution
  GROUP BY 1,2,3
),

vehicle_map AS (
  SELECT
    REGEXP_REPLACE(LOWER(TRIM(vehicle)), r'[^a-z0-9]', '') AS vehicle_key,
    ANY_VALUE(Segment)  AS Segment,
    ANY_VALUE(Pod)      AS Pod,
    ANY_VALUE(Category) AS Category
  FROM `bqsqltesting.AIM.aim-vehiclemapping`
  GROUP BY 1
),

final AS (
  SELECT
    v.snapshot_date AS Date,
    v.journey_category,
    v.vehicle AS Vehicle,
    v.Size,
    v.SlotPosition,
    v.MerchandisingPosition,
    v.Manufacturer,
    v.ModelName,
    v.Variant,
    v.SKU,

    -- UNIQUE USER FLAGS
    CASE WHEN vbf.rn_user_day = 1 THEN 1 ELSE 0 END AS UsersUniqueDay,

    vm.Segment,
    vm.Pod,
    vm.Category,
    COALESCE(pv.Orders, 0)           AS Orders,
    COALESCE(pv.Purchasers, 0)       AS Purchasers,
    COALESCE(pv.PurchasedUnits, 0)   AS PurchasedUnits,
    COALESCE(pv.PurchasedRevenue, 0) AS PurchasedRevenue,
    CASE WHEN vbf.rn_user_day_journey = 1 THEN 1 ELSE 0 END AS UsersUniqueDayJourney
  FROM visibility_best_flagged vbf
  JOIN visibility_best v
    ON v.user_id   = vbf.user_id
   AND v.SKU      = vbf.SKU
   AND v.visit_ts = vbf.visit_ts
  LEFT JOIN purchases_by_visit pv
    ON v.user_id   = pv.user_id
   AND v.SKU      = pv.SKU
   AND v.visit_ts = pv.visit_ts
  LEFT JOIN vehicle_map vm
    ON v.vehicle = vm.vehicle_key
  WHERE v.snapshot_date BETWEEN start_output_date AND end_output_date
)

-- Mask bad period metrics but keep the dates/rows
SELECT
  Date,
  journey_category,
  Vehicle,
  Size,
  SlotPosition,
  MerchandisingPosition,
  Manufacturer,
  ModelName,
  Variant,
  SKU,

  -- UNIQUE USER FLAGS
  CASE WHEN Date BETWEEN bad_start_date AND bad_end_date THEN NULL ELSE UsersUniqueDay        END AS TotalUsers,

  Segment,
  Pod,
  Category,

  -- PURCHASE METRICS
  CASE WHEN Date BETWEEN bad_start_date AND bad_end_date THEN NULL ELSE Orders           END AS Orders,
  CASE WHEN Date BETWEEN bad_start_date AND bad_end_date THEN NULL ELSE Purchasers       END AS Purchasers,
  CASE WHEN Date BETWEEN bad_start_date AND bad_end_date THEN NULL ELSE PurchasedUnits   END AS PurchasedUnits,
  CASE WHEN Date BETWEEN bad_start_date AND bad_end_date THEN NULL ELSE PurchasedRevenue END AS PurchasedRevenue,
  CASE WHEN Date BETWEEN bad_start_date AND bad_end_date THEN NULL ELSE UsersUniqueDayJourney END AS UsersUniqueDayJourney
FROM final;

COMMIT TRANSACTION;