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

-- 2) Recompute the rolling window and insert it
INSERT INTO `bqsqltesting.AIM.aim-merchandising-report`
WITH
visibility_raw AS (
  SELECT
    PARSE_DATE('%Y%m%d', e.event_date) AS snapshot_date,
    TIMESTAMP_MICROS(e.event_timestamp) AS visit_ts,
    bqsqltesting.functions.BC_UID(e.user_pseudo_id, e.user_id) AS user_id,
    e.geo.country AS country,
    e.event_name AS event,
    REGEXP_EXTRACT((SELECT ep.value.string_value FROM UNNEST(e.event_params) ep WHERE ep.key = 'page_location'), r'=([^&?]*)') AS page_location_cam,
    (SELECT ip.value.string_value FROM UNNEST(it.item_params) ip WHERE ip.key = 'item_category8') AS item_category8,

    CASE
    --AIM
      WHEN UPPER((SELECT ip.value.string_value FROM UNNEST(it.item_params) ip WHERE ip.key = 'item_category8')) LIKE 'PSUCMM-AIM%'
        THEN 'AIM U - SWITCH'
      WHEN UPPER((SELECT ip.value.string_value FROM UNNEST(it.item_params) ip WHERE ip.key = 'item_category8')) LIKE 'SUCMM-AIM%'
        THEN 'AIM U'
      WHEN UPPER((SELECT ip.value.string_value FROM UNNEST(it.item_params) ip WHERE ip.key = 'item_category8')) LIKE 'PXSUCMM-AIM%'
        THEN 'AIM U - SWITCH'
      WHEN UPPER((SELECT ip.value.string_value FROM UNNEST(it.item_params) ip WHERE ip.key = 'item_category8')) LIKE 'XSUCMM-AIM%'
        THEN 'AIM U'
      WHEN UPPER((SELECT ip.value.string_value FROM UNNEST(it.item_params) ip WHERE ip.key = 'item_category8')) LIKE '%-AIM%'
        THEN 'AIM'
      WHEN UPPER((SELECT ip.value.string_value FROM UNNEST(it.item_params) ip WHERE ip.key = 'item_category8')) LIKE 'PSCMM-AIM%'
        THEN 'AIM - SWITCH'


    --CAM WAVES U
      WHEN UPPER((SELECT ip.value.string_value FROM UNNEST(it.item_params) ip WHERE ip.key = 'item_category8')) LIKE 'SUCMM%'
        THEN 'CAM WAVES U'
      WHEN UPPER((SELECT ip.value.string_value FROM UNNEST(it.item_params) ip WHERE ip.key = 'item_category8')) LIKE 'SUCMM-MAN%'
        THEN 'CAM WAVES U'
      WHEN UPPER((SELECT ip.value.string_value FROM UNNEST(it.item_params) ip WHERE ip.key = 'item_category8')) LIKE 'XSUCMM%'
        THEN 'CAM WAVES U'
      WHEN UPPER((SELECT ip.value.string_value FROM UNNEST(it.item_params) ip WHERE ip.key = 'item_category8')) LIKE 'SUCMM-NONCAM%'
        THEN 'CAM WAVES U'
      WHEN UPPER((SELECT ip.value.string_value FROM UNNEST(it.item_params) ip WHERE ip.key = 'item_category8')) LIKE 'PSUCMM%'
        THEN 'CAM WAVES U - SWITCH'
      WHEN UPPER((SELECT ip.value.string_value FROM UNNEST(it.item_params) ip WHERE ip.key = 'item_category8')) LIKE 'PSUCMM-MAN%'
        THEN 'CAM WAVES U - SWITCH'
      WHEN UPPER((SELECT ip.value.string_value FROM UNNEST(it.item_params) ip WHERE ip.key = 'item_category8')) LIKE 'PXSUCMM%'
        THEN 'CAM WAVES U - SWITCH'
      WHEN UPPER((SELECT ip.value.string_value FROM UNNEST(it.item_params) ip WHERE ip.key = 'item_category8')) LIKE 'PSUCMM-NONCAM%'
        THEN 'CAM WAVES U - SWITCH'

    --NON MERCH U
      WHEN UPPER((SELECT ip.value.string_value FROM UNNEST(it.item_params) ip WHERE ip.key = 'item_category8')) LIKE 'XSUCMM-NONCAM%'
        THEN 'CAM U'
      WHEN UPPER((SELECT ip.value.string_value FROM UNNEST(it.item_params) ip WHERE ip.key = 'item_category8')) LIKE 'SUCMM-NONCAM%'
        THEN 'CAM U'
      WHEN UPPER((SELECT ip.value.string_value FROM UNNEST(it.item_params) ip WHERE ip.key = 'item_category8')) LIKE 'XUCMM-NONCAM%'
        THEN 'CAM U'
      WHEN UPPER((SELECT ip.value.string_value FROM UNNEST(it.item_params) ip WHERE ip.key = 'item_category8')) LIKE 'PXSUCMM-NONCAM%'
        THEN 'CAM U - SWITCH'
      WHEN UPPER((SELECT ip.value.string_value FROM UNNEST(it.item_params) ip WHERE ip.key = 'item_category8')) LIKE 'PSUCMM-NONCAM%'
        THEN 'CAM U - SWITCH'
      WHEN UPPER((SELECT ip.value.string_value FROM UNNEST(it.item_params) ip WHERE ip.key = 'item_category8')) LIKE 'PXUCMM-NONCAM%'
        THEN 'CAM U - SWITCH'

    --CAM U
      WHEN UPPER((SELECT ip.value.string_value FROM UNNEST(it.item_params) ip WHERE ip.key = 'item_category8')) LIKE 'UCMM%'
        THEN 'CAM U'
      WHEN UPPER((SELECT ip.value.string_value FROM UNNEST(it.item_params) ip WHERE ip.key = 'item_category8')) LIKE 'XUCMM%'
        THEN 'CAM U'
      WHEN UPPER((SELECT ip.value.string_value FROM UNNEST(it.item_params) ip WHERE ip.key = 'item_category8')) LIKE 'UCMM-NONCAM%'
        THEN 'CAM U'
      WHEN UPPER((SELECT ip.value.string_value FROM UNNEST(it.item_params) ip WHERE ip.key = 'item_category8')) LIKE 'PUCMM%'
        THEN 'CAM U - SWITCH'
      WHEN UPPER((SELECT ip.value.string_value FROM UNNEST(it.item_params) ip WHERE ip.key = 'item_category8')) LIKE 'PXUCMM%'
        THEN 'CAM U - SWITCH'
      WHEN UPPER((SELECT ip.value.string_value FROM UNNEST(it.item_params) ip WHERE ip.key = 'item_category8')) LIKE 'PUCMM-NONCAM%'
        THEN 'CAM U - SWITCH'

    --CAM WAVES
      WHEN UPPER((SELECT ip.value.string_value FROM UNNEST(it.item_params) ip WHERE ip.key = 'item_category8')) LIKE 'SCMM%'
        THEN 'CAM WAVES'
      WHEN UPPER((SELECT ip.value.string_value FROM UNNEST(it.item_params) ip WHERE ip.key = 'item_category8')) LIKE 'PSCMM%'
        THEN 'CAM WAVES - SWITCH'

    --CAM
      WHEN UPPER((SELECT ip.value.string_value FROM UNNEST(it.item_params) ip WHERE ip.key = 'item_category8')) LIKE 'CMM%'
        THEN 'CAM'
      WHEN UPPER((SELECT ip.value.string_value FROM UNNEST(it.item_params) ip WHERE ip.key = 'item_category8')) LIKE 'PCMM%'
        THEN 'CAM - SWITCH'

    --SIZE
      WHEN UPPER(TRIM(it.item_category5)) = 'NO VRM SEARCH'
        THEN 'SIZE'

    --NO VRM
      WHEN UPPER((SELECT ip.value.string_value FROM UNNEST(it.item_params) ip WHERE ip.key = 'item_category8')) = 'NONCAM'
        THEN 'NON MERCH VRM'

      ELSE 'NON MERCH VRM'
    END AS journey_category,

    REPLACE(TRIM(UPPER(it.item_category5)), ' ', '') AS vehicle,

    CASE
      WHEN it.item_list_index = '(not set)' THEN NULL
      WHEN SAFE_CAST(it.item_list_index AS INT64) = 200 THEN 4
      ELSE SAFE_CAST(it.item_list_index AS INT64)
    END AS SlotPosition,

    it.item_list_id AS Size,
    it.item_brand    AS Manufacturer,
    it.item_category AS ModelName,
    it.item_variant  AS Variant,
    it.item_id       AS SKU,

    CASE
      WHEN SAFE_CAST(it.item_list_index AS INT64) = 1   THEN 'Hotbox 1'
      WHEN SAFE_CAST(it.item_list_index AS INT64) = 2   THEN 'Hotbox 2'
      WHEN SAFE_CAST(it.item_list_index AS INT64) = 3   THEN 'Hotbox 3'
      WHEN SAFE_CAST(it.item_list_index AS INT64) IN (4,200) THEN 'Hotbox 4'
      WHEN SAFE_CAST(it.item_list_index AS INT64) = 99  THEN 'BrandSelector'
      ELSE 'Waves'
    END AS MerchandisingPosition
  FROM `bqsqltesting.analytics_249515915.events_*` e, UNNEST(e.items) it
  WHERE
    e._TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', start_scan_date) AND FORMAT_DATE('%Y%m%d', end_output_date)
    AND bqsqltesting.functions.BC_UID(e.user_pseudo_id, e.user_id) IS NOT NULL
    AND e.event_name = 'view_item_list'
    AND e.geo.country = 'United Kingdom'
    AND NOT REGEXP_CONTAINS(
      (SELECT ep.value.string_value FROM UNNEST(e.event_params) ep WHERE ep.key = 'page_location'),
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
    PARSE_DATE('%Y%m%d', e.event_date) AS purchase_date,
    TIMESTAMP_MICROS(e.event_timestamp) AS purchase_ts,
    bqsqltesting.functions.BC_UID(e.user_pseudo_id, e.user_id) AS user_id,
    it.item_id AS SKU,
    COALESCE(it.quantity, 1) AS Units,
    COALESCE(it.price, 0) * COALESCE(it.quantity, 1) AS ItemRevenue,
    (SELECT ep.value.string_value FROM UNNEST(e.event_params) ep WHERE ep.key = 'transaction_id') AS transaction_id
  FROM `bqsqltesting.analytics_249515915.events_*` e, UNNEST(e.items) it
  WHERE
    e._TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', start_scan_date) AND FORMAT_DATE('%Y%m%d', end_output_date)
    AND bqsqltesting.functions.BC_UID(e.user_pseudo_id, e.user_id) IS NOT NULL
    AND e.event_name = 'purchase'
    AND e.geo.country = 'United Kingdom'
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
    REPLACE(TRIM(UPPER(Vehicle)), ' ', '') AS vehicle_key,
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
    v.SlotPosition,
    v.MerchandisingPosition,
    v.Manufacturer,
    v.ModelName,
    v.Variant,
    v.SKU,

    -- UNIQUE USER FLAGS
    CASE WHEN vbf.rn_user_day = 1 THEN 1 ELSE 0 END AS UsersUniqueDay,
    CASE WHEN vbf.rn_user_day_journey = 1 THEN 1 ELSE 0 END AS UsersUniqueDayJourney,

    vm.Segment,
    vm.Pod,
    vm.Category,
    COALESCE(pv.Orders, 0)           AS Orders,
    COALESCE(pv.Purchasers, 0)       AS Purchasers,
    COALESCE(pv.PurchasedUnits, 0)   AS PurchasedUnits,
    COALESCE(pv.PurchasedRevenue, 0) AS PurchasedRevenue
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
  SlotPosition,
  MerchandisingPosition,
  Manufacturer,
  ModelName,
  Variant,
  SKU,

  -- UNIQUE USER FLAGS
  CASE WHEN Date BETWEEN bad_start_date AND bad_end_date THEN NULL ELSE UsersUniqueDay        END AS UsersUniqueDay,
  CASE WHEN Date BETWEEN bad_start_date AND bad_end_date THEN NULL ELSE UsersUniqueDayJourney END AS UsersUniqueDayJourney,

  Segment,
  Pod,
  Category,

  -- PURCHASE METRICS
  CASE WHEN Date BETWEEN bad_start_date AND bad_end_date THEN NULL ELSE Orders           END AS Orders,
  CASE WHEN Date BETWEEN bad_start_date AND bad_end_date THEN NULL ELSE Purchasers       END AS Purchasers,
  CASE WHEN Date BETWEEN bad_start_date AND bad_end_date THEN NULL ELSE PurchasedUnits   END AS PurchasedUnits,
  CASE WHEN Date BETWEEN bad_start_date AND bad_end_date THEN NULL ELSE PurchasedRevenue END AS PurchasedRevenue
FROM final;

COMMIT TRANSACTION;
