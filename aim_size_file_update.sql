-- Replace the table with the new 9-column format
DECLARE latest_date DATE DEFAULT (
  SELECT MAX(Today) FROM `bqsqltesting.AIM.AIM-override-file`
);

INSERT INTO `bqsqltesting.AIM.AIM-override-file-size`
WITH latest AS (
  SELECT *
  FROM `bqsqltesting.AIM.AIM-override-file`
  WHERE Today = latest_date
),
unpivoted AS (
  -- Keep UNPIVOT at existing columns (SKU..SKU20)
  SELECT
    CAST(Width AS STRING)   AS Width,
    CAST(Profile AS STRING) AS Profile,
    CAST(Rim AS STRING)     AS Rim,
    token_position,
    CAST(product_id AS STRING) AS product_id
  FROM latest
  UNPIVOT (
    product_id FOR token_position IN (
      SKU   AS 1,  SKU2  AS 2,  SKU3  AS 3,  SKU4  AS 4,
      SKU5  AS 5,  SKU6  AS 6,  SKU7  AS 7,  SKU8  AS 8,
      SKU9  AS 9,  SKU10 AS 10, SKU11 AS 11, SKU12 AS 12,
      SKU13 AS 13, SKU14 AS 14, SKU15 AS 15, SKU16 AS 16,
      SKU17 AS 17, SKU18 AS 18, SKU19 AS 19, SKU20 AS 20
    )
  )
  WHERE product_id IS NOT NULL
    AND LENGTH(CAST(product_id AS STRING)) >= 8
),
-- Popularity across ALL positions (1..20) at size level
pop_counts AS (
  SELECT
    Width, Profile, Rim, product_id,
    COUNT(*) AS cnt
  FROM unpivoted
  GROUP BY Width, Profile, Rim, product_id
),
ranked AS (
  SELECT
    Width, Profile, Rim, product_id, cnt,
    ROW_NUMBER() OVER (
      PARTITION BY Width, Profile, Rim
      ORDER BY cnt DESC, product_id
    ) AS rnk
  FROM pop_counts
),
-- Map 30 slots to ranks (with intended duplicates)
-- Duplicates: rnk 1 -> slot 9; rnk 2 -> slot 13; rnk 3 -> slot 17; rnk 4 -> slot 21
-- Then continue with unique ranks up to rnk 26 to fill slots 22..30
rnk_slot_map AS (
  SELECT * FROM UNNEST([
    STRUCT(1  AS rnk, NUMERIC '1.001'  AS slot_number),   -- SKU
    STRUCT(2,          NUMERIC '2.001'),                  -- SKU2
    STRUCT(3,          NUMERIC '3.001'),                  -- SKU3
    STRUCT(4,          NUMERIC '4.001'),                  -- SKU4
    STRUCT(5,          NUMERIC '5.001'),                  -- SKU5
    STRUCT(6,          NUMERIC '6.001'),                  -- SKU6
    STRUCT(7,          NUMERIC '7.001'),                  -- SKU7
    STRUCT(8,          NUMERIC '8.001'),                  -- SKU8
    STRUCT(1,          NUMERIC '9.001'),                  -- SKU9 (dup of rnk 1)
    STRUCT(9,          NUMERIC '10.001'),                 -- SKU10
    STRUCT(10,         NUMERIC '11.001'),                 -- SKU11
    STRUCT(11,         NUMERIC '12.001'),                 -- SKU12
    STRUCT(2,          NUMERIC '13.001'),                 -- SKU13 (dup of rnk 2)
    STRUCT(12,         NUMERIC '14.001'),                 -- SKU14
    STRUCT(13,         NUMERIC '15.001'),                 -- SKU15
    STRUCT(14,         NUMERIC '16.001'),                 -- SKU16
    STRUCT(3,          NUMERIC '17.001'),                 -- SKU17 (dup of rnk 3)
    STRUCT(15,         NUMERIC '18.001'),                 -- SKU18
    STRUCT(16,         NUMERIC '19.001'),                 -- SKU19
    STRUCT(17,         NUMERIC '20.001'),                 -- SKU20
    STRUCT(4,          NUMERIC '21.001'),                 -- SKU21 (dup of rnk 4)
    STRUCT(18,         NUMERIC '22.001'),                 -- SKU22
    STRUCT(19,         NUMERIC '23.001'),                 -- SKU23
    STRUCT(20,         NUMERIC '24.001'),                 -- SKU24
    STRUCT(21,         NUMERIC '25.001'),                 -- SKU25
    STRUCT(22,         NUMERIC '26.001'),                 -- SKU26
    STRUCT(23,         NUMERIC '27.001'),                 -- SKU27
    STRUCT(24,         NUMERIC '28.001'),                 -- SKU28  (fixed from rnk 23)
    STRUCT(25,         NUMERIC '29.001'),                 -- SKU29
    STRUCT(26,         NUMERIC '30.001'),                 -- SKU30
    STRUCT(27,         NUMERIC '31.001'),                 -- SKU31
    STRUCT(28,         NUMERIC '32.001'),                 -- SKU32
    STRUCT(29,         NUMERIC '33.001'),                 -- SKU33
    STRUCT(30,         NUMERIC '34.001'),                 -- SKU34
    STRUCT(31,         NUMERIC '35.001'),                 -- SKU35
    STRUCT(32,         NUMERIC '36.001'),                 -- SKU36
    STRUCT(33,         NUMERIC '37.001'),                 -- SKU37
    STRUCT(34,         NUMERIC '38.001'),                 -- SKU38
    STRUCT(35,         NUMERIC '39.001'),                 -- SKU39
    STRUCT(36,         NUMERIC '40.001'),                 -- SKU40
    STRUCT(37,         NUMERIC '41.001'),                 -- SKU41
    STRUCT(38,         NUMERIC '42.001'),                 -- SKU42
    STRUCT(39,         NUMERIC '43.001'),                 -- SKU43
    STRUCT(40,         NUMERIC '44.001'),                 -- SKU44
    STRUCT(41,         NUMERIC '45.001'),                 -- SKU45
    STRUCT(42,         NUMERIC '46.001'),                 -- SKU46
    STRUCT(43,         NUMERIC '47.001'),                 -- SKU47
    STRUCT(44,         NUMERIC '48.001'),                 -- SKU48
    STRUCT(45,         NUMERIC '49.001'),                 -- SKU49
    STRUCT(46,         NUMERIC '50.001'),                 -- SKU50
    STRUCT(47,         NUMERIC '51.001'),                 -- SKU51
    STRUCT(48,         NUMERIC '52.001'),                 -- SKU52
    STRUCT(49,         NUMERIC '53.001'),                 -- SKU53
    STRUCT(50,         NUMERIC '54.001'),                 -- SKU54
    STRUCT(51,         NUMERIC '55.001'),                 -- SKU55
    STRUCT(52,         NUMERIC '56.001'),                 -- SKU56
    STRUCT(53,         NUMERIC '57.001'),                 -- SKU57
    STRUCT(54,         NUMERIC '58.001'),                 -- SKU58
    STRUCT(55,         NUMERIC '59.001'),                 -- SKU59
    STRUCT(56,         NUMERIC '60.001')                  -- SKU60
  ])
),
-- Join the ranked products to the slot layout described above
slotted AS (
  SELECT
    r.Width, r.Profile, r.Rim,
    r.product_id,
    m.slot_number
  FROM ranked r
  JOIN rnk_slot_map m
    ON r.rnk = m.rnk
  WHERE r.rnk <= 56  -- highest rnk referenced in rnk_slot_map
),
-- Attach brand from the product catalogue
with_brand AS (
  SELECT
    s.product_id,
    s.slot_number,
    'AIM_Size' AS Rationale,
    'AIM_Size' AS Initiative,
    'AIM_Size' AS Notes,
    CURRENT_DATE('Europe/London') AS Start_date,
    DATE_ADD(CURRENT_DATE('Europe/London'), INTERVAL 30 DAY) AS Review_date,
    pc.Brand AS Brand,
    CONCAT(s.Width, '/', s.Profile, ' R', s.Rim) AS Size
  FROM slotted s
  LEFT JOIN `bqsqltesting.Reference_Tables.Product_catalogue_with_price` pc
    ON CAST(pc.SKU AS STRING) = s.product_id
  WHERE s.product_id IS NOT NULL
)
SELECT
  product_id,
  slot_number,
  Rationale,
  Initiative,
  Notes,
  Start_date,
  Review_date,
  Brand,
  Size
FROM with_brand
WHERE
  Brand IS NOT NULL
  AND Size IS NOT NULL
ORDER BY
  slot_number;
