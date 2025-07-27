WITH session_info AS (
  SELECT
    user_pseudo_id,
    (SELECT value.int_value FROM e.event_params WHERE key = 'ga_session_id') AS ga_session_id,
    CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM e.event_params WHERE key = 'ga_session_id') AS STRING)) AS user_session_id,
    event_date,
    TIMESTAMP_MICROS(event_timestamp) AS session_start_time,
    geo.country,
    device.category AS device_category,
    device.language AS device_language,
    device.operating_system AS device_os,
    traffic_source.source,
    traffic_source.medium,
    traffic_source.name AS campaign,
    REGEXP_EXTRACT((SELECT value.string_value FROM e.event_params WHERE key = 'page_location'), r'(?:https?://)?[^/]+/(.*)') AS landing_page_location
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` e
  WHERE
    event_name = 'session_start'
),

conversion_events AS (
  SELECT
    CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM e.event_params WHERE key = 'ga_session_id') AS STRING)) AS user_session_id,
    event_name,
    TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
    CASE
      WHEN event_name = 'session_start' THEN 1
      WHEN event_name = 'view_item' THEN 2
      WHEN event_name = 'add_to_cart' THEN 3
      WHEN event_name = 'begin_checkout' THEN 4
      WHEN event_name = 'add_shipping_info' THEN 5
      WHEN event_name = 'add_payment_info' THEN 6
      WHEN event_name = 'purchase' THEN 7
    END AS funnel_step,
    (SELECT value.string_value FROM e.event_params WHERE key = 'currency') AS currency,
    (SELECT value.float_value FROM e.event_params WHERE key = 'value') AS revenue,
    ecommerce.transaction_id
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` e
  WHERE
    event_name IN ('session_start', 'view_item', 'add_to_cart', 'begin_checkout', 'add_shipping_info', 'add_payment_info', 'purchase')
)

SELECT
  s.user_session_id,
  s.event_date,
  s.session_start_time,
  s.country,
  s.device_category,
  s.device_language,
  s.device_os,
  s.source,
  s.medium,
  s.campaign,
  s.landing_page_location,
  c.event_name,
  c.event_timestamp,
  c.funnel_step,
  c.currency,
  c.revenue,
  c.transaction_id
FROM
  session_info s
LEFT JOIN
  conversion_events c
ON
  s.user_session_id = c.user_session_id
ORDER BY
  s.user_session_id,
  c.funnel_step