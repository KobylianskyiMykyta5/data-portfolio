WITH session_start AS (
  SELECT
    e.user_pseudo_id,
     (SELECT value.int_value 
     FROM UNNEST(event_params) 
     WHERE key = 'ga_session_id') AS session_id,
          e.user_pseudo_id || CAST((SELECT value.int_value 
            FROM UNNEST(event_params) 
            WHERE key = 'ga_session_id') AS STRING) AS user_session_id,

    e.traffic_source.source,
    e.traffic_source.medium,
    e.traffic_source.name AS campaign,
    e.geo.country,
    e.device.category AS device_category,
    e.device.language AS device_language,
    e.device.operating_system,

    REGEXP_EXTRACT(
      (SELECT value.string_value 
       FROM UNNEST(event_params) 
       WHERE key = 'page_location'),
      r'(?:https:\/\/)?[^\/]+\/(.*)'
    ) AS landing_page_location

  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` e
  WHERE e.event_name = "session_start"
),

all_events AS (
  SELECT
    e.user_pseudo_id,
     (SELECT value.int_value 
     FROM UNNEST(event_params) 
     WHERE key = 'ga_session_id') AS session_id,
          e.user_pseudo_id || CAST((SELECT value.int_value 
            FROM UNNEST(event_params) 
            WHERE key = 'ga_session_id') AS STRING) AS user_session_id,
    e.event_name,
    TIMESTAMP_MICROS(e.event_timestamp) AS event_timestamp

  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` e
where event_name in ('session_start', 'view_item', 'add_to_cart', 'begin_checkout',
'add_shipping_info', 'add_payment_info', 'purchase')
)

SELECT
  ss.*,
  ae.event_name,
  ae.event_timestamp
FROM session_start ss
LEFT JOIN all_events ae
  USING (user_session_id);