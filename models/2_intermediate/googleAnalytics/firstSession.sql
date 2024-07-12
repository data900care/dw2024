SELECT user_id, first_value(ga_session_id) OVER (PARTITION BY user_id ORDER BY event_timestamp ASC ) as firstSession
FROM {{ ref('stg_ga4_events') }}
where user_id is not null and ga_session_id is not null