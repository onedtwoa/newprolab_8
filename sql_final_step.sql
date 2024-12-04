CREATE MATERIALIZED VIEW mv_union_events
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (event_time)
AS
SELECT
    be.event_id AS event_id,
    be.event_timestamp AS event_time,
    be.event_type AS event_type,
    be.click_id AS click_id,
    de.user_custom_id AS user_custom_id,
    be.browser_name AS browser_name,
    de.os_name AS os_name,
    de.device_type AS device_type,
    ge.geo_country AS geo_country,
    ge.geo_region_name AS geo_region_name,
    le.page_url AS page_url,
    le.referer_url AS referer_url,
    le.utm_source AS utm_source,
    le.utm_medium AS utm_medium,
    le.utm_campaign AS utm_campaign
FROM
    browser_events AS be
LEFT JOIN device_events AS de ON be.click_id = de.click_id
LEFT JOIN geo_events AS ge ON be.click_id = ge.click_id
LEFT JOIN location_events AS le ON be.event_id = le.event_id;


CREATE VIEW purchase_sessions AS
SELECT
    click_id,
    payment_time,
    confirmation_time
FROM (
	    SELECT
	    click_id,
	    MIN(CASE WHEN page_url LIKE '%/payment%' THEN event_time END) AS payment_time,
	    MIN(CASE WHEN page_url LIKE '%/confirmation%' THEN event_time END) AS confirmation_time
	FROM mv_union_events
	WHERE page_url LIKE '%/payment%' OR page_url LIKE '%/confirmation%'
	GROUP BY click_id
	HAVING
	    payment_time IS NOT NULL
	    AND confirmation_time IS NOT NULL
	    AND payment_time < confirmation_time
) AS t;
