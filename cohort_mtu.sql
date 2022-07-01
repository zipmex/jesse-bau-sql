WITH base AS (
SELECT ma.created_at::DATE , ma.signup_hostcountry , ma.mtu_1::INT ap_account_id
FROM mappings.mtu_account_2021 ma 
UNION ALL
SELECT ma2.created_at::DATE , ma2.signup_hostcountry , ma2.mtu_3::INT 
FROM mappings.mtu_account_2022 ma2 
)	, month_0 AS (
	SELECT 
		ap_account_id
		, MIN(created_at) m0
	FROM base
	GROUP BY 1
)	, cohort_group AS (
	SELECT
		b.*
		, m0
		, CASE WHEN b.created_at = m0 THEN 'm0'
				WHEN b.created_at = m0 + '1 month'::INTERVAL THEN 'm1'
				WHEN b.created_at = m0 + '2 month'::INTERVAL THEN 'm2'
				WHEN b.created_at = m0 + '3 month'::INTERVAL THEN 'm3'
				WHEN b.created_at = m0 + '4 month'::INTERVAL THEN 'm4'
				WHEN b.created_at = m0 + '5 month'::INTERVAL THEN 'm5'
				WHEN b.created_at = m0 + '6 month'::INTERVAL THEN 'm6'
				WHEN b.created_at = m0 + '7 month'::INTERVAL THEN 'm7'
				WHEN b.created_at = m0 + '8 month'::INTERVAL THEN 'm8'
				WHEN b.created_at = m0 + '9 month'::INTERVAL THEN 'm9'
				WHEN b.created_at = m0 + '10 month'::INTERVAL THEN 'm10'
				WHEN b.created_at = m0 + '11 month'::INTERVAL THEN 'm11'
				WHEN b.created_at = m0 + '12 month'::INTERVAL THEN 'm12'
				WHEN b.created_at = m0 + '13 month'::INTERVAL THEN 'm13'
				WHEN b.created_at = m0 + '14 month'::INTERVAL THEN 'm14'
				WHEN b.created_at = m0 + '15 month'::INTERVAL THEN 'm15'
				END AS gap 
	FROM base b
		LEFT JOIN
			month_0 m 
			ON b.ap_account_id = m.ap_account_id
	WHERE b.created_at < '2022-05-01'
)
SELECT 
	m0
	, gap 
	, COUNT(DISTINCT ap_account_id) mtu_count
FROM cohort_group
GROUP BY 1,2