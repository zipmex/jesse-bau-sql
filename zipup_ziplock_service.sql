---- zipup service snapshot 
SELECT 
	d.snapshot_utc
--	, s.signup_hostcountry 
	, s.product_id 
	, SUM(s.balance)
FROM generate_series('2021-08-10', '2021-08-18', '1 day'::INTERVAL) d (snapshot_utc)
LEFT JOIN LATERAL (
  		SELECT 
  			DISTINCT ON 
  			(b.user_id, product_id) b.user_id, u.signup_hostcountry , UPPER(SPLIT_PART(b.product_id, '.',1)) product_id , balance, b.created_at
  		FROM zip_up_service_public.balance_snapshots b 
  			LEFT JOIN analytics.users_master u
  			ON b.user_id = u.user_id 
  		WHERE 
  			b.created_at <= d.snapshot_utc
  		ORDER BY b.user_id, product_id, b.created_at DESC
		) s ON TRUE
WHERE 
	s.balance > 0
--	AND d.snapshot_utc = DATE_TRUNC('day', NOW())
--	AND s.user_id = '01F0BV36CJX570T14YFQ1BFWC0'
GROUP BY 
	1,2
ORDER BY 
	d.snapshot_utc DESC , s.product_id
;



---- ziplock service snapshot 
SELECT
	d.snapshot_utc
--	, s.signup_hostcountry 
	, product_id
	, sum(s.balance)
FROM generate_series('2021-08-10', '2021-08-18', '1 day'::INTERVAL) d (snapshot_utc)
	LEFT JOIN LATERAL (
  		SELECT 
  			DISTINCT ON (b.user_id, b.product_id) b.user_id, UPPER(SPLIT_PART(b.product_id, '.',1)) product_id , balance, b.balance_datetime
  		FROM zip_lock_service_public.vault_accumulated_balances b 
  			LEFT JOIN analytics.users_master u
  			ON b.user_id = u.user_id 
   		WHERE b.balance_datetime <= d.snapshot_utc
  		ORDER BY b.user_id, b.product_id, b.balance_datetime DESC
		) s ON TRUE
WHERE 
	s.balance > 0
--	AND d.snapshot_utc = DATE_TRUNC('day', NOW())
--	AND s.user_id = '01F0BV36CJX570T14YFQ1BFWC0'
GROUP BY 
	1,2
ORDER BY d.snapshot_utc DESC, s.product_id;


---- z_wallet balance snapshot 
WITH zipup_service AS (
SELECT 
	d.snapshot_utc created_at
	, s.user_id 
	, s.product_id 
	, SUM(s.balance) balance
FROM generate_series('2021-08-04', NOW()::DATE, '1 day'::INTERVAL) d (snapshot_utc)
LEFT JOIN LATERAL (
  		SELECT 
  			DISTINCT ON 
  			(b.user_id, product_id) b.user_id, u.signup_hostcountry , UPPER(SPLIT_PART(b.product_id, '.',1)) product_id , balance, b.created_at
  		FROM zip_up_service_public.balance_snapshots b 
  			LEFT JOIN analytics.users_master u
  			ON b.user_id = u.user_id 
  		WHERE 
  			b.created_at <= d.snapshot_utc
  		ORDER BY b.user_id, product_id, b.created_at DESC
		) s ON TRUE
WHERE 
	s.balance > 0
--	AND d.snapshot_utc = DATE_TRUNC('day', NOW())
--	AND s.user_id = '01F0BV36CJX570T14YFQ1BFWC0'
GROUP BY 
	1,2,3
ORDER BY 
	d.snapshot_utc DESC , s.product_id
)	, ziplock_service AS (
SELECT
	d.snapshot_utc created_at
	, s.user_id 
	, product_id
	, sum(s.balance) balance
FROM generate_series('2021-08-04', NOW()::DATE, '1 day'::INTERVAL) d (snapshot_utc)
	LEFT JOIN LATERAL (
  		SELECT 
  			DISTINCT ON (b.user_id, b.product_id) b.user_id, UPPER(SPLIT_PART(b.product_id, '.',1)) product_id , balance, b.balance_datetime
  		FROM zip_lock_service_public.vault_accumulated_balances b 
  			LEFT JOIN analytics.users_master u
  			ON b.user_id = u.user_id 
   		WHERE b.balance_datetime <= d.snapshot_utc
  		ORDER BY b.user_id, b.product_id, b.balance_datetime DESC
		) s ON TRUE
WHERE 
	s.balance > 0
--	AND d.snapshot_utc = DATE_TRUNC('day', NOW())
--	AND s.user_id = '01F0BV36CJX570T14YFQ1BFWC0'
GROUP BY 
	1,2,3
ORDER BY d.snapshot_utc DESC, s.product_id
) 
SELECT 
	COALESCE (zu.created_at , zl.created_at) created_at 
	, COALESCE ( zu.user_id, zl.user_id) user_id
	, COALESCE ( zu.product_id, zl.product_id ) product_id
	, COALESCE (zu.balance, 0) zipup_balance
	, COALESCE (zl.balance, 0) zlock_balance
FROM 
	zipup_service zu 
	FULL OUTER JOIN ziplock_service zl 
		ON zu.user_id = zl.user_id 
		AND zu.created_at = zl.created_at 
WHERE COALESCE ( zu.user_id, zl.user_id) = '01F0BV36CJX570T14YFQ1BFWC0'
ORDER BY 1 DESC 