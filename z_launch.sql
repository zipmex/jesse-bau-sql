WITH zlaunch_base AS (
-- all z launch transaction (lock, unlock, released)
	SELECT
		DATE_TRUNC('day', event_timestamp) created_at
		, user_id 
		, UPPER(SPLIT_PART(lock_product_id,'.',1)) symbol
		, pool_id project_id
		, SUM(CASE WHEN event_type = 'lock' THEN amount END) lock_amount
		, SUM(CASE WHEN event_type IN ('unlock','release') THEN amount END) released_amount
	FROM 
		z_launch_service_public.lock_unlock_histories luh 
--	WHERE user_id = '01F67663GD1K5PT8HE2GGMD3RM'
	GROUP BY 1,2,3,4
)	, zlaunch_snapshot AS (
-- calculate daily staked balance
	SELECT 
		p.created_at 
		, z.user_id
		, u.ap_account_id 
		, u.signup_hostcountry 
		, symbol
		, project_id
		, SUM(COALESCE (lock_amount, 0)) - SUM(COALESCE (released_amount, 0)) zmt_amount
	FROM 
	-- period master for daily balance
	analytics.period_master p
		LEFT JOIN zlaunch_base z 
			ON p.created_at >= z.created_at
		-- get account id, country
		LEFT JOIN analytics.users_master u
			ON z.user_id = u.user_id 
	WHERE 
	-- period master is daily
		p."period" = 'day'
		-- z launch start date
		AND p.created_at >= '2021-10-26'
		-- data from yesterday backward only
		AND p.created_at <= DATE_TRUNC('day', NOW())
	GROUP BY 1,2,3,4,5,6
)--	, zlaunch AS (
	SELECT 
		DATE_TRUNC('day', z.created_at)::date created_at
--		, DATE_TRUNC('week', z.created_at)::date created_week
--		, DATE_TRUNC('month', z.created_at)::date created_month
--		, ap_account_id
--		, signup_hostcountry
		, project_id
		, COUNT(DISTINCT ap_account_id) user_count
		, SUM(zmt_amount) amount
		, SUM(zmt_amount * r.price) amount_usd
	FROM zlaunch_snapshot z 
	-- get coin prices
		LEFT JOIN analytics.rates_master r
		ON z.symbol = r.product_1_symbol 
		AND z.created_at = r.created_at 
	WHERE 
		zmt_amount > 0
--		AND (z.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL
--			OR z.created_at = DATE_TRUNC('month', z.created_at) + '1 month - 1 day'::INTERVAL)
	GROUP BY 1,2
	ORDER BY 1 DESC 
;
)
	SELECT 
		created_month 
	--	created_week
		, signup_hostcountry
		, project_id
		, COUNT(DISTINCT CASE WHEN zmt_amount > 0 THEN ap_account_id END) user_count
		, SUM(zmt_amount) / COUNT(DISTINCT created_at) avg_zmt_amount
		, SUM(zmt_amount_usd) / COUNT(DISTINCT created_at) avg_zmt_amount_usd
--		, SUM(zmt_amount) / COUNT(DISTINCT CASE WHEN zmt_amount > 0 THEN ap_account_id END) avg_zmt_per_user
	FROM zlaunch
	GROUP BY 1,2,3
;


SELECT * FROM asset_manager_public.ledgers_v2 lv 
WHERE account_id = '01F67663GD1K5PT8HE2GGMD3RM'
AND product_id LIKE 'zmt%'
AND ref_action NOT IN ('distribute_reward','distribute_interest')


SELECT * FROM z_launch_service_public.lock_unlock_histories