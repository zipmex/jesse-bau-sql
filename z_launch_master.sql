DROP TABLE IF EXISTS warehouse.analytics.z_launch_daily_balance;

CREATE TABLE IF NOT EXISTS warehouse.analytics.z_launch_daily_balance
(
	id						SERIAL PRIMARY KEY
	, created_at			TIMESTAMPTZ
	, ap_account_id			INTEGER
	, signup_hostcountry	VARCHAR(255)
	, symbol				VARCHAR(255)
	, project_id			VARCHAR(255)
	, amount				NUMERIC
	, amount_usd			NUMERIC
);

CREATE INDEX IF NOT EXISTS z_launch_daily_balance_idx ON warehouse.analytics.z_launch_daily_balance 
(ap_account_id);

DROP TABLE IF EXISTS tmp_z_launch;

CREATE TEMP TABLE IF NOT EXISTS tmp_z_launch AS
(	
	WITH zlaunch_base AS 
		(
			SELECT
				DATE_TRUNC('day', event_timestamp) created_at
				, user_id 
				, UPPER(SPLIT_PART(lock_product_id,'.',1)) symbol
				, pool_id project_id
				, SUM(CASE WHEN event_type = 'lock' THEN amount END) lock_amount
				, SUM(CASE WHEN event_type IN ('unlock','release') THEN amount END) released_amount
			FROM 
				z_launch_service_public.lock_unlock_histories luh 
			GROUP BY 1,2,3,4
		)	
			, zlaunch_snapshot AS 
		(
			SELECT 
				p.created_at 
				, z.user_id
				, u.ap_account_id 
				, u.email 
				, u.signup_hostcountry 
				, symbol
				, project_id
				, COALESCE (lock_amount, 0) lock_amount
				, COALESCE (released_amount, 0) released_amount
				, SUM(COALESCE (lock_amount, 0)) - SUM(COALESCE (released_amount, 0)) zmt_amount
			FROM analytics.period_master p
				LEFT JOIN zlaunch_base z 
					ON p.created_at >= z.created_at
				LEFT JOIN analytics.users_master u
					ON z.user_id = u.user_id 
			WHERE 
				p."period" = 'day'
				AND p.created_at >= '2021-10-26 00:00:00'
				AND p.created_at < DATE_TRUNC('day', NOW())
			GROUP BY 1,2,3,4,5,6,7,8,9
		)
			SELECT 
				DATE_TRUNC('day', z.created_at)::date created_at
				, ap_account_id
				, signup_hostcountry
				, 'ZMT' symbol
				, project_id
				, SUM(zmt_amount) amount
				, SUM(zmt_amount * r.price) amount_usd
			FROM zlaunch_snapshot z 
				LEFT JOIN analytics.rates_master r
				ON z.symbol = r.product_1_symbol 
				AND z.created_at = r.created_at 
--			WHERE 
--				(z.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL
--				OR z.created_at = DATE_TRUNC('month', z.created_at) + '1 month - 1 day'::INTERVAL)
			GROUP BY 1,2,3,4,5
			ORDER BY 1
);

INSERT INTO warehouse.analytics.z_launch_daily_balance (created_at, ap_account_id, signup_hostcountry, symbol, project_id, amount, amount_usd)
(SELECT * FROM tmp_z_launch);

DROP TABLE IF EXISTS tmp_z_launch;
