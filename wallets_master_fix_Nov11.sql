SET timezone TO 'GMT';

CREATE TABLE IF NOT EXISTS warehouse.analytics.tmp_wallets_balance_eod
(
	id							SERIAL PRIMARY KEY 
	, created_at	 					TIMESTAMPTZ
	, ap_account_id	 					INTEGER 
	, symbol 							VARCHAR(255)
	, trade_wallet_amount				NUMERIC
	, z_wallet_amount					NUMERIC
	, ziplock_amount					NUMERIC
	, zlaunch_amount					NUMERIC
);

TRUNCATE TABLE warehouse.analytics.tmp_wallets_balance_eod;

CREATE INDEX IF NOT EXISTS tmp_wallets_balance_eod_idx ON warehouse.analytics.tmp_wallets_balance_eod
(ap_account_id, symbol, created_at);


DROP TABLE IF EXISTS tmp_z_wallet_balance;
--DROP TABLE IF EXISTS tmp_zipup_balance;
--DROP TABLE IF EXISTS tmp_ziplock_balance;
DROP TABLE IF EXISTS tmp_trade_wallet_balance;


---- z_wallet daily consolidation after 2021-08-04
CREATE TEMP TABLE tmp_z_wallet_balance AS
(
	WITH base AS (
		SELECT 
			d.created_at 
			, u.ap_account_id
			, UPPER(SPLIT_PART(l.product_id,'.',1)) symbol
			, 0.0 trade_wallet_amount
			, COALESCE (SUM( CASE WHEN l.service_id = 'main_wallet' THEN credit - debit END), 0) z_wallet_amount 
			, COALESCE (SUM( CASE WHEN l.service_id = 'zip_lock' THEN credit - debit END), 0) ziplock_amount
		FROM (
			SELECT
				DISTINCT  "date" AS created_at 
				,u.account_id 
			FROM 
				GENERATE_SERIES('2021-12-01'::timestamp, DATE_TRUNC('day', NOW()), '1 day') "date"
				CROSS JOIN (SELECT DISTINCT account_id FROM asset_manager_public.ledgers_v2 ) u
			)d 
			LEFT JOIN 
				asset_manager_public.ledgers_v2 l 
					ON d.account_id = l.account_id 
					AND d.created_at >= DATE_TRUNC('day', l.created_at)
			LEFT JOIN 
				analytics.users_master u 
					ON l.account_id = u.user_id 
		WHERE 
			u.ap_account_id IS NOT NULL
			AND d.created_at < DATE_TRUNC('day', NOW())
		GROUP BY 1,2,3,4
		ORDER BY 1
	)
	SELECT 
		*
	FROM 
		base
);


--	 zipup daily consolidation 
CREATE TEMP TABLE tmp_zipup_balance AS
(
	SELECT
		d.snapshot_utc created_at
		, ap_account_id
		, UPPER(SPLIT_PART(s.product_id,'.',1)) symbol
		, 0.0 trade_wallet_amount
		, SUM(s.balance) z_wallet_amount
		, 0.0 ziplock_amount
	FROM
		generate_series(NOW()::DATE - '5 day'::INTERVAL, NOW()::DATE, '1 day'::interval) d (snapshot_utc)
		LEFT JOIN LATERAL (
			SELECT 
				DISTINCT ON (user_id, product_id) user_id, product_id, balance, created_at
			FROM zip_up_service_public.balance_snapshots
			WHERE DATE_TRUNC('day', balance_snapshots.created_at) <= d.snapshot_utc
			ORDER BY user_id, product_id, created_at DESC 
				) s ON TRUE
		LEFT JOIN analytics.users_master u
			ON s.user_id = u.user_id 
	WHERE 
		d.snapshot_utc < DATE_TRUNC('day', NOW())
		AND u.ap_account_id = 143639
	GROUP BY 1,2,3,4,6
	ORDER BY 1,2
);

--	 ziplock daily consolidation 
CREATE TEMP TABLE tmp_ziplock_balance AS
(
	SELECT
		d.snapshot_utc
		, ap_account_id
		, UPPER(SPLIT_PART(s.product_id,'.',1)) symbol
		, 0.0 trade_wallet_amount
		, 0.0 z_wallet_amount
		, SUM(s.balance) ziplock_amount
	FROM
		generate_series(NOW()::DATE - '5 day'::INTERVAL, NOW()::DATE, '1 day'::interval) d (snapshot_utc)
		LEFT JOIN LATERAL (
			SELECT 
				DISTINCT ON (user_id, product_id) user_id, product_id, balance, balance_datetime
			FROM zip_lock_service_public.vault_accumulated_balances
			WHERE DATE_TRUNC('day', balance_datetime) <= d.snapshot_utc
			ORDER BY user_id, product_id, balance_datetime DESC
				) s ON TRUE
		LEFT JOIN analytics.users_master u
			ON s.user_id = u.user_id 
	WHERE 
		d.snapshot_utc < DATE_TRUNC('day', NOW())
	GROUP BY 1,2,3,4,5
	ORDER BY 1,2
);



--	 trade_wallet daily consolidation 
CREATE TEMP TABLE tmp_trade_wallet_balance AS
(
	SELECT  
	-- balance on 2021-05-19 was delayed to 2021-05-20 03:43:52, convert it back to 2021-05-19
		CASE WHEN DATE_TRUNC('hour', a.created_at) = '2021-05-20 03:00:00' THEN '2021-05-19 00:00:00' ELSE DATE_TRUNC('day', a.created_at) END AS created_at 
		, a.account_id 
		, p.symbol 
		, a.amount trade_wallet_amount
		, 0.0 z_wallet_amount
		, 0.0 ziplock_amount
	FROM 
		oms_data_public.accounts_positions_daily a
		LEFT JOIN apex.products p 
			ON a.product_id = p.product_id 
		LEFT JOIN analytics.users_master u 
			ON a.account_id = u.ap_account_id 
	WHERE 
		u.user_id IS NOT NULL 
		AND DATE_TRUNC('day',a.created_at) >= DATE_TRUNC('day', NOW()) - '5 day'::INTERVAL
		AND DATE_TRUNC('day',a.created_at) < DATE_TRUNC('day', NOW())
	ORDER BY 1 DESC 
);

-- z launch daily consolidation
CREATE TEMP TABLE tmp_z_launch_balance AS
(
	WITH zlaunch_base AS 
		(
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
			WHERE user_id = '01F67663GD1K5PT8HE2GGMD3RM'
			GROUP BY 1,2,3,4
		)	
	, zlaunch_snapshot AS 
		(
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
				AND p.created_at < DATE_TRUNC('day', NOW())
			GROUP BY 1,2,3,4,5,6
	)
	SELECT 
		DATE_TRUNC('day', z.created_at)::date created_at
		, ap_account_id
		, symbol 
		, SUM(zmt_amount) zlaunch_amount
	FROM zlaunch_snapshot z 
	-- get coin prices
		LEFT JOIN analytics.rates_master r
		ON z.symbol = r.product_1_symbol 
		AND z.created_at = r.created_at 
	WHERE 
		zmt_amount > 0
	GROUP BY 1,2,3
	ORDER BY 1
);


INSERT INTO warehouse.analytics.tmp_wallets_balance_eod (created_at, ap_account_id, symbol, zlaunch_amount)
(
	SELECT * FROM tmp_z_launch_balance
);

INSERT INTO warehouse.analytics.tmp_wallets_balance_eod (created_at, ap_account_id, symbol, z_wallet_amount, ziplock_amount)
(
	SELECT * FROM tmp_z_wallet_balance
);

--INSERT INTO warehouse.analytics.tmp_wallets_balance_eod (created_at, ap_account_id, symbol, trade_wallet_amount, z_wallet_amount, ziplock_amount)
--(
--	SELECT * FROM tmp_ziplock_balance
--);

INSERT INTO warehouse.analytics.tmp_wallets_balance_eod (created_at, ap_account_id, symbol, trade_wallet_amount)
(
	SELECT * FROM tmp_trade_wallet_balance
);


DROP TABLE IF EXISTS tmp_z_wallet_balance;
--DROP TABLE IF EXISTS tmp_zipup_balance;
--DROP TABLE IF EXISTS tmp_ziplock_balance;
DROP TABLE IF EXISTS tmp_trade_wallet_balance;
DROP TABLE IF EXISTS tmp_z_launch_balance;

CREATE TABLE IF NOT EXISTS warehouse.analytics.wallets_balance_eod
(
	id							SERIAL PRIMARY KEY 
	, created_at	 					TIMESTAMPTZ
	, ap_account_id						INTEGER 
	, symbol 							VARCHAR(255)
	, trade_wallet_amount				NUMERIC
	, z_wallet_amount					NUMERIC
	, ziplock_amount					NUMERIC
	, zlaunch_amount					NUMERIC
);

CREATE INDEX IF NOT EXISTS wallets_balance_eod_idx ON warehouse.analytics.wallets_balance_eod
(ap_account_id, symbol, created_at);

DELETE FROM warehouse.analytics.wallets_balance_eod WHERE created_at >= DATE_TRUNC('day', NOW()) - '5 day'::INTERVAL;

CREATE TEMP TABLE tmp_wallet_balance AS
(
	WITH base AS 
	(
		SELECT 
			created_at
			, ap_account_id
			, symbol
			, SUM(COALESCE (trade_wallet_amount, 0.0)) trade_wallet_amount
			, SUM(COALESCE (z_wallet_amount, 0.0)) z_wallet_amount
			, SUM(COALESCE (ziplock_amount, 0.0)) ziplock_amount
			, SUM(COALESCE (zlaunch_amount, 0.0)) zlaunch_amount
		FROM 
			warehouse.analytics.tmp_wallets_balance_eod
		GROUP BY 
			1,2,3
	)
	SELECT 
		*
	FROM 
		base
);

INSERT INTO warehouse.analytics.wallets_balance_eod (created_at, ap_account_id, symbol, trade_wallet_amount, z_wallet_amount, ziplock_amount, zlaunch_amount)
(
	SELECT * FROM tmp_wallet_balance
);

DROP TABLE IF EXISTS tmp_wallet_balance;



CREATE TEMP TABLE tmp_z_launch_balance AS
(
	WITH zlaunch_base AS 
		(
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
			WHERE user_id = '01F67663GD1K5PT8HE2GGMD3RM'
			GROUP BY 1,2,3,4
		)	
	, zlaunch_snapshot AS 
		(
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
				AND p.created_at < DATE_TRUNC('day', NOW())
			GROUP BY 1,2,3,4,5,6
	)
	SELECT 
		DATE_TRUNC('day', z.created_at)::date created_at
		, ap_account_id
		, 'ZMT' symbol 
		, 0.0 trade_wallet_amount
		, 0.0 z_wallet_amount
		, 0.0 ziplock_amount
		, SUM(zmt_amount) zlaunch_amount
	FROM zlaunch_snapshot z 
	-- get coin prices
		LEFT JOIN analytics.rates_master r
		ON z.symbol = r.product_1_symbol 
		AND z.created_at = r.created_at 
	WHERE 
		zmt_amount > 0
	GROUP BY 1,2,3,4,5,6
	ORDER BY 1
);