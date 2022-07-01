--DROP TABLE IF EXISTS warehouse.analytics.tmp_wallets_balance_eod;

--ALTER TABLE warehouse.analytics.tmp_wallets_balance_eod
--ADD COLUMN IF NOT EXISTS zlaunch_amount NUMERIC ;

CREATE TABLE IF NOT EXISTS warehouse.analytics.tmp_wallets_balance_eod
(
	id									SERIAL PRIMARY KEY 
	, created_at	 					TIMESTAMPTZ
	, ap_account_id	 					INTEGER 
	, symbol 							VARCHAR(255)
	, trade_wallet_amount				NUMERIC
	, z_wallet_amount					NUMERIC
	, ziplock_amount					NUMERIC
	, zlaunch_amount					NUMERIC
);

CREATE INDEX tmp_wallets_balance_eod_keys ON warehouse.analytics.tmp_wallets_balance_eod
(ap_account_id, symbol, created_at);

DROP TABLE IF EXISTS tmp_z_wallet_balance;
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
			, 0.0 zlaunch_amount
		FROM (
			SELECT
				DISTINCT  "date" AS created_at 
				,u.account_id 
			FROM 
				GENERATE_SERIES(DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL, DATE_TRUNC('day', NOW()), '1 day') "date"
				CROSS JOIN (SELECT DISTINCT account_id FROM asset_manager_public.ledgers ) u
			)d 
			LEFT JOIN 
				asset_manager_public.ledgers l 
					ON d.account_id = l.account_id 
					AND d.created_at >= DATE_TRUNC('day', l.updated_at)
			LEFT JOIN 
				analytics.users_master u 
					ON l.account_id = u.user_id 
		WHERE 
			u.ap_account_id IS NOT NULL
			AND d.created_at < DATE_TRUNC('day', NOW())
		GROUP BY 1,2,3,4
		ORDER BY 1 DESC 
	)
	SELECT 
		*
	FROM 
		base
	WHERE 
		(z_wallet_amount <> 0 OR ziplock_amount <> 0 OR zlaunch_amount <> 0)
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
		, 0.0 zlaunch_amount
	FROM 
		public.accounts_positions_daily a
		LEFT JOIN apex.products p 
			ON a.product_id = p.product_id 
		LEFT JOIN analytics.users_master u 
			ON a.account_id = u.ap_account_id 
	WHERE 
		u.user_id IS NOT NULL 
		AND a.created_at >= DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL
		AND a.created_at < DATE_TRUNC('day', NOW())
	ORDER BY 1 DESC 
);

-- z_launch daily snapshot
CREATE TEMP TABLE tmp_z_launch_balance AS
(
	WITH zlaunch_snapshot AS 
	(
		SELECT
			DATE_TRUNC('day', event_timestamp) created_at
			, user_id 
			, UPPER(SPLIT_PART(lock_product_id,'.',1)) symbol
			, SUM(CASE WHEN event_type = 'lock' THEN amount END) lock_amount
			, SUM(CASE WHEN event_type = 'unlock' THEN amount END) released_amount
		FROM 
			z_launch_service_public.lock_unlock_histories luh 
		GROUP BY 1,2,3
	)
	SELECT 
		p.created_at 
		, u.ap_account_id 
		, symbol
		, 0.0 trade_wallet_amount
		, 0.0 z_wallet_amount
		, 0.0 ziplock_amount
		, SUM(COALESCE (lock_amount, 0)) - SUM(COALESCE (released_amount, 0)) zlaunch_amount
	FROM analytics.period_master p
		LEFT JOIN zlaunch_snapshot z 
			ON p.created_at >= z.created_at
		LEFT JOIN analytics.users_master u
			ON z.user_id = u.user_id 
	WHERE 
		p.created_at >= '2021-10-26 00:00:00'
		AND p.created_at < DATE_TRUNC('day', NOW())
	GROUP BY 1,2,3,4,5,6
);

INSERT INTO warehouse.analytics.tmp_wallets_balance_eod (created_at, ap_account_id, symbol, trade_wallet_amount, z_wallet_amount, ziplock_amount, zlaunch_amount)
(
	SELECT * FROM tmp_z_wallet_balance
);

INSERT INTO warehouse.analytics.tmp_wallets_balance_eod (created_at, ap_account_id, symbol, trade_wallet_amount, z_wallet_amount, ziplock_amount, zlaunch_amount)
(
	SELECT * FROM tmp_trade_wallet_balance
);

INSERT INTO warehouse.analytics.tmp_wallets_balance_eod (created_at, ap_account_id, symbol, trade_wallet_amount, z_wallet_amount, ziplock_amount, zlaunch_amount)
(
	SELECT * FROM tmp_trade_wallet_balance
);

DROP TABLE IF EXISTS tmp_z_wallet_balance;
DROP TABLE IF EXISTS tmp_trade_wallet_balance;

--ALTER TABLE warehouse.analytics.wallets_balance_eod
--ADD COLUMN IF NOT EXISTS zlaunch_amount NUMERIC ;

CREATE TEMP TABLE tmp_wallet_balance AS
(
	WITH base AS 
	(
		SELECT 
			created_at
			, ap_account_id
			, symbol
			, SUM(COALESCE (trade_wallet_amount, 0)) trade_wallet_amount
			, SUM(COALESCE (z_wallet_amount, 0)) z_wallet_amount
			, SUM(COALESCE (ziplock_amount, 0)) ziplock_amount
			, SUM(COALESCE (zlaunch_amount, 0)) zlaunch_amount
		FROM 
			warehouse.analytics.tmp_wallets_balance_eod
		GROUP BY 
			1,2,3
	)
	SELECT 
		*
	FROM 
		base
	WHERE 
		(trade_wallet_amount <> 0 OR z_wallet_amount <> 0 OR ziplock_amount <> 0 OR zlaunch_amount <> 0)
);

DELETE FROM warehouse.analytics.wallets_balance_eod WHERE created_at >= DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL;

INSERT INTO warehouse.analytics.wallets_balance_eod (created_at, ap_account_id, symbol, trade_wallet_amount, z_wallet_amount, ziplock_amount, zlaunch_amount)
(
	SELECT * FROM tmp_wallet_balance
);

DROP TABLE IF EXISTS tmp_wallet_balance;

/*
 * adding crypto price to get usd value
*/

DROP TABLE IF EXISTS warehouse.data_team_staging.wallets_master;

CREATE TABLE IF NOT EXISTS warehouse.data_team_staging.wallets_master
(
	, id								SERIAL PRIMARY KEY
	, created_at						TIMESTAMPTZ
	, ap_account_id	 					INTEGER 
	, symbol 							VARCHAR(255)
	, price								NUMERIC
	, trade_wallet_amount				NUMERIC
	, z_wallet_amount					NUMERIC
	, ziplock_amount					NUMERIC
	, zlaunch_amount					NUMERIC
	, trade_wallet_amount_usd			NUMERIC
	, z_wallet_amount_usd				NUMERIC
	, ziplock_amount_usd				NUMERIC
	, zlaunch_amount_usd				NUMERIC
);

CREATE INDEX IF NOT EXISTS wallets_master_idx ON warehouse.data_team_staging.wallets_master
(created_at, ap_account_id, symbol);

CREATE TEMP TABLE IF NOT EXISTS tmp_wallets_master
(
	SELECT
		w.created_at 
		, w.ap_account_id 
		, w.symbol 
		, r.price 
		, SUM(COALESCE (trade_wallet_amount, 0)) trade_wallet_amount
		, SUM(COALESCE (z_wallet_amount, 0)) z_wallet_amount
		, SUM(COALESCE (ziplock_amount, 0)) ziplock_amount
		, SUM( CASE 
					WHEN r.product_type = 1 THEN (COALESCE (trade_wallet_amount, 0) * 1/r.price)
					WHEN r.product_type = 2 THEN (COALESCE (trade_wallet_amount, 0) * r.price)
				END) AS trade_wallet_amount_usd
		, SUM( COALESCE(z_wallet_amount * r.price, 0)) z_wallet_amount_usd
		, SUM( COALESCE(ziplock_amount * price, 0)) ziplock_amount_usd
		, SUM( COALESCE(zlaunch_amount * price, 0)) zlaunch_amount_usd
	FROM 
		analytics.wallets_balance_eod w
		LEFT JOIN 
			analytics.rates_master r
			ON w.symbol = r.product_1_symbol 
			AND DATE_TRUNC('day', w.created_at) = DATE_TRUNC('day', r.created_at)
	GROUP BY 
		1,2,3,4
	ORDER BY 
		1,2,3
);

INSERT INTO warehouse.data_team_staging.wallets_master 
(created_at, ap_account_id, symbol, price, trade_wallet_amount, z_wallet_amount, ziplock_amount, zlaunch_amount, trade_wallet_amount_usd, z_wallet_amount_usd, ziplock_amount_usd, zlaunch_amount_usd)
(
SELECT * FROM tmp_wallets_master
);

DROP TABLE IF EXISTS tmp_wallets_master;


