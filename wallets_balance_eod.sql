/*
 * TABLE 1 + 2
 * Double wallets consolidation --> coin balance
 * 1. Trade wallet from warehouse.public.accounts_positions_daily
 * 2. ZMT Staked before 2021-08-04 from warehouse.user_app_public.zip_crew_stakes 
 * 3. Z Wallet balance + Zip Lock after 2021-08-04 from warehouse.asset_manager_public.ledgers 
 */

DROP TABLE IF EXISTS warehouse.analytics.tmp_wallets_balance_eod;

CREATE TABLE IF NOT EXISTS warehouse.analytics.tmp_wallets_balance_eod
(
	id									SERIAL PRIMARY KEY 
	, created_at	 					TIMESTAMPTZ
	, ap_account_id	 					INTEGER 
	, symbol 							VARCHAR(255)
	, trade_wallet_amount				NUMERIC
	, z_wallet_amount					NUMERIC
	, ziplock_amount					NUMERIC
);

CREATE INDEX tmp_wallets_balance_eod_keys ON warehouse.analytics.tmp_wallets_balance_eod
(ap_account_id, symbol, created_at);


DROP TABLE IF EXISTS tmp_z_wallet_balance;
DROP TABLE IF EXISTS tmp_zmt_staked_balance;
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
				GENERATE_SERIES('2021-08-04'::TIMESTAMP, DATE_TRUNC('day', NOW()), '1 day') "date"
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
			l.account_id IS NOT NULL
		GROUP BY 1,2,3,4
		ORDER BY 1 DESC
	)
	SELECT 
		*
	FROM 
		base
	WHERE 
		(z_wallet_amount <> 0 OR ziplock_amount <> 0)
);

---- zmt_staked daily consolidation before 2021-08-04
CREATE TEMP TABLE tmp_zmt_staked_balance AS
(
	SELECT
		d.created_at
		, u.ap_account_id 
		, p.symbol 
		, 0.0 trade_wallet_amount
		, 0.0 z_wallet_amount
		, SUM(s.amount) "ziplock_amount"
	FROM
		(
		SELECT
			DISTINCT  "date" AS created_at 
			,s.user_id
		FROM GENERATE_SERIES('2020-12-01'::TIMESTAMP, '2021-08-03'::TIMESTAMP, '1 day') "date"
		CROSS JOIN
			(SELECT DISTINCT user_id FROM warehouse.user_app_public.zip_crew_stakes ) s
		) d
	LEFT JOIN
		warehouse.user_app_public.zip_crew_stakes s
			ON d.user_id = s.user_id
			AND d.created_at >= DATE_TRUNC('day', s.staked_at)
			AND d.created_at < DATE_TRUNC('day', COALESCE(s.released_at, s.releasing_at))
	LEFT JOIN
		warehouse.mysql_replica_apex.products p
			ON s.product_id = p.product_id
	LEFT JOIN 
		warehouse.analytics.users_master u
			ON s.user_id = u.user_id 
	WHERE 
		s.user_id IS NOT NULL 
	GROUP by 1,2,3,4,5
	ORDER BY 1 DESC 
);
	
--	 trade_wallet daily consolidation 
CREATE TEMP TABLE tmp_trade_wallet_balance AS
(
	SELECT  
	-- balance on 2021-05-19 was delayed to 2021-05-20 03:43:52, convert it back to 2021-05-19
		CASE WHEN DATE_TRUNC('hour', a.created_at) = '2021-05-20 03:00:00' THEN '2021-05-19 00:00:00' ELSE DATE_TRUNC('day', a.created_at) END AS created_at 
		, a.account_id ap_account_id
		, p.symbol 
		, a.amount trade_wallet_amount
		, 0.0 z_wallet_amount
		, 0.0 ziplock_amount
	FROM 
		warehouse.public.accounts_positions_daily a
		LEFT JOIN warehouse.mysql_replica_apex.products p 
			ON a.product_id = p.product_id 
		LEFT JOIN analytics.users_master u 
			ON a.account_id = u.ap_account_id 
	WHERE 
		u.user_id IS NOT NULL 
	ORDER BY 1 DESC 
);

INSERT INTO warehouse.analytics.tmp_wallets_balance_eod (created_at, ap_account_id, symbol, trade_wallet_amount, z_wallet_amount, ziplock_amount)
(
	SELECT * FROM tmp_z_wallet_balance
);

INSERT INTO warehouse.analytics.tmp_wallets_balance_eod (created_at, ap_account_id, symbol, trade_wallet_amount, z_wallet_amount, ziplock_amount)
(
	SELECT * FROM tmp_zmt_staked_balance
);

INSERT INTO warehouse.analytics.tmp_wallets_balance_eod (created_at, ap_account_id, symbol, trade_wallet_amount, z_wallet_amount, ziplock_amount)
(
	SELECT * FROM tmp_trade_wallet_balance
);

DROP TABLE IF EXISTS tmp_z_wallet_balance;
DROP TABLE IF EXISTS tmp_zmt_staked_balance;
DROP TABLE IF EXISTS tmp_trade_wallet_balance;


DROP TABLE IF EXISTS warehouse.analytics.wallets_balance_eod;

CREATE TABLE IF NOT EXISTS warehouse.analytics.wallets_balance_eod
(
	id									SERIAL PRIMARY KEY 
	, created_at	 					TIMESTAMPTZ
	, ap_account_id						INTEGER 
	, symbol 							VARCHAR(255)
	, trade_wallet_amount				NUMERIC
	, z_wallet_amount					NUMERIC
	, ziplock_amount					NUMERIC
);

CREATE INDEX wallets_balance_eod_keys ON warehouse.analytics.wallets_balance_eod
(ap_account_id, symbol, created_at);


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
		FROM 
			warehouse.data_team_staging.tmp_wallets_balance_eod
		GROUP BY 
			1,2,3
	)
	SELECT 
		*
	FROM 
		base
	WHERE 
		(trade_wallet_amount > 0 OR z_wallet_amount > 0 OR ziplock_amount > 0)
);

INSERT INTO warehouse.analytics.wallets_balance_eod (created_at, ap_account_id, symbol, trade_wallet_amount, z_wallet_amount, ziplock_amount)
(
	SELECT * FROM tmp_wallet_balance
);

--DROP TABLE IF EXISTS tmp_wallet_balance;
--DROP TABLE IF EXISTS warehouse.analytics.tmp_wallets_balance_eod;