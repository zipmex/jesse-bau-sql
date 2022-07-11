
DROP TABLE IF EXISTS warehouse.bo_testing.dm_zw_hourly_transations;

CREATE TABLE IF NOT EXISTS warehouse.bo_testing.dm_zw_hourly_transations
(
	created_at							TIMESTAMP
	, ap_account_id						INTEGER
	, product_1_symbol					VARCHAR(255)
	, zw_deposit_count					NUMERIC
	, transfer_to_zwallet_amount		NUMERIC
	, transfer_to_zwallet_usd			NUMERIC
	, zw_withdraw_count					NUMERIC
	, withdraw_from_zwallet_amount		NUMERIC
	, withdraw_from_zwallet_usd			NUMERIC
	, count_ziplock_transactions		INTEGER
	, ziplock_amount					NUMERIC
	, ziplock_usd						NUMERIC
	, count_unlock_transactions			INTEGER
	, unlock_amount						NUMERIC
	, unlock_usd						NUMERIC
	, count_zlaunch_staked				INTEGER
	, zlaunch_stake_amount				NUMERIC
	, zlaunch_stake_usd					NUMERIC
	, count_zlaunch_unstaked			INTEGER
	, zlaunch_unlock_amount				NUMERIC
	, zlaunch_unlock_usd				NUMERIC
);

CREATE INDEX IF NOT EXISTS idx_dm_zw_hourly_transations ON warehouse.bo_testing.dm_zw_hourly_transations
(created_at, ap_account_id, product_1_symbol);

ALTER TABLE warehouse.bo_testing.dm_zw_hourly_transations REPLICA IDENTITY FULL;

--TRUNCATE TABLE warehouse.bo_testing.dm_zw_hourly_transations;

CREATE TEMP TABLE IF NOT EXISTS tmp_dm_zw_hourly_transations AS 
(
-- zwallet transaction daily at account id level - from Aug 4 2021
	WITH zw_deposit AS (
		SELECT 
			DATE_TRUNC('hour', dt.created_at) created_at
			, dt.account_id 
			, um.ap_account_id 
			, um.signup_hostcountry 
			, UPPER(SPLIT_PART(dt.product_id,'.',1)) product_1_symbol
			, COUNT(DISTINCT id) zw_deposit_count
			, SUM(amount) transfer_to_zwallet_amount
		FROM 
		-- transfer from trade wallet to Z wallet
			asset_manager_public.deposit_transactions dt 
			LEFT JOIN 
				analytics.users_master um 
				ON dt.account_id = um.user_id 
		WHERE um.signup_hostcountry IN ('TH','AU','ID','global')
			AND dt.created_at < NOW()::DATE
            AND service_id = 'main_wallet'
            AND ref_action = 'deposit'
		GROUP BY 1,2,3,4,5
	)	, zw_withdraw AS (
		SELECT 
			DATE_TRUNC('hour', wt.created_at) created_at
			, wt.account_id 
			, um.ap_account_id 
			, um.signup_hostcountry 
			, UPPER(SPLIT_PART(wt.product_id,'.',1)) product_1_symbol
			, COUNT(DISTINCT id) zw_withdraw_count
			, SUM(amount) withdraw_from_zwallet_amount
		FROM 
		-- transfer from Z wallet to trade wallet
			asset_manager_public.withdrawal_transactions wt  
			LEFT JOIN 
				analytics.users_master um 
				ON wt.account_id = um.user_id 
		WHERE um.signup_hostcountry IN ('TH','AU','ID','global')
			AND wt.created_at < NOW()::DATE
            AND service_id = 'main_wallet'
            AND ref_action = 'withdraw'
		GROUP BY 1,2,3,4,5
	)	, zw_deposit_withdraw AS (
	-- join deposit and withdraw transactions
		SELECT 
			COALESCE(d.created_at, w.created_at) created_at 
			, COALESCE(d.ap_account_id, w.ap_account_id) ap_account_id
			, COALESCE (d.product_1_symbol, w.product_1_symbol) product_1_symbol 
			, SUM( COALESCE(d.zw_deposit_count, 0)) zw_deposit_count 
			, SUM( COALESCE(transfer_to_zwallet_amount, 0)) transfer_to_zwallet_amount
			, SUM( COALESCE(w.zw_withdraw_count, 0)) zw_withdraw_count
			, SUM( COALESCE(withdraw_from_zwallet_amount, 0)) withdraw_from_zwallet_amount
		FROM zw_deposit d
			FULL OUTER JOIN 
			zw_withdraw w 
			ON d.ap_account_id = w.ap_account_id 
				AND d.signup_hostcountry = w.signup_hostcountry 
				AND d.created_at = w.created_at 
				AND d.product_1_symbol = w.product_1_symbol 
		GROUP BY 1,2,3
	)
	SELECT 
		z.created_at
		, z.ap_account_id
		, z.product_1_symbol
		, z.zw_deposit_count
		, z.transfer_to_zwallet_amount
		, (z.transfer_to_zwallet_amount * rm.price) transfer_to_zwallet_usd
		, z.zw_withdraw_count
		, z.withdraw_from_zwallet_amount
		, (z.withdraw_from_zwallet_amount * rm.price) withdraw_from_zwallet_usd
	FROM zw_deposit_withdraw z
	-- join rates master to convert to USD
		LEFT JOIN 
			analytics.rates_master rm 
			ON z.product_1_symbol = rm.product_1_symbol 
			AND z.created_at::DATE = rm.created_at::DATE
	ORDER BY 1 DESC 
);


CREATE TEMP TABLE IF NOT EXISTS tmp_dm_ziplock_hourly_transations AS 
(
	-- ziplock transactions 
	WITH ziplock_base AS (
		SELECT 
			DATE_TRUNC('hour', tt.created_at) created_at
			, tt.to_account_id user_id
			, um.ap_account_id 
			, UPPER(SPLIT_PART(tt.product_id,'.',1)) product_1_symbol
			, COALESCE(COUNT( DISTINCT CASE WHEN tt.ref_caller = 'zip_lock' AND tt.ref_action = 'lock' THEN id END ), 0) count_ziplock_transactions
			, COALESCE(COUNT( DISTINCT CASE WHEN tt.ref_caller = 'zip_lock' AND tt.ref_action = 'release' THEN id END ), 0) count_unlock_transactions
			, COALESCE(COUNT( DISTINCT CASE WHEN tt.ref_caller = 'z_launch' AND tt.ref_action = 'lock' THEN id END ), 0) count_zlaunch_stake
			, COALESCE(COUNT( DISTINCT CASE WHEN tt.ref_caller = 'z_launch' AND tt.ref_action = 'release' THEN id END ), 0) count_zlaunch_unstake
			, COALESCE(SUM( CASE WHEN tt.ref_caller = 'zip_lock' AND tt.ref_action = 'lock' THEN amount END ), 0) ziplock_amount
			, COALESCE(SUM( CASE WHEN tt.ref_caller = 'zip_lock' AND tt.ref_action = 'release' THEN amount END ), 0) unlock_amount
			, COALESCE(SUM( CASE WHEN tt.ref_caller = 'z_launch' AND tt.ref_action = 'lock' THEN amount END ), 0) zlaunch_stake_amount
			, COALESCE(SUM( CASE WHEN tt.ref_caller = 'z_launch' AND tt.ref_action = 'release' THEN amount END ), 0) zlaunch_unlock_amount
		FROM 
			asset_manager_public.transfer_transactions tt
			LEFT JOIN 
				analytics.users_master um 
				ON tt.to_account_id = um.user_id 
		WHERE um.signup_hostcountry IN ('TH','AU','ID','global')
			AND tt.ref_action IN ('lock','release')
			AND tt.created_at < NOW()::DATE 
		GROUP BY 1,2,3,4
		ORDER BY 1 DESC 
	)
	SELECT 
		zl.created_at
		, zl.user_id
		, zl.ap_account_id
		, zl.product_1_symbol
		, zl.count_ziplock_transactions
		, zl.ziplock_amount
		, ziplock_amount * rm.price ziplock_usd
		, zl.count_unlock_transactions
		, zl.unlock_amount
		, unlock_amount * rm.price unlock_usd
		, zl.count_zlaunch_stake
		, zl.zlaunch_stake_amount
		, zlaunch_stake_amount * rm.price zlaunch_stake_usd
		, zl.count_zlaunch_unstake
		, zl.zlaunch_unlock_amount
		, zlaunch_unlock_amount * rm.price zlaunch_unlock_usd
	FROM ziplock_base zl
		LEFT JOIN analytics.rates_master rm 
			ON zl.created_at::DATE = rm.created_at::DATE 
			AND zl.product_1_symbol = rm.product_1_symbol 
);


CREATE TEMP TABLE IF NOT EXISTS tmp_dm_zw_hourly_final AS 
(
	SELECT 
		COALESCE(z.created_at, l.created_at) created_at 
		, COALESCE(z.ap_account_id, l.ap_account_id) ap_account_id
		, COALESCE (z.product_1_symbol, l.product_1_symbol) product_1_symbol 
		, SUM( COALESCE(z.zw_deposit_count, 0)) zw_deposit_count
		, SUM( COALESCE(z.transfer_to_zwallet_amount, 0)) transfer_to_zwallet_amount
		, SUM( COALESCE(z.transfer_to_zwallet_usd, 0)) transfer_to_zwallet_usd
		, SUM( COALESCE(z.zw_withdraw_count, 0)) zw_withdraw_count
		, SUM( COALESCE(z.withdraw_from_zwallet_amount, 0)) withdraw_from_zwallet_amount
		, SUM( COALESCE(z.withdraw_from_zwallet_usd, 0)) withdraw_from_zwallet_usd
		, SUM( COALESCE(l.count_ziplock_transactions, 0)) count_ziplock_transactions
		, SUM( COALESCE(l.ziplock_amount, 0)) ziplock_amount
		, SUM( COALESCE(l.ziplock_usd, 0)) ziplock_usd
		, SUM( COALESCE(l.count_unlock_transactions, 0)) count_unlock_transactions
		, SUM( COALESCE(l.unlock_amount, 0)) unlock_amount
		, SUM( COALESCE(l.unlock_usd, 0)) unlock_usd
		, SUM( COALESCE(l.count_zlaunch_stake, 0)) count_zlaunch_stake
		, SUM( COALESCE(l.zlaunch_stake_amount, 0)) zlaunch_stake_amount
		, SUM( COALESCE(l.zlaunch_stake_usd, 0)) zlaunch_stake_usd
		, SUM( COALESCE(l.count_zlaunch_unstake, 0)) count_zlaunch_unstake
		, SUM( COALESCE(l.zlaunch_unlock_amount, 0)) zlaunch_unlock_amount
		, SUM( COALESCE(l.zlaunch_unlock_usd, 0)) zlaunch_unlock_usd
	FROM tmp_dm_zw_hourly_transations z
		FULL OUTER JOIN
			tmp_dm_ziplock_hourly_transations l 
			ON z.created_at = l.created_at
			AND z.ap_account_id = l.ap_account_id
			AND z.product_1_symbol = l.product_1_symbol
	GROUP BY 1,2,3
);


INSERT INTO warehouse.bo_testing.dm_zw_hourly_transations
( SELECT * FROM tmp_dm_zw_hourly_final);


DROP TABLE IF EXISTS tmp_dm_zw_hourly_transations;
DROP TABLE IF EXISTS tmp_dm_ziplock_hourly_transations;
DROP TABLE IF EXISTS tmp_dm_zw_hourly_final;
