--DROP TABLE IF EXISTS warehouse.bo_testing.dm_cohort_aum;

CREATE TABLE IF NOT EXISTS warehouse.bo_testing.dm_product_per_mtu 
(
	created_at					DATE
	, signup_hostcountry		VARCHAR(255)
	, ap_account_id				INTEGER
	, is_trader 				INTEGER
	, is_zmt_trade_wallet 		INTEGER
	, is_nonzmt_trade_wallet 	INTEGER
	, is_zmt_ziplock 			INTEGER
	, is_nonzmt_ziplock 		INTEGER
	, is_zmt_zipup 				INTEGER
	, is_nonzmt_zipup 			INTEGER
	, is_zlaunch				INTEGER 
);

CREATE INDEX IF NOT EXISTS idx_dm_product_per_mtu ON warehouse.bo_testing.dm_product_per_mtu 
(created_at, ap_account_id, signup_hostcountry);

DROP TABLE IF EXISTS tmp_product_per_mtu;

CREATE TEMP TABLE IF NOT EXISTS tmp_product_per_mtu AS
(
	-- product per active account PPAC
	WITH mtu_base AS (
		SELECT created_at , signup_hostcountry , mtu_1 
		FROM mappings.mtu_account_2021 ma 
		WHERE ma.created_at::DATE >= '2021-12-01'
			UNION ALL 
		SELECT *
		FROM mappings.mtu_account_2022 ma2 
	--	WHERE ma2.created_at::DATE = '2022-01-01'
	)	, mtu_list AS (
		SELECT 
			created_at::DATE 
			, signup_hostcountry 
			, mtu_1::INT ap_account_id
		FROM mtu_base
	)	, trader_tag AS (
		SELECT 
			ml.*
			, CASE WHEN tm.ap_account_id IS NOT NULL THEN 1 ELSE 0 END AS is_trader
		FROM mtu_list ml
			LEFT JOIN 
					(SELECT DISTINCT DATE_TRUNC('month', tm.created_at)::DATE created_at  , ap_account_id 
					FROM analytics.trades_master tm 
					WHERE DATE_TRUNC('month', tm.created_at) >= '2021-12-01'
						AND tm.created_at < '2022-06-01') tm 
				ON ml.ap_account_id = tm.ap_account_id
				AND ml.created_at = DATE_TRUNC('month', tm.created_at)
		ORDER BY ap_account_id	
	)	, aum_base AS (
		SELECT 
			a.created_at 
			, a.ap_account_id 
		-- zipup subscribe status to identify zipup amount
			, CASE WHEN u.signup_hostcountry = 'TH' THEN
				(CASE WHEN a.created_at < '2022-05-08' THEN s.tnc_accepted_at ELSE u.zipup_subscribed_at END)
				WHEN u.signup_hostcountry = 'ID' THEN
				(CASE WHEN a.created_at < '2022-07-04' THEN s.tnc_accepted_at ELSE u.zipup_subscribed_at END)
				WHEN u.signup_hostcountry IN ('AU','global') THEN
				(CASE WHEN a.created_at < '2022-06-29' THEN s.tnc_accepted_at ELSE u.zipup_subscribed_at END)
				END AS zipup_subscribed_at
			, a.symbol 
			, CASE	WHEN r.product_type = 1 THEN trade_wallet_amount * 1/r.price 
					WHEN r.product_type = 2 THEN trade_wallet_amount * r.price
					END AS trade_wallet_amount_usd
			, z_wallet_amount * r.price z_wallet_amount_usd
			, ziplock_amount * r.price ziplock_amount_usd
			, zlaunch_amount * r.price zlaunch_amount_usd
		FROM 
			analytics.wallets_balance_eod a 
		-- get country and join with pii data
			RIGHT JOIN trader_tag tm 
				ON tm.ap_account_id = a.ap_account_id 
				AND tm.created_at = DATE_TRUNC('month', a.created_at)::DATE 
			LEFT JOIN 
				analytics.users_master u 
				ON a.ap_account_id = u.ap_account_id 
			LEFT JOIN 
				analytics.rates_master r 
				ON a.symbol = r.product_1_symbol
				AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', r.created_at)
			LEFT JOIN 
				warehouse.zip_up_service_public.user_settings s
				ON u.user_id = s.user_id 
		WHERE 
			a.created_at >= '2021-12-01' AND a.created_at < '2022-06-01'
		-- exclude test products
			AND a.symbol NOT IN ('TST1','TST2')
	--	    AND ((a.created_at = DATE_TRUNC('month', a.created_at) + '1 month' - '1 day'::INTERVAL) OR (a.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL))
			AND u.signup_hostcountry IN ('TH','ID','AU','global')
		ORDER BY 1 DESC 
	)	, aum_snapshot AS (
		SELECT 
			DATE_TRUNC('day', b.created_at)::DATE created_at
			, b.ap_account_id
			, COUNT(b.created_at) day_count
			, COALESCE (SUM( CASE WHEN b.symbol = 'ZMT' THEN COALESCE (trade_wallet_amount_usd, 0) END), 0) avg_zmt_trade_wallet_usd
			, COALESCE (SUM( CASE WHEN b.symbol <> 'ZMT' THEN COALESCE (trade_wallet_amount_usd, 0) END), 0) avg_nonzmt_trade_wallet_usd
			, COALESCE (SUM( CASE WHEN b.symbol = 'ZMT' THEN COALESCE (ziplock_amount_usd, 0) END), 0) avg_zmt_ziplock_usd
			, COALESCE (SUM( CASE WHEN b.symbol <> 'ZMT' THEN COALESCE (ziplock_amount_usd, 0) END), 0) avg_nonzmt_ziplock_usd
			, COALESCE (SUM( CASE WHEN b.symbol = 'ZMT' THEN 
						COALESCE (CASE WHEN zipup_subscribed_at IS NOT NULL AND b.created_at >= DATE_TRUNC('day', zipup_subscribed_at) 
									AND symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')
						THEN
							(CASE 	WHEN b.created_at <= '2021-09-02 00:00:00' THEN COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0)
									WHEN b.created_at > '2021-09-02 00:00:00' THEN COALESCE (z_wallet_amount_usd, 0) END)
						END, 0) END), 0) AS avg_zmt_zipup_usd
			, COALESCE (SUM( CASE WHEN b.symbol <> 'ZMT' THEN 
						COALESCE (CASE WHEN zipup_subscribed_at IS NOT NULL AND b.created_at >= DATE_TRUNC('day', zipup_subscribed_at) 
									AND symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')
						THEN
							(CASE 	WHEN b.created_at <= '2021-09-02 00:00:00' THEN COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0)
									WHEN b.created_at > '2021-09-02 00:00:00' THEN COALESCE (z_wallet_amount_usd, 0) END)
						END, 0) END), 0) AS avg_nonzmt_zipup_usd
			, COALESCE (SUM( COALESCE (zlaunch_amount_usd, 0)), 0) avg_zlaunch_usd
		FROM 
			aum_base b
		GROUP BY 
			1,2
		ORDER BY 
			1 	
	)--	, user_level AS (
		SELECT 
			tm.*
			, CASE WHEN (avg_zmt_trade_wallet_usd / day_count) >= 1 THEN 1 ELSE 0 END is_zmt_trade_wallet
			, CASE WHEN (avg_nonzmt_trade_wallet_usd / day_count) >= 1 THEN 1 ELSE 0 END is_nonzmt_trade_wallet
			, CASE WHEN (avg_zmt_ziplock_usd / day_count) >= 1 THEN 1 ELSE 0 END is_zmt_ziplock
			, CASE WHEN (avg_nonzmt_ziplock_usd / day_count) >= 1 THEN 1 ELSE 0 END is_nonzmt_ziplock
			, CASE WHEN (avg_zmt_zipup_usd / day_count) >= 1 THEN 1 ELSE 0 END is_zmt_zipup
			, CASE WHEN (avg_nonzmt_zipup_usd / day_count) >= 1 THEN 1 ELSE 0 END is_nonzmt_zipup
			, CASE WHEN (avg_zlaunch_usd / day_count) >= 1 THEN 1 ELSE 0 END is_zlaunch
		FROM trader_tag tm
			LEFT JOIN aum_snapshot a  
				ON tm.ap_account_id = a.ap_account_id 
				AND tm.created_at = a.created_at
);

INSERT INTO warehouse.bo_testing.dm_product_per_mtu 
(SELECT * FROM tmp_product_per_mtu)
;

DROP TABLE IF EXISTS tmp_product_per_mtu;