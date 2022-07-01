-- ziplock transaction
WITH base AS (
	SELECT 
		a.created_at 
		, CASE WHEN u.signup_hostcountry IS NULL THEN 'unknown' ELSE u.signup_hostcountry END AS signup_hostcountry
		, a.ap_account_id 
	-- filter nominee accounts from users_mapping
		, CASE WHEN a.ap_account_id IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping WHERE ap_account_id <> 496001)
				THEN TRUE ELSE FALSE END AS is_nominee 
	-- filter asset_manager account
		, CASE WHEN a.ap_account_id = 496001 THEN TRUE ELSE FALSE END AS is_asset_manager
	-- zipup subscribe status to identify zipup amount
		, CASE WHEN a.created_at < '2022-09-29' THEN s.tnc_accepted_at ELSE u.zipup_subscribed_at END AS zipup_subscribed_at
		, a.symbol 
		, r.price usd_rate 
		, trade_wallet_amount
		, z_wallet_amount
		, ziplock_amount
		, zlaunch_amount
		, CASE	WHEN r.product_type = 1 THEN trade_wallet_amount * 1/r.price 
				WHEN r.product_type = 2 THEN trade_wallet_amount * r.price
				END AS trade_wallet_amount_usd
		, z_wallet_amount * r.price z_wallet_amount_usd
		, ziplock_amount * r.price ziplock_amount_usd
		, zlaunch_amount * r.price zlaunch_amount_usd
	FROM 
		analytics.wallets_balance_eod a 
	-- get country and join with pii data
		LEFT JOIN 
			analytics.users_master u 
			ON a.ap_account_id = u.ap_account_id 
	-- coin prices and exchange rates (USD)
		LEFT JOIN 
			analytics.rates_master r 
			ON a.symbol = r.product_1_symbol
			AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', r.created_at)
		LEFT JOIN 
			warehouse.zip_up_service_public.user_settings s
			ON u.user_id = s.user_id 
	WHERE 
		a.created_at >= '2022-03-01' AND a.created_at < DATE_TRUNC('month', NOW())::DATE
		AND u.signup_hostcountry IN ('TH')--,'ID','AU','global')
	-- snapshot by end of month or yesterday
		AND ((a.created_at = DATE_TRUNC('month', a.created_at) + '1 month' - '1 day'::INTERVAL) OR (a.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL))
	-- exclude test products
		AND a.symbol IN ('BTC','ETH','GOLD','LTC','USDC','USDT','ZMT') 
--		AND a.ap_account_id = 143639
	ORDER BY 1 DESC 
)	, aum_snapshot AS (
	SELECT 
		DATE_TRUNC('month', created_at)::DATE created_at
		, signup_hostcountry
		, ap_account_id
		, CASE WHEN symbol = 'ZMT' THEN 1 ELSE 0 END AS asset_group
		, SUM( COALESCE (trade_wallet_amount_usd, 0)) trade_wallet_amount_usd
		, SUM( COALESCE (z_wallet_amount_usd, 0)) z_wallet_amount_usd
		, SUM( COALESCE (ziplock_amount_usd, 0)) ziplock_amount_usd
		, SUM( COALESCE (zlaunch_amount_usd, 0)) zlaunch_amount_usd
		, SUM( COALESCE (CASE WHEN zipup_subscribed_at IS NOT NULL AND created_at >= DATE_TRUNC('day', zipup_subscribed_at) AND symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')
					THEN
						(CASE 	WHEN created_at <= '2021-09-02 00:00:00' THEN COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0)
								WHEN created_at > '2021-09-02 00:00:00' THEN COALESCE (z_wallet_amount_usd, 0) END)
					END, 0)) AS interest_zipup_usd
		, SUM( COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0)
				+ COALESCE (ziplock_amount_usd, 0) + COALESCE (zlaunch_amount_usd, 0)) total_amount_usd
	FROM 
		base 
	WHERE 
		is_asset_manager = FALSE AND is_nominee = FALSE
	GROUP BY 
		1,2,3,4
	ORDER BY 
		1 
)	, aum_zmt AS (
	SELECT 
		created_at
		, signup_hostcountry
		, ap_account_id 
		, SUM( CASE WHEN asset_group = 1 THEN trade_wallet_amount_usd END) zmt_tradew_usd
		, SUM( CASE WHEN asset_group = 0 THEN trade_wallet_amount_usd END) non_zmt_tradew_usd
		, SUM( CASE WHEN asset_group = 1 THEN interest_zipup_usd END) zmt_zipup_usd
		, SUM( CASE WHEN asset_group = 0 THEN interest_zipup_usd END) non_zmt_zipup_usd
		, SUM( CASE WHEN asset_group = 1 THEN ziplock_amount_usd END) zmt_ziplock_usd
		, SUM( CASE WHEN asset_group = 0 THEN ziplock_amount_usd END) non_zmt_ziplock_usd
		, SUM( CASE WHEN asset_group = 1 THEN zlaunch_amount_usd END) zmt_zlaunch_usd
		, SUM( CASE WHEN asset_group = 0 THEN zlaunch_amount_usd END) non_zmt_zlaunch_usd
		, SUM( CASE WHEN asset_group = 1 THEN total_amount_usd END) total_zmt_usd
	FROM aum_snapshot
	GROUP BY 1,2,3
)	, duplicate_check AS (
-- check if user was active in Mar but not in Apr
	SELECT 
		--created_at
		signup_hostcountry
		, ap_account_id 
		, CASE WHEN created_at = '2022-03-01' THEN 1 ELSE 0 END AS mar_count
		, CASE WHEN created_at = '2022-04-01' THEN 1 ELSE 0 END AS apr_count
		, COUNT(DISTINCT CASE WHEN (zmt_zipup_usd >= 1 OR non_zmt_zipup_usd >= 1 
								OR zmt_ziplock_usd >= 1 
								OR non_zmt_ziplock_usd >= 1) THEN ap_account_id END) AS active_saver_count
	FROM aum_zmt
	GROUP BY 1,2,3,4
)	, user_count AS (
-- isolate inactive user in Apr
	SELECT 
		ap_account_id
		, active_saver_count
		, SUM(mar_count) mar_count
		, SUM(apr_count) apr_count
	FROM duplicate_check
	GROUP BY 1,2
)	, user_list AS (
	SELECT 
		*
	FROM user_count
	WHERE 
		active_saver_count = 1 AND mar_count = 1 AND apr_count = 0
)	, zlock_trans AS (
-- check ziplock release 
	SELECT 
		tt.created_at::DATE 
		, from_account_id 
		, to_account_id 
		, um.ap_account_id 
		, UPPER(SPLIT_PART(tt.product_id,'.',1)) product_symbol
		, ref_caller 
		, ref_action 
		, SUM( CASE WHEN ref_action = 'lock' THEN amount END) AS lock_amount
		, SUM( CASE WHEN ref_action = 'release' THEN amount END) AS release_amount
		, SUM( CASE WHEN ref_action = 'release' THEN amount * rm.price  END) AS release_amount_usd
	FROM 
		asset_manager_public.transfer_transactions tt 
		LEFT JOIN 
			analytics.users_master um 
			ON tt.from_account_id = um.user_id 
		LEFT JOIN 
			analytics.rates_master rm 
			ON tt.created_at::DATE = rm.created_at::DATE 
			AND UPPER(SPLIT_PART(tt.product_id,'.',1)) = rm.product_1_symbol 
		RIGHT JOIN 
			user_list ul 
			ON um.ap_account_id = ul.ap_account_id 
	WHERE 
		to_account_id = from_account_id
	-- from zip_lock 
		AND ref_caller = 'zip_lock'
	-- release transactions only 
		AND ref_action = 'release' 
		AND tt.created_at >= '2022-04-08'
	-- zipup coins only 
		AND UPPER(SPLIT_PART(tt.product_id,'.',1)) IN ('BTC','ETH','GOLD','LTC','USDC','USDT','ZMT')
	GROUP BY 
		1,2,3,4,5,6,7
)	, withdraw_trans AS (
	SELECT 
		wtm.created_at::DATE 
		, wtm.ap_account_id
		, wtm.product_symbol
		, wtm.product_type withdraw_product_type
		, SUM(amount) withdraw_amount
		, SUM(amount_usd) withdraw_amount_usd
	FROM 
		analytics.withdraw_tickets_master wtm 
		RIGHT JOIN
			user_list ul 
			ON wtm.ap_account_id = ul.ap_account_id
	WHERE 
		wtm.status = 'FullyProcessed'
		AND wtm.created_at >= '2022-04-08'
	-- zipup coins or fiat withdrawal
		AND (wtm.product_symbol IN ('BTC','ETH','GOLD','LTC','USDC','USDT','ZMT') OR wtm.product_type = 'NationalCurrency')
	GROUP BY 1,2,3,4
)	, release_withdraw AS (
	SELECT 
--		COALESCE(tt.created_at, wtm.created_at::DATE) created_at 
		COALESCE (tt.ap_account_id, wtm.ap_account_id ) ap_account_id
		, COALESCE (tt.product_symbol , wtm.product_symbol ) product_symbol
		, SUM(tt.release_amount) ziplock_release_amount
		, SUM(tt.release_amount_usd) ziplock_release_amount_usd
		, wtm.withdraw_product_type
		, SUM(withdraw_amount) withdraw_amount
		, SUM(withdraw_amount_usd) withdraw_amount_usd
	FROM 
		withdraw_trans wtm 
		FULL OUTER JOIN 
			zlock_trans tt 
			ON wtm.ap_account_id = tt.ap_account_id 
			AND wtm.product_symbol = tt.product_symbol
--			AND wtm.created_at::DATE = tt.created_at::DATE 
	GROUP BY 1,2,5
	ORDER BY 1
)
SELECT 
	ap_account_id 
	, CASE WHEN product_symbol = 'THB' THEN 'THB' ELSE 'other' END AS symbol_group
	, SUM( COALESCE (ziplock_release_amount_usd, 0)) ziplock_release_amount_usd
	, withdraw_product_type
	, SUM( COALESCE (withdraw_amount_usd, 0)) withdraw_amount_usd
FROM release_withdraw
GROUP BY 1,2,4
--LIMIT 10
;