-- zwallet transaction daily at account id level - from Aug 4 2021
WITH zw_deposit AS (
	SELECT 
		dt.created_at::DATE 
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
	GROUP BY 1,2,3,4,5
)	, zw_withdraw AS (
	SELECT 
		wt.created_at::DATE 
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
	GROUP BY 1,2,3,4,5
)	, zw_deposit_withdraw AS (
-- join deposit and withdraw transactions
	SELECT 
		COALESCE(d.created_at, w.created_at)::DATE created_at 
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
)	, final_tem AS (
SELECT 
	z.created_at
	, z.ap_account_id
	, um2.signup_hostcountry 
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
		AND z.created_at = rm.created_at 
	LEFT JOIN 
		analytics.users_master um2 
		ON z.ap_account_id = um2.ap_account_id 
WHERE 
	z.created_at >= '2022-06-10'
ORDER BY 1 DESC 
)
SELECT 
	signup_hostcountry
	, product_1_symbol
	, SUM(transfer_to_zwallet_amount) transfer_to_zwallet_amount
	, SUM(withdraw_from_zwallet_amount) withdraw_from_zwallet_amount
FROM final_tem
WHERE product_1_symbol = 'ETH'
GROUP BY 1,2
;


-- ziplock transactions 
WITH ziplock_base AS (
	SELECT 
		tt.created_at::DATE 
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
		ON zl.created_at = rm.created_at 
		AND zl.product_1_symbol = rm.product_1_symbol 
WHERE ap_account_id = 143639
;



