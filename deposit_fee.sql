SELECT 
--	DATE_TRUNC('month', created_at)::DATE created_month
--	DATE_TRUNC('week', created_at)::DATE created_week
	d.created_at 
	, d.ap_account_id 
	, d.signup_hostcountry 
	, d.product_1_symbol 
	, CASE WHEN DATE_TRUNC('month', d.created_at)::DATE = DATE_TRUNC('month', um.created_at)::DATE THEN 'new_user' ELSE 'existing_user' END AS is_new_user
	, CASE WHEN DATE_TRUNC('month', d.created_at)::DATE = DATE_TRUNC('month', um.created_at)::DATE THEN 'new_mtu'
			WHEN dmm.ap_account_id IS NOT NULL THEN 'current_mtu' ELSE 'activated' END AS is_mtu
	, COUNT(DISTINCT CASE WHEN sum_coin_deposit_amount > 0 THEN d.ap_account_id END) count_depositor
	, SUM(d.sum_coin_deposit_amount) sum_coin_deposit_amount 
	, SUM(d.sum_usd_deposit_amount) sum_usd_deposit_amount 
FROM
	reportings_data.dm_user_transactions_dwt_daily d
	LEFT JOIN 
		analytics.users_master um 
		ON d.user_id = um.user_id 
	LEFT JOIN 
		analytics.dm_mtu_monthly dmm 
		ON d.ap_account_id = dmm.ap_account_id 
		AND dmm.mtu = TRUE
		AND DATE_TRUNC('month', d.created_at)::DATE - '1 month'::INTERVAL = dmm.mtu_month
WHERE 
	d.signup_hostcountry = 'global'
	AND d.product_1_symbol = 'SGD'
	AND d.created_at >= DATE_TRUNC('month', NOW()::DATE) - '5 month'::INTERVAL 
	AND d.created_at < '2022-07-01'
	AND d.sum_coin_deposit_amount > 0
GROUP BY 1,2,3,4,5,6
;


SELECT *
FROM bo_testing.dm_double_wallet