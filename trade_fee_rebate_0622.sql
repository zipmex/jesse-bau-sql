SELECT 
	tm.created_at::DATE created_at_utc
	, (tm.created_at + '7 hour'::INTERVAL)::DATE created_at_gmt7
	, (tm.created_at + '8 hour'::INTERVAL)::DATE created_at_gmt8
	, tm.signup_hostcountry 
	, up.email 
	, tm.product_1_symbol 
	, tm.side 
	, SUM(tm.amount_usd) sum_trade_amount
	, COUNT( DISTINCT tm.ap_account_id) count_trader
	, SUM(fm.fee_usd_amount) fee_usd_amount
FROM analytics.trades_master tm 
	LEFT JOIN 
		analytics_pii.users_pii up 
		ON tm.ap_account_id = up.ap_account_id 
	LEFT JOIN 
		analytics.fees_master fm 
		ON tm.execution_id = fm.fee_reference_id 
		AND fm.fee_type = 'Trade'
WHERE 
	tm.product_1_symbol = 'BTC'
	AND tm.signup_hostcountry IN ('AU','ID','global')
	AND tm.created_at >= '2022-06-23'
	AND tm.ap_account_id NOT IN (SELECT ap_account_id FROM mappings.users_mapping um)
GROUP BY 1,2,3,4,5,6,7
;


