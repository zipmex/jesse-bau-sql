SELECT 
	release_datetime 
	, user_id 
	, UPPER(SPLIT_PART(product_id ,'.',1)) symbol
	, lock_days 
	, SUM(amount) locked_amount
FROM zip_lock_service_public.lock_transactions lt 
WHERE 
	release_datetime IS NOT NULL 
	AND status = 'completed'
GROUP BY 1,2,3,4
ORDER BY 1 DESC 
;


SELECT 
	created_at 
	, signup_hostcountry 
	, product_1_symbol 
	, sum(usd_net_buy_amount)
FROM reportings_data.dm_user_transactions_dwt_daily d
WHERE created_at >= NOW()::DATE - '2 day'::INTERVAL
AND product_1_symbol IN ('BTC','ETH','USDC','USDT')
AND signup_hostcountry = 'ID'
--AND ap_account_id NOT IN (SELECT ap_account_id FROM mappings.users_mapping um)
GROUP BY 1,2,3
ORDER BY 1 DESC 


SELECT 
	created_at::DATE
	, signup_hostcountry 
	, product_1_symbol 
	, SUM(CASE WHEN side = 'Buy' THEN quantity  END) buy_unit
	, SUM(CASE WHEN side = 'Sell' THEN quantity END) sell_unit
	, SUM(CASE WHEN side = 'Buy' THEN amount_usd END) buy_usd
	, SUM(CASE WHEN side = 'Sell' THEN amount_usd END) sell_usd
FROM analytics.trades_master tm 
WHERE created_at >= NOW()::DATE - '1 day'::INTERVAL
AND product_1_symbol IN ('BTC','ETH','USDC','USDT')
AND signup_hostcountry = 'ID'
GROUP BY 1,2,3



SELECT 
	wtm.created_at::DATE
	, wtm.signup_hostcountry 
	, up.email 
	, product_symbol 
	, COUNT(DISTINCT wtm.ap_account_id) withdrawer_count
	, SUM(amount) withdraw_unit
	, SUM(amount_usd) withdraw_usd
FROM analytics.withdraw_tickets_master wtm 
	LEFT JOIN analytics_pii.users_pii up 
	ON wtm.ap_account_id = up.ap_account_id 
WHERE wtm.created_at >= NOW()::DATE - '1 day'::INTERVAL
AND product_symbol IN ('BTC','ETH','USDC','USDT')
AND signup_hostcountry = 'ID'
GROUP BY 1,2,3,4
