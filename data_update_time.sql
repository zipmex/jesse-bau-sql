-- data update status
SELECT 'users_master' "service", max(created_at) "last_updated"
FROM analytics.users_master um 
	UNION ALL 
SELECT 'trades_master', max(created_at)
FROM analytics.trades_master tm 
	UNION ALL 
SELECT 'deposit', max(created_at)
FROM analytics.deposit_tickets_master dtm 
	UNION ALL 
SELECT 'withdraw', max(created_at)
FROM analytics.withdraw_tickets_master wtm  
	UNION ALL 
SELECT 'fees_master', max(created_at)
FROM analytics.fees_master fm 
	UNION ALL
SELECT 'account_state', to_timestamp((max("time_stamp") - 621355968000000000) / 10000000) 
FROM warehouse.apex.account_product_state_accumulations b
	UNION ALL
SELECT 'ziplock_service', max(balance_datetime)
FROM zip_lock_service_public.vault_accumulated_balances vab
	UNION ALL 
SELECT 'zipup_service', max(updated_at)
FROM zip_up_service_public.balance_snapshots bs 
	UNION ALL
SELECT 'ledgers_balances', max(updated_at)
FROM asset_manager_public.ledger_balances_v2 lv 
	UNION ALL
SELECT 'z_launch', max(inserted_at)
FROM z_launch_service_public.reward_distributions rt
	UNION ALL
SELECT 'rates_master', max(created_at)
FROM analytics.rates_master rm 
	UNION ALL
SELECT 'crypto', max(last_updated)
FROM oms_data_public.cryptocurrency_prices cp 
	UNION ALL
SELECT 'crypto_mapping', max(last_updated)
FROM mappings.public_cryptocurrency_prices pcp
	UNION ALL
SELECT 'ap_prices', max(inserted_at)
FROM oms_data_public.ap_prices ap 
;

SELECT 'wallets_master', max(created_at)
FROM analytics.wallets_balance_eod wbe 
--WHERE wbe.ap_account_id = 143639
--ORDER BY 1 DESC 

SELECT DISTINCT 'accounts_positions', created_at
FROM oms_data_public.accounts_positions_daily apd 
ORDER BY created_at DESC 
LIMIT 4


SELECT 'zipup_service' service, user_id , UPPER(SPLIT_PART(product_id,'.',1)) asset , balance, updated_at
FROM zip_up_service_public.balance_snapshots bs 
WHERE user_id = '01FX4MX9DZ2XQJWCPCDD1F9RZG'
	AND UPPER(SPLIT_PART(product_id,'.',1)) = 'ETH'
ORDER BY updated_at DESC 
LIMIT 3

UNION ALL
SELECT 'ledger_balances', account_id , UPPER(SPLIT_PART(product_id,'.',1)) asset,  balance, max(updated_at) updated_at
FROM asset_manager_public.ledger_balances_v2 lv 
WHERE account_id = '01FX4MX9DZ2XQJWCPCDD1F9RZG'
	AND UPPER(SPLIT_PART(product_id,'.',1)) = 'ETH'
GROUP BY 1,2,3,4
;

