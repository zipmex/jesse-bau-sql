-- dm-kyc
SELECT 
	*
FROM reportings_data.dm_user_funnel_monthly dufm 
ORDER BY 2 DESC 



-- dm - asset trade
SELECT 
	*
FROM reportings_data.dm_trade_asset_monthly dtam 
ORDER BY 2 DESC 

-- dm- trade zmt 
SELECT 
	*
FROM reportings_data.dm_trade_zmt_organic_monthly dtzom 
ORDER BY 2 DESC 

-- dm-deposit-withdraw
SELECT 
	created_at 
	, signup_hostcountry 
	, product_type 
	, is_whales 
	, SUM( COALESCE (deposit_count, 0)) deposit_count 
	, SUM( COALESCE (deposit_amount_unit, 0)) deposit_amount_unit 
	, SUM( COALESCE (deposit_amount_usd, 0)) deposit_amount_usd 
	, SUM( COALESCE (withdraw_count, 0)) withdraw_count 
	, SUM( COALESCE (withdraw_amount_unit, 0)) withdraw_amount_unit 
	, SUM( COALESCE (withdraw_amount_usd, 0)) withdraw_amount_usd 
FROM 
	reportings_data.dm_deposit_withdraw_monthly ddwm 
GROUP BY 1,2,3,4
ORDER BY 1 DESC 



--
