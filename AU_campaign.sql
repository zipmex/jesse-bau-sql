---- AU campaign june 21 - buy crypto 250$ and refer friends
SELECT DATE_TRUNC('day',t.created_at) trading_date 
	, u.ap_user_id 
	, u.email 
--	, t.signup_hostcountry 
--	, u.zip_user_id , r.invited_user_id 
	, r.referrer_id 
	, r.referral_date 
	, t.base_fiat 
	, SUM(t.amount_base_fiat) amount_fiat
	, SUM(t.amount_usd) amount_usd 
FROM analytics.trades_master t 
	LEFT JOIN analytics.users_master u
	ON t.ap_account_id = u.ap_account_id 
	LEFT JOIN (SELECT r.invited_user_id , u.ap_user_id referrer_id , DATE_TRUNC('day',r.referral_created_at) referral_date 
				FROM referral_service.referral_status r 
				LEFT JOIN analytics.users_master u 
				ON r.referring_user_id = u.user_id ) r 
		ON u.user_id = r.invited_user_id 
WHERE t.signup_hostcountry = 'AU'
AND t.ap_account_id NOT IN ('0','186','187','869','870','1356','1357','4344','18866','25041','25224','25225','25226','25227',27443
,'38263','38262','38260','38121','37955','40706','40683','37807','48948','48870','44679','49659','49658','52018','52019','44057','161347')
AND t.side = 'Buy' 
AND t.created_at >= '2021-06-21 00:00:00'
GROUP BY 1,2,3,4,5,6 
ORDER BY 1 DESC 



WITH user_temp AS (
SELECT u.id user_id 
	, u.email 
	, a.ap_account_id 
	, a.ap_user_id 
	, CASE	WHEN signup_hostname IN ('au.zipmex.com', 'trade.zipmex.com.au') 						THEN 'AU'
			WHEN signup_hostname IN ('id.zipmex.com', 'trade.zipmex.co.id') 						THEN 'ID'
			WHEN signup_hostname IN ('th.zipmex.com', 'trade.zipmex.co.th') 						THEN 'TH'
			WHEN signup_hostname IN ('sg.zipmex.com', 'exchange.zipmex.com', 'trade.zipmex.com') 	THEN 'global'
			WHEN signup_hostname IN ('trade.xbullion.io') 											THEN 'xbullion'
			WHEN signup_hostname IN ('global-staging.zipmex.com', 'localhost')						THEN 'test'
			ELSE 'error'
			END "signup_hostcountry" 
FROM user_app_public.users u 
	LEFT JOIN user_app_public.alpha_point_users a 
	ON u.id = a.user_id 
)
SELECT 
--	DATE_TRUNC('day', t.converted_trade_time AT time zone 'Australia/Sydney') trading_date
	DATE_TRUNC('day', t.converted_trade_time) trading_date
	, u.signup_hostcountry
--	, u.ap_user_id 
--	, u.email 
--	, r.referrer_id 
--	, r.referral_date 
--	, CASE WHEN RIGHT(i.symbol, 4) = 'USDT' THEN 'USD' ELSE RIGHT(i.symbol, 3) END AS base_fiat
	, i.symbol 
	, SUM(t.quantity) quantity
	, SUM(t.quantity * price) fiat_vol 
	, SUM( CASE WHEN RIGHT(i.symbol,3) = 'USD' THEN (t.quantity * price) * 1 
				WHEN RIGHT(i.symbol,4) = 'USDT' THEN (t.quantity * price) * 1
				ELSE (t.quantity * price) * 1/COALESCE(e.exchange_rate, b.exchange_rate)
				END) AS usd_vol 
FROM public.trades t 
	LEFT JOIN user_temp u 
		ON t.account_id = u.ap_account_id 
--	LEFT JOIN (SELECT r.invited_user_id , u.ap_user_id referrer_id , DATE_TRUNC('day',r.referral_created_at) referral_date 
--				FROM referral_service.referral_status r 
--				LEFT JOIN analytics.users_master u 
--				ON r.referring_user_id = u.user_id ) r 
--		ON u.user_id = r.invited_user_id 
	LEFT JOIN mysql_replica_apex.instruments i 
		ON t.instrument_id = i.instrument_id 
	LEFT JOIN mysql_replica_apex.products p 
		ON i.product_1_id = p.product_id 
	LEFT JOIN public.cryptocurrency_prices_hourly h 
		ON DATE_TRUNC('hour', converted_trade_time) = DATE_TRUNC('hour', h.last_updated)
		AND i.symbol = h.instrument_symbol
		AND h."source" = 'coinmarketcap'
	LEFT JOIN public.exchange_rates e 
		ON RIGHT(i.symbol, 3) = RIGHT(e.instrument_symbol, 3) 
		AND DATE_TRUNC('day', converted_trade_time) = DATE_TRUNC('day', e.created_at::timestamp)
		AND e."source" = 'coinmarketcap' 
		AND RIGHT(i.symbol, 3) <> 'THB'
	LEFT JOIN public.bank_of_thailand_usdthb_filled_holes b 
		ON DATE_TRUNC('day', converted_trade_time) = DATE_TRUNC('day', b.created_at::timestamp) 
		AND RIGHT(i.symbol, 3) = 'THB'
WHERE 
	u.signup_hostcountry NOT IN ('error','test','xbullion')
	AND t.is_block_trade = FALSE 
	AND t.account_id NOT IN ('0','186','187','869','870','1356','1357','4344','18866','25041','25224','25225','25226','25227',27443
	,'38263','38262','38260','38121','37955','40706','40683','37807','48948','48870','44679','49659','49658','52018','52019','44057','161347')
--AND t.side = 'Buy'
--AND t.converted_trade_time AT time zone 'Australia/Sydney' >= '2021-06-21 00:00:00' 
--AND t.converted_trade_time AT time zone 'Australia/Sydney' < DATE_TRUNC('day', NOW() AT time zone 'Australia/Sydney')
	AND i.symbol LIKE 'AXS%'
GROUP BY 1,2,3
ORDER BY 1 




---- KYC/ register real-time
WITH base AS (
SELECT 
	date_trunc('day', u.inserted_at) register_date 
	, CASE WHEN o.level_increase_status = 'pass' THEN date_trunc('day', o.updated_at) END AS kyc_date  
	, CASE	WHEN signup_hostname IN ('au.zipmex.com', 'trade.zipmex.com.au') THEN 'AU'
			WHEN signup_hostname IN ('id.zipmex.com', 'trade.zipmex.co.id') THEN 'ID'
			WHEN signup_hostname IN ('th.zipmex.com', 'trade.zipmex.co.th') THEN 'TH'
			WHEN signup_hostname IN ('sg.zipmex.com', 'exchange.zipmex.com', 'trade.zipmex.com') THEN 'global'
			WHEN signup_hostname IN ('trade.xbullion.io') THEN 'xbullion'
			WHEN signup_hostname IN ('global-staging.zipmex.com', 'localhost')	THEN 'test'
			ELSE 'error'
			END "signup_hostcountry" 
	, u.id 
	, u.email_verified_at 
	, u.mobile_number_verified_at 
	, o.level_increase_status 
FROM user_app_public.users u 
	LEFT JOIN user_app_public.onfido_applicants o 
		ON u.id = o.user_id 
), new_user AS (
SELECT date_trunc('month', register_date) datamonth 
	, signup_hostcountry 
	, COUNT(DISTINCT id) new_user
	, COUNT(DISTINCT CASE WHEN email_verified_at IS NOT NULL THEN id END) AS email_verified_user
FROM base 
GROUP BY 1,2 
ORDER BY 2,1 DESC
), verified_user AS (
SELECT date_trunc('month', kyc_date) datamonth 
	, signup_hostcountry
	, COUNT(DISTINCT CASE WHEN level_increase_status = 'pass' AND mobile_number_verified_at IS NOT NULL AND email_verified_at IS NOT NULL THEN id END) AS verified_user
FROM base 
GROUP BY 1,2 
ORDER BY 2,1 DESC 
)
SELECT n.datamonth
	, n.signup_hostcountry
	, new_user
	, email_verified_user 
	, verified_user
	, SUM(new_user) OVER(PARTITION BY n.signup_hostcountry ORDER BY n.datamonth) total_new_user
	, SUM(email_verified_user) OVER(PARTITION BY n.signup_hostcountry ORDER BY n.datamonth) total_email_verified_user
	, SUM(verified_user) OVER(PARTITION BY v.signup_hostcountry ORDER BY v.datamonth) total_verified_user
FROM new_user n 
	LEFT JOIN verified_user v 
	ON n.datamonth = v.datamonth AND n.signup_hostcountry = v.signup_hostcountry
WHERE n.signup_hostcountry NOT IN ('test','error','xbullion')
ORDER BY 2,1 DESC 
;
	



---- AU campaign june 21 - buy crypto 250$ and refer friends
WITH user_temp AS (
SELECT u.id user_id 
	, u.email 
	, a.ap_account_id 
	, a.ap_user_id 
	, CASE WHEN o.level_increase_status = 'pass' THEN date_trunc('day', o.updated_at) END AS kyc_date  
	, CASE	WHEN signup_hostname IN ('au.zipmex.com', 'trade.zipmex.com.au') THEN 'AU'
			WHEN signup_hostname IN ('id.zipmex.com', 'trade.zipmex.co.id') THEN 'ID'
			WHEN signup_hostname IN ('th.zipmex.com', 'trade.zipmex.co.th') THEN 'TH'
			WHEN signup_hostname IN ('sg.zipmex.com', 'exchange.zipmex.com', 'trade.zipmex.com') THEN 'global'
			WHEN signup_hostname IN ('trade.xbullion.io') THEN 'xbullion'
			WHEN signup_hostname IN ('global-staging.zipmex.com', 'localhost')	THEN 'test'
			ELSE 'error'
			END "signup_hostcountry" 
FROM 
	user_app_public.users u 
	LEFT JOIN user_app_public.alpha_point_users a 
		ON u.id = a.user_id 
	LEFT JOIN user_app_public.onfido_applicants o 
		ON u.id = o.user_id 
)
SELECT 
	DATE_TRUNC('day', t.converted_trade_time AT time zone 'Australia/Sydney') trading_date
	, u.ap_user_id 
	, u.email 
	, r.referrer_id 
	, r.referral_date 
	, CASE WHEN RIGHT(i.symbol, 4) = 'USDT' THEN 'USD' ELSE RIGHT(i.symbol, 3) END AS base_fiat
	, SUM(t.quantity * price) fiat_vol 
	, SUM( CASE WHEN RIGHT(i.symbol,3) = 'USD' THEN (t.quantity * price) * 1 
				WHEN RIGHT(i.symbol,4) = 'USDT' THEN (t.quantity * price) * 1
				ELSE (t.quantity * price) * 1/COALESCE(e.exchange_rate, b.exchange_rate)
				END) AS usd_vol 
FROM public.trades t 
	LEFT JOIN user_temp u 
		ON t.account_id = u.ap_account_id 
	LEFT JOIN (SELECT r.invited_user_id , u.ap_user_id referrer_id , DATE_TRUNC('day',r.referral_created_at) referral_date 
				FROM referral_service.referral_status r 
				LEFT JOIN analytics.users_master u 
				ON r.referring_user_id = u.user_id ) r 
		ON u.user_id = r.invited_user_id 
	LEFT JOIN mysql_replica_apex.instruments i 
		ON t.instrument_id = i.instrument_id 
	LEFT JOIN mysql_replica_apex.products p 
		ON i.product_1_id = p.product_id 
	LEFT JOIN public.cryptocurrency_prices_hourly h 
		ON DATE_TRUNC('hour', converted_trade_time) = DATE_TRUNC('hour', h.last_updated)
		AND i.symbol = h.instrument_symbol
		AND h."source" = 'coinmarketcap'
	LEFT JOIN public.exchange_rates e 
		ON RIGHT(i.symbol, 3) = RIGHT(e.instrument_symbol, 3) 
		AND DATE_TRUNC('day', converted_trade_time) = DATE_TRUNC('day', e.created_at::timestamp)
		AND e."source" = 'coinmarketcap' 
		AND RIGHT(i.symbol, 3) <> 'THB'
	LEFT JOIN public.bank_of_thailand_usdthb_filled_holes b 
		ON DATE_TRUNC('day', converted_trade_time) = DATE_TRUNC('day', b.created_at::timestamp) 
		AND RIGHT(i.symbol, 3) = 'THB'
WHERE u.signup_hostcountry = 'AU'
AND t.is_block_trade = FALSE 
AND t.side = 'Buy'
AND t.converted_trade_time AT time zone 'Australia/Sydney' >= '2021-06-21 00:00:00' 
AND t.converted_trade_time AT time zone 'Australia/Sydney' < DATE_TRUNC('day', NOW() AT time zone 'Australia/Sydney')
GROUP BY 1,2,3,4,5,6
ORDER BY 1 DESC 