---- avg first deposit by user 
WITH base AS
( 
SELECT *
	, Rank() OVER(PARTITION BY ap_account_id ORDER BY updated_at) AS rank_
FROM 
	analytics.deposit_tickets_master d 
WHERE 
	status = 'FullyProcessed'
	AND signup_hostcountry IN ('TH','AU','global','ID')
	AND ap_account_id NOT IN (0, 27308,48870,48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63312, 63313, 161347)
)
SELECT 
	DATE_TRUNC('month',updated_at) datadate
--	, ap_account_id 
	, signup_hostcountry 
--	, product_symbol 
	, COUNT(DISTINCT ap_account_id) user_count 
	, SUM(amount_usd) AS first_deposit_usd 
FROM 
	base 
WHERE 
	rank_ = 1 
GROUP BY 1,2
ORDER BY 1 



---- avg first trade by asset per user 
WITH base AS (
SELECT 
	*
	, RANK() OVER(PARTITION BY ap_account_id, product_1_symbol ORDER BY created_at) rank_ 
FROM 
	analytics.trades_master t 
WHERE 
	ap_account_id NOT IN ('0','186','187','869','870','1356','1357','4344','18866','25041','25224','25225','25226','25227',27443
,'38263','38262','38260','38121','37955','40706','40683','37807','48948','48870','44679','49659','49658','52018','52019','44057','161347')
	AND signup_hostcountry IN ('TH','AU','global','ID')
	AND side = 'Buy'
ORDER BY created_at 
)
SELECT 
	DATE_TRUNC('month', created_at) created_at 
	, signup_hostcountry 
	, product_1_symbol 
	, SUM(quantity) quantity 
	, COUNT(DISTINCT ap_account_id) user_count 
	, SUM(amount_usd) first_trade_usd 
FROM 
	base 
WHERE 
	rank_ = 1
GROUP BY 
	1,2,3
ORDER BY 
	1