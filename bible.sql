email: 'lecongthinh.255@gmail.com'
account_id: '143639'
user_id: '143718'
zip_user_id: '01F0BV36CJX570T14YFQ1BFWC0'
'01F67663GD1K5PT8HE2GGMD3RM'

------ coin type: 1 = fiat, 2 = crypto
--		AND a.product_id IN (1, 2, 3, 14, 15, 25, 26, 27, 30, 33, 34, 35)
		/*
		1	BTC		Bitcoin 		2	LTC		Litecoin 		3	ETH		Ethereum
		14	USDT	Tether USD 		15	GOLD	XBullion 		25	BTC		Bitcoin
		26	LTC		Litecoin 		27	ETH		Ethereum 		30	USDT	Tether USD
		33	USDC	USD Coin 		34	USDC	USD Coin 		35	GOLD	XBullion
		13 BCH		29 BCH
		 */

-- exclude WHALES: 
email not in ('vipadatam@gmail.com','cybersafta@gmail.com','elisrisna@gmail.com','kifujiki@hotmail.com','alfathan.foundation@gmail.com','dady.dermawan@yahoo.com','rizcanorlita@gmail.com','kikisaknana2@gmail.com','brosuranus@gmail.com','felix.henry.kurniawan@gmail.com','royniagara3@gmail.com')
account_id not in (1373,1432,13266,16211,16308,22576,34535,48900,53463,80871,84319)
;

SELECT tick_to_timestamp((1632463800000 - 621355968000000000) / 10000000)


SELECT 
	up.email 
	, um.*
FROM analytics_pii.users_pii up 
	LEFT JOIN analytics.users_master um 
	ON up.user_id = um.user_id 
WHERE up.ap_account_id IN (1047)
;



SELECT 
    DISTINCT symbol , "type" 
FROM apex.products p 
WHERE p.product_id IN (10, 12, 11, 6, 14, 30, 33, 34, 8, 1, 25, 3, 27)

----- date time function 
SELECT DATE_TRUNC('day',NOW()) date_only
	, NOW() date_time 
	, NOW()::date date_without_time
	, DATE_PART('epoch','2021-05-28 00:00:00'::timestamp) * 10000000 + 621355968000000000 epoch_converted 
	, DATE_TRUNC('week', NOW()) beginning_week
	, DATE_TRUNC('week', NOW()) + '6 days'::interval end_of_week 
	, DATE_TRUNC('day',NOW()) - '1 day'::interval yesterday 
	, DATE_TRUNC('month',NOW()) - '1 day'::interval end_of_last_month 
	, DATE_TRUNC('month',NOW() - '1 day'::interval) month_of_yesterday  
	, AGE(DATE_TRUNC('month', NOW()), '2020-12-01')
	, NOW()::date - '1990-05-25' age_days
	, (EXTRACT('year' FROM AGE(DATE_TRUNC('month', NOW()), '2020-12-01'))*12 + EXTRACT('month' FROM AGE(DATE_TRUNC('month', NOW()), '2020-12-01'))) + 1 "trade_period_counter"
	, TRUNC(DATE_PART('day', DATE_TRUNC('week', NOW()) - '2020-12-01')/7) +1 "trade_period_counter"
	
SELECT 
	NOW() --date with timestamp 
	, NOW() + '7 hours'::interval 
	, NOW() + '7 hour'::interval 
	, NOW()::Date --date without timestamp
	, EXTRACT(DOW from NOW())::INTEGER  -- 1 = Monday
		
	--condition: 
	WHERE t.created_at BETWEEN -- to filter 1 week in the past start from 00:00 midnight (last week)
		NOW()::DATE - (EXTRACT(DOW FROM NOW())::INTEGER-7) -- return last 2 weeks
		NOW()::DATE - (EXTRACT(DOW from NOW())::INTEGER-1) -- return yesterday
 		
 		
 		
----- PARSE STRING - data like json
SELECT d.account_id 
	, d.asset_id 
	, d.asset_name 
--	, TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM d.deposit_info),',',1),':',2)) provider 
	, TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM d.deposit_info),',',2),':',2)) txid 
	, TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM d.deposit_info),',',3),':',2)) from_add 
	, TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM d.deposit_info),',',4),':',2)) to_add 
	, SUM(d.amount) amount
FROM oms_data.mysql_replica_apex.deposit_tickets d
	LEFT JOIN oms_data.mysql_replica_apex.products p 
	ON d.asset_id = p.product_id 
WHERE status = 5 
AND p."type" = 2 
GROUP BY 1,2,3,4,5,6
;


SELECT w.account_id 
	, w.asset_id 
	, p.symbol 
	, TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM w.withdraw_transaction_details),',',1),':',2)) txid 
	, TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM w.withdraw_transaction_details),',',4),':',2)) status  
	, LEFT(TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM w.withdraw_transaction_details),',',5),':',2)),10)::date send_date 
	, SUM(w.amount) amount
FROM oms_data.mysql_replica_apex.withdraw_tickets w 
	LEFT JOIN oms_data.mysql_replica_apex.products p 
	ON w.asset_id = p.product_id 
WHERE status = 5 
AND p."type" = 2 
AND w.account_id = 143639 
GROUP BY 1,2,3,4,5,6 
;

