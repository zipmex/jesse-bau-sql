------------ MAU deposit count
with trade_summary as
(
select DATE_TRUNC('month', t.created_at) "month_"
	,t.account_id
	,u.email
	,CASE WHEN t.counter_party IN ('0', '37807', '37955', '38121', '38260', '38262', '38263', '40683', '40706','161347') THEN FALSE ELSE TRUE END "is_organic_trade"
	,COUNT(DISTINCT t.order_id) "count_orders"
--	,COUNT(DISTINCT CASE WHEN t.counter_party IN ('0', '37807', '37955', '38121', '38260', '38262', '38263', '40683', '40706') THEN NULL ELSE t.order_id END) "count_organic_orders"
	,COUNT(DISTINCT t.trade_id) "count_trades"
	,COUNT(DISTINCT t.execution_id) "count_executions"
	,COALESCE(ROUND(SUM(t.amount_usd), 2), 0) "sum_usd_trade_volume"
FROM oms_data.analytics.trades_master t
LEFT join oms_data.analytics.users_master u
	ON t.account_id = u.account_id
where t.account_id NOT IN (0 , 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 161347, 27308, 48870, 48948)
	and t.signup_hostcountry in ('TH','IND','AU','global')
	and t.created_at >= '2021-01-01'
GROUP BY 1, 2, 3, 4
), active_user as 
(
select month_
	, account_id
from trade_summary
where sum_usd_trade_volume >= 20 --<<<<<<========= adjust active user threshold 
), deposit_summary as 
(
select signup_hostcountry 
	, date_trunc('day', updated_at) deposit_date 
	, account_id 
	, COUNT(distinct case when status = 'FullyProcessed' then ticket_id end) as deposit_c 
--	, COUNT(distinct case when status = 'FullyProcessed' then account_id end) as user_c
from analytics.deposit_tickets_master dtm  --withdraw_tickets_master wtm  
where signup_hostcountry in ('TH','AU','ID','global')
and account_id not in (0,2,3,37955,38260,38262,38263,40683,63312,63313,161347,40706,37807,38121,27308,63611)
and status = 'FullyProcessed'
and updated_at >= '2021-01-01'
group by 1,2,3
)
select a.month_
	, u.signup_hostcountry 
	, a.account_id 
	, coalesce (SUM(d.deposit_c), 0) deposit_c 
from active_user a 
left join deposit_summary d on a.month_ = date_trunc('month',d.deposit_date) and a.account_id = d.account_id 
left join analytics.users_master u on a.account_id = u.account_id 
group by 1,2,3 


--- deposit + withdrawl 
WITH deposit_ AS ( 
SELECT 
	date_trunc('day', d.updated_at) AS month_  
	, d.ap_account_id 
	, d.signup_hostcountry 
	, d.product_type 
	, d.product_symbol 
	,CASE WHEN d.ap_account_id in (1373,1432,13266,16211,16308,22576,34535,48900,53463,80871,84319) THEN TRUE ELSE FALSE END AS is_whale
	, COUNT(d.*) AS deposit_number 
	, SUM(d.amount) AS deposit_amount 
	, SUM(d.amount_usd) AS deposit_usd 
--	, SUM( CASE WHEN amount_usd IS NOT NULL THEN amount_usd
--			ELSE (CASE WHEN product_symbol = 'USD' THEN amount	ELSE amount * COALESCE(c.average_high_low, g.mid_price, z.price, 1/e.exchange_rate) END) END) AS deposit_usd
FROM 
	analytics.deposit_tickets_master d 
WHERE 
	d.status = 'FullyProcessed' 
	AND d.signup_hostcountry IN ('TH','AU','ID','global')
	AND d.updated_at::date >= '2021-01-01' AND d.updated_at::date < NOW()::date 
	AND d.ap_account_id NOT IN (0, 27308,48870,48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63312, 63313, 161347) 
GROUP  BY  1,2,3,4,5,6
), withdraw_ AS (
SELECT 
	date_trunc('day', w.updated_at) AS month_  
	, w.ap_account_id 
	, w.signup_hostcountry 
	, w.product_type 
	, w.product_symbol 
	,CASE WHEN w.ap_account_id IN (1373,1432,13266,16211,16308,22576,34535,48900,53463,80871,84319) THEN TRUE ELSE FALSE END AS is_whale
	, COUNT(w.*) AS withdraw_number 
	, SUM(w.amount) AS withdraw_amount 
	, SUM(w.amount_usd) AS withdraw_usd 
--	, SUM( CASE WHEN amount_usd IS NOT NULL THEN amount_usd
--				ELSE (CASE WHEN product_symbol = 'USD' THEN amount ELSE amount * COALESCE(c.average_high_low, g.mid_price, z.price, 1/e.exchange_rate) END)	END) AS withdraw_usd 
FROM  
	analytics.withdraw_tickets_master w 
WHERE 
	w.status = 'FullyProcessed'
	AND w.signup_hostcountry IN ('TH','AU','ID','global')
	AND w.updated_at::date >= '2021-01-01' AND w.updated_at::date < NOW()::date 
	AND w.ap_account_id NOT IN (0, 27308,48870,48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63312, 63313, 161347)
GROUP BY 1,2,3,4,5,6
)
SELECT 
	DATE_TRUNC('month', COALESCE(d.month_, w.month_)) datadate  
	, COALESCE(d.signup_hostcountry, w.signup_hostcountry) signup_hostcountry
--	, COALESCE (d.ap_account_id, w.ap_account_id) ap_account_id 
--	, COALESCE (d.product_type, w.product_type) product_type 
--	, COALESCE (d.product_symbol, w.product_symbol) symbol 
--	, CASE WHEN COALESCE (d.product_symbol, w.product_symbol) IN ('AXS', 'BAT', 'SOL', 'C8P', 'TOK', 'ENJ') THEN FALSE ELSE TRUE END AS is_monitored 
--	, COALESCE(d.is_whale, w.is_whale) is_whale
	, SUM( COALESCE(d.deposit_number, 0)) depost_count 
	, SUM( COALESCE(d.deposit_usd, 0)) deposit_usd
	, SUM( COALESCE(w.withdraw_number, 0)) withdraw_count
	, SUM( COALESCE(w.withdraw_usd, 0)) withdraw_usd
FROM deposit_ d 
	FULL OUTER JOIN withdraw_ w 
		ON d.ap_account_id = w.ap_account_id 
		AND d.signup_hostcountry = w.signup_hostcountry 
		AND d.product_type = w.product_type 
		AND d.month_ = w.month_ 
		AND d.product_symbol = w.product_symbol 
WHERE 
	COALESCE(d.month_, w.month_) >= '2021-01-01 00:00:00'
	AND COALESCE(d.month_, w.month_) < '2021-02-01 00:00:00' -- DATE_TRUNC('month', NOW())
--	AND COALESCE (d.product_symbol, w.product_symbol) IN ('AXS', 'BAT', 'SOL', 'C8P', 'TOK', 'ENJ')
--	AND COALESCE (d.ap_account_id, w.ap_account_id) = '' --<<<<<< change test account HERE
GROUP BY 1,2
ORDER BY 1,2 


-- deposit full usd value
SELECT
	DATE_TRUNC('month', d.created_at) created_at 
	, product_symbol 
--	, product_type 
--	, base_fiat 
--	, cryptobase_pair 
--	, usdbase_pair 
--	, cryptobase_price 
--	, usdbase_rate 
	, COUNT(ap_account_id) user_count
	, COUNT(DISTINCT ap_account_id) distinct_user_count
	, SUM(amount) amount 
	, SUM(amount_base_fiat) amount_base_fiat 
	, SUM(amount_usd) amount_usd 
	, SUM( CASE WHEN amount_usd IS NOT NULL THEN amount_usd
				ELSE 
				(CASE WHEN product_symbol = 'USD' THEN amount
				ELSE amount * COALESCE(c.average_high_low, g.mid_price, z.price, 1/e.exchange_rate) END)
				END) AS amount_usd_new
FROM analytics.deposit_tickets_master d
	LEFT JOIN oms_data.public.cryptocurrency_prices c 
	    ON ((CONCAT(d.product_symbol, 'USD') = c.instrument_symbol) OR (c.instrument_symbol = 'MIOTAUSD' AND d.product_symbol ='IOTA') OR (c.instrument_symbol = 'USDPUSD' AND d.product_symbol ='PAX'))
	    AND DATE_TRUNC('day', d.created_at) = DATE_TRUNC('day', c.last_updated) + '1 day'::INTERVAL 
	LEFT join oms_data.public.daily_closing_gold_prices g
		ON ((DATE_TRUNC('day', d.created_at) = DATE_TRUNC('day', g.created_at)) 
		OR (DATE_TRUNC('day', d.created_at) = '2021-07-31 00:00:00' AND DATE_TRUNC('day', g.created_at) = '2021-07-30 00:00:00'))
		AND d.product_symbol = 'GOLD'
	LEFT JOIN oms_data.public.daily_ap_prices z
		ON DATE_TRUNC('day', d.created_at) = DATE_TRUNC('day', z.created_at) + '1 day'::INTERVAL
		AND ((z.instrument_symbol = 'ZMTUSD' AND d.product_symbol = 'ZMT')
		OR (z.instrument_symbol = 'C8PUSDT' AND d.product_symbol = 'C8P'))
	LEFT JOIN public.exchange_rates e
		ON date_trunc('day', e.created_at) = date_trunc('day', d.created_at)
		AND e.product_2_symbol  = d.product_symbol
		AND e."source" = 'coinmarketcap'
WHERE status = 'FullyProcessed'
AND d.created_at >= '2021-01-01 00:00:00'
GROUP BY 1,2
ORDER BY 8 DESC 


---- erc20 wallet count - external/ internal
WITH base AS (
SELECT d.updated_at 
	, d.account_id 
	, u.signup_hostcountry 
	, d.product_symbol 
--	, COUNT(DISTINCT CASE WHEN d.product_id NOT IN (1,25,29,13,26,2,28,4) THEN external_id END) AS erc20_wallet_c
	, TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM t.deposit_info),',',3),':',2)) from_add 
	, TRIM(BOTH '"..."' FROM split_part(split_part(TRIM(BOTH '{...}' FROM t.deposit_info),',',4),':',2)) to_add 
	, SUM(d.amount) coin_amt
	, SUM(d.amount_base_fiat) fiat_amt
	, SUM(d.amount_usd) usd_amt 
FROM analytics.deposit_tickets_master d
	LEFT JOIN mysql_replica_apex.deposit_tickets t ON d.ticket_id = t.deposit_ticket_id 
	LEFT JOIN analytics.users_master u ON d.account_id = u.account_id 
WHERE d.status = 'FullyProcessed' 
AND d.product_type = 'CryptoCurrency' 
AND u.signup_hostcountry IN ('AU','global','ID','TH')
AND d.account_id NOT IN (0,27308,48870,48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63611, 63312, 63313, 161347) 
AND d.product_id NOT IN (1,25,29,13,26,2,28,4) --<<<<<<<<<<< -- exclude BTC, LTC, BCH, XRP
GROUP BY 1,2,3,4,5,6
ORDER BY 1
)
SELECT --DATE_TRUNC('month', updated_at) datamonth
	 signup_hostcountry
	, account_id 
	, product_symbol 
	, COUNT(DISTINCT from_add) external_address_count
	, COUNT(DISTINCT to_add) zipmex_address_count
	, SUM(coin_amt) coin_amt
	, SUM(fiat_amt) fiat_amt
	, SUM(usd_amt) usd_amt 
FROM base	
GROUP BY 1,2,3
ORDER BY 5 DESC, 2,3
	




---- count of users deposti thru a bank
select date_trunc('month',d.updated_at) month_
	, d.signup_hostcountry 
	, b.name_en 
	, count(distinct d.account_id) depositor_count
	, sum(d.amount_base_fiat) amount_base_fiat
	, sum(d.amount_usd) amount_usd
from analytics.deposit_tickets_master d   
	left join analytics.users_master u on u.account_id = d.account_id 
	left join oms_data.user_app_public.bank_accounts a on a.user_id = u.zip_user_id 
	left join user_app_public.banks b on a.bank_code = b.code 
where d.product_type = 'NationalCurrency'
and d.status = 'FullyProcessed'
and a.bank_code = '025' -- Krungsi - BANK OF AYUDHYA PUBLIC COMPANY LTD.
group by 1,2,3
order by 1 


----- account balance - erc20
SELECT date_trunc('day',a.created_at) AS datadate 
	, CASE WHEN a.account_id IS NOT NULL THEN 'user' END AS user_mask 
	, COUNT(DISTINCT p.symbol) symbol_count --, a.product_id
	, SUM(amount) coin_amount
	, SUM(a.amount * COALESCE(c.average_high_low, g.mid_price, z.price, 1/e.exchange_rate)) usd_amount
FROM oms_data.public.accounts_positions_daily a
	LEFT JOIN oms_data.mysql_replica_apex.products p
		ON a.product_id = p.product_id
	LEFT JOIN oms_data.public.cryptocurrency_prices c
	    ON CONCAT(p.symbol, 'USD') = c.instrument_symbol
		AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', c.created_at)
	LEFT JOIN oms_data.public.daily_closing_gold_prices g
		ON DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', g.created_at)
		AND a.product_id IN (15, 35)
	LEFT JOIN oms_data.public.daily_ap_prices z
		ON DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', z.created_at)
		AND z.instrument_symbol  = 'ZMTUSD'
		AND a.product_id in (16, 50)
	LEFT JOIN public.exchange_rates e
		ON date_trunc('day', e.created_at) = date_trunc('day', a.created_at)
		AND e.product_2_symbol  = p.symbol
		AND e.source = 'coinmarketcap'
WHERE date_trunc('day', a.created_at) = date_trunc('day', NOW()) - '1 day'::INTERVAL --<<<<<<<<CHANGE DATE HERE
AND a.account_id NOT IN (0, 27308,48870,48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63611, 63312, 63313, 161347)
AND a.product_id NOT IN (1,25,29,13,26,2,28,4) --<<<<<<<<<<<<<<<<<<<<<<<< -- exclude BTC, LTC, BCH, XRP
AND p."type" = 2 -- crypto ONLY 
group by 1,2

SELECT *
FROM oms_data.public.accounts_positions_daily a
WHERE account_id = 143639


---- user conversion 
WITH base AS 
(
SELECT month_, signup_hostcountry
	, COUNT(DISTINCT CASE WHEN sum_deposit_usd > 0 AND sum_deposit_usd <= 100 THEN d.account_id END) deposit_user_1
	, COUNT(DISTINCT CASE WHEN sum_deposit_usd > 100 THEN d.account_id END) deposit_user_2
FROM (SELECT date_trunc('month',d.created_at) month_, d.signup_hostcountry, d.account_id , SUM(d.amount_usd) sum_deposit_usd 
		FROM analytics.deposit_tickets_master d 
		LEFT JOIN analytics.users_master u ON d.account_id = u.account_id 
		WHERE u.is_verified = TRUE AND d.created_at >= u.onfido_completed_at GROUP BY 1,2,3) d 
GROUP BY 1,2
)
SELECT date_trunc('month',created_at) datamonth
	, u.signup_hostcountry 
	, COUNT(DISTINCT user_id) AS register_users 
	, k.kyc_user 
	, b.bankbook_verified 
	, d.deposit_user_1 
	, d.deposit_user_2 
FROM analytics.users_master u 
	LEFT JOIN (SELECT date_trunc('month',b.is_verified_at) month_, u.signup_hostcountry, COUNT(DISTINCT b.user_id) AS bankbook_verified 
				FROM user_app_public.bank_accounts b LEFT JOIN analytics.users_master u ON b.user_id = u.zip_user_id 
				WHERE is_verified_at IS NOT NULL AND b.is_verified_at >= u.onfido_completed_at GROUP BY 1,2) b 
		ON date_trunc('month',u.created_at) = b.month_ AND u.signup_hostcountry = b.signup_hostcountry 
	LEFT JOIN (SELECT date_trunc('month',onfido_completed_at) month_, signup_hostcountry
				, COUNT(DISTINCT CASE WHEN is_verified = TRUE THEN user_id END) AS kyc_user
				FROM analytics.users_master WHERE onfido_completed_at IS NOT NULL GROUP BY 1,2) k 
		ON date_trunc('month',u.created_at) = k.month_ AND u.signup_hostcountry = k.signup_hostcountry 
	LEFT JOIN base d 
		ON date_trunc('month',u.created_at) = d.month_ AND u.signup_hostcountry = d.signup_hostcountry
WHERE u.account_id NOT IN (0, 27308,48870,48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63312, 63313, 161347)
AND u.signup_hostcountry NOT IN ('error','test','xbullion')
GROUP BY 1,2,4,5,6,7
ORDER BY 2,1

