----- trade vol by country
WITH pluang_trade_all AS (
	SELECT 
		DATE_TRUNC('day', q.created_at) created_at 
		, 'ID' signup_hostcountry
		, q.user_id
		, UPPER(LEFT(SPLIT_PART(q.instrument_id,'.',1),3)) product_1_symbol  
		, q.quote_id
		, q.order_id
		, q.side
		, CASE WHEN q.side IS NOT NULL THEN TRUE ELSE FALSE END AS is_organic_trade
		, UPPER(SPLIT_PART(q.instrument_id,'.',1)) instrument_symbol 
		, UPPER(RIGHT(SPLIT_PART(q.instrument_id,'.',1),3)) product_2_symbol 
		, q.quoted_quantity 
		, q.quoted_price 
		, SUM(q.quoted_quantity) "quantity"
		, SUM(q.quoted_value) "amount_idr"
		, SUM(q.quoted_value * 1/e.exchange_rate) amount_usd
	FROM 
		zipmex_otc_public.quote_statuses q
		LEFT JOIN 
			oms_data.public.exchange_rates e
			ON DATE_TRUNC('day', e.created_at) = DATE_TRUNC('day', q.created_at)
			AND UPPER(RIGHT(SPLIT_PART(q.instrument_id,'.',1),3))  = e.product_2_symbol
			AND e."source" = 'coinmarketcap'
	WHERE
		q.status='completed'
		AND q.user_id IN ('01F14GTKR63YS7QSPGCQDNVJRR')
	--	AND DATE_TRUNC('day',q.created_at) >= '2021-09-01 00:00:00'
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
	ORDER BY 1 DESC 
)	, pluang_trade AS (
	SELECT 
		DATE_TRUNC('day', created_at) created_at 
		, signup_hostcountry
		, 0101 ap_account_id 
		, 'pluang' user_type
		, product_1_symbol
		, side 
		, is_organic_trade 
		, CASE WHEN product_1_symbol = 'ZMT' THEN TRUE ELSE FALSE END AS is_zmt_trade
		, COUNT(DISTINCT order_id) count_orders
		, COUNT(DISTINCT quote_id) count_trades 
		, SUM(quantity) quantity 
		, SUM(amount_usd) amount_usd
	FROM 
		pluang_trade_all
	GROUP BY 1,2,3,4,5,6,7,8
)	, zipmex_trade AS (
	SELECT
		DATE_TRUNC('day', t.created_at) created_at 
		, t.signup_hostcountry 
		, t.ap_account_id 
		, 'zipmex' user_type
		, t.product_1_symbol
		, t.side 
		, CASE WHEN t.counter_party IN ('0','186','187','869','870','1356','1357','4344','18866','25041','25224','25225','25226','25227','27443'
			,'38263','38262','38260','38121','37955','40706','40683','37807','48948','48870','44679','44057','161347','316078','44056','63152')
			THEN FALSE ELSE TRUE END "is_organic_trade" --('0','37807','37955','38121','38260','38262','38263','40683','40706','161347')
		, CASE WHEN product_1_id IN (16,50) THEN TRUE ELSE FALSE END AS is_zmt_trade
		, COUNT(DISTINCT t.order_id) "count_orders"
		, COUNT(DISTINCT t.trade_id) "count_trades"
--		, COUNT(DISTINCT t.execution_id) "count_executions"
		, SUM(t.quantity) "sum_coin_volume"
		, SUM(t.amount_usd) "sum_usd_trade_volume" 
	FROM 
		analytics.trades_master t
		LEFT JOIN analytics.users_master u
			ON t.ap_account_id = u.ap_account_id
	WHERE 
		t.ap_account_id NOT IN ('0','186','187','869','870','1356','1357','4344','18866','25041','25224','25225','25226','25227','27443'
		,'38263','38262','38260','38121','37955','40706','40683','37807','48948','48870','44679','44057','161347','316078','44056','63152')
		AND t.signup_hostcountry NOT IN ('error','test','xbullion')
	GROUP BY 
		1,2,3,4,5,6,7,8
	ORDER BY 1,2,3
)	, all_trade AS (
	SELECT * FROM zipmex_trade
	UNION ALL
	SELECT * FROM pluang_trade
)	, temp_t AS (
SELECT 
	DATE_TRUNC('month', a.created_at) created_at 
	, a.signup_hostcountry 
	, a.ap_account_id 
	, user_type
	, is_organic_trade
	, product_1_symbol
--	, CASE WHEN product_1_symbol = 'ZMT' THEN 'zmt' ELSE 'non-zmt' END AS is_zmt
--	, is_zmt_trade 
	, COUNT(DISTINCT ap_account_id) count_traders
	, SUM( COALESCE(count_orders, 0) ) count_orders
	, SUM( COALESCE(count_trades, 0) ) count_trades
	, SUM( COALESCE(sum_coin_volume, 0)) sum_coin_volume 
	, SUM( COALESCE(sum_usd_trade_volume, 0)) sum_usd_trade_volume
FROM 
	all_trade a 
WHERE 
	DATE_TRUNC('day', a.created_at) >= '2021-01-01 00:00:00'
GROUP BY 
	1,2,3,4,5,6
---- rank trade volume to get top 50 traders, count account id to get total trader and calculate trader attribution
)	, rank_trade AS (
SELECT 
	created_at 
	, ap_account_id
	, count_orders
	, count_trades
	, sum_coin_volume
	, sum_usd_trade_volume
	, RANK() OVER(PARTITION BY created_at ORDER BY sum_usd_trade_volume DESC) rank_ 
	, 1.0 / COUNT(ap_account_id) OVER(PARTITION BY created_at) trader_attribution
FROM temp_t
---- calculate cumulative attribution of traders
)	, trade_percentage AS (
SELECT 
	created_at 
	, ap_account_id
	, count_orders
	, count_trades
	, sum_coin_volume
	, sum_usd_trade_volume
	, rank_
	, trader_attribution 
	, SUM(trader_attribution) OVER(PARTITION BY created_at ORDER BY sum_usd_trade_volume DESC) cumulative_attribution
FROM rank_trade
---- SUM trade volume USD to get the result
)
SELECT
	created_at 
--	, signup_hostcountry
	, SUM( CASE WHEN rank_ <= 50 THEN sum_usd_trade_volume END) AS top50_usd_trade_volume
	, SUM( CASE WHEN cumulative_attribution <= 0.01 THEN sum_usd_trade_volume END) AS top1p_usd_trade_volume
	, SUM( CASE WHEN cumulative_attribution > 0.01 AND cumulative_attribution <= 0.05 THEN sum_usd_trade_volume END) AS top2to5p_usd_trade_volume
	, SUM( CASE WHEN cumulative_attribution > 0.05 AND cumulative_attribution <= 0.1 THEN sum_usd_trade_volume END) AS top5to10p_usd_trade_volume
	, SUM( CASE WHEN cumulative_attribution > 0.1 AND cumulative_attribution <= 0.2 THEN sum_usd_trade_volume END) AS top10to20p_usd_trade_volume
	, SUM( CASE WHEN cumulative_attribution > 0.2 AND cumulative_attribution <= 0.5 THEN sum_usd_trade_volume END) AS top20to50p_usd_trade_volume
	, SUM( CASE WHEN cumulative_attribution > 0.5 AND cumulative_attribution <= 0.8 THEN sum_usd_trade_volume END) AS top50to80p_usd_trade_volume
	, SUM( CASE WHEN cumulative_attribution > 0.8 THEN sum_usd_trade_volume END) AS top80to100p_usd_trade_volume
FROM trade_percentage
GROUP BY 1
;

SELECT *
FROM analytics.trades_master tm 
WHERE amount_usd IS NULL 
AND created_at <= date_trunc('day', NOW())
ORDER BY created_at DESC 

	/* list of accounts to exclude
	account_id
	0		remarketer account
	37807	raphael.ghislain999@gmail.com
	37955	pipshunter330@gmail.com
	38121	tangmo82@gmail.com
	38260	makemarket.id@gmail.com
	38262	whenyousorich@gmail.com
	38263	andreas.rellstab135@gmail.com
	40683	jack.napier7888@gmail.com
	40706	arthur.crypto789@gmail.com
	63312	zipmexasia+zmt@zipmex.com
	63313	accounts+zmt@zipmex.com
	161347	zmt.trader@zipmex.com
	27308	accounts+zipmktth@zipmex.com
	48870	accounts+zipup@zipmex.com
	48948	zipmexasia@zipmex.com
	*/
	/* list of accounts already excluded when creating trades_master
	user_id		account_id		user_name
	184			186				james+seedone@zipmex.com
	185			187				james+seedtwo@zipmex.com
	867			869				james+seedthree@zipmex.com
	868			870				james+seedfour@zipmex.com
	1354		1356			seedfive
	1355		1357			seedsix
	*/



---- base query for trade 
SELECT
	DATE_TRUNC('month', t.created_at) created_at 
	, t.signup_hostcountry 
--	, t.ap_account_id 
	, CASE WHEN t.ap_account_id NOT IN ('0','186','187','869','870','1356','1357','4344','18866','25041','25224','25225','25226','25227','27443'
	,'38263','38262','38260','38121','37955','40706','40683','37807','48948','48870','44679','44057','161347','316078','44056','63152')
			THEN FALSE ELSE TRUE END AS is_nominee
--	, 'zipmex' user_type
--	, t.product_1_symbol
	, CASE WHEN product_1_symbol IN ('AXS', 'BAT', 'SOL', 'C8P', 'TOK', 'ENJ') THEN FALSE ELSE TRUE END AS is_monitored
--	, t.side 
	, CASE WHEN t.counter_party IN ('0','186','187','869','870','1356','1357','4344','18866','25041','25224','25225','25226','25227','27443'
		,'38263','38262','38260','38121','37955','40706','40683','37807','48948','48870','44679','44057','161347','316078','44056','63152')
		THEN FALSE ELSE TRUE END "is_organic_trade" --('0','37807','37955','38121','38260','38262','38263','40683','40706','161347')
	, CASE WHEN product_1_id IN (16,50) THEN TRUE ELSE FALSE END AS is_zmt_trade
	, COUNT(DISTINCT t.order_id) "count_orders"
	, COUNT(DISTINCT t.trade_id) "count_trades"
--		, COUNT(DISTINCT t.execution_id) "count_executions"
	, SUM(t.quantity) "sum_coin_volume"
	, SUM(t.amount_usd) "sum_usd_trade_volume" 
FROM 
	analytics.trades_master t
	LEFT JOIN analytics.users_master u
		ON t.ap_account_id = u.ap_account_id
WHERE 
	DATE_TRUNC('month', t.created_at) >= '2021-01-01 00:00:00' -- DATE_TRUNC('month', NOW()) - '6 month'::INTERVAL
--	AND amount_usd IS NULL 
GROUP BY 
	1,2,3,4,5,6
ORDER BY 1,2,3 



---- TOK end of day usd rate
WITH daily_closing_price AS (
SELECT 
	created_at 
	, signup_hostcountry 
	, product_1_symbol 
	, product_2_symbol 
	, price 
	, side 
	, quantity 
	, amount_usd 
	, ROW_NUMBER() OVER(PARTITION BY signup_hostcountry, product_1_id, product_2_id, DATE_TRUNC('day', created_at) ORDER BY created_at DESC) row_ 
FROM analytics.trades_master tm 
WHERE 
	product_1_symbol = 'TOK'
	AND DATE_TRUNC('day', created_at) = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL -- >= '2021-09-07 00:00:00'
)
SELECT 
	*
	, amount_usd / quantity close_price
FROM daily_closing_price
WHERE 
	row_ = 1
ORDER BY 1




---- PLUANG trade value
SELECT 
	DATE_TRUNC('month', q.created_at) created_at 
	, 'ID' signup_hostcountry
	, q.order_id
	, q.quote_id
	, q.user_id
	, q.side
	, UPPER(SPLIT_PART(q.instrument_id,'.',1)) instrument_symbol 
	, UPPER(LEFT(SPLIT_PART(q.instrument_id,'.',1),3)) product_1_symbol  
	, UPPER(RIGHT(SPLIT_PART(q.instrument_id,'.',1),3)) product_2_symbol 
	, q.quoted_quantity 
	, q.quoted_price 
	, SUM(q.quoted_quantity) "quantity"
	, SUM(q.quoted_value) "amount_idr"
	, SUM(q.quoted_value * 1/e.exchange_rate) amount_usd
FROM 
	zipmex_otc_public.quote_statuses q
	LEFT JOIN 
		oms_data_public.exchange_rates e
		ON date_trunc('day', e.created_at) = date_trunc('day', q.created_at)
		AND UPPER(RIGHT(SPLIT_PART(q.instrument_id,'.',1),3))  = e.product_2_symbol
		AND e."source" = 'coinmarketcap'
WHERE
	q.status='completed'
	AND q.user_id IN ('01F14GTKR63YS7QSPGCQDNVJRR')
	AND date_trunc('day',q.created_at) >= '2021-06-01 00:00:00'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
ORDER BY 1 DESC 
;


----PLUANG AUM - UTC 12 
WITH hourly_accumulated_balances AS (
	SELECT *
	FROM (
		SELECT * , date_trunc('day', created_at) AS thour
		, ROW_NUMBER() OVER(PARTITION BY user_id, product_id , date_trunc('day', created_at) ORDER BY created_at DESC) AS r
		FROM zipmex_otc_public.accumulated_balances
		) t
	WHERE t.r = 1
)	
SELECT
	thour, user_id, UPPER(h.product_id) symbol , h.balance, h.created_at, h.id
	, h.balance * c.average_high_low usd_amount
	, ROW_NUMBER() OVER(PARTITION BY user_id, UPPER(h.product_id), DATE_TRUNC('month', thour) ORDER BY thour DESC) rank_ 
FROM 
	hourly_accumulated_balances h 
	LEFT JOIN 
		oms_data.public.cryptocurrency_prices c 
	    ON CONCAT(UPPER(h.product_id), 'USD') = c.instrument_symbol
	    AND DATE_TRUNC('day', thour) = DATE_TRUNC('day', c.last_updated) + '1 day'::INTERVAL 
WHERE
	user_id = '01F14GTKR63YS7QSPGCQDNVJRR'
--	AND extract(hour from thour) = 12
ORDER BY thour DESC, user_id, product_id;





-- pcs --- membership level 1st of month - double wallet
	, period_master AS (  
SELECT 
	p.created_at 
	, u.user_id 
	, u.ap_account_id 
	, u.signup_hostcountry 
	, p2.symbol
FROM 
	analytics.period_master p
	CROSS JOIN ( ---- getting USER info FROM users_master 
				SELECT DISTINCT user_id , ap_account_id, signup_hostcountry FROM analytics.users_master ) u 
	CROSS JOIN (SELECT DISTINCT symbol FROM mysql_replica_apex.products --) p2
				WHERE symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')) p2
WHERE 
		p."period" = 'day' 
	AND p.created_at >= '2021-09-01 00:00:00' AND p.created_at = DATE_TRUNC('month', p.created_at) 
)	, zmt_stake_balance AS (
	SELECT 
		d.created_at 
		, CASE WHEN d.signup_hostcountry IS NULL THEN 'unknown' ELSE d.signup_hostcountry END AS signup_hostcountry
		, d.ap_account_id
		, CASE WHEN d.ap_account_id IN (0, 27308, 48870, 48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63611, 63312, 63313, 161347, 317029, 496001)
			THEN TRUE ELSE FALSE END AS is_nominee
		, d.symbol
		, l.service_id 
		, SUM( COALESCE (credit,0) - COALESCE (debit,0) ) amount  
	FROM period_master d 
		LEFT JOIN 
			asset_manager_public.ledgers l 
			ON d.user_id = l.account_id 
			AND d.created_at >= DATE_TRUNC('day', l.updated_at)
			AND d.symbol = UPPER(SPLIT_PART(l.product_id,'.',1))
		LEFT JOIN
			analytics.users_master u
			ON l.account_id = u.user_id
	WHERE 
		l.account_id IS NOT NULL 
		AND l.service_id = 'zip_lock'
		AND d.symbol = 'ZMT'
	GROUP BY 1,2,3,4,5,6
)	, membership AS (
SELECT 
	*
	, CASE WHEN amount >= 100 AND amount < 20000 THEN 'Zip_Member' 
			WHEN amount >= 20000 THEN 'Zip_Crew'
			ELSE 'Zip_Starter'
			END AS membership_level 
FROM zmt_stake_balance
WHERE 
	is_nominee = FALSE