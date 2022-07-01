-- trade report incl order state
SELECT 
	tick_to_timestamp(o.last_updated_time_ticks)
	, o.account_id 
	, o.order_id 
	, p.symbol product_1_symbol
	, p2.symbol product_2_symbol
	, CASE WHEN o.state = 1 THEN 'Working'
			WHEN o.state = 2 THEN 'Rejected'
			WHEN o.state = 3 THEN 'Canceled'
			WHEN o.state = 5 THEN 'FullyExecuted'
			END AS state 
	, CASE WHEN ot.order_type = 1 THEN 'Market'
			WHEN ot.order_type = 2 THEN 'Limit'
			END AS order_type
--	, ot.price::float
--	, ot.quantity::float
--	, o.price::float
	, COUNT(DISTINCT o.order_id) orders_count
	, SUM(o.original_quantity::float) original_quantity
	, SUM(o.executed_quantity::float) executed_quantity
	, SUM(ot.amount_usd) amount_usd
FROM 
	apex.oms_orders o
	LEFT JOIN analytics.trades_master ot 
		ON o.order_id = ot.order_id 
	LEFT JOIN apex.instruments i 
		ON o.instrument_id = i.instrument_id 
	LEFT JOIN apex.products p 
		ON i.product_1_id = p.product_id 
	LEFT JOIN apex.products p2 
		ON i.product_2_id = p2.product_id 
WHERE 
--	o.account_id = 314760
	last_updated_time_ticks >= 637740612061320888 -- 2021-09-01
--	AND last_updated_time_ticks < 637694511999990000 -- 2021-10-10
	AND o.instrument_id IN (1,25) --(52, 118, 119, 120, 121, 122, 123)
	AND o.account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
GROUP BY 1,2,3,4,5,6,7
ORDER BY 1 DESC
;


SELECT tick_to_timestamp(637749482061320888)

-- trade report incl order state
WITH min_order AS ( 
-- get latest order_id from yesterday in trade master
SELECT MAX(order_id) min_order_id FROM analytics.trades_master tm2 
WHERE DATE_TRUNC('day', created_at) = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL
)

SELECT 
--	DATE_TRUNC('hour', tick_to_timestamp(o.last_updated_time_ticks)) "created_at"
	tick_to_timestamp(o."time_stamp")
--	, o.account_id 
	, um.signup_hostcountry 
--	, o.order_id 
	, CASE WHEN o.side = 0 THEN 'Buy'
			WHEN o.side = 1 THEN 'Sell'
			ELSE 'error'
			END AS side 
--	, o.trade_id 
	, i.symbol 
--	, CASE WHEN o.state = 1 THEN 'Working'
--			WHEN o.state = 2 THEN 'Rejected'
--			WHEN o.state = 3 THEN 'Canceled'
--			WHEN o.state = 5 THEN 'FullyExecuted'
--			END AS state 
	, CASE WHEN o.order_type = 1 THEN 'Market'
			WHEN o.order_type = 2 THEN 'Limit'
			END AS order_type
	, o.price 
	, COUNT(DISTINCT o.order_id) orders_count
--	, COUNT(DISTINCT ot.trade_id) trades_count
	, SUM(o.original_quantity::float) original_quantity
	, SUM(o.executed_quantity::float) executed_quantity
FROM 
	mysql_replica_apex.oms_orders o
	LEFT JOIN apex.instruments i 
		ON o.instrument_id = i.instrument_id 
	LEFT JOIN analytics.users_master um 
		ON o.account_id = um.ap_account_id 
--	LEFT JOIN min_order mo 
--		ON o.order_id >= mo.min_order_id
WHERE 
	o.account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
	-- select orders within today
	AND o.order_id > 1132748273
	AND o.instrument_id IN (1,25) -- BTC (52, 118, 119, 120, 121, 122, 123) -- ZMT pairs
	AND o.state = 5
--	state: 	1 = Working
--			2 = Rejected
--			3 = Canceled
--			5 = FullyExecuted
GROUP BY 1,2,3,4,5,6
ORDER BY 1
;


SELECT 
	min(trade_id)
FROM analytics.trades_master tm 
WHERE created_at >= '2021-12-03'
;

SELECT trade_id, trade_time , order_id 
FROM apex.oms_orders oo  
WHERE trade_id = 33481799
;



WITH min_order AS ( 
-- get latest order_id from yesterday in trade master
SELECT MAX(order_id) min_order_id FROM analytics.trades_master tm2 
WHERE DATE_TRUNC('day', created_at) = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL
)
SELECT
	t.trade_id
	,tick_to_timestamp(t.trade_time) "created_at"
--	,t.order_id
--	,t.execution_id
--	,oo.state 
--	,u.ap_user_id
	,t.account_id 
	,u.signup_hostcountry
--	,t.counter_party_account_id
--	,t.instrument_id
	,i.symbol
	,CASE WHEN t.side = 0 THEN 'Buy' WHEN t.side = 1 THEN 'Sell' END AS side
	,cast(t.price as numeric(60, 30)) AS price 
	,cast(t.value as numeric(60, 30)) AS value 
	,cast(t.quantity as numeric(60, 30)) AS quantity 
FROM
	warehouse.apex.oms_trades t
	LEFT JOIN
		warehouse.apex.instruments i
		ON t.instrument_id = i.instrument_id
	LEFT JOIN
		warehouse.analytics.users_master u
		ON t.account_id = u.ap_account_id
	LEFT JOIN 
		warehouse.apex.oms_orders oo 
		ON t.order_id = oo.order_id 
	-- select orders within today
	LEFT JOIN 
		min_order mo 
		ON t.order_id >= mo.min_order_id
WHERE
	-- remove test accounts
	t.account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
--	AND t.instrument_id IN (52, 118, 119, 120, 121, 122, 123)
	AND i.product_1_id IN (17,21,72,19,67,18,20,69,70,68,71)
	AND t.counter_party_account_id NOT IN ('186', '187', '869', '870', '1049', '1356', '1357')
	AND t.trade_time > mo.min_order_id
;

SELECT max("time_stamp")
FROM apex.oms_trades ot 