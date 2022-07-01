-- check for duplicates in source data table
SELECT
	t.status
	, date_trunc('month',tick_to_timestamp(t.created_on_ticks)) created_at 
--	,deposit_ticket_id 
--	,t.asset_id "product_id"
--	,p.symbol 
	,COUNT(t.deposit_ticket_id) "count_deposit_tickets"
	,COUNT(DISTINCT t.deposit_ticket_id) "count_distinct_deposit_tickets"
	,COUNT(t.deposit_ticket_id) - COUNT(DISTINCT t.deposit_ticket_id) "delta_distinct"
	,SUM(t.amount::NUMERIC) "product_amount"
FROM
	warehouse.apex.deposit_tickets t
LEFT JOIN	
	warehouse.apex.products p
	ON t.asset_id = p.product_id
WHERE
	t.status = 5
--	AND deposit_ticket_id <= 798374
--	AND tick_to_timestamp(t.created_on_ticks) >= '2021-01-01 00:00:00'
--	AND tick_to_timestamp(t.created_on_ticks) <= '2021-09-16 02:25:20'
GROUP BY
	1, 2
ORDER BY
	1 ASC, 2 ASC

;

-- check duplicate in deposit tickets master
SELECT
	t.status
	, date_trunc('month', created_at) created_at 
--	,t.product_id
--	,t.product_symbol
	,COUNT(t.ticket_id) count_deposit_tickets
	,COUNT(DISTINCT t.ticket_id) count_distinct_deposit_tickets
	,COUNT(t.ticket_id) - COUNT(DISTINCT t.ticket_id) delta_distinct
	,SUM(t.amount) product_amount
FROM
	warehouse.analytics.deposit_tickets_master t
WHERE
	t.status = 'FullyProcessed'
--	AND t.created_at >= '2021-01-01 00:00:00'
GROUP BY
	1, 2
ORDER BY
	1,2 
;


SELECT
	t.status
	, date_trunc('day', t.created_at) created_at 
	, ticket_id 
	, amount_type 
	, product_symbol 
	, cryptobase_pair 
	, t.usdbase_pair 
	,COUNT(t.ticket_id) count_deposit_tickets
	,SUM(t.amount) product_amount
	,SUM(amount_usd) amount_usd 
FROM
	warehouse.analytics.deposit_tickets_master t
	LEFT JOIN analytics.users_master u
	ON t.ap_account_id = u.ap_account_id 
WHERE
	t.status = 'FullyProcessed'
	AND t.created_at < DATE_TRUNC('day', NOW())
	AND amount_usd IS NULL 
	AND t.ap_account_id = 3
GROUP BY
	1, 2, 3, 4, 5, 6, 7
ORDER BY
	1,2 
;


SELECT 
*
FROM analytics.deposit_tickets_master dtm 
WHERE ticket_id = 545097


SELECT 
--	ap_account_id 
--	, cryptobase_pair 
	 product_symbol 
	, ticket_id 
	, base_fiat 
	, COUNT(DISTINCT ticket_id) ticket_count
	, SUM(amount) amount 
	, SUM(amount_usd) amount_usd 
FROM analytics.deposit_tickets_master dtm 
WHERE 
	status = 'FullyProcessed'
	AND amount_usd IS NULL AND base_fiat IS NOT NULL 
	AND created_at < date_trunc('day', NOW())
	AND base_fiat = 'USD'
GROUP BY 1,2,3