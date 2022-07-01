---- trade value
SELECT 
	DATE_TRUNC('month', q.created_at) created_at 
--	, q.order_id
--	, q.quote_id
	, q.user_id
--	, q.side
	, UPPER(SPLIT_PART(q.instrument_id,'.',1)) instrument_symbol 
	, UPPER(LEFT(SPLIT_PART(q.instrument_id,'.',1),3)) product_1_symbol  
	, UPPER(RIGHT(SPLIT_PART(q.instrument_id,'.',1),3)) product_2_symbol  
	, SUM(q.quoted_quantity) "Transaction Quantity"
	, SUM(q.quoted_value) "Transaction Value"
FROM 
	quote_statuses q
WHERE
	q.status='completed'
	AND q.user_id IN ('01F14GTKR63YS7QSPGCQDNVJRR')
	AND date_trunc('day',q.created_at) >= '2021-06-01 00:00:00'
GROUP BY 1,2,3,4,5
ORDER BY 1 DESC 
;

zipmex_otc_prod.public.accumulated_balances
zipmex_otc_prod.public.balances
zipmex_otc_prod.public.users
zipmex_otc_prod.public.quote_statuses


-----Balance as at time of pulling data
SELECT
	users."name" 
	, balances.*
FROM users
	JOIN balances 
	ON users.id = balances.user_id
WHERE 
	users.id = '01F14GTKR63YS7QSPGCQDNVJRR'
;


----UTC 23 
WITH hourly_accumulated_balances AS (
	SELECT *
	FROM (
		SELECT * , date_trunc('day', created_at) AS thour
		, ROW_NUMBER() OVER(PARTITION BY user_id, product_id , date_trunc('day', created_at) ORDER BY created_at DESC) AS r
		FROM accumulated_balances
		) t
	WHERE t.r = 1
)
SELECT
	thour, user_id, UPPER(product_id) symbol , balance, created_at, id
FROM 
	hourly_accumulated_balances
WHERE
	user_id = '01F14GTKR63YS7QSPGCQDNVJRR'
--	AND extract(hour from thour) = 12
ORDER BY thour DESC, user_id, product_id;