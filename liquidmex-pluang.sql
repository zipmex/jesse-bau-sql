SELECT DATE_TRUNC('day',q.completed_at) date_ 
	, u."name" 
	, SUM(q.quantity) quantity 
	, SUM(q.value) sum_value 
	, SUM(q.quoted_value) sum_quoted_value
FROM quote_statuses q 
LEFT JOIN public.users u 
ON q.user_id = u.id 
WHERE q.status='completed' AND u."name" <> 'Test'
GROUP BY 1,2
ORDER BY 1  
--and q.user_id in ('01F14GTKR63YS7QSPGCQDNVJRR')
--and q.created_at >= '2021-06-03'
;


