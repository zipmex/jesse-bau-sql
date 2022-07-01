SELECT t.order_id 
	, t.account_id 
	, t.time_stamp 
	, s.`state`
	, t.order_type
	, t.instrument_id
	, t.side 
	, SUM(quantity) total_amount  
	, SUM(executed_quantity) executed_amount 
FROM apex.oms_trades t 
left join apex.oms_orders_states s on t.order_id = s.order_id 
where t.is_block_trade = 0 
and t.account_id not in (0, 27308,48870,48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63312, 63313, 161347)
and s.state = 5
and t.time_stamp >= 637577568000000000 -- 637554240000000000 
group by 1,2,3,4,5,6,7



select * 
FROM apex.oms_trades t 
where trade_id >= 12798308 -- may 1
and trade_id < (12798308 + 500000) -- 13733972 may 9
and t.is_block_trade = 0 
-- and t.account_id not in (0, 27308,48870,48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63312, 63313, 161347)
-- and t.remaining_quantity = 0 
-- and order_id = 559804770
order by trade_id 


select 16010644 - 500000


select trade_id 
	, order_id 
	, order_type 
FROM apex.oms_trades t 
where trade_id >= 15751961 
and trade_id < 15751961 + 500000
 -- 13298308 -- 13028815 
and t.is_block_trade = 0 
and t.account_id not in (0, 27308,48870,48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63312, 63313, 161347)




SELECT *
FROM apex.withdraw_tickets wt 
WHERE last_updated_ticks > 637635281478906389
AND last_updated_ticks < 637635308586582241 
;


	-- substring MYSQL
-- 	, REPLACE (SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(withdraw_transaction_details,':',2),':',-1),',',1),'"','') txid	
-- 	, REPLACE (SUBSTRING_INDEX(SUBSTRING_INDEX(template_form,':',2),',',-1),'"','') subst
-- 	, REPLACE(REPLACE (SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(template_form,':',3),':',-1),',',1),'"',''),'}','') ext_add

WITH withdraw_base AS (
	SELECT 
		w.created_on_ticks
		, withdraw_ticket_id
		, withdraw_transaction_details
		, REPLACE (SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(withdraw_transaction_details,':',2),':',-1),',',1),'"','') txid	
		, template_form
		, CASE 
				WHEN REPLACE(REPLACE (SUBSTRING_INDEX(template_form,':',1),'"',''),'{','') = 'ExternalAddress'
				THEN REPLACE(REPLACE (SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(template_form,':',2),':',2),',',1),':',-1),'"',''),'{','')
				WHEN REPLACE (SUBSTRING_INDEX(SUBSTRING_INDEX(template_form,':',2),',',-1),'"','') = 'ExternalAddress'
				THEN REPLACE(REPLACE (SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(template_form,':',3),':',-1),',',1),'"',''),'}','')
				ELSE REPLACE(REPLACE(SUBSTRING_INDEX(template_form,':',-1),'"',''),'}','')
				END AS ext_address
		, request_user_id , request_username
		, p.symbol 
	FROM apex.withdraw_tickets w 
		LEFT JOIN apex.products p 
			ON w.asset_id = p.product_id 
	WHERE withdraw_ticket_id IN (511463,511547,511559,511347,511685)
)
SELECT 
	txid "TransactionID"
	, ext_address "OutputAddress"
	, CASE WHEN symbol IS NOT NULL THEN 'withdrawal' END AS "Direction"
	, request_username "CustomerID"
	, NULL "LogIndex"
	, symbol "Asset"
FROM withdraw_base
	
	
;
