-- trade wallet real-time balance 
SELECT
	b.account_id 
	, u.signup_hostcountry 
	, b.product_amount::NUMERIC 
	, b.product_hold::NUMERIC 
	, p.symbol 
	, to_timestamp(("time_stamp" - 621355968000000000) / 10000000) "time_stamp" 
FROM
	warehouse.apex.account_product_state_accumulations b
LEFT JOIN
	warehouse.analytics.users_master u
	ON b.account_id = u.ap_account_id
LEFT JOIN 
	apex.products p 
	ON b.product_id = p.product_id 
WHERE 
	p.symbol = 'AFIN'
ORDER BY
	b.product_amount DESC
;

SELECT 	to_timestamp((max("time_stamp") - 621355968000000000) / 10000000) "time_stamp" 
FROM apex.account_product_state_accumulations b


-- Z wallet + Zip Lock real-time balance 
SELECT
	b.service_id 
	, u.ap_account_id 
	, u.signup_hostcountry 
	, b.balance::float
	, u.is_zipup_subscribed 
	, UPPER(SPLIT_PART(product_id,'.',1)) symbol
FROM
	warehouse.asset_manager_public.ledger_balances_v2 b
LEFT JOIN
	warehouse.analytics.users_master u
	ON b.account_id = u.user_id
WHERE
	--b.product_id IN ('gold.th', 'gold.global')
	b.balance <> 0
--	AND u.ap_account_id = 123 -- TEST ACCOUNT HERE
ORDER BY
	b.updated_at DESC
;


