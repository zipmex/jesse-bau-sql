WITH deposit_ AS ( 
	SELECT 
		date_trunc('month', d.updated_at + '7 hour'::INTERVAL) AS created_at_gmt7
		, d.ap_account_id 
		, d.signup_hostcountry 
		, c.segment 
		, COUNT(DISTINCT d.ap_account_id) depositor_count
		, COUNT(d.ticket_id) AS ticket_count 
		, SUM(d.amount) AS deposit_amount 
		, SUM(d.amount_usd) deposit_usd
		, SUM(d.amount_usd * rm.price) deposit_thb
	FROM 
		analytics.deposit_tickets_master d 
		LEFT JOIN analytics.users_master um 
			ON d.ap_account_id = um.ap_account_id 
		RIGHT JOIN 
			mappings.cp_grw_2202_gl_paydaydepositandearn c 
			ON um.user_id = c.user_id 
		LEFT JOIN analytics.rates_master rm 
			ON rm.product_1_symbol = 'THB'
			AND rm.created_at::DATE = d.updated_at::DATE 
	WHERE 
		d.status = 'FullyProcessed' 
		AND d.signup_hostcountry IN ('TH')
		AND (d.updated_at + '7 hour'::INTERVAL)::date >= '2022-02-25' 
		AND (d.updated_at + '7 hour'::INTERVAL)::date < '2022-03-01'
		AND d.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping) 
	GROUP  BY 
		1,2,3,4
)--	, final_result AS (
	SELECT 
		*
--		, SUM(deposit_usd) OVER(PARTITION BY ap_account_id ORDER BY created_at_gmt7) cumulative_deposit_usd
--		, SUM(deposit_thb) OVER(PARTITION BY ap_account_id ORDER BY created_at_gmt7) cumulative_deposit_thb
		, CASE WHEN deposit_thb >= 499.9999999 THEN TRUE ELSE FALSE END AS is_qualified
	FROM deposit_ 
;)
SELECT 
	DATE_TRUNC('month', created_at_gmt7) created_at_gmt7
	, segment 
	, is_qualified
	, COUNT(DISTINCT ap_account_id) total_depositor_count
	, SUM(ticket_count) total_deposit_ticket
	, SUM(deposit_usd) total_deposit_usd
	, SUM(deposit_thb) total_deposit_thb
FROM final_result
GROUP BY 1,2,3
ORDER BY 2,1
;