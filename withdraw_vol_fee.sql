SELECT 
	DATE_TRUNC('month', wtm.created_at)::DATE created_at 
	, wtm.signup_hostcountry 
--	, wtm.ap_account_id 
	, wtm.product_symbol 
	, wtm.product_type 
	, SUM(COALESCE (wtm.amount, 0)) withdraw_vol_unit
	, SUM(COALESCE (wtm.amount_usd, 0)) withdraw_vol_usd
	, SUM(COALESCE (fm.fee_usd_amount, 0)) withdraw_fee_usd
FROM 
	analytics.withdraw_tickets_master wtm 
	LEFT JOIN 
		analytics.fees_master fm 
		ON wtm.ticket_id = fm.fee_reference_id 
WHERE 
	wtm.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping um)
	AND wtm.signup_hostcountry IN ('TH','ID','AU','global')
	AND wtm.status = 'FullyProcessed'
	AND wtm.created_at >= '2021-01-01'
GROUP BY 1,2,3,4