WITH user_base AS (
	SELECT 
		cefr.zip_acc_email 
		, up.ap_account_id 
		, um.signup_hostcountry 
		, CASE WHEN up.ap_account_id IS NULL THEN 'invalid' ELSE 'valid' END AS valid_email
	FROM 
		mappings.commercial_emp_fee_rebate cefr 
		LEFT JOIN analytics_pii.users_pii up 
			ON lower(cefr.zip_acc_email) = up.email 
		LEFT JOIN analytics.users_master um 
			ON up.ap_account_id = um.ap_account_id
)--	, fee_date AS (
SELECT 
--	NOW()::DATE reporting_date_utc
--	, DATE_TRUNC('month', fm.created_at)::DATE fee_incurred_month_utc
--	, fm.created_at::DATE fee_incurred_date_utc
--	, up.ap_account_id 
	 lower(up.zip_acc_email)
	, up.valid_email 
	, up.signup_hostcountry 
	, fm.fee_type 
--	, fm.fee_product 
	, SUM(fm.fee_usd_amount) sum_trade_fee_usd
FROM 
	user_base up 
	LEFT JOIN
		analytics.fees_master fm 
		ON fm.ap_account_id = up.ap_account_id 
		AND fm.fee_type = 'Trade'
		AND fm.created_at >= '2022-04-01'
GROUP BY 1,2,3,4--,5--,6,7,8,9
;


