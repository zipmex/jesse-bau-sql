/*
 * Email, Full name, KYC status, register date, KYC's approved date, age, total estimate monthly income and trade
 * invitation code IN ('NATION200', 'NT200', 'CMU200', 'PTTXZIPMEX')
 */


SELECT 
	um.created_at::TIMESTAMP register_date_utc
	, up.email 
	, up.first_name 
	, up.last_name 
	, um.invitation_code 
	, um.is_verified 
	, um.onfido_completed_at::TIMESTAMP kyc_approved_date
	, up.age 
	, ss.survey ->> 'total_estimate_monthly_income' total_estimate_monthly_income
	, um.sum_trade_volume_usd life_time_trade_vol_usd
FROM 
	analytics.users_master um 
	LEFT JOIN 
		user_app_public.suitability_surveys ss 
		ON um.user_id = ss.user_id 
		AND ss.archived_at IS NULL 
	LEFT JOIN 
		analytics_pii.users_pii up 
		ON um.user_id = up.user_id 
WHERE 
	um.invitation_code IN ('NATION200', 'NT200', 'CMU200', 'PTTXZIPMEX')
	AND CASE WHEN um.invitation_code = 'NATION200' THEN um.created_at::DATE BETWEEN '2022-03-29' AND '2022-04-27'
		WHEN um.invitation_code = 'NT200' THEN um.created_at::DATE BETWEEN '2022-05-27' AND '2022-06-27'
		WHEN um.invitation_code = 'CMU200' THEN um.created_at::DATE BETWEEN '2022-05-28' AND '2022-06-27'
		WHEN um.invitation_code = 'PTTXZIPMEX' THEN um.created_at::DATE BETWEEN '2022-06-01' AND '2022-06-30'
		END
ORDER BY 1
;

