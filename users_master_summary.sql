	WITH zipmex_start AS (
	SELECT DISTINCT DATE_TRUNC('day', MIN(created_at)) zipmex_start
	FROM analytics.users_master u 
	)
	SELECT
		DISTINCT "dates"
		, signup_hostcountry 
		, dates start_date 
		, dates + '1 day'::INTERVAL end_date 
		, 'day' "period"
	FROM zipmex_start, GENERATE_SERIES(zipmex_start::timestamp, NOW(), '1 day'::INTERVAL) "dates"
	CROSS JOIN (SELECT DISTINCT signup_hostcountry FROM analytics.users_master WHERE signup_hostcountry IN ('AU','ID','global','TH')) c
	ORDER BY 2,1


			
 /* conversion rate by steps: register -> email verified -> kyc -> deposit -> trade
 * conversion rate by base: each step compare to the base (register)
 * USING FLOAT FUNCTION so that results come back WITH decimal
 */
-- user_master_summary v.2 --cohort analysis, using register_date for all metrics
WITH temp_ AS (
SELECT 
	d.created_at register_at
	, d.signup_hostcountry 
	, d."period"
	, COALESCE(COUNT(DISTINCT user_id),0) user_register -- COALESCE IS used here so that cumulative sum still populate WHEN the results ARE NULL 
	, COALESCE(COUNT(DISTINCT CASE WHEN is_email_verified IS TRUE THEN user_id END),0) user_email_verified 
	, COALESCE(COUNT(DISTINCT CASE WHEN is_mobile_verified IS TRUE THEN user_id END),0) user_mobile_verified 
	, COALESCE(COUNT(DISTINCT CASE WHEN is_verified IS TRUE THEN user_id END),0) user_kyc  
	, COALESCE(COUNT(DISTINCT CASE WHEN has_deposited IS TRUE THEN user_id END),0) user_deposited 
	, COALESCE(COUNT(DISTINCT CASE WHEN has_traded IS TRUE THEN user_id END),0) user_traded 
	, COALESCE(COUNT(DISTINCT CASE WHEN is_zipup_subscribed IS TRUE THEN user_id END),0) user_zipup  
FROM 
	oms_data.analytics.period_country_master d 
	LEFT JOIN analytics.users_master u 
	ON d.created_at = DATE_TRUNC('day', u.created_at) 
	AND d.signup_hostcountry = u.signup_hostcountry 
WHERE d."period" = 'day'
GROUP BY 1,2,3
ORDER BY 1 
), cum_temp AS ( -- cumulative sum FROM previous count 
SELECT 
	*
	, SUM(user_register) OVER(PARTITION BY signup_hostcountry ORDER BY register_at) total_user_register 
	, SUM(user_email_verified) OVER(PARTITION BY signup_hostcountry ORDER BY register_at) total_email_verified 
	, SUM(user_mobile_verified) OVER(PARTITION BY signup_hostcountry ORDER BY register_at) total_mobile_verified 
	, SUM(user_kyc) OVER(PARTITION BY signup_hostcountry ORDER BY register_at) total_user_kyc 
	, SUM(user_deposited) OVER(PARTITION BY signup_hostcountry ORDER BY register_at) total_user_deposited 
	, SUM(user_traded) OVER(PARTITION BY signup_hostcountry ORDER BY register_at) total_user_traded 
	, SUM(user_zipup) OVER(PARTITION BY signup_hostcountry ORDER BY register_at) total_user_zipup
FROM temp_  
), final_d AS (
SELECT
	*
	, CASE WHEN user_register = 0 THEN 0 ELSE user_email_verified / user_register::float END AS cvr_steps_registered_email_verified 
	, CASE WHEN user_email_verified = 0 THEN 0 ELSE user_kyc / user_email_verified::float END AS cvr_steps_email_verified_verified  
	, CASE WHEN user_kyc = 0 THEN 0 ELSE user_deposited / user_kyc::float END AS cvr_steps_verified_deposited  
	, CASE WHEN user_deposited = 0 THEN 0 ELSE user_traded / user_deposited::float END AS cvr_steps_deposited_traded 
	, CASE WHEN user_register = 0 THEN 0 ELSE user_email_verified / user_register::float END AS cvr_base_registered_email_verified 
	, CASE WHEN user_register = 0 THEN 0 ELSE user_kyc / user_register::float END AS cvr_base_registered_verified 
	, CASE WHEN user_register = 0 THEN 0 ELSE user_deposited / user_register::float END AS cvr_base_registered_deposited 
	, CASE WHEN user_register = 0 THEN 0 ELSE user_traded / user_register::float END AS cvr_base_registered_traded 
	, CASE WHEN user_kyc = 0 THEN 0 ELSE user_zipup / user_kyc::float END AS cvr_base_verified_zipup 
FROM cum_temp 
	WHERE register_at <= DATE_TRUNC('day', NOW())
	ORDER BY 1 DESC , 2 
	), temp_w AS 
	(
		SELECT 
			DATE_TRUNC('week', register_at) register_at
			, signup_hostcountry 
			, 'week' "period"
			, SUM(user_register) user_register
			, SUM(user_email_verified) user_email_verified
			, SUM(user_mobile_verified) user_mobile_verified
			, SUM(user_kyc) user_kyc
			, SUM(user_deposited) user_deposited
			, SUM(user_traded) user_traded
			, SUM(user_zipup) user_zipup
		FROM 
			temp_ 
		GROUP BY 
			1,2,3
		ORDER BY 
			1,2,3
	)
	, cum_temp_w AS 
	( -- cumulative sum FROM previous count 
		SELECT 
			*
			, SUM(user_register) OVER(PARTITION BY signup_hostcountry ORDER BY register_at) total_user_register 
			, SUM(user_email_verified) OVER(PARTITION BY signup_hostcountry ORDER BY register_at) total_user_email_verified 
			, SUM(user_mobile_verified) OVER(PARTITION BY signup_hostcountry ORDER BY register_at) total_user_mobile_verified 
			, SUM(user_kyc) OVER(PARTITION BY signup_hostcountry ORDER BY register_at) total_user_kyc 
			, SUM(user_deposited) OVER(PARTITION BY signup_hostcountry ORDER BY register_at) total_user_deposited 
			, SUM(user_traded) OVER(PARTITION BY signup_hostcountry ORDER BY register_at) total_user_traded 
			, SUM(user_zipup) OVER(PARTITION BY signup_hostcountry ORDER BY register_at) total_user_zipup
		FROM 
			temp_w   
	), final_w AS (  -- use FLOAT FUNCTION so that results come back WITH decimal
	SELECT
		*
		, CASE WHEN user_register = 0 THEN 0 ELSE user_email_verified / user_register::float END AS cvr_steps_registered_email_verified 
		, CASE WHEN user_email_verified = 0 THEN 0 ELSE user_kyc / user_email_verified::float END AS cvr_steps_email_verified_verified  
		, CASE WHEN user_kyc = 0 THEN 0 ELSE user_deposited / user_kyc::float END AS cvr_steps_verified_deposited  
		, CASE WHEN user_deposited = 0 THEN 0 ELSE user_traded / user_deposited::float END AS cvr_steps_deposited_traded 
		, CASE WHEN user_register = 0 THEN 0 ELSE user_email_verified / user_register::float END AS cvr_base_registered_email_verified 
		, CASE WHEN user_register = 0 THEN 0 ELSE user_kyc / user_register::float END AS cvr_base_registered_verified 
		, CASE WHEN user_register = 0 THEN 0 ELSE user_deposited / user_register::float END AS cvr_base_registered_deposited 
		, CASE WHEN user_register = 0 THEN 0 ELSE user_traded / user_register::float END AS cvr_base_registered_traded 
		, CASE WHEN user_kyc = 0 THEN 0 ELSE user_zipup / user_kyc::float END AS cvr_base_verified_zipup 
	FROM 
		cum_temp_w
	WHERE register_at <= DATE_TRUNC('day', NOW())
	ORDER BY 1 DESC , 2 
), temp_m AS 
	(
		SELECT 
			DATE_TRUNC('month', register_at) register_at
			, signup_hostcountry 
			, 'month' "period"
			, SUM(user_register) user_register
			, SUM(user_email_verified) user_email_verified
			, SUM(user_mobile_verified) user_mobile_verified
			, SUM(user_kyc) user_kyc
			, SUM(user_deposited) user_deposited
			, SUM(user_traded) user_traded
			, SUM(user_zipup) user_zipup
		FROM 
			temp_ 
		GROUP BY 
			1,2,3
		ORDER BY 
			1,2,3
	)
	, cum_temp_m AS 
	( -- cumulative sum FROM previous count 
		SELECT 
			*
			, SUM(user_register) OVER(PARTITION BY signup_hostcountry ORDER BY register_at) total_user_register 
			, SUM(user_email_verified) OVER(PARTITION BY signup_hostcountry ORDER BY register_at) total_user_email_verified 
			, SUM(user_mobile_verified) OVER(PARTITION BY signup_hostcountry ORDER BY register_at) total_user_mobile_verified 
			, SUM(user_kyc) OVER(PARTITION BY signup_hostcountry ORDER BY register_at) total_user_kyc 
			, SUM(user_deposited) OVER(PARTITION BY signup_hostcountry ORDER BY register_at) total_user_deposited 
			, SUM(user_traded) OVER(PARTITION BY signup_hostcountry ORDER BY register_at) total_user_traded 
			, SUM(user_zipup) OVER(PARTITION BY signup_hostcountry ORDER BY register_at) total_user_zipup
		FROM 
			temp_m  
	) , final_m AS ( -- use FLOAT FUNCTION so that results come back WITH decimal
	SELECT
		*
		, CASE WHEN user_register = 0 THEN 0 ELSE user_email_verified / user_register::float END AS cvr_steps_registered_email_verified 
		, CASE WHEN user_email_verified = 0 THEN 0 ELSE user_kyc / user_email_verified::float END AS cvr_steps_email_verified_verified  
		, CASE WHEN user_kyc = 0 THEN 0 ELSE user_deposited / user_kyc::float END AS cvr_steps_verified_deposited  
		, CASE WHEN user_deposited = 0 THEN 0 ELSE user_traded / user_deposited::float END AS cvr_steps_deposited_traded 
		, CASE WHEN user_register = 0 THEN 0 ELSE user_email_verified / user_register::float END AS cvr_base_registered_email_verified 
		, CASE WHEN user_register = 0 THEN 0 ELSE user_kyc / user_register::float END AS cvr_base_registered_verified 
		, CASE WHEN user_register = 0 THEN 0 ELSE user_deposited / user_register::float END AS cvr_base_registered_deposited 
		, CASE WHEN user_register = 0 THEN 0 ELSE user_traded / user_register::float END AS cvr_base_registered_traded 
		, CASE WHEN user_kyc = 0 THEN 0 ELSE user_zipup / user_kyc::float END AS cvr_base_verified_zipup 
	FROM 
		cum_temp_m 
	WHERE register_at <= DATE_TRUNC('day', NOW())
	ORDER BY 1 DESC , 2 
)
SELECT * FROM final_d
UNION ALL 
SELECT * FROM final_w
UNION ALL 
SELECT * FROM final_m
