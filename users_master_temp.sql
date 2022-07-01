 /* 
  * users_master_summary is created for user funnel/ cohort analysis
  * conversion rate by steps: 
  * 	compare last step to previous step: register -> email verified -> kyc -> deposit -> trade
  * conversion rate by base: 
  * 	each step compare to the base (register)
  * ZipUp not part of conversion funnel as users can skip steps
  */


DROP TABLE IF EXISTS oms_data.analytics.users_master_summary;

CREATE TABLE IF NOT EXISTS oms_data.analytics.users_master_summary 
(
	register_at								TIMESTAMPTZ
	,signup_hostcountry 					VARCHAR(255)
	,"period"								VARCHAR(255)
	,user_register							INTEGER
	,user_email_verified					INTEGER
	,user_mobile_verified					INTEGER
	,user_kyc								INTEGER
	,user_deposited							INTEGER
	,user_traded							INTEGER
	,user_zipup								INTEGER
	,total_user_register					INTEGER
	,total_user_email_verified				INTEGER
	,total_user_mobile_verified				INTEGER
	,total_user_kyc							INTEGER
	,total_user_deposited					INTEGER
	,total_user_traded						INTEGER
	,total_user_zipup						INTEGER
	,cvr_steps_registered_email_verified	NUMERIC
	,cvr_steps_email_verified_verified		NUMERIC
	,cvr_steps_verified_deposited			NUMERIC
	,cvr_steps_deposited_traded				NUMERIC
	,cvr_base_registered_email_verified		NUMERIC
	,cvr_base_registered_verified			NUMERIC
	,cvr_base_registered_deposited			NUMERIC
	,cvr_base_registered_traded				NUMERIC
	,cvr_base_verified_zipup				NUMERIC
);


DROP TABLE IF EXISTS tmp_user_master_summary_day;
DROP TABLE IF EXISTS tmp_user_master_summary_week;
DROP TABLE IF EXISTS tmp_user_master_summary_month;

CREATE TEMP TABLE tmp_user_master_summary_day AS -- TEMP TABLE FOR daily period
(
	WITH temp_ AS 
	(
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
			oms_data.analytics.period_country_master d -- use date-series TO populate ALL dates even the results = 0 
			LEFT JOIN 
				oms_data.analytics.users_master u 
				ON d.created_at = DATE_TRUNC('day', u.created_at) 
				AND d.signup_hostcountry = u.signup_hostcountry 
		WHERE 
			d."period" = 'day' -- daily table
		GROUP BY 
			1,2,3
		ORDER BY 
			1,2,3
	)
	, cum_temp AS 
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
			temp_  
	)  -- use FLOAT FUNCTION so that results come back WITH decimal
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
		cum_temp 
);

CREATE TEMP TABLE tmp_user_master_summary_week AS -- TEMP TABLE FOR weekly period
(
	WITH temp_ AS 
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
			tmp_user_master_summary_day
		GROUP BY 
			1,2,3
		ORDER BY 
			1,2,3
	)
	, cum_temp AS 
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
			temp_  
	)  -- use FLOAT FUNCTION so that results come back WITH decimal
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
		cum_temp 
);

CREATE TEMP TABLE tmp_user_master_summary_month AS -- TEMP TABLE FOR monthly period
(
	WITH temp_ AS 
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
			tmp_user_master_summary_day
		GROUP BY 
			1,2,3
		ORDER BY 
			1,2,3
	)
	, cum_temp AS 
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
			temp_  
	)  -- use FLOAT FUNCTION so that results come back WITH decimal
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
		cum_temp 
);


INSERT INTO oms_data.analytics.users_master_summary 
(	SELECT * FROM tmp_user_master_summary_day);

INSERT INTO oms_data.analytics.users_master_summary 
(	SELECT * FROM tmp_user_master_summary_week);

INSERT INTO oms_data.analytics.users_master_summary 
(	SELECT * FROM tmp_user_master_summary_month);


DROP TABLE IF EXISTS tmp_user_master_summary_day;
DROP TABLE IF EXISTS tmp_user_master_summary_week;
DROP TABLE IF EXISTS tmp_user_master_summary_month;


DROP TABLE IF EXISTS oms_data.analytics.users_master_zipmex_summary;

CREATE TABLE IF NOT EXISTS oms_data.analytics.users_master_zipmex_summary 
(
	register_at							TIMESTAMPTZ
	,"period"								VARCHAR(255)
	,user_register							INTEGER
	,user_email_verified					INTEGER
	,user_mobile_verified					INTEGER
	,user_kyc								INTEGER
	,user_deposited							INTEGER
	,user_traded							INTEGER
	,user_zipup								INTEGER
	,total_user_register					INTEGER
	,total_user_email_verified				INTEGER
	,total_user_mobile_verified				INTEGER
	,total_user_kyc							INTEGER
	,total_user_deposited					INTEGER
	,total_user_traded						INTEGER
	,total_user_zipup						INTEGER
	,cvr_steps_registered_email_verified	NUMERIC
	,cvr_steps_email_verified_verified		NUMERIC
	,cvr_steps_verified_deposited			NUMERIC
	,cvr_steps_deposited_traded				NUMERIC
	,cvr_base_registered_email_verified		NUMERIC
	,cvr_base_registered_verified			NUMERIC
	,cvr_base_registered_deposited			NUMERIC
	,cvr_base_registered_traded				NUMERIC
	,cvr_base_verified_zipup				NUMERIC
);


DROP TABLE IF EXISTS tmp_user_master_zipmex;

CREATE TEMP TABLE tmp_user_master_zipmex AS 
(
	WITH temp_ AS 
	(
		SELECT 
			register_at
			, "period"
			, SUM(user_register) user_register
			, SUM(user_email_verified) user_email_verified
			, SUM(user_mobile_verified) user_mobile_verified
			, SUM(user_kyc) user_kyc
			, SUM(user_deposited) user_deposited
			, SUM(user_traded) user_traded
			, SUM(user_zipup) user_zipup
		FROM 
			oms_data.analytics.users_master_summary
		GROUP BY 
			1,2
		ORDER BY 
			1,2
	)
	, cum_temp AS 
	( -- cumulative sum FROM previous count 
		SELECT 
			*
			, SUM(user_register) OVER(PARTITION BY "period" ORDER BY register_at) total_user_register 
			, SUM(user_email_verified) OVER(PARTITION BY "period" ORDER BY register_at) total_user_email_verified 
			, SUM(user_mobile_verified) OVER(PARTITION BY "period" ORDER BY register_at) total_user_mobile_verified 
			, SUM(user_kyc) OVER(PARTITION BY "period" ORDER BY register_at) total_user_kyc 
			, SUM(user_deposited) OVER(PARTITION BY "period" ORDER BY register_at) total_user_deposited 
			, SUM(user_traded) OVER(PARTITION BY "period" ORDER BY register_at) total_user_traded 
			, SUM(user_zipup) OVER(PARTITION BY "period" ORDER BY register_at) total_user_zipup
		FROM 
			temp_  
	)  -- use FLOAT FUNCTION so that results come back WITH decimal
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
		cum_temp 
);

INSERT INTO oms_data.analytics.users_master_zipmex_summary
(
	SELECT * FROM tmp_user_master_zipmex
);

DROP TABLE IF EXISTS tmp_user_master_zipmex;

