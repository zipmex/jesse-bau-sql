 /* 
  * users_master_summary is created for user funnel/ cohort analysis
  * conversion rate by steps: 8 steps
  * 	compare last step to previous step: 
  * 	register -> email verified -> mobile verfied -> frankieone_submitted -> onfido submitted -> onfido verified -> verified -> deposit -> trade
  * conversion rate by base: 
  * 	each step compare to the base (register)
  * ZipUp not part of conversion funnel as users can skip steps
  */


DROP TABLE IF EXISTS warehouse.analytics.users_master_summary;

CREATE TABLE IF NOT EXISTS warehouse.analytics.users_master_platform_summary 
(
	register_at							TIMESTAMPTZ
	,signup_hostcountry 				VARCHAR(255)
	,"period"							VARCHAR(255)
	,signup_platform					VARCHAR(255)
	,user_register						INTEGER
	,email_verified						INTEGER
	,mobile_verified					INTEGER
	,frankieone_submitted				INTEGER
	,onfido_submitted					INTEGER
	,onfido_verified					INTEGER
	,user_verified						INTEGER
	,user_deposited						INTEGER
	,user_traded						INTEGER
	,user_zipup							INTEGER
	,total_user_register								INTEGER
	,total_email_verified								INTEGER
	,total_mobile_verified								INTEGER
	,total_frankieone_submitted							INTEGER
	,total_onfido_submitted								INTEGER
	,total_onfido_verified								INTEGER
	,total_user_verified								INTEGER
	,total_user_deposited								INTEGER
	,total_user_traded									INTEGER
	,total_user_zipup									INTEGER
	,cvr_steps_registered_email_verified				NUMERIC
	,cvr_steps_email_verified_mobile_verified			NUMERIC
	,cvr_steps_mobile_verified_frankieone_submitted		NUMERIC
	,cvr_steps_frankieone_submitted_onfido_submitted	NUMERIC
	,cvr_steps_onfido_submitted_onfido_verified			NUMERIC
	,cvr_steps_onfido_verified_verified					NUMERIC
	,cvr_steps_verified_deposited						NUMERIC
	,cvr_steps_deposited_traded							NUMERIC
	,cvr_base_registered_email_verified					NUMERIC
	,cvr_base_registered_mobile_verified				NUMERIC
	,cvr_base_registered_frankieone_submitted			NUMERIC
	,cvr_base_registered_onfido_submitted				NUMERIC
	,cvr_base_registered_onfido_verified				NUMERIC
	,cvr_base_registered_verified						NUMERIC
	,cvr_base_registered_deposited						NUMERIC
	,cvr_base_registered_traded							NUMERIC
	,cvr_base_verified_zipup							NUMERIC
);

CREATE INDEX IF NOT EXISTS users_master_platform_summary_idx ON warehouse.analytics.users_master_platform_summary
(register_at, signup_hostcountry, signup_platform);

TRUNCATE TABLE warehouse.analytics.users_master_platform_summary;

-- DROP TABLE IF EXISTS tmp_user_master_summary_day;
-- DROP TABLE IF EXISTS tmp_user_master_summary_week;
-- DROP TABLE IF EXISTS tmp_user_master_summary_month;

CREATE TEMP TABLE tmp_user_master_summary_day AS -- TEMP TABLE FOR daily period
(
	WITH temp_ AS 
	(
		SELECT 
			DATE_TRUNC('day', d.created_at) register_at
			, d.signup_hostcountry 
			, d."period"
			, u.signup_platform
			, COALESCE(COUNT(DISTINCT user_id),0) user_register -- COALESCE IS used here so that cumulative sum still populate WHEN the results ARE NULL 
			, COALESCE(COUNT(DISTINCT CASE WHEN is_email_verified IS TRUE THEN user_id END),0) email_verified 
			, COALESCE(COUNT(DISTINCT CASE WHEN is_mobile_verified IS TRUE THEN user_id END),0) mobile_verified 
			, COALESCE(COUNT(DISTINCT CASE WHEN frankieone_smart_ui_submitted_at IS NOT NULL THEN user_id END),0) frankieone_submitted 
			, COALESCE(COUNT(DISTINCT CASE WHEN onfido_submitted_at IS NOT NULL THEN user_id END),0) onfido_submitted 
			, COALESCE(COUNT(DISTINCT CASE WHEN is_onfido_verified IS TRUE THEN user_id END),0) onfido_verified 
			, COALESCE(COUNT(DISTINCT CASE WHEN is_verified IS TRUE THEN user_id END),0) user_verified  
			, COALESCE(COUNT(DISTINCT CASE WHEN has_deposited IS TRUE THEN user_id END),0) user_deposited 
			, COALESCE(COUNT(DISTINCT CASE WHEN has_traded IS TRUE THEN user_id END),0) user_traded 
			, COALESCE(COUNT(DISTINCT CASE WHEN is_zipup_subscribed IS TRUE THEN user_id END),0) user_zipup  
		FROM 
			warehouse.analytics.period_country_master d -- use date-series TO populate ALL dates even the results = 0 
			LEFT JOIN 
				warehouse.analytics.users_master u 
				ON DATE_TRUNC('day', d.created_at) = DATE_TRUNC('day', u.created_at) 
				AND d.signup_hostcountry = u.signup_hostcountry 
		WHERE 
			d."period" = 'day' -- daily TABLE
			AND d.created_at <= DATE_TRUNC('day', NOW())
		GROUP BY 
			1,2,3,4
		ORDER BY 
			2,3,4,1 DESC
	)
		, cum_temp AS 
	( -- cumulative sum FROM previous count 
		SELECT 
			*
			, SUM(user_register) OVER(PARTITION BY signup_hostcountry, signup_platform ORDER BY register_at) total_user_register 
			, SUM(email_verified) OVER(PARTITION BY signup_hostcountry, signup_platform ORDER BY register_at) total_email_verified 
			, SUM(mobile_verified) OVER(PARTITION BY signup_hostcountry, signup_platform ORDER BY register_at) total_mobile_verified 
			, SUM(frankieone_submitted) OVER(PARTITION BY signup_hostcountry, signup_platform ORDER BY register_at) total_frankieone_submitted 
			, SUM(onfido_submitted) OVER(PARTITION BY signup_hostcountry, signup_platform ORDER BY register_at) total_onfido_submitted 
			, SUM(onfido_verified) OVER(PARTITION BY signup_hostcountry, signup_platform ORDER BY register_at) total_onfido_verified 
			, SUM(user_verified) OVER(PARTITION BY signup_hostcountry, signup_platform ORDER BY register_at) total_user_verified
			, SUM(user_deposited) OVER(PARTITION BY signup_hostcountry, signup_platform ORDER BY register_at) total_user_deposited 
			, SUM(user_traded) OVER(PARTITION BY signup_hostcountry, signup_platform ORDER BY register_at) total_user_traded 
			, SUM(user_zipup) OVER(PARTITION BY signup_hostcountry, signup_platform ORDER BY register_at) total_user_zipup
		FROM 
			temp_  
	)  -- use FLOAT FUNCTION so that results come back WITH decimal
	SELECT
		*
		---- calculate STEP conversion rate - 8 steps
		, CASE WHEN user_register = 0 THEN 0 ELSE email_verified / user_register::float END AS cvr_steps_registered_email_verified 
		, CASE WHEN email_verified = 0 THEN 0 ELSE mobile_verified / email_verified::float END AS cvr_steps_email_verified_mobile_verified  
		, CASE WHEN mobile_verified = 0 THEN 0 ELSE frankieone_submitted / mobile_verified::float END AS cvr_steps_mobile_verified_frankieone_submitted  
		, CASE WHEN frankieone_submitted = 0 THEN 0 ELSE onfido_submitted / frankieone_submitted::float END AS cvr_steps_frankieone_submitted_onfido_submitted  
		, CASE WHEN onfido_submitted = 0 THEN 0 ELSE onfido_verified / onfido_submitted::float END AS cvr_steps_onfido_submitted_onfido_verified  
		, CASE WHEN onfido_verified = 0 THEN 0 ELSE user_verified / onfido_verified::float END AS cvr_steps_onfido_verified_verified  
		, CASE WHEN user_verified = 0 THEN 0 ELSE user_deposited / user_verified::float END AS cvr_steps_verified_deposited  
		, CASE WHEN user_deposited = 0 THEN 0 ELSE user_traded / user_deposited::float END AS cvr_steps_deposited_traded 
		---- calculate BASE conversion rate - 8 steps
		, CASE WHEN user_register = 0 THEN 0 ELSE email_verified / user_register::float END AS cvr_base_registered_email_verified 
		, CASE WHEN user_register = 0 THEN 0 ELSE mobile_verified / user_register::float END AS cvr_base_registered_mobile_verified 
		, CASE WHEN user_register = 0 THEN 0 ELSE frankieone_submitted / user_register::float END AS cvr_base_registered_frankieone_submitted 
		, CASE WHEN user_register = 0 THEN 0 ELSE onfido_submitted / user_register::float END AS cvr_base_registered_onfido_submitted 
		, CASE WHEN user_register = 0 THEN 0 ELSE onfido_verified / user_register::float END AS cvr_base_registered_onfido_verified 
		, CASE WHEN user_register = 0 THEN 0 ELSE user_verified / user_register::float END AS cvr_base_registered_verified 
		, CASE WHEN user_register = 0 THEN 0 ELSE user_deposited / user_register::float END AS cvr_base_registered_deposited 
		, CASE WHEN user_register = 0 THEN 0 ELSE user_traded / user_register::float END AS cvr_base_registered_traded 
		---- zipup is not part of conversion funnel
		, CASE WHEN user_verified = 0 THEN 0 ELSE user_zipup / user_verified::float END AS cvr_base_verified_zipup 
	FROM 
		cum_temp 
	ORDER BY 1 DESC 
);

CREATE TEMP TABLE tmp_user_master_summary_week AS -- TEMP TABLE FOR weekly period
(
	WITH temp_ AS 
	(
		SELECT 
			DATE_TRUNC('week', register_at) register_at
			, signup_hostcountry 
			, 'week' "period"
			, signup_platform
			, SUM(user_register) user_register
			, SUM(email_verified) email_verified
			, SUM(mobile_verified) mobile_verified
			, SUM(frankieone_submitted) frankieone_submitted
			, SUM(onfido_submitted) onfido_submitted
			, SUM(onfido_verified) onfido_verified
			, SUM(user_verified) user_verified
			, SUM(user_deposited) user_deposited
			, SUM(user_traded) user_traded
			, SUM(user_zipup) user_zipup
		FROM 
			tmp_user_master_summary_day
		GROUP BY 
			1,2,3,4
		ORDER BY 
			1,2,3
	)
		, cum_temp AS 
	( -- cumulative sum FROM previous count 
		SELECT 
			*
			, SUM(user_register) OVER(PARTITION BY signup_hostcountry, signup_platform ORDER BY register_at) total_user_register 
			, SUM(email_verified) OVER(PARTITION BY signup_hostcountry, signup_platform ORDER BY register_at) total_email_verified 
			, SUM(mobile_verified) OVER(PARTITION BY signup_hostcountry, signup_platform ORDER BY register_at) total_mobile_verified 
			, SUM(frankieone_submitted) OVER(PARTITION BY signup_hostcountry, signup_platform ORDER BY register_at) total_frankieone_submitted 
			, SUM(onfido_submitted) OVER(PARTITION BY signup_hostcountry, signup_platform ORDER BY register_at) total_onfido_submitted 
			, SUM(onfido_verified) OVER(PARTITION BY signup_hostcountry, signup_platform ORDER BY register_at) total_onfido_verified 
			, SUM(user_verified) OVER(PARTITION BY signup_hostcountry, signup_platform ORDER BY register_at) total_user_verified
			, SUM(user_deposited) OVER(PARTITION BY signup_hostcountry, signup_platform ORDER BY register_at) total_user_deposited 
			, SUM(user_traded) OVER(PARTITION BY signup_hostcountry, signup_platform ORDER BY register_at) total_user_traded 
			, SUM(user_zipup) OVER(PARTITION BY signup_hostcountry, signup_platform ORDER BY register_at) total_user_zipup
		FROM 
			temp_  
	)  -- use FLOAT FUNCTION so that results come back WITH decimal
	SELECT
		*
		---- calculate STEP conversion rate - 8 steps
		, CASE WHEN user_register = 0 THEN 0 ELSE email_verified / user_register::float END AS cvr_steps_registered_email_verified 
		, CASE WHEN email_verified = 0 THEN 0 ELSE mobile_verified / email_verified::float END AS cvr_steps_email_verified_mobile_verified  
		, CASE WHEN mobile_verified = 0 THEN 0 ELSE frankieone_submitted / mobile_verified::float END AS cvr_steps_mobile_verified_frankieone_submitted  
		, CASE WHEN frankieone_submitted = 0 THEN 0 ELSE onfido_submitted / frankieone_submitted::float END AS cvr_steps_frankieone_submitted_onfido_submitted  
		, CASE WHEN onfido_submitted = 0 THEN 0 ELSE onfido_verified / onfido_submitted::float END AS cvr_steps_onfido_submitted_onfido_verified  
		, CASE WHEN onfido_verified = 0 THEN 0 ELSE user_verified / onfido_verified::float END AS cvr_steps_onfido_verified_verified  
		, CASE WHEN user_verified = 0 THEN 0 ELSE user_deposited / user_verified::float END AS cvr_steps_verified_deposited  
		, CASE WHEN user_deposited = 0 THEN 0 ELSE user_traded / user_deposited::float END AS cvr_steps_deposited_traded 
		---- calculate BASE conversion rate - 8 steps
		, CASE WHEN user_register = 0 THEN 0 ELSE email_verified / user_register::float END AS cvr_base_registered_email_verified 
		, CASE WHEN user_register = 0 THEN 0 ELSE mobile_verified / user_register::float END AS cvr_base_registered_mobile_verified 
		, CASE WHEN user_register = 0 THEN 0 ELSE frankieone_submitted / user_register::float END AS cvr_base_registered_frankieone_submitted 
		, CASE WHEN user_register = 0 THEN 0 ELSE onfido_submitted / user_register::float END AS cvr_base_registered_onfido_submitted 
		, CASE WHEN user_register = 0 THEN 0 ELSE onfido_verified / user_register::float END AS cvr_base_registered_onfido_verified 
		, CASE WHEN user_register = 0 THEN 0 ELSE user_verified / user_register::float END AS cvr_base_registered_verified 
		, CASE WHEN user_register = 0 THEN 0 ELSE user_deposited / user_register::float END AS cvr_base_registered_deposited 
		, CASE WHEN user_register = 0 THEN 0 ELSE user_traded / user_register::float END AS cvr_base_registered_traded 
		---- zipup is not part of conversion funnel
		, CASE WHEN user_verified = 0 THEN 0 ELSE user_zipup / user_verified::float END AS cvr_base_verified_zipup 
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
			, signup_platform
			, SUM(user_register) user_register
			, SUM(email_verified) email_verified
			, SUM(mobile_verified) mobile_verified
			, SUM(frankieone_submitted) frankieone_submitted
			, SUM(onfido_submitted) onfido_submitted
			, SUM(onfido_verified) onfido_verified
			, SUM(user_verified) user_verified
			, SUM(user_deposited) user_deposited
			, SUM(user_traded) user_traded
			, SUM(user_zipup) user_zipup
		FROM 
			tmp_user_master_summary_day
		GROUP BY 
			1,2,3,4
		ORDER BY 
			1,2,3
	)
		, cum_temp AS 
	( -- cumulative sum FROM previous count 
		SELECT 
			*
			, SUM(user_register) OVER(PARTITION BY signup_hostcountry, signup_platform ORDER BY register_at) total_user_register 
			, SUM(email_verified) OVER(PARTITION BY signup_hostcountry, signup_platform ORDER BY register_at) total_email_verified 
			, SUM(mobile_verified) OVER(PARTITION BY signup_hostcountry, signup_platform ORDER BY register_at) total_mobile_verified 
			, SUM(frankieone_submitted) OVER(PARTITION BY signup_hostcountry, signup_platform ORDER BY register_at) total_frankieone_submitted 
			, SUM(onfido_submitted) OVER(PARTITION BY signup_hostcountry, signup_platform ORDER BY register_at) total_onfido_submitted 
			, SUM(onfido_verified) OVER(PARTITION BY signup_hostcountry, signup_platform ORDER BY register_at) total_onfido_verified 
			, SUM(user_verified) OVER(PARTITION BY signup_hostcountry, signup_platform ORDER BY register_at) total_user_verified
			, SUM(user_deposited) OVER(PARTITION BY signup_hostcountry, signup_platform ORDER BY register_at) total_user_deposited 
			, SUM(user_traded) OVER(PARTITION BY signup_hostcountry, signup_platform ORDER BY register_at) total_user_traded 
			, SUM(user_zipup) OVER(PARTITION BY signup_hostcountry, signup_platform ORDER BY register_at) total_user_zipup
		FROM 
			temp_  
	)  -- use FLOAT FUNCTION so that results come back WITH decimal
	SELECT
		*
		---- calculate STEP conversion rate - 8 steps
		, CASE WHEN user_register = 0 THEN 0 ELSE email_verified / user_register::float END AS cvr_steps_registered_email_verified 
		, CASE WHEN email_verified = 0 THEN 0 ELSE mobile_verified / email_verified::float END AS cvr_steps_email_verified_mobile_verified  
		, CASE WHEN mobile_verified = 0 THEN 0 ELSE frankieone_submitted / mobile_verified::float END AS cvr_steps_mobile_verified_frankieone_submitted  
		, CASE WHEN frankieone_submitted = 0 THEN 0 ELSE onfido_submitted / frankieone_submitted::float END AS cvr_steps_frankieone_submitted_onfido_submitted  
		, CASE WHEN onfido_submitted = 0 THEN 0 ELSE onfido_verified / onfido_submitted::float END AS cvr_steps_onfido_submitted_onfido_verified  
		, CASE WHEN onfido_verified = 0 THEN 0 ELSE user_verified / onfido_verified::float END AS cvr_steps_onfido_verified_verified  
		, CASE WHEN user_verified = 0 THEN 0 ELSE user_deposited / user_verified::float END AS cvr_steps_verified_deposited  
		, CASE WHEN user_deposited = 0 THEN 0 ELSE user_traded / user_deposited::float END AS cvr_steps_deposited_traded 
		---- calculate BASE conversion rate - 8 steps
		, CASE WHEN user_register = 0 THEN 0 ELSE email_verified / user_register::float END AS cvr_base_registered_email_verified 
		, CASE WHEN user_register = 0 THEN 0 ELSE mobile_verified / user_register::float END AS cvr_base_registered_mobile_verified 
		, CASE WHEN user_register = 0 THEN 0 ELSE frankieone_submitted / user_register::float END AS cvr_base_registered_frankieone_submitted 
		, CASE WHEN user_register = 0 THEN 0 ELSE onfido_submitted / user_register::float END AS cvr_base_registered_onfido_submitted 
		, CASE WHEN user_register = 0 THEN 0 ELSE onfido_verified / user_register::float END AS cvr_base_registered_onfido_verified 
		, CASE WHEN user_register = 0 THEN 0 ELSE user_verified / user_register::float END AS cvr_base_registered_verified 
		, CASE WHEN user_register = 0 THEN 0 ELSE user_deposited / user_register::float END AS cvr_base_registered_deposited 
		, CASE WHEN user_register = 0 THEN 0 ELSE user_traded / user_register::float END AS cvr_base_registered_traded 
		---- zipup is not part of conversion funnel
		, CASE WHEN user_verified = 0 THEN 0 ELSE user_zipup / user_verified::float END AS cvr_base_verified_zipup 
	FROM 
		cum_temp 
);


INSERT INTO warehouse.analytics.users_master_platform_summary 
(	SELECT * FROM tmp_user_master_summary_day);

INSERT INTO warehouse.analytics.users_master_platform_summary 
(	SELECT * FROM tmp_user_master_summary_week);

INSERT INTO warehouse.analytics.users_master_platform_summary 
(	SELECT * FROM tmp_user_master_summary_month);


DROP TABLE IF EXISTS tmp_user_master_summary_day;
DROP TABLE IF EXISTS tmp_user_master_summary_week;
DROP TABLE IF EXISTS tmp_user_master_summary_month;


---- conversion rate for whole ZIPMEX, by platform
CREATE TABLE IF NOT EXISTS warehouse.analytics.users_master_platform_zipmex_summary 
(
	register_at									TIMESTAMPTZ
	,"period"									VARCHAR(255)
	,signup_platform							VARCHAR(255)
	,user_register								INTEGER
	,email_verified								INTEGER
	,mobile_verified							INTEGER
	,frankieone_submitted						INTEGER
	,onfido_submitted							INTEGER
	,onfido_verified							INTEGER
	,user_verified								INTEGER
	,user_deposited								INTEGER
	,user_traded								INTEGER
	,user_zipup									INTEGER
	,total_user_register								INTEGER
	,total_email_verified								INTEGER
	,total_mobile_verified								INTEGER
	,total_frankieone_submitted							INTEGER
	,total_onfido_submitted								INTEGER
	,total_onfido_verified								INTEGER
	,total_user_verified								INTEGER
	,total_user_deposited								INTEGER
	,total_user_traded									INTEGER
	,total_user_zipup									INTEGER
	,cvr_steps_registered_email_verified				NUMERIC
	,cvr_steps_email_verified_mobile_verified			NUMERIC
	,cvr_steps_mobile_verified_frankieone_submitted		NUMERIC
	,cvr_steps_frankieone_submitted_onfido_submitted	NUMERIC
	,cvr_steps_onfido_submitted_onfido_verified			NUMERIC
	,cvr_steps_onfido_verified_verified					NUMERIC
	,cvr_steps_verified_deposited						NUMERIC
	,cvr_steps_deposited_traded							NUMERIC
	,cvr_base_registered_email_verified					NUMERIC
	,cvr_base_registered_mobile_verified				NUMERIC
	,cvr_base_registered_frankieone_submitted			NUMERIC
	,cvr_base_registered_onfido_submitted				NUMERIC
	,cvr_base_registered_onfido_verified				NUMERIC
	,cvr_base_registered_verified						NUMERIC
	,cvr_base_registered_deposited						NUMERIC
	,cvr_base_registered_traded							NUMERIC
	,cvr_base_verified_zipup							NUMERIC
);

CREATE INDEX IF NOT EXISTS users_master_platform_zipmex_summary_idx ON warehouse.analytics.users_master_platform_zipmex_summary
(register_at, signup_platform);

TRUNCATE TABLE warehouse.analytics.users_master_platform_zipmex_summary;


CREATE TEMP TABLE tmp_users_master_platform_zipmex_summary AS 
(
	WITH temp_ AS 
	(
		SELECT 
			register_at
			, "period"
			, signup_platform
			, SUM(user_register) user_register
			, SUM(email_verified) email_verified
			, SUM(mobile_verified) mobile_verified
			, SUM(frankieone_submitted) frankieone_submitted
			, SUM(onfido_submitted) onfido_submitted
			, SUM(onfido_verified) onfido_verified
			, SUM(user_verified) user_verified
			, SUM(user_deposited) user_deposited
			, SUM(user_traded) user_traded
			, SUM(user_zipup) user_zipup
		FROM 
			warehouse.analytics.users_master_platform_summary
		WHERE 
			signup_hostcountry IN ('AU','ID','TH','global')
		GROUP BY 
			1,2,3
		ORDER BY 
			1,2
	)
		, cum_temp AS 
	( -- cumulative sum FROM previous count 
		SELECT 
			*
			, SUM(user_register) OVER(PARTITION BY "period", signup_platform ORDER BY register_at) total_user_register 
			, SUM(email_verified) OVER(PARTITION BY "period", signup_platform ORDER BY register_at) total_email_verified 
			, SUM(mobile_verified) OVER(PARTITION BY "period", signup_platform ORDER BY register_at) total_mobile_verified 
			, SUM(frankieone_submitted) OVER(PARTITION BY "period", signup_platform ORDER BY register_at) total_frankieone_submitted 
			, SUM(onfido_submitted) OVER(PARTITION BY "period", signup_platform ORDER BY register_at) total_onfido_submitted 
			, SUM(onfido_verified) OVER(PARTITION BY "period", signup_platform ORDER BY register_at) total_onfido_verified 
			, SUM(user_verified) OVER(PARTITION BY "period", signup_platform ORDER BY register_at) total_user_verified
			, SUM(user_deposited) OVER(PARTITION BY "period", signup_platform ORDER BY register_at) total_user_deposited 
			, SUM(user_traded) OVER(PARTITION BY "period", signup_platform ORDER BY register_at) total_user_traded 
			, SUM(user_zipup) OVER(PARTITION BY "period", signup_platform ORDER BY register_at) total_user_zipup
		FROM 
			temp_  
	)  -- use FLOAT FUNCTION so that results come back WITH decimal
	SELECT
		*
		---- calculate STEP conversion rate - 8 steps
		, CASE WHEN user_register = 0 THEN 0 ELSE email_verified / user_register::float END AS cvr_steps_registered_email_verified 
		, CASE WHEN email_verified = 0 THEN 0 ELSE mobile_verified / email_verified::float END AS cvr_steps_email_verified_mobile_verified  
		, CASE WHEN mobile_verified = 0 THEN 0 ELSE frankieone_submitted / mobile_verified::float END AS cvr_steps_mobile_verified_frankieone_submitted  
		, CASE WHEN frankieone_submitted = 0 THEN 0 ELSE onfido_submitted / frankieone_submitted::float END AS cvr_steps_frankieone_submitted_onfido_submitted  
		, CASE WHEN onfido_submitted = 0 THEN 0 ELSE onfido_verified / onfido_submitted::float END AS cvr_steps_onfido_submitted_onfido_verified  
		, CASE WHEN onfido_verified = 0 THEN 0 ELSE user_verified / onfido_verified::float END AS cvr_steps_onfido_verified_verified  
		, CASE WHEN user_verified = 0 THEN 0 ELSE user_deposited / user_verified::float END AS cvr_steps_verified_deposited  
		, CASE WHEN user_deposited = 0 THEN 0 ELSE user_traded / user_deposited::float END AS cvr_steps_deposited_traded 
		---- calculate BASE conversion rate - 8 steps
		, CASE WHEN user_register = 0 THEN 0 ELSE email_verified / user_register::float END AS cvr_base_registered_email_verified 
		, CASE WHEN user_register = 0 THEN 0 ELSE mobile_verified / user_register::float END AS cvr_base_registered_mobile_verified 
		, CASE WHEN user_register = 0 THEN 0 ELSE frankieone_submitted / user_register::float END AS cvr_base_registered_frankieone_submitted 
		, CASE WHEN user_register = 0 THEN 0 ELSE onfido_submitted / user_register::float END AS cvr_base_registered_onfido_submitted 
		, CASE WHEN user_register = 0 THEN 0 ELSE onfido_verified / user_register::float END AS cvr_base_registered_onfido_verified 
		, CASE WHEN user_register = 0 THEN 0 ELSE user_verified / user_register::float END AS cvr_base_registered_verified 
		, CASE WHEN user_register = 0 THEN 0 ELSE user_deposited / user_register::float END AS cvr_base_registered_deposited 
		, CASE WHEN user_register = 0 THEN 0 ELSE user_traded / user_register::float END AS cvr_base_registered_traded 
		---- zipup is not part of conversion funnel
		, CASE WHEN user_verified = 0 THEN 0 ELSE user_zipup / user_verified::float END AS cvr_base_verified_zipup 
	FROM 
		cum_temp 
);

INSERT INTO warehouse.analytics.users_master_platform_zipmex_summary
(
	SELECT * FROM tmp_users_master_platform_zipmex_summary
);

DROP TABLE IF EXISTS tmp_users_master_platform_zipmex_summary;

---- conversion rate for whole ZIPMEX, by Country
CREATE TABLE IF NOT EXISTS warehouse.analytics.users_master_country_zipmex_summary 
(
	register_at									TIMESTAMPTZ
	,"period"									VARCHAR(255)
	,signup_hostcountry							VARCHAR(255)
	,user_register								INTEGER
	,email_verified								INTEGER
	,mobile_verified							INTEGER
	,frankieone_submitted						INTEGER
	,onfido_submitted							INTEGER
	,onfido_verified							INTEGER
	,user_verified								INTEGER
	,user_deposited								INTEGER
	,user_traded								INTEGER
	,user_zipup									INTEGER
	,total_user_register								INTEGER
	,total_email_verified								INTEGER
	,total_mobile_verified								INTEGER
	,total_frankieone_submitted							INTEGER
	,total_onfido_submitted								INTEGER
	,total_onfido_verified								INTEGER
	,total_user_verified								INTEGER
	,total_user_deposited								INTEGER
	,total_user_traded									INTEGER
	,total_user_zipup									INTEGER
	,cvr_steps_registered_email_verified				NUMERIC
	,cvr_steps_email_verified_mobile_verified			NUMERIC
	,cvr_steps_mobile_verified_frankieone_submitted		NUMERIC
	,cvr_steps_frankieone_submitted_onfido_submitted	NUMERIC
	,cvr_steps_onfido_submitted_onfido_verified			NUMERIC
	,cvr_steps_onfido_verified_verified					NUMERIC
	,cvr_steps_verified_deposited						NUMERIC
	,cvr_steps_deposited_traded							NUMERIC
	,cvr_base_registered_email_verified					NUMERIC
	,cvr_base_registered_mobile_verified				NUMERIC
	,cvr_base_registered_frankieone_submitted			NUMERIC
	,cvr_base_registered_onfido_submitted				NUMERIC
	,cvr_base_registered_onfido_verified				NUMERIC
	,cvr_base_registered_verified						NUMERIC
	,cvr_base_registered_deposited						NUMERIC
	,cvr_base_registered_traded							NUMERIC
	,cvr_base_verified_zipup							NUMERIC
);

CREATE INDEX IF NOT EXISTS users_master_country_zipmex_summary_idx ON warehouse.analytics.users_master_country_zipmex_summary
(register_at, signup_hostcountry);

TRUNCATE TABLE warehouse.analytics.users_master_country_zipmex_summary;

CREATE TEMP TABLE tmp_users_master_country_zipmex_summary AS 
(
	WITH temp_ AS 
	(
		SELECT 
			register_at
			, "period"
			, signup_hostcountry
			, SUM(user_register) user_register
			, SUM(email_verified) email_verified
			, SUM(mobile_verified) mobile_verified
			, SUM(frankieone_submitted) frankieone_submitted
			, SUM(onfido_submitted) onfido_submitted
			, SUM(onfido_verified) onfido_verified
			, SUM(user_verified) user_verified
			, SUM(user_deposited) user_deposited
			, SUM(user_traded) user_traded
			, SUM(user_zipup) user_zipup
		FROM 
			warehouse.analytics.users_master_platform_summary
		WHERE 
			signup_hostcountry IN ('AU','ID','TH','global')
		GROUP BY 
			1,2,3
		ORDER BY 
			1,2
	)
		, cum_temp AS 
	( -- cumulative sum FROM previous count 
		SELECT 
			*
			, SUM(user_register) OVER(PARTITION BY "period", signup_hostcountry ORDER BY register_at) total_user_register 
			, SUM(email_verified) OVER(PARTITION BY "period", signup_hostcountry ORDER BY register_at) total_email_verified 
			, SUM(mobile_verified) OVER(PARTITION BY "period", signup_hostcountry ORDER BY register_at) total_mobile_verified 
			, SUM(frankieone_submitted) OVER(PARTITION BY "period", signup_hostcountry ORDER BY register_at) total_frankieone_submitted 
			, SUM(onfido_submitted) OVER(PARTITION BY "period", signup_hostcountry ORDER BY register_at) total_onfido_submitted 
			, SUM(onfido_verified) OVER(PARTITION BY "period", signup_hostcountry ORDER BY register_at) total_onfido_verified 
			, SUM(user_verified) OVER(PARTITION BY "period", signup_hostcountry ORDER BY register_at) total_user_verified
			, SUM(user_deposited) OVER(PARTITION BY "period", signup_hostcountry ORDER BY register_at) total_user_deposited 
			, SUM(user_traded) OVER(PARTITION BY "period", signup_hostcountry ORDER BY register_at) total_user_traded 
			, SUM(user_zipup) OVER(PARTITION BY "period", signup_hostcountry ORDER BY register_at) total_user_zipup
		FROM 
			temp_  
	)  -- use FLOAT FUNCTION so that results come back WITH decimal
	SELECT
		*
		---- calculate STEP conversion rate - 8 steps
		, CASE WHEN user_register = 0 THEN 0 ELSE email_verified / user_register::float END AS cvr_steps_registered_email_verified 
		, CASE WHEN email_verified = 0 THEN 0 ELSE mobile_verified / email_verified::float END AS cvr_steps_email_verified_mobile_verified  
		, CASE WHEN mobile_verified = 0 THEN 0 ELSE frankieone_submitted / mobile_verified::float END AS cvr_steps_mobile_verified_frankieone_submitted  
		, CASE WHEN frankieone_submitted = 0 THEN 0 ELSE onfido_submitted / frankieone_submitted::float END AS cvr_steps_frankieone_submitted_onfido_submitted  
		, CASE WHEN onfido_submitted = 0 THEN 0 ELSE onfido_verified / onfido_submitted::float END AS cvr_steps_onfido_submitted_onfido_verified  
		, CASE WHEN onfido_verified = 0 THEN 0 ELSE user_verified / onfido_verified::float END AS cvr_steps_onfido_verified_verified  
		, CASE WHEN user_verified = 0 THEN 0 ELSE user_deposited / user_verified::float END AS cvr_steps_verified_deposited  
		, CASE WHEN user_deposited = 0 THEN 0 ELSE user_traded / user_deposited::float END AS cvr_steps_deposited_traded 
		---- calculate BASE conversion rate - 8 steps
		, CASE WHEN user_register = 0 THEN 0 ELSE email_verified / user_register::float END AS cvr_base_registered_email_verified 
		, CASE WHEN user_register = 0 THEN 0 ELSE mobile_verified / user_register::float END AS cvr_base_registered_mobile_verified 
		, CASE WHEN user_register = 0 THEN 0 ELSE frankieone_submitted / user_register::float END AS cvr_base_registered_frankieone_submitted 
		, CASE WHEN user_register = 0 THEN 0 ELSE onfido_submitted / user_register::float END AS cvr_base_registered_onfido_submitted 
		, CASE WHEN user_register = 0 THEN 0 ELSE onfido_verified / user_register::float END AS cvr_base_registered_onfido_verified 
		, CASE WHEN user_register = 0 THEN 0 ELSE user_verified / user_register::float END AS cvr_base_registered_verified 
		, CASE WHEN user_register = 0 THEN 0 ELSE user_deposited / user_register::float END AS cvr_base_registered_deposited 
		, CASE WHEN user_register = 0 THEN 0 ELSE user_traded / user_register::float END AS cvr_base_registered_traded 
		---- zipup is not part of conversion funnel
		, CASE WHEN user_verified = 0 THEN 0 ELSE user_zipup / user_verified::float END AS cvr_base_verified_zipup 
	FROM 
		cum_temp 
);

INSERT INTO warehouse.analytics.users_master_platform_zipmex_summary
(
	SELECT * FROM tmp_users_master_country_zipmex_summary
);

DROP TABLE IF EXISTS tmp_users_master_country_zipmex_summary;

---- conversion rate for whole ZIPMEX, no segregation
CREATE TABLE IF NOT EXISTS warehouse.analytics.users_master_zipmex_summary 
(
	register_at									TIMESTAMPTZ
	,"period"									VARCHAR(255)
	,user_register								INTEGER
	,email_verified								INTEGER
	,mobile_verified							INTEGER
	,frankieone_submitted						INTEGER
	,onfido_submitted							INTEGER
	,onfido_verified							INTEGER
	,user_verified								INTEGER
	,user_deposited								INTEGER
	,user_traded								INTEGER
	,user_zipup									INTEGER
	,total_user_register								INTEGER
	,total_email_verified								INTEGER
	,total_mobile_verified								INTEGER
	,total_frankieone_submitted							INTEGER
	,total_onfido_submitted								INTEGER
	,total_onfido_verified								INTEGER
	,total_user_verified								INTEGER
	,total_user_deposited								INTEGER
	,total_user_traded									INTEGER
	,total_user_zipup									INTEGER
	,cvr_steps_registered_email_verified				NUMERIC
	,cvr_steps_email_verified_mobile_verified			NUMERIC
	,cvr_steps_mobile_verified_frankieone_submitted		NUMERIC
	,cvr_steps_frankieone_submitted_onfido_submitted	NUMERIC
	,cvr_steps_onfido_submitted_onfido_verified			NUMERIC
	,cvr_steps_onfido_verified_verified					NUMERIC
	,cvr_steps_verified_deposited						NUMERIC
	,cvr_steps_deposited_traded							NUMERIC
	,cvr_base_registered_email_verified					NUMERIC
	,cvr_base_registered_mobile_verified				NUMERIC
	,cvr_base_registered_frankieone_submitted			NUMERIC
	,cvr_base_registered_onfido_submitted				NUMERIC
	,cvr_base_registered_onfido_verified				NUMERIC
	,cvr_base_registered_verified						NUMERIC
	,cvr_base_registered_deposited						NUMERIC
	,cvr_base_registered_traded							NUMERIC
	,cvr_base_verified_zipup							NUMERIC
);

CREATE INDEX IF NOT EXISTS users_master_zipmex_summary_idx ON warehouse.analytics.users_master_zipmex_summary
(register_at);

TRUNCATE TABLE warehouse.analytics.users_master_zipmex_summary;

-- DROP TABLE IF EXISTS tmp_user_master_zipmex;

CREATE TEMP TABLE tmp_users_master_zipmex_summary AS 
(
	WITH temp_ AS 
	(
		SELECT 
			register_at
			, "period"
			, SUM(user_register) user_register
			, SUM(email_verified) email_verified
			, SUM(mobile_verified) mobile_verified
			, SUM(frankieone_submitted) frankieone_submitted
			, SUM(onfido_submitted) onfido_submitted
			, SUM(onfido_verified) onfido_verified
			, SUM(user_verified) user_verified
			, SUM(user_deposited) user_deposited
			, SUM(user_traded) user_traded
			, SUM(user_zipup) user_zipup
		FROM 
			warehouse.analytics.users_master_platform_summary
		WHERE 
			signup_hostcountry IN ('AU','ID','TH','global')
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
			, SUM(email_verified) OVER(PARTITION BY "period" ORDER BY register_at) total_email_verified 
			, SUM(mobile_verified) OVER(PARTITION BY "period" ORDER BY register_at) total_mobile_verified 
			, SUM(frankieone_submitted) OVER(PARTITION BY "period" ORDER BY register_at) total_frankieone_submitted 
			, SUM(onfido_submitted) OVER(PARTITION BY "period" ORDER BY register_at) total_onfido_submitted 
			, SUM(onfido_verified) OVER(PARTITION BY "period" ORDER BY register_at) total_onfido_verified 
			, SUM(user_verified) OVER(PARTITION BY "period" ORDER BY register_at) total_user_verified
			, SUM(user_deposited) OVER(PARTITION BY "period" ORDER BY register_at) total_user_deposited 
			, SUM(user_traded) OVER(PARTITION BY "period" ORDER BY register_at) total_user_traded 
			, SUM(user_zipup) OVER(PARTITION BY "period" ORDER BY register_at) total_user_zipup
		FROM 
			temp_  
	)  -- use FLOAT FUNCTION so that results come back WITH decimal
	SELECT
		*
		---- calculate STEP conversion rate - 8 steps
		, CASE WHEN user_register = 0 THEN 0 ELSE email_verified / user_register::float END AS cvr_steps_registered_email_verified 
		, CASE WHEN email_verified = 0 THEN 0 ELSE mobile_verified / email_verified::float END AS cvr_steps_email_verified_mobile_verified  
		, CASE WHEN mobile_verified = 0 THEN 0 ELSE frankieone_submitted / mobile_verified::float END AS cvr_steps_mobile_verified_frankieone_submitted  
		, CASE WHEN frankieone_submitted = 0 THEN 0 ELSE onfido_submitted / frankieone_submitted::float END AS cvr_steps_frankieone_submitted_onfido_submitted  
		, CASE WHEN onfido_submitted = 0 THEN 0 ELSE onfido_verified / onfido_submitted::float END AS cvr_steps_onfido_submitted_onfido_verified  
		, CASE WHEN onfido_verified = 0 THEN 0 ELSE user_verified / onfido_verified::float END AS cvr_steps_onfido_verified_verified  
		, CASE WHEN user_verified = 0 THEN 0 ELSE user_deposited / user_verified::float END AS cvr_steps_verified_deposited  
		, CASE WHEN user_deposited = 0 THEN 0 ELSE user_traded / user_deposited::float END AS cvr_steps_deposited_traded 
		---- calculate BASE conversion rate - 8 steps
		, CASE WHEN user_register = 0 THEN 0 ELSE email_verified / user_register::float END AS cvr_base_registered_email_verified 
		, CASE WHEN user_register = 0 THEN 0 ELSE mobile_verified / user_register::float END AS cvr_base_registered_mobile_verified 
		, CASE WHEN user_register = 0 THEN 0 ELSE frankieone_submitted / user_register::float END AS cvr_base_registered_frankieone_submitted 
		, CASE WHEN user_register = 0 THEN 0 ELSE onfido_submitted / user_register::float END AS cvr_base_registered_onfido_submitted 
		, CASE WHEN user_register = 0 THEN 0 ELSE onfido_verified / user_register::float END AS cvr_base_registered_onfido_verified 
		, CASE WHEN user_register = 0 THEN 0 ELSE user_verified / user_register::float END AS cvr_base_registered_verified 
		, CASE WHEN user_register = 0 THEN 0 ELSE user_deposited / user_register::float END AS cvr_base_registered_deposited 
		, CASE WHEN user_register = 0 THEN 0 ELSE user_traded / user_register::float END AS cvr_base_registered_traded 
		---- zipup is not part of conversion funnel
		, CASE WHEN user_verified = 0 THEN 0 ELSE user_zipup / user_verified::float END AS cvr_base_verified_zipup 
	FROM 
		cum_temp 
);

INSERT INTO warehouse.analytics.users_master_zipmex_summary
(
	SELECT * FROM tmp_users_master_zipmex_summary
);

DROP TABLE IF EXISTS tmp_users_master_zipmex_summary;
