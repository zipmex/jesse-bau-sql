---- Onfido vs FrankieOne
GET 'applicant_id' from user_app_public.applicant_data mapped with 'id' from user_app_public.onfido_applicants TO GET 'user_id'


---- kyc time by last touchpoint
SELECT DISTINCT 
	date_trunc('month', u.inserted_at) month_
	, u.signup_hostname 
	, u.email , u.id , (NOW()::date - m.dob::date)/ 365 age_
--	, a.frankie_entity_id 
	, a.action_recommended frankie_action
	, u.inserted_at users_signup 
--	, u.email_verified_at 
--	, u.mobile_number_verified_at 
	, COALESCE(p.inserted_at, d.inserted_at) kyc_start 
	, o.level_increase_status 
	, o.updated_at kyc_end 
FROM user_app_public.users u 
	LEFT JOIN user_app_public.onfido_applicants o
		ON o.user_id = u.id 
	LEFT JOIN user_app_public.applicant_data a 
		ON a.applicant_id = o.id 
	LEFT JOIN ( SELECT user_id , inserted_at , ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY inserted_at DESC) row_ 
				FROM user_app_public.personal_infos
				) p 
		ON o.user_id = p.user_id AND p.row_ = 1  
	LEFT JOIN ( SELECT applicant_id , inserted_at , ROW_NUMBER() OVER(PARTITION BY applicant_id ORDER BY inserted_at DESC) row_ 
				FROM user_app_public.onfido_documents
				) d 
		ON o.id = d.applicant_id AND d.row_ = 1 
	LEFT JOIN analytics.users_master m 
		ON o.user_id = m.zip_user_id 
WHERE u.signup_hostname IN ('au.zipmex.com','trade.zipmex.com.au','trade.zipmex.co.th','th.zipmex.com','id.zipmex.com','trade.zipmex.co.id')
AND o.level_increase_status = 'pass'
AND o.updated_at >= '2021-04-01 00:00:00'
--AND u.id = '01F8WS8XMEG813X23MCXJ0WG2A'
ORDER BY 1,2


---- KYC'ed user by time frame by last touchpoint 
WITH base AS (
	SELECT DISTINCT 
		date_trunc('month', o.updated_at) month_
		, m.signup_hostcountry 
		, u.id 
	--	, a.action_recommended frankie_action
		, u.inserted_at users_signup 
		, COALESCE(p.inserted_at, d.inserted_at) kyc_start 
		, o.level_increase_status 
		, o.updated_at kyc_end 
	FROM user_app_public.users u 
	-- kyc END time: level_increase_status = 'pass'
		LEFT JOIN user_app_public.onfido_applicants o 
	-- FrankieOne entity ID
			ON o.user_id = u.id 
		LEFT JOIN user_app_public.applicant_data a 
			ON a.applicant_id = o.id 
	-- kyc START time FOR TH, ID 
		LEFT JOIN ( 
					SELECT user_id , inserted_at , ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY inserted_at DESC) row_ 
					FROM user_app_public.personal_infos
					) p 
			ON o.user_id = p.user_id AND p.row_ = 1 
	-- kyc START time FOR AU, SG 
		LEFT JOIN ( 
					SELECT applicant_id , inserted_at , ROW_NUMBER() OVER(PARTITION BY applicant_id ORDER BY inserted_at DESC) row_ 
					FROM user_app_public.onfido_documents) d 
			ON o.id = d.applicant_id AND d.row_ = 1 
		LEFT JOIN analytics.users_master m 
			ON o.user_id = m.user_id 
	WHERE 
		o.level_increase_status = 'pass'
	--	AND o.updated_at >= DATE_TRUNC('month', DATE_TRUNC('day', NOW() - '1 day'::INTERVAL))
		AND m.signup_hostcountry IN ('TH','ID','AU','global')
		AND m.ap_account_id = 564463
	ORDER BY 1,2
), kyc_time AS (
	SELECT 
		*
		, EXTRACT(epoch FROM (kyc_end - kyc_start))/ 3600 kyc_duration_hr
	FROM base
---- average kyc time per country
--)
--	SELECT month_
--		, signup_hostcountry
--	--	, id 
--		, CASE WHEN AVG(CASE WHEN kyc_duration_hr < 0 THEN 0 ELSE kyc_duration_hr END) <= 6 THEN '<= 6hr'
--				WHEN AVG(CASE WHEN kyc_duration_hr < 0 THEN 0 ELSE kyc_duration_hr END) > 6 AND AVG(CASE WHEN kyc_duration_hr < 0 THEN 0 ELSE kyc_duration_hr END) <= 12 THEN '6-12hr'
--				WHEN AVG(CASE WHEN kyc_duration_hr < 0 THEN 0 ELSE kyc_duration_hr END) > 12 AND AVG(CASE WHEN kyc_duration_hr < 0 THEN 0 ELSE kyc_duration_hr END) <= 18 THEN '12-18hr'
--				WHEN AVG(CASE WHEN kyc_duration_hr < 0 THEN 0 ELSE kyc_duration_hr END) > 18 AND AVG(CASE WHEN kyc_duration_hr < 0 THEN 0 ELSE kyc_duration_hr END) <= 24 THEN '18-24hr'
--				WHEN AVG(CASE WHEN kyc_duration_hr < 0 THEN 0 ELSE kyc_duration_hr END) > 24 AND AVG(CASE WHEN kyc_duration_hr < 0 THEN 0 ELSE kyc_duration_hr END) <= 48 THEN '24-48hr'
--				ELSE '> 48hr'
--				END AS kyc_time
--		, AVG(CASE WHEN kyc_duration_hr < 0 THEN 0 ELSE kyc_duration_hr END) AS avg_kyc_duration_hr
--		, COUNT(DISTINCT CASE WHEN level_increase_status = 'pass' THEN id END) kyc_user_c
--	FROM kyc_time 
--	GROUP BY 1,2
--
--- kyc timeframe without average
)
SELECT 
	*
--	month_
--	, signup_hostcountry
----	, id 
--	, CASE WHEN kyc_duration_hr <= 6 THEN '<= 6hr'
--			WHEN kyc_duration_hr > 6 AND kyc_duration_hr <= 12 THEN '6-12hr'
--			WHEN kyc_duration_hr > 12 AND kyc_duration_hr <= 18 THEN '12-18hr'
--			WHEN kyc_duration_hr > 18 AND kyc_duration_hr <= 24 THEN '18-24hr'
--			WHEN kyc_duration_hr > 24 AND kyc_duration_hr <= 48 THEN '24-48hr'
--			ELSE '> 48hr'
--			END AS kyc_time
----	, AVG(CASE WHEN kyc_duration_hr < 0 THEN 0 ELSE kyc_duration_hr END) AS avg_kyc_duration_hr
--	, COUNT(DISTINCT CASE WHEN level_increase_status = 'pass' THEN id END) kyc_user_c
FROM kyc_time 



SELECT split_part(split_part('applicant_id:55d55343-4b26-4b95-9514-2f1cb2334d03 from:pass to:redo',' ',1),':',2)
	, (DATE_TRUNC('day',NOW()) - '2021-07-01 00:00:00') 
	

---- list of user gone thru kyc for QA purpose 
WITH base AS (
SELECT 
	a.actor 
	, DATE_TRUNC('day', a.updated_at + '7 hour'::INTERVAL) updated_at 
	, split_part(a."object",':',2) user_id 
	, split_part(a.description,' ',3) AS level_increase_status 
	, b.verification_level 
	, ROW_NUMBER() OVER(PARTITION BY a."object" ORDER BY a.updated_at DESC) row_  
FROM exchange_admin_public.audit_logs a 
	LEFT JOIN (
		SELECT "object" 
			, description verification_level 
			, updated_at 
			, ROW_NUMBER() OVER(PARTITION BY "object" ORDER BY updated_at DESC) rank_ 
		FROM exchange_admin_public.audit_logs WHERE "action" = 'edit_verification_level') b 
			ON a."object" = b."object"
			AND rank_ = 1 
WHERE "action" = 'edit_level_increase_status' 
)--, temp_ AS ( 
SELECT b.updated_at verified_date 
	, b.user_id 
	, u.signup_hostname 
	, b.level_increase_status 
	, b.verification_level 
	, b.actor kyc_agent 
FROM base b 
	LEFT JOIN user_app_public.users u 
	ON b.user_id = u.id 
WHERE b.updated_at >= DATE_TRUNC('day', NOW() + '7 hour'::INTERVAL ) - '1 day'::INTERVAL 
AND b.updated_at < DATE_TRUNC('day', NOW() + '7 hour'::INTERVAL ) 
AND b.row_ = 1 
AND b.level_increase_status LIKE '%pass%'
ORDER BY 1 DESC 
)
SELECT verified_date 
--	, signup_hostname 
	, kyc_agent 
	, level_increase_status 
	, verification_level 
	, COUNT(*) 
FROM temp_ 
WHERE kyc_agent = 'dennis@zipmex.com'
GROUP BY 1,2,3,4

SELECT date_trunc('day', updated_at + '7 hour'::INTERVAL)
	, actor 
--	, split_part(description,' ',2) previous_status 
	, split_part(description,' ',3) new_status
	, count(*)
FROM exchange_admin_public.audit_logs a 
WHERE "action" = 'edit_level_increase_status' 
AND actor = 'dennis@zipmex.com'
GROUP BY 1,2,3
ORDER BY 1 DESC
;


---- KYC time frame from start to end - whole journey 
WITH audit_log AS (
	SELECT 
		SPLIT_PART(al."object",':',2) user_id 
		, COUNT(*) submission_count 
	FROM 
		exchange_admin_public.audit_logs al 
	WHERE 
		al."action" = 'edit_level_increase_status'
	GROUP BY 1
)	, base AS (
	SELECT DISTINCT 
		date_trunc('month', u.inserted_at)::DATE register_month_utc
		, m.signup_hostcountry 
		, u.id 
--		, d.document_type
--		, d.country 
	--	, a.action_recommended frankie_action
		, u.inserted_at::DATE register_date_utc 
		, o.level_increase_status 
		, COALESCE(p.inserted_at, d.inserted_at) kyc_start_utc 
		, o.updated_at kyc_end_utc
		, al.submission_count	 
	FROM user_app_public.users u 
	-- kyc END time: level_increase_status = 'pass'
		LEFT JOIN user_app_public.onfido_applicants o 
			ON o.user_id = u.id 
		LEFT JOIN audit_log al 
			ON u.id = al.user_id 
	-- FrankieOne entity ID
		LEFT JOIN user_app_public.applicant_data a 
			ON a.applicant_id = o.id 
	-- kyc START time FOR TH, ID 
		LEFT JOIN ( 
					SELECT user_id , inserted_at , ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY inserted_at) row_ 
					FROM user_app_public.personal_infos
					) p 
			ON o.user_id = p.user_id AND p.row_ = 1 
	-- kyc START time FOR AU, SG 
		LEFT JOIN ( 
					SELECT applicant_id , document_type , country , inserted_at , ROW_NUMBER() OVER(PARTITION BY applicant_id ORDER BY inserted_at) row_ 
					FROM user_app_public.onfido_documents) d 
			ON o.id = d.applicant_id AND d.row_ = 1 
		LEFT JOIN analytics.users_master m 
			ON o.user_id = m.user_id 
	WHERE 
		o.level_increase_status IS NOT NULL
		AND m.signup_hostcountry IN ('AU','global')
--		AND m.ap_account_id = 143639
	ORDER BY 1,2
), kyc_time AS (
	SELECT 
		*
		, CASE WHEN kyc_start_utc IS NULL THEN NULL ELSE EXTRACT(epoch FROM (kyc_end_utc - kyc_start_utc))/ 3600 END kyc_duration_hr
		, CASE WHEN kyc_start_utc IS NULL THEN NULL ELSE EXTRACT(epoch FROM (kyc_end_utc - kyc_start_utc))/ 3600/ 24 END kyc_duration_day
	FROM base
)
SELECT 
	*
	, CASE WHEN kyc_duration_hr <= 6 THEN '<= 6hr'
			WHEN kyc_duration_hr > 6 AND kyc_duration_hr <= 12 THEN '6-12hr'
			WHEN kyc_duration_hr > 12 AND kyc_duration_hr <= 18 THEN '12-18hr'
			WHEN kyc_duration_hr > 18 AND kyc_duration_hr <= 24 THEN '18-24hr'
			WHEN kyc_duration_hr > 24 AND kyc_duration_hr <= 48 THEN '24-48hr'
			WHEN kyc_duration_hr > 48 THEN '> 48hr'
			ELSE NULL
			END AS kyc_duration_group
--	, AVG(CASE WHEN kyc_duration_hr < 0 THEN 0 ELSE kyc_duration_hr END) AS avg_kyc_duration_hr
FROM kyc_time 
;



