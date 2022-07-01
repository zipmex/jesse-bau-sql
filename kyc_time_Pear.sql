SELECT 
	s.updated_at + INTERVAL '7 HOURS' survey_update_at
	, l.inserted_at + INTERVAL '7 HOURS' doc_received_at
	, u.ap_account_id
	, ui.id 
	, pii.email
	, u.level_increase_status
	, REPLACE(l.description, left(l.description, 50), '') AS status_changed
	, (EXTRACT(epoch FROM (l.inserted_at + INTERVAL '7 HOURS')) - EXTRACT(epoch FROM (s.updated_at + INTERVAL '7 HOURS'))) / 3600 AS aging_hr
	, l.inserted_at + INTERVAL '7 HOURS' - s.updated_at + INTERVAL '7 HOURS' AS aging_hr_original
FROM  user_app_public.applicant_data a
	LEFT JOIN analytics.users_master u on a.user_id = u.user_id
	LEFT JOIN analytics_pii.users_pii pii on a.user_id = pii.user_id
	LEFT JOIN user_app_public.users ui on ui.id = u.user_id
	LEFT JOIN user_app_public.suitability_surveys s on s.user_id = a.user_id
	LEFT JOIN (
			SELECT
			substring(object, 6) AS user_id
			,inserted_at
			,description
			,actor
			FROM
			exchange_admin_public.audit_logs
			WHERE
			action = 'edit_level_increase_status'
			-- AND audit_logs.inserted_at + INTERVAL '7 HOURS' >= '2021-07-01 00:00:00'
			-- AND audit_logs.inserted_at + INTERVAL '7 HOURS' <= '2021-07-31 23:59:59'
			GROUP BY
			object, inserted_at,description, actor
			) l 
			ON l.user_id = a.user_id
WHERE
	u.signup_hostcountry = 'TH'
--	AND s.updated_at + INTERVAL '7 HOURS' >= '2021-09-01 00:00:00'
--	AND s.updated_at + INTERVAL '7 HOURS' <= '2021-09-30 23:59:59'
	--and u.level_increase_status = 'underReview'
	AND u.ap_account_id = 564463
ORDER BY 1,2


SELECT user_id , inserted_at , ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY inserted_at) row_ 
FROM user_app_public.personal_infos pi2 
WHERE user_id = '01FDAJS0RGG02E0BGPYAJH6DVP'
;


SELECT applicant_id , inserted_at , archived_at , updated_at , document_number , ROW_NUMBER() OVER(PARTITION BY applicant_id ORDER BY inserted_at DESC) row_ 
FROM user_app_public.onfido_documents
ORDER BY applicant_id DESC 
;

