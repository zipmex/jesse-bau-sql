
DROP TABLE IF EXISTS warehouse.bo_testing.dm_redo_survey_snapshot;

CREATE TABLE IF NOT EXISTS warehouse.bo_testing.dm_redo_survey_snapshot
(
	data_updated_at				DATE
	, user_id					VARCHAR(255)
	, signup_hostcountry		VARCHAR(255)
	, level_increase_status		VARCHAR(255)
	, survey_started_at			TIMESTAMP
	, survey_completed_at		TIMESTAMP
	, re_kyc_time				VARCHAR(255)
	, re_kyc_due_date			DATE
	, risk_type					INTEGER
	, pcs_type					VARCHAR(255)
	, pcs_updated_at			TIMESTAMP
	, re_kyc_deadline			DATE
--	, has_redo_survey			BOOLEAN
--	, new_survey_completed_at	TIMESTAMP
--	, within_deadline			BOOLEAN
);

CREATE INDEX IF NOT EXISTS idx_dm_redo_survey_snapshot ON warehouse.bo_testing.dm_redo_survey_snapshot
(data_updated_at, user_id, signup_hostcountry, risk_type, pcs_type, re_kyc_deadline);

ALTER TABLE warehouse.bo_testing.dm_redo_survey_snapshot REPLICA IDENTITY FULL;

CREATE TEMP TABLE IF NOT EXISTS tmp_dm_redo_survey_snapshot AS 
(
	WITH base AS (
		SELECT
			NOW()::DATE data_updated_at
			, um.user_id 
			, um.signup_hostcountry
			, um.level_increase_status 
			, ss.inserted_at survey_started_at
			, ss.updated_at survey_completed_at
			, CASE WHEN akd.risk_type IN (1,2) THEN '2 years'
					WHEN akd.risk_type = 3 THEN '1 year'
					ELSE 'error' END AS re_kyc_time
			, CASE WHEN akd.risk_type IN (1,2) THEN (ss.updated_at + '2 year'::INTERVAL)::DATE
					WHEN akd.risk_type = 3 THEN (ss.updated_at + '1 year'::INTERVAL)::DATE
					ELSE NULL END AS re_kyc_due_date
			, akd.risk_type 
			, f.code pcs_type
			, uf.updated_at pcs_updated_at
			, up.email 
			, up.first_name 
			, up.last_name 
			, up.mobile_number 
		FROM 
			analytics.users_master um 
			LEFT JOIN 
				analytics_pii.users_pii up 
				ON um.user_id = up.user_id 
			LEFT JOIN 
				user_app_public.suitability_surveys ss 
				ON um.user_id = ss.user_id 
			LEFT JOIN 
				user_app_public.additional_kyc_details akd 
				ON ss.user_id = akd.user_id 
			LEFT JOIN 
				(SELECT * , ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY updated_at DESC) row_
				FROM user_app_public.user_features) uf 
				ON ss.user_id = uf.user_id 
				AND uf.row_ = 1
			LEFT JOIN 
				user_app_public.features f 
				ON uf.feature_id = f.id 
		WHERE 
			ss.archived_at IS NULL
			AND um.signup_hostname = 'trade.zipmex.co.th'
			AND um.level_increase_status = 'pass'
			AND um.verification_level <> 888
		--	AND akd.risk_type IS NOT NULL
		--	AND akd.user_id = ''
		ORDER BY 5
	)
	SELECT
		data_updated_at
		, user_id
		, signup_hostcountry
		, level_increase_status
		, survey_started_at
		, survey_completed_at
		, re_kyc_time
		, re_kyc_due_date
		, risk_type
		, pcs_type
		, pcs_updated_at
		, CASE WHEN (re_kyc_due_date + '30 days'::INTERVAL)::DATE < '2022-08-25' THEN '2022-08-25'
				ELSE (re_kyc_due_date + '30 days'::INTERVAL)::DATE END AS re_kyc_deadline
	FROM base 
	WHERE 
		re_kyc_due_date < DATE_TRUNC('month', NOW()) + '1 month'::INTERVAL
--		AND re_kyc_due_date >= DATE_TRUNC('month', NOW())
	ORDER BY 
		survey_completed_at DESC 
);


INSERT INTO warehouse.bo_testing.dm_redo_survey_snapshot
(SELECT * FROM tmp_dm_redo_survey_snapshot);

DROP TABLE IF EXISTS tmp_dm_redo_survey_snapshot;
