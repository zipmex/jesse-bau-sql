SELECT
	COUNT(u.first_traded_at - u.first_deposit_at) AS n
	, AVG(u.first_traded_at - u.first_deposit_at) AS AVG_time
	, MIN(u.first_traded_at - u.first_deposit_at) AS MIN_time
	, MAX(u.first_traded_at - u.first_deposit_at) AS MAX_time
	, percentile_cont(0.25) WITHIN GROUP(ORDER BY(u.first_traded_at - u.first_deposit_at)) AS Q1_time
	, percentile_cont(0.5) WITHIN GROUP(ORDER BY(u.first_traded_at - u.first_deposit_at)) AS MED_time
	, percentile_cont(0.75) WITHIN GROUP(ORDER BY(u.first_traded_at - u.first_deposit_at)) AS Q3_time
FROM analytics.users_master u 
WHERE
	u.first_traded_at IS NOT NULL
	AND u.first_traded_at + INTERVAL '7 HOURS' < '2022-03-14 00:00:00'
	AND u.signup_hostcountry = 'TH'
	AND u.level_increase_status = 'pass'
	AND u.first_deposit_at IS NOT NULL
	AND u.first_deposit_at + INTERVAL '7 HOURS' < '2022-03-14 00:00:00'
	AND u.first_traded_at > u.first_deposit_at
;


WITH base AS (
	SELECT
		u.id
		, u.inserted_at register_at
		, u.email_verified_at 
		, u.mobile_number_verified_at 
		, ss.updated_at submit_survey_at
		, um.onfido_submitted_at submit_kyc
		, um.onfido_completed_at kyc_approved
		, ba.inserted_at provide_bank_acc
		, ba.is_verified_at bank_acc_verified_at 
		, um.first_deposit_at first_success_deposit
		, um.first_traded_at 
		, um.zipup_subscribed_at 
	FROM 
		user_app_public.users u 
		LEFT JOIN 
			user_app_public.alpha_point_users apu 
			ON u.id = apu.user_id 
		LEFT JOIN 
			analytics.users_master um 
			ON u.id = um.user_id 
	-- submit survey info and answer Risk questions
		LEFT JOIN 
			user_app_public.suitability_surveys ss 
			ON u.id = ss.user_id
			AND ss.archived_at IS NULL
	-- bank account verification
		LEFT JOIN 
			user_app_public.bank_accounts ba 
			ON u.id = ba.user_id
	WHERE 
		um.first_traded_at IS NOT NULL
		AND um.first_traded_at + INTERVAL '7 HOURS' < '2022-03-14 00:00:00'
		AND um.signup_hostcountry = 'TH'
		AND um.level_increase_status = 'pass'
		AND um.first_deposit_at IS NOT NULL
		AND um.first_deposit_at + INTERVAL '7 HOURS' < '2022-03-14 00:00:00'
		AND um.first_traded_at > um.first_deposit_at
)	, base_time AS (
	SELECT 
		id
		, email_verified_at - register_at register_to_email
		, submit_kyc - email_verified_at email_to_kyc
		, kyc_approved - submit_kyc kyc_submit_to_approved
		, provide_bank_acc - kyc_approved kyc_approved_to_bank_submit
		, bank_acc_verified_at - provide_bank_acc bank_submit_to_approve
		, first_success_deposit - bank_acc_verified_at bank_approved_to_deposit
		, first_traded_at - first_success_deposit deposit_to_trade
	FROM base
)
SELECT 
	COUNT(deposit_to_trade) user_traded_count
	, AVG(deposit_to_trade) avg_deposit_to_trade
	, MIN(deposit_to_trade) min_deposit_to_trade
	, MAX(deposit_to_trade) max_deposit_to_trade
FROM base_time 
;