-- user funnel journey
	/*
	Complete Registration - Y
	Click on link in email - Y
	Log into Zipmex Website/site
	Click next
	Start Mobile Verification - Y
	Complete Mobile Verification - Y
	Determine Thai ID Card or Passport - Y
	Enter Personal Information - Y
	Do not get DOPA Error - Y
	Take ID Pic and Selfie Video
	Ensure Upload Successful
	Enter Occupation Details - Y 
	Answer Risk Acceptance Qns - Y
	Click Accept
	Submit Supporting Docs /.Display QR Code
	Submitted / Code Success
	Provide Bank Acc Info - Y
	Zipmex Verifies Bank Acc - Y
	Scan Zipmex's QR Code 
	Successfully Deposit - Y 
	Trade on ZipExchange - Y
	Subscribe to ZipUp - Y
	Deposit into Z-Wallet - Y
	Has ZipLock History - Y
	*/
WITH base AS (
	SELECT
		u.id
		, u.inserted_at register_at
		, u.terms_accepted_at 
		, u.email_verified_at 
		, mnvt.issued_at start_mobile_verification_at
		, u.mobile_number_verified_at 
		, od.document_type
		, od.updated_at submit_id_card
		, pi2.updated_at submit_occupation_info
		, CASE WHEN d.result_code = 0 THEN 'normal' 
				WHEN d.result_code IS NULL THEN NULL 
				ELSE 'error' END AS dopas_status
		, d.updated_at dopas_status_updated_at
		, ss.updated_at submit_survey_at
		, ba.inserted_at provide_bank_acc
		, ba.is_verified_at bank_acc_verified_at 
		, tick_to_timestamp(dt.created_on_ticks) first_deposit_request
		, um.first_deposit_at first_success_deposit
		, um.first_traded_at 
		, um.zipup_subscribed_at 
		, dt2.created_at first_z_wallet_deposit
		, vb.last_updated_at ziplock_history
	FROM 
		user_app_public.users u 
		LEFT JOIN 
			user_app_public.alpha_point_users apu 
			ON u.id = apu.user_id 
		LEFT JOIN 
			analytics.users_master um 
			ON u.id = um.user_id 
	-- start mobile verification
		LEFT JOIN 
			user_app_public.mobile_number_verification_tokens mnvt 
			ON u.id = mnvt.user_id
	-- submit personal info and id card verification
		LEFT JOIN 
			user_app_public.onfido_documents od
			ON u.id = od.user_id
			AND od.archived_at IS NULL
	-- submit occupation info 
		LEFT JOIN 
			user_app_public.personal_infos pi2 
			ON u.id = pi2.user_id
			AND pi2.archived_at IS NULL 
	-- checking DOPA status
		LEFT JOIN 
			(SELECT *
			, ROW_NUMBER() OVER(PARTITION BY d.user_id ORDER BY d.inserted_at DESC) row_ 
			FROM exchange_admin_public.dopas d ) d 
			ON u.id = d.user_id 
			AND d.row_ = 1
	-- submit survey info and answer Risk questions
		LEFT JOIN 
			user_app_public.suitability_surveys ss 
			ON u.id = ss.user_id
			AND ss.archived_at IS NULL
	-- bank account verification
		LEFT JOIN 
			user_app_public.bank_accounts ba 
			ON u.id = ba.user_id
	-- deposit info 
		LEFT JOIN 
			(SELECT * 
			, ROW_NUMBER() OVER(PARTITION BY account_id ORDER BY created_on_ticks) row_
			FROM apex.deposit_tickets) dt 
			ON apu.ap_account_id = dt.account_id 
			AND dt.row_ = 1
	-- transfer to z wallet
		LEFT JOIN 
			(SELECT *
			, ROW_NUMBER () OVER(PARTITION BY account_id ORDER BY created_at) row_ 
			FROM asset_manager_public.deposit_transactions
			WHERE service_id = 'main_wallet'
			AND ref_action = 'deposit'
			) dt2 
			ON u.id = dt2.account_id 
			AND dt2.row_ = 1
		LEFT JOIN
			(SELECT *
			, ROW_NUMBER () OVER(PARTITION BY vb.user_id ORDER BY vb.last_updated_at) row_
			FROM zip_lock_service_public.vault_balances vb) vb 
			ON u.id = vb.user_id 
			AND vb.row_ = 1
	WHERE 
		um.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping um2)
		AND um.signup_hostcountry IN ('TH','ID','AU','global')
)
SELECT 
	COUNT(DISTINCT id) register_user_count
	, COUNT( DISTINCT CASE WHEN terms_accepted_at IS NOT NULL THEN id END) accept_tnc_count
	, COUNT( DISTINCT CASE WHEN email_verified_at IS NOT NULL THEN id END) verified_email_count
	, COUNT( DISTINCT CASE WHEN start_mobile_verification_at IS NOT NULL THEN id END) start_mobile_ver_count
	, COUNT( DISTINCT CASE WHEN mobile_number_verified_at IS NOT NULL THEN id END) mobile_verified_count
	, COUNT( DISTINCT CASE WHEN submit_id_card IS NOT NULL THEN id END) submit_id_card_count
	, COUNT( DISTINCT CASE WHEN submit_occupation_info IS NOT NULL THEN id END) submit_occupation_info_count
	, COUNT( DISTINCT CASE WHEN dopas_status_updated_at IS NOT NULL THEN id END) dopa_check_count
	, COUNT( DISTINCT CASE WHEN submit_survey_at IS NOT NULL THEN id END) answer_risk_qns_count
	, COUNT( DISTINCT CASE WHEN provide_bank_acc IS NOT NULL THEN id END) submit_bank_acc_count
	, COUNT( DISTINCT CASE WHEN bank_acc_verified_at IS NOT NULL THEN id END) bank_acc_verified_count
	, COUNT( DISTINCT CASE WHEN first_deposit_request IS NOT NULL THEN id END) submit_first_deposit_count
	, COUNT( DISTINCT CASE WHEN first_success_deposit IS NOT NULL THEN id END) success_first_deposit_count
	, COUNT( DISTINCT CASE WHEN first_traded_at IS NOT NULL THEN id END) has_traded_count
	, COUNT( DISTINCT CASE WHEN zipup_subscribed_at IS NOT NULL THEN id END) subscribed_zipup_count
	, COUNT( DISTINCT CASE WHEN first_z_wallet_deposit IS NOT NULL THEN id END) transfer_z_wallet_count
	, COUNT( DISTINCT CASE WHEN ziplock_history IS NOT NULL THEN id END) ziplock_history_count
FROM base
;

