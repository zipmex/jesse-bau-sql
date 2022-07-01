WITH user_info AS (
	SELECT
		z.ap_account_id email
		, u.user_id 
		, u.signup_hostcountry 
		, u.ap_account_id 
		, u.has_deposited 
		, u.has_traded 
		, DATE_TRUNC('day', u.created_at) register_date
		, DATE_TRUNC('day', u.onfido_completed_at) onfido_completed_date
	FROM bo_testing.zipplay_s2_list z
		LEFT JOIN analytics.users_master u
			ON z.ap_account_id = u.email
)	, trade_sum AS (
	SELECT
		u.*
		, SUM(t.amount_usd) trade_vol_usd_oct22_nov30
	FROM user_info u
		LEFT JOIN analytics.trades_master t
			ON u.ap_account_id = t.ap_account_id 
			AND t.created_at BETWEEN '2021-10-22' AND '2021-11-30'
	GROUP BY 1,2,3,4,5,6,7,8
)--	, referral_info AS (
	SELECT
		u.*
		, um.user_id referral_user_id
		, um.email referral_email
		, um.created_at referral_register_date
		, um.onfido_completed_at referral_onfido_verified_date
		, um.level_increase_status referral_status
	FROM user_info u
		LEFT JOIN analytics.users_master um 
			ON u.user_id = um.referring_user_id 
;


