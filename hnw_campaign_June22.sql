/*
 * High Net Worth (HNW) campaign
 * zwallet balance min 500K THB = 5 slots
 * ZMT, USDC, USDT, BTC, ETH --> mappings table
 * bonus rates on top of current zipup+
 * - 500K -> 1.5m THB = 0.5%
 * - 1.5m THB and above = 1%
 */

WITH base_wallet AS (
	SELECT 
		wbe.created_at::DATE 
		, wbe.ap_account_id 
		, u.signup_hostcountry 
		, u.created_at::DATE register_date
		, u.verification_approved_at::DATE verified_date
		, u.verification_approved_at::DATE + '90 day'::INTERVAL reward_end_date
		, u.invitation_code 
		, u.referring_user_id 
		, th.email referrer_email
		, u.zipup_subscribed_at::DATE 
		, wbe.symbol 
		, wbe.trade_wallet_amount 
		, wbe.z_wallet_amount 
		, wbe.ziplock_amount 
		, CASE WHEN r.product_type = 1 THEN wbe.trade_wallet_amount * 1/r.price 
				WHEN r.product_type = 2 THEN wbe.trade_wallet_amount * r.price 
				END AS trade_wallet_amount_usd
		, wbe.z_wallet_amount * r.price z_wallet_amount_usd
		, wbe.ziplock_amount * r.price ziplock_amount_usd
	FROM 
		analytics.wallets_balance_eod wbe 
		LEFT JOIN 
			analytics.users_master u 
			ON wbe.ap_account_id = u.ap_account_id 
			AND u.created_at >= '2022-06-13'
		RIGHT JOIN 
			mappings.th_campaign_hnw_062022 th 
			ON u.invitation_code = th.referral_code
		LEFT JOIN 
			analytics_pii.users_pii up 
			ON u.user_id = up.user_id 
		LEFT JOIN 
			analytics.rates_master r 
			ON wbe.symbol = r.product_1_symbol
			AND DATE_TRUNC('day', wbe.created_at) = DATE_TRUNC('day', r.created_at)
	WHERE 
		u.signup_hostcountry IN ('TH')
		AND wbe.created_at >= '2022-06-13'
		AND wbe.symbol IN ('BTC', 'ETH', 'ZMT', 'USDT', 'USDC', 'ADA', 'XRP', 'SOL')
)	, daily_wallet AS (
	SELECT 
		created_at 
		, ap_account_id 
		, register_date
		, verified_date
		, reward_end_date::DATE 
		, invitation_code
		, referring_user_id
		, referrer_email
		, signup_hostcountry
		, symbol 
		, trade_wallet_amount 
		, z_wallet_amount + ziplock_amount total_zwallet_amount
		, trade_wallet_amount_usd
		, z_wallet_amount_usd + ziplock_amount_usd total_zwallet_usd
	FROM base_wallet
	WHERE created_at >= verified_date
		AND created_at <= reward_end_date
)	, slot_calc AS (
	SELECT 
		dw.created_at 
		, dw.ap_account_id 
		, dw.symbol 
		, br.min_amount
		, dw.total_zwallet_amount
		, ROUND(dw.total_zwallet_amount / br.min_amount,0) slot_count
	FROM daily_wallet dw 
		LEFT JOIN bo_testing.test_hnw_bonus_rate br 
			ON dw.symbol = br.symbol
)	, daily_slot AS (
	SELECT 
		created_at 
		, ap_account_id 
		, SUM(slot_count) total_slot_in_zwallet
	FROM slot_calc
	GROUP BY 1,2	
)	, bonus_rate AS (
	SELECT 
		dw.*
		, ds.total_slot_in_zwallet
		, CASE WHEN ds.total_slot_in_zwallet < 5 THEN 0 
				WHEN ds.total_slot_in_zwallet >= 5 AND ds.total_slot_in_zwallet < 15 THEN 0.005
				WHEN ds.total_slot_in_zwallet >= 15 THEN 0.01 END AS bonus_rate
	FROM daily_wallet dw 
		LEFT JOIN daily_slot ds 
			ON dw.created_at = ds.created_at
			AND dw.ap_account_id = ds.ap_account_id
)
SELECT 
	b.*
	, b.total_zwallet_amount * b.bonus_rate payout_amount
	, d.sum_usd_deposit_amount 
	, d.sum_usd_trade_amount 
	, d.sum_usd_withdraw_amount 
FROM bonus_rate b 
	LEFT JOIN reportings_data.dm_user_transactions_dwt_daily d
		ON b.ap_account_id = d.ap_account_id 
		AND b.created_at = d.created_at 
ORDER BY 2,1
;