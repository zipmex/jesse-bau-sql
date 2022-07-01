WITH user_list AS (

SELECT 
		um.user_id 
		, um.ap_account_id 
		, um.invitation_code 
		, um.created_at 
		, um.verification_approved_at 
		, um.verification_level 
		, pii.email as pii_email
		, pii.mobile_number as pii_mobile_number 
		, pii.document_number as id_card_number
	FROM 
		analytics.users_master um
		LEFT JOIN 
			analytics_pii.users_pii pii 
			ON pii.user_id = um.user_id
	WHERE 
		um.invitation_code = 'FINNOMENA'
		AND pii.email LIKE 'dree%'
--		AND pii.email NOT IN (SELECT DISTINCT old_email FROM old_email)

		)
,trade_vol as (
	SELECT 
		tm.ap_account_id AS tm_ap_account_id
		, SUM(amount_usd) as tradevol_usd
		, SUM(amount_base_fiat) as tradevol_thb
		, SUM(fee_usd_amount) as trade_fee_usd
		, SUM(fee_base_fiat_amount) as trade_fee_thb
	FROM 
		analytics.trades_master tm
		LEFT JOIN 
			analytics.fees_master fm 
			ON tm.execution_id = fm.fee_reference_id 
	--WHERE (tm.created_at + INTERVAL '7 HOURS') >= (DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL)
	--AND (tm.created_at + INTERVAL '7 HOURS' ) < (DATE_TRUNC('day', NOW()))
	GROUP BY 1
	ORDER BY 1 DESC
)
SELECT 
	a.invitation_code
	, NOW()::DATE reporting_date
	, pii_email AS email
	, a.id_card_number
	, CASE WHEN a.verification_level = 888 THEN 'closed' ELSE 'active' END AS account_status
	, pii_mobile_number AS mobile_number
	, a.created_at AS register_date
	--, a.level_increase_status AS kyc_status
	, a.verification_approved_at AS passed_kyc_date
	, SUM(t.tradevol_thb) trade_vol_thb
	, SUM(t.trade_fee_thb) trade_fee_thb
	, COUNT(distinct pii_email) register_user_count
	, COUNT(distinct case when a.verification_approved_at is not null then pii_email end) verified_user_count
FROM user_list a
	LEFT JOIN 
		trade_vol t 
		ON t.tm_ap_account_id = a.ap_account_id
GROUP BY 1,2,3,4,5,6,7,8