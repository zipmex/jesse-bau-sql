WITH user_list AS (
	SELECT 
		um.invitation_code 
		, um.created_at 
		, um.verification_approved_at 
		, um.ap_account_id 
		, um.user_id 
		, pii.email as pii_email
		, pii.mobile_number as pii_mobile_number 
		, pii.document_number as id_card_number
	FROM analytics.users_master um
		LEFT JOIN 
			analytics_pii.users_pii pii 
			ON pii.user_id = um.user_id
	WHERE 
-- include all required invitation_code for filter 
		um.invitation_code IN ('BROOK','RADARS','TRADERKP','stockerday','FINNOMENA')
-- using OR syntax to collect all users from the required codes
		AND 
			CASE WHEN um.invitation_code = 'RADARS' THEN um.created_at + INTERVAL '7 HOURS' >= (DATE_TRUNC('year', NOW()) - '1 year'::INTERVAL)
				WHEN um.invitation_code = 'BROOK' THEN um.created_at + INTERVAL '7 HOURS' >= (DATE_TRUNC('year', NOW()) - '1 year'::INTERVAL)
				ELSE um.created_at >= '2018-12-01'
				END		
)
SELECT 
	a.invitation_code
	, NOW()::DATE reporting_date
	, pii_email AS email
-- all dates will be gmt+7 for consistency
	, a.created_at + INTERVAL '7 HOURS' AS register_date_gmt7
	, a.verification_approved_at + INTERVAL '7 HOURS' AS passed_kyc_date_gmt7
	, (a.created_at + INTERVAL '7 HOURS') + '365 day'::interval as removed_date_gmt7
	, SUM(tm.amount_usd) as tradevol_usd
	, SUM(tm.amount_base_fiat ) as tradevol_thb
	, SUM(fm.fee_usd_amount) as trade_fee_usd
	, SUM(fm.fee_base_fiat_amount) as trade_fee_thb
	, COUNT(DISTINCT pii_email) register_user_count
	, COUNT(DISTINCT CASE WHEN a.verification_approved_at IS NOT NULL THEN pii_email END) verified_user_count
FROM 
	user_list a 
	LEFT JOIN 
		analytics.trades_master tm 
		ON a.ap_account_id = tm.ap_account_id  
		AND (tm.created_at + INTERVAL '7 HOURS') >= (DATE_TRUNC('month', NOW()) - '1 month'::INTERVAL)
		AND (tm.created_at + INTERVAL '7 HOURS' ) < (DATE_TRUNC('month', NOW()))
	LEFT JOIN 
		analytics.fees_master fm 
		ON tm.execution_id = fm.fee_reference_id 
		AND tm.ap_account_id = fm.ap_account_id 
		AND fm.fee_type = 'Trade'
GROUP BY 1,2,3,4,5,6
ORDER BY 1 DESC

)
SELECT 
	invitation_code
	, reporting_date
	, email
-- all dates will be gmt+7 for consistency
	, register_date_gmt7
	, passed_kyc_date_gmt7
	, removed_date_gmt7
	, sum_usd_trade_amount tradevol_usd
	, sum_usd_trade_amount * rm.price as tradevol_thb
	, sum_usd_fee_trade trade_fee_usd
	, sum_usd_fee_trade * rm.price as trade_fee_thb
	, register_user_count
	, verified_user_count
FROM tmp_base tb
	LEFT JOIN 
		analytics.rates_master rm 
		ON rm.product_1_symbol = 'THB'
		AND rm.created_at = NOW()::DATE - '1 day'::INTERVAL
; 



SELECT 
	invitation_code 
	, DATE_TRUNC('month', created_at ) created_month
	, COUNT(DISTINCT user_id) user_count
	, SUM(sum_usd_trade_amount) sum_usd_trade_amount 
	, SUM(sum_usd_deposit_amount) sum_usd_deposit_amount 
FROM reportings_data.dm_user_transactions_dwt_hourly d
WHERE 
	d.invitation_code IN ('BROOK','RADARS','TRADERKP','stockerday','FINNOMENA')
	AND created_at >= '2022-01-01'
GROUP BY 1,2
ORDER BY 1,2 DESC 
;


