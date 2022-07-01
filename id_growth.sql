
-- refer bff, get cb in trading fee from 2021-12-22 to 2022-02-22
WITH referral_user AS (
-- users registered during campaign period
	SELECT 
		um.created_at::DATE
		, um.signup_hostcountry 
		, um.ap_account_id 
		, invitation_code 
		, CASE 	
				WHEN invitation_code IS NULL THEN 'A_organic'
				WHEN invitation_code IN ('ARNOLDZMT','ASTRONACCI','CATCHMEUP','HANS1804','JEJOUW','jRtttiURUe','PAOPAO','RITA2020','RIVA2310','SIGIT2020'
										,'TERNAKUANG','TIMOTHY','yennikristiani')
									THEN 'B_influencer'
				WHEN invitation_code IN ('25KAF','25KFA','25KGD','25KIN','25KMV','25KSE','25KXA','35KAP','35KC2','35KES','35KFA','35KGD','35KID','35KIM',
										'35KMG','35KMV','35KSE','35KSI','35KTT','35KXA','5DOGE','A2588','NUSA','NUSATALENT')
									THEN 'C_partner'
				WHEN invitation_code IN ('bitcoin200','btc200','cuanbanyak','CUANFEST','DOGE','GETAXS','INVESTDAY','JUSTCO','ladiesweek','langsunguntung',
										'menangbanyak','MERDEKA','NONTONKBP','zipmexcp','zipmexpi','zipmexuntung','zipstocks100ribu')
									THEN 'D_cp_other'
				WHEN invitation_code = 'kopikenangan' THEN 'D_kopikenangan' 
				WHEN invitation_code = 'zipmexcitilink' THEN 'D_cp_flight'				
				WHEN invitation_code IN ('zipmexfriend','ZLAUNCHAN','ZLAUNCHFE')
									THEN 'E_unknown'
			ELSE 'F_user_code' 
			END AS campaign_group 
		, is_verified 
		, has_deposited 
		, has_traded 
	FROM 
		analytics.users_master um 
	WHERE 
		um.signup_hostcountry = 'ID'
	-- campaign period
		AND um.created_at >= '2021-01-01'
)	, trade_sum AS (
-- trade volume during campaign
	SELECT 
		ru.*
		, SUM(tm.amount_usd) m0_trade_vol_usd
		, SUM(fm.fee_usd_amount) m0_fee_usd
	FROM 
		referral_user ru
		LEFT JOIN analytics.trades_master tm 
			ON ru.ap_account_id = tm.ap_account_id 
		-- trade in the same register month
			-- trade within 7/14/30 days 
			AND tm.created_at = ru.created_at 
		-- trading fee 
		LEFT JOIN analytics.fees_master fm 
			ON tm.execution_id = fm.fee_reference_id 
	GROUP BY 1,2,3,4,5,6,7,8
)	, deposit_sum AS (
	SELECT
		ts.*
		, SUM(dtm.amount_usd) m0_deposit_amount_usd 
	FROM 
		trade_sum ts
		LEFT JOIN analytics.deposit_tickets_master dtm
			ON ts.ap_account_id = dtm.ap_account_id 
		-- deposit in the same register month
			AND dtm.created_at = ts.created_at
	GROUP BY 1,2,3,4,5,6,7,8,9,10
--)	, eligible_sum AS (
--	SELECT 
--		*
--	-- eligible users with trade vol >= 1 USD during campaign trading fee CB
--		, CASE WHEN sum_trade_vol_usd >= 1 THEN TRUE ELSE FALSE END AS is_trade_eligible
--	-- eligible users with deposit vol >= 100,000 IDR (6.99 USD) during campaign kopikenangan
--		, CASE WHEN sum_deposit_amount_usd >= 6.99 THEN TRUE ELSE FALSE END AS is_deposit_eligible
--	FROM 
--		deposit_sum ds 
)
SELECT 
	DATE_TRUNC('month', created_at) register_month
	, signup_hostcountry 
	, campaign_group
	, COUNT(DISTINCT invitation_code) referrer_count
	, COUNT(DISTINCT ap_account_id) register_count
	, COUNT(DISTINCT CASE WHEN is_verified IS TRUE THEN ap_account_id END) verified_count
	, COUNT(DISTINCT CASE WHEN has_deposited IS TRUE THEN ap_account_id END) depositor_count
	, COUNT(DISTINCT CASE WHEN has_traded IS TRUE THEN ap_account_id END) trader_count
	, SUM(m0_trade_vol_usd) m0_trade_vol_usd
	, SUM(m0_deposit_amount_usd) m0_deposit_amount_usd
FROM 
	deposit_sum
GROUP BY 1,2,3
;



-- buy and lock 0.0008 BTC and get cashback of IDR100k 1-7 Jan 2022
WITH btc_buy_sum AS (
	SELECT 
		DATE_TRUNC('day', tm.created_at + '7 hour'::INTERVAL)::DATE created_at 
		, DATE_TRUNC('month', tm.created_at + '7 hour'::INTERVAL)::DATE - DATE_TRUNC('month', um.created_at + '7 hour'::INTERVAL)::DATE register_period
		, tm.ap_account_id 
		, tm.product_1_symbol 
		, SUM(quantity) sum_btc_buy_amount
	FROM 
		analytics.trades_master tm 
		LEFT JOIN analytics.users_master um 
			ON tm.ap_account_id = um.ap_account_id 
	WHERE 
	-- campaign period 
		tm.created_at + '7 hour'::INTERVAL BETWEEN '2022-01-01' AND '2022-01-08'
		AND tm.signup_hostcountry = 'ID'
	-- buy BTC
		AND product_1_symbol = 'BTC'
	-- buy to lock 
		AND side = 'Buy'
	GROUP BY 1,2,3,4
)	, btc_lock_sum AS (
	SELECT 
		DATE_TRUNC('day', lt.locked_at + '7 hour'::INTERVAL)::DATE created_at 
		, um2.ap_account_id 
		, lt.product_id 
		, COALESCE (SUM(lt.amount), 0) btc_lock_amount
	FROM 
		zip_lock_service_public.lock_transactions lt 
		LEFT JOIN 
			analytics.users_master um2 
			ON um2.user_id = lt.user_id 
	WHERE 
	-- campgaign period
		lt.locked_at + '7 hour'::INTERVAL BETWEEN '2022-01-01' AND '2022-01-08'
		AND um2.signup_hostcountry = 'ID'
		AND lt.product_id LIKE 'btc%'
	GROUP BY 
		1,2,3
)	, btc_lock_cumulative AS (
	SELECT 
		*
	-- calculate cumulative btc lock amount
		, SUM(btc_lock_amount) OVER(PARTITION BY ap_account_id ORDER BY created_at) cum_btc_lock
	FROM btc_lock_sum
)	, btc_lock_rank AS (
	SELECT 
		*
	-- rank to get the first day when cumulative lock amount met requirement
		, RANK() OVER(PARTITION BY ap_account_id ORDER BY created_at) btc_lock_rank
	FROM btc_lock_cumulative 
	WHERE 
		cum_btc_lock >= 0.0008
)
SELECT 
	bt.*
	, COALESCE (bl.btc_lock_amount, 0) btc_lock_amount
	, bl.created_at lock_at
-- count users buying BTC >= required amount during campaign
	, CASE WHEN sum_btc_buy_amount >= 0.0008 THEN 1 ELSE 0 END AS is_eli_btc_buyer
-- count eligible users during campaign
	, CASE WHEN sum_btc_buy_amount >= 0.0008 AND btc_lock_amount >= 0.0008 THEN 1 ELSE 0 END AS is_eligible
	, CASE	WHEN bt.register_period = 0 THEN 'A_new_user'
			WHEN bt.register_period BETWEEN 1 AND 3 THEN 'B_reg_1-3M'
			WHEN bt.register_period BETWEEN 4 AND 6 THEN 'C_reg_4-6M'
			WHEN bt.register_period > 6 THEN 'D_reg_>_6M'
			END AS register_group
FROM 
	btc_buy_sum bt 
	LEFT JOIN btc_lock_rank bl 
		ON bt.ap_account_id = bl.ap_account_id
	-- the date when lock amount met requirement
		AND btc_lock_rank = 1
-- only first 500 users are eligible
ORDER BY 
	bl.created_at
;


-- flashdeal CB up to 200K IDR from 2021-12-16 to 2022-01-07
-- deposit 1 time: >= 1mil IDR and Buy minimum 1mil IDR --> 10% CB --> 3000 users
-- deposit 1 time: between 500K - 999,999 IDR and Buy minimum 500K - 999,999 IDR --> 5% CB --> 6000 users