-- dynamic variables for airdrop dashboards
WITH user_base AS (
	SELECT 
		u.id 
		, u.inserted_at::DATE register_date
		, u.email 
		, u.invitation_code 
		, dsh.signup_hostcountry
		, apu.ap_account_id 
		, oa.level_increase_status 
		, oa.updated_at::DATE verifed_date
		, u.referral_code 
	FROM 
		user_app_public.users u 
		LEFT JOIN 
			user_app_public.alpha_point_users apu 
			ON u.id = apu.user_id 
		LEFT JOIN 
			user_app_public.onfido_applicants oa 
			ON u.id = oa.user_id 
		LEFT JOIN
		    mappings.data_signup_hostname dsh 
		    ON u.signup_hostname = dsh.signup_hostname
 	WHERE 1=1
		AND LOWER(u.invitation_code) = 'zipmex200'
		AND oa.updated_at::DATE >= '2022-04-01'
		AND dsh.signup_hostcountry = 'TH'
		AND oa.level_increase_status = 'pass'		    
-- 	    u.invitation_code IN ( 'ZIPMEX200')
-- 	AND u.id = ''
)   , user_input AS (
	SELECT 
		*
		, CASE WHEN level_increase_status = 'pass' THEN 'THB' ELSE NULL END AS airdrop_currency
		, CASE WHEN level_increase_status = 'pass' THEN 200 ELSE NULL END AS airdrop_amount_fiat
		, CASE WHEN level_increase_status = 'pass' THEN 'BTC' ELSE NULL END AS airdrop_token
	FROM 
		user_base u
)   , product_country AS (
    SELECT
        *
        , CASE WHEN row_ = 1 THEN 'global' ELSE 'TH' END AS product_country
        , CASE WHEN row_ = 1 THEN CONCAT(lower(symbol),'.','gl') ELSE CONCAT(lower(symbol),'.','th') END AS product_name
    FROM (
        SELECT 
            product_id
            , symbol
            , ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY product_id) row_
        FROM
            apex.products 
        ORDER BY symbol, product_id
        ) p 
)--	, airdrop_calc AS (
SELECT 
	u.*
	, CASE WHEN u.signup_hostcountry = 'TH' THEN pct.product_id ELSE pcg.product_id END AS "product_id" 
	, ROUND( airdrop_amount_fiat::NUMERIC / COALESCE (pcp.average_high_low, pdap.price)::NUMERIC , 8) "amount"
	, '' "notes"
    , CASE WHEN u.signup_hostcountry = 'TH' THEN 27308
            WHEN u.signup_hostcountry = 'ID' THEN 6147 
            WHEN u.signup_hostcountry = 'global' THEN 9249 
            WHEN u.signup_hostcountry = 'AU' THEN 719754
            END AS "from_account_id"
    , u.email "to_email"
    , tm.product_symbol airdropped_token
    , tm.notes 
    , tm.amount airdropped_amount
    , ub.email referrer_email 
FROM 
	user_input u
	LEFT JOIN 
	    product_country pct 
	    ON pct.symbol = u.airdrop_token
	    AND pct.product_country = 'TH'
	LEFT JOIN 
	    product_country pcg 
	    ON pcg.symbol = u.airdrop_token
	    AND pcg.product_country <> 'TH'
	LEFT JOIN 
		oms_data_public.cryptocurrency_prices pcp 
		ON pcp.product_1_symbol = airdrop_token
		AND pcp.product_2_symbol = airdrop_currency
		AND pcp.created_at::DATE = NOW()::DATE 
		AND pcp.product_1_symbol NOT IN ('ZMT')
	LEFT JOIN 
		public.daily_ap_prices pdap  
		ON pdap.product_1_symbol = airdrop_token
		AND pdap.product_2_symbol = airdrop_currency
		AND pdap.created_at::DATE = NOW()::DATE 
	LEFT JOIN 
		analytics.transfers_master tm 
		ON u.ap_account_id = tm.receiver_ap_account_id
		AND LOWER(tm.notes) LIKE '%growth_acq%'
--		AND tm.product_symbol = u.airdrop_token
	LEFT JOIN 
		user_base ub 
		ON u.invitation_code = ub.referral_code
		AND u.invitation_code IS NOT NULL
WHERE 1=1 
ORDER BY register_date
;


-- airdrop both referrer and referee 
WITH user_base AS (
	SELECT 
		u.id 
		, u.inserted_at::DATE register_date
		, u.email 
		, u.invitation_code 
		, dsh.signup_hostcountry
		, apu.ap_account_id 
		, oa.level_increase_status 
		, oa.updated_at::DATE verifed_date
		, u2.email referrer_email 
	FROM 
		user_app_public.users u 
		LEFT JOIN 
			user_app_public.alpha_point_users apu 
			ON u.id = apu.user_id 
		LEFT JOIN 
			user_app_public.onfido_applicants oa 
			ON u.id = oa.user_id 
		LEFT JOIN
			mappings.data_signup_hostname dsh 
			ON u.signup_hostname = dsh.signup_hostname
		LEFT JOIN 
			user_app_public.users u2 
			ON u.invitation_code = u2.referral_code 
 	WHERE 1=1
 		AND u2.referral_code IS NOT NULL 
 		AND u.invitation_code NOT IN (SELECT referral_code FROM mappings.growth_referral_code grc ) 
 		AND (u.email NOT LIKE '%zipmex%' OR u.email NOT LIKE '%campaign%')
--		AND u.invitation_code = 'ZIPMEX200'
		AND oa.updated_at::DATE >= '2022-05-01'
		AND dsh.signup_hostcountry = 'AU'
		AND oa.level_increase_status = 'pass'		    
)   , user_input AS (
	SELECT 
		*
		, CASE WHEN level_increase_status = 'pass' THEN 'THB' ELSE NULL END AS airdrop_currency
		, CASE WHEN level_increase_status = 'pass' THEN 200 ELSE NULL END AS airdrop_amount_fiat
		, CASE WHEN level_increase_status = 'pass' THEN 'BTC' ELSE NULL END AS airdrop_token
		, CASE WHEN level_increase_status = 'pass' THEN 200 ELSE NULL END AS airdrop_amount_referrer
	FROM 
		user_base u
)   , product_country AS (
    SELECT
        *
        , CASE WHEN row_ = 1 THEN 'global' ELSE 'TH' END AS product_country
        , CASE WHEN row_ = 1 THEN CONCAT(lower(symbol),'.','gl') ELSE CONCAT(lower(symbol),'.','th') END AS product_name
    FROM (
        SELECT 
            product_id
            , symbol
            , ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY product_id) row_
        FROM
            apex.products 
        ORDER BY symbol, product_id
        ) p 
)--	, airdrop_calc AS (
SELECT 
	u.*
	, CASE WHEN u.signup_hostcountry = 'TH' THEN pct.product_id ELSE pcg.product_id END AS "product_id" 
	, ROUND( airdrop_amount_fiat::NUMERIC / COALESCE (pcp.average_high_low, pdap.price)::NUMERIC , 8) "amount"
	, '' "notes"
    , CASE WHEN u.signup_hostcountry = 'TH' THEN 27308
            WHEN u.signup_hostcountry = 'ID' THEN 6147 
            WHEN u.signup_hostcountry = 'global' THEN 9249 
            WHEN u.signup_hostcountry = 'AU' THEN 719754
            END AS "from_account_id"
    , u.email "to_email"
    , tm.product_symbol airdropped_token
    , tm.notes 
    , tm.amount airdropped_amount
    , u.referrer_email 
	, ROUND( airdrop_amount_referrer::NUMERIC / COALESCE (pcp.average_high_low, pdap.price)::NUMERIC , 8) "amount"
FROM 
	user_input u
	LEFT JOIN 
	    product_country pct 
	    ON pct.symbol = u.airdrop_token
	    AND pct.product_country = 'TH'
	LEFT JOIN 
	    product_country pcg 
	    ON pcg.symbol = u.airdrop_token
	    AND pcg.product_country <> 'TH'
	LEFT JOIN 
		oms_data_public.cryptocurrency_prices pcp 
		ON pcp.product_1_symbol = airdrop_token
		AND pcp.product_2_symbol = airdrop_currency
		AND pcp.created_at::DATE = NOW()::DATE 
		AND pcp.product_1_symbol NOT IN ('ZMT')
	LEFT JOIN 
		public.daily_ap_prices pdap  
		ON pdap.product_1_symbol = airdrop_token
		AND pdap.product_2_symbol = airdrop_currency
		AND pdap.created_at::DATE = NOW()::DATE 
	LEFT JOIN 
		analytics.transfers_master tm 
		ON u.ap_account_id = tm.receiver_ap_account_id
		AND LOWER(tm.notes) LIKE '%growth_acq%'
WHERE 1=1 
ORDER BY register_date
;


-- airdrop generation for all campaigns into 1 file
WITH product_country AS (
    SELECT
        *
        , CASE WHEN row_ = 1 THEN 'global' ELSE 'TH' END AS product_country
        , CASE WHEN row_ = 1 THEN CONCAT(lower(symbol),'.','gl') ELSE CONCAT(lower(symbol),'.','th') END AS product_name
    FROM (
        SELECT 
            product_id
            , symbol
            , ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY product_id) row_
        FROM
            apex.products 
        ORDER BY symbol, product_id
        ) p 
)	, summary_section AS (
	SELECT
	-- airdrop section
		NOW()::DATE "reporting_date"
		, CASE WHEN um.signup_hostcountry = 'TH' THEN pct.product_id ELSE pcg.product_id END AS "product_id" 
		, cact.from_account_id 
		, ROUND( cact.amount_fiat::NUMERIC / COALESCE (pcp.average_high_low, pdap.price)::NUMERIC , 8) "paid_out_amount"
		, u.email "to_email"
	-- for approval
		, cact.amount_fiat::INT 
		, cact.product_name "Token_type"
		, cact.currency "paidout_currency"
	-- validate if users have got airdropped in the past
		, tm.created_at::DATE past_airdropped_date
		, tm.product_symbol past_airdropped_symbol
		, tm.amount "past_airdropped_amount"
		, tm.notes past_airdropped_notes	
	-- preliminary info
		, cact.start_date "campaign_start_date"
		, cact.end_date "campaign_end_date"
		, cact.campaign 
		, cact.team 
		, cact.campaign_name 
		, cact.campaign_type 
		, cact.referral_code 
		, cact.referral_group 
	-- user info
		, um.user_id 
		, u.email
		, um.invitation_code 
		, um.created_at::DATE register_date
		, um.verification_approved_at::DATE verified_date
	FROM 
		analytics.users_master um 
		LEFT JOIN 
			mappings.commercial_au_cp_tracker cact 
			ON um.invitation_code = cact.referral_code 
		LEFT JOIN 
			user_app_public.users u 
			ON um.user_id = u.id 
		LEFT JOIN 
			mappings.public_cryptocurrency_prices pcp 
			ON cact.product_name = pcp.product_1_symbol 
			AND cact.currency = pcp.product_2_symbol 
			AND pcp.created_at::DATE = NOW()::DATE - '1 day'::INTERVAL 
		LEFT JOIN 
			mappings.public_daily_ap_prices pdap 
			ON cact.product_name = pdap.product_1_symbol 
			AND cact.currency = pdap.product_2_symbol 
			AND pdap.created_at::DATE = NOW()::DATE - '1 day'::INTERVAL 
		LEFT JOIN 
			analytics.transfers_master tm 
			ON um.ap_account_id = tm.receiver_ap_account_id
			AND LOWER(tm.notes) LIKE '%growth_acq%'
	--		AND cact.product_name = tm.product_symbol 
		LEFT JOIN 
		    product_country pct 
		    ON pct.symbol = cact.product_name 
		    AND pct.product_country = 'TH'
		LEFT JOIN 
		    product_country pcg 
		    ON pcg.symbol = cact.product_name 
		    AND pcg.product_country <> 'TH'
	WHERE 
		cact.referral_code IS NOT NULL 
		AND um.verification_approved_at >= '2022-05-01'
	ORDER BY um.created_at , um.verification_approved_at 
)
SELECT 
	*
FROM summary_section
ORDER BY "Token_type"
;


-- 'ZIPUP100USDC', 'ZIPUP100ZMT'
WITH base AS (
	SELECT 
		um.ap_account_id 
		, up.email 
		, um.invitation_code 
		, um.is_verified 
		, um.has_traded 
		, um.has_deposited 
		, um.sum_deposit_amount_usd 
	FROM 
		analytics.users_master um 
		LEFT JOIN 
			analytics_pii.users_pii up 
			ON um.user_id = up.user_id 
	WHERE 
		invitation_code IN ('ZIPUP100USDC', 'ZIPUP100ZMT')
		AND has_deposited = TRUE 
)
SELECT 
	b.*
	, wbe.symbol 
	, CASE WHEN rm.product_type = 1 THEN wbe.trade_wallet_amount * 1/rm.price 
			WHEN rm.product_type = 2 THEN wbe.trade_wallet_amount * rm.price 
			END AS trade_wallet_usd
	, wbe.z_wallet_amount * rm.price z_wallet_usd
	, wbe.ziplock_amount * rm.price ziplock_usd
FROM base b 
	LEFT JOIN 
		analytics.wallets_balance_eod wbe 
		ON b.ap_account_id = wbe.ap_account_id 
		AND wbe.created_at = NOW()::DATE - '1 day'::INTERVAL 
	LEFT JOIN 
		analytics.rates_master rm 
		ON wbe.symbol = rm.product_1_symbol 
		AND wbe.created_at = rm.created_at 
ORDER BY 2,1
;

