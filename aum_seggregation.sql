-- aum snapshot by rates_master
WITH register_base AS (
    SELECT 
        DATE_TRUNC('month', um2.created_at)::DATE register_month
        , COUNT( DISTINCT um2.user_id) register_count
    FROM analytics.users_master um2 
    WHERE 
        um2.signup_hostcountry IN ('TH','ID','AU','global')
        AND um2.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
    GROUP BY 1
)   , verified_base AS (
    SELECT 
        DATE_TRUNC('month', um2.onfido_completed_at)::DATE verified_month
        , COUNT( DISTINCT CASE WHEN is_verified IS TRUE THEN um2.user_id END) verify_count
    FROM analytics.users_master um2 
    WHERE 
        um2.signup_hostcountry IN ('TH','ID','AU','global')
        AND um2.ap_account_id NOT IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping)
    GROUP BY 1
)   , user_base AS (
    SELECT 
        register_month 
        , register_count
        , COALESCE (verify_count, 0) verify_count
        , SUM(register_count) OVER ( ORDER BY register_month) cumulative_register
        , CASE WHEN verified_month IS NULL THEN 0 
            ELSE SUM(verify_count) OVER ( ORDER BY verified_month) END cumulative_verify
    FROM register_base r
        LEFT JOIN verified_base v 
            ON r.register_month = verified_month
)   , hourly_accumulated_balances AS (
	SELECT *
	FROM (
		SELECT * , date_trunc('day', created_at) AS thour
		, ROW_NUMBER() OVER(PARTITION BY user_id, product_id , date_trunc('day', created_at) ORDER BY created_at DESC) AS r
		FROM zipmex_otc_prod_public.accumulated_balances
		) t
	WHERE t.r = 1
	AND date_trunc('day', thour) < '2021-08-25'
)	, plaung_aum AS (
	SELECT
		thour, user_id, UPPER(h.product_id) symbol , h.balance, h.created_at, h.id
		, CASE WHEN UPPER(h.product_id) = 'IDR' THEN h.balance * 1/rm.price 
				ELSE h.balance * rm.price END usd_amount
		, ROW_NUMBER() OVER(PARTITION BY user_id, UPPER(h.product_id), DATE_TRUNC('month', thour) ORDER BY thour DESC) rank_ 
	FROM 
		hourly_accumulated_balances h 
		LEFT JOIN 
			analytics.rates_master rm 
		    ON UPPER(h.product_id) = rm.product_1_symbol 
		    AND DATE_TRUNC('day', thour) = rm.created_at 
	WHERE
		user_id = '01F14GTKR63YS7QSPGCQDNVJRR'
	--	AND extract(day from thour) = 23
	ORDER BY thour DESC, user_id, product_id
)	, coin_base AS (
	SELECT 
		DISTINCT UPPER(SPLIT_PART(product_id,'.',1)) symbol
		, started_at effective_date
		, ended_at expired_date
	FROM zip_up_service_public.interest_rates
	ORDER BY 1
)	, zipup_coin AS (
	SELECT 
		DISTINCT
		symbol
		, (CASE WHEN effective_date < '2022-03-22' THEN '2018-01-01' ELSE effective_date END)::DATE AS effective_date
		, (CASE WHEN expired_date IS NULL THEN COALESCE( LEAD(effective_date) OVER(PARTITION BY symbol),'2999-12-31') ELSE expired_date END)::DATE AS expired_date
	FROM coin_base 
	ORDER BY 3,2
)	, base AS (
	SELECT 
		a.created_at 
		, CASE WHEN u.signup_hostcountry IS NULL THEN 'unknown' ELSE u.signup_hostcountry END AS signup_hostcountry
		, a.ap_account_id , email
		, CASE WHEN a.created_at < '2022-05-05' THEN  
			( CASE WHEN a.ap_account_id IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping WHERE ap_account_id NOT IN (496001))
			THEN TRUE ELSE FALSE END)
			ELSE
			( CASE WHEN a.ap_account_id IN (SELECT DISTINCT ap_account_id FROM mappings.users_mapping WHERE ap_account_id NOT IN (38121 ,496001))
			THEN TRUE ELSE FALSE END)
			END AS is_nominee 
		, CASE WHEN a.ap_account_id = 496001 THEN TRUE ELSE FALSE END AS is_asset_manager
	-- zipup subscribe status to identify zipup amount
		, (CASE WHEN u.signup_hostcountry = 'TH' THEN
			(CASE WHEN a.created_at < '2022-05-24' THEN s.tnc_accepted_at ELSE u.zipup_subscribed_at END)
			WHEN u.signup_hostcountry = 'ID' THEN
			(CASE WHEN a.created_at < '2022-07-04' THEN s.tnc_accepted_at ELSE u.zipup_subscribed_at END)
			WHEN u.signup_hostcountry IN ('AU','global') THEN
			(CASE WHEN a.created_at < '2022-06-29' THEN s.tnc_accepted_at ELSE u.zipup_subscribed_at END)
			END)::DATE AS zipup_subscribed_at
		, a.symbol 
		, CASE WHEN a.symbol = 'ZMT' THEN TRUE WHEN zc.symbol IS NOT NULL THEN TRUE ELSE FALSE END AS zipup_coin 
		, u.is_zipup_subscribed 
		, trade_wallet_amount
		, z_wallet_amount
		, ziplock_amount
		, zlaunch_amount
		, r.price usd_rate 
		, CASE	WHEN r.product_type = 1 THEN trade_wallet_amount * 1/r.price 
				WHEN r.product_type = 2 THEN trade_wallet_amount * r.price 
				END AS trade_wallet_amount_usd
		, z_wallet_amount * r.price z_wallet_amount_usd
		, ziplock_amount * r.price ziplock_amount_usd
		, zlaunch_amount * r.price zlaunch_amount_usd
	FROM 
		analytics.wallets_balance_eod a 
		LEFT JOIN 
			zipup_coin zc 
			ON a.symbol = zc.symbol
			AND a.created_at >= zc.effective_date
			AND a.created_at < zc.expired_date
		LEFT JOIN 
			analytics.users_master u 
			ON a.ap_account_id = u.ap_account_id 
		LEFT JOIN 
			warehouse.zip_up_service_public.user_settings s
			ON u.user_id = s.user_id 
		LEFT JOIN 
			analytics.rates_master r 
			ON a.symbol = r.product_1_symbol 
		    AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', r.created_at)
	WHERE 
		a.created_at >= '2022-06-01' AND a.created_at < DATE_TRUNC('month', NOW()) 
		AND u.signup_hostcountry IN ('TH','ID','AU','global')
		AND ((a.created_at = DATE_TRUNC('month', a.created_at) + '1 month' - '1 day'::INTERVAL) OR (a.created_at = DATE_TRUNC('day', NOW()) - '1 day'::INTERVAL))
		AND a.symbol NOT IN ('TST1','TST2')
	ORDER BY 1 DESC 
)	, aum_snapshot AS (
	SELECT 
		DATE_TRUNC('month', created_at)::DATE created_at
		, signup_hostcountry
		, ap_account_id
		, SUM( COALESCE (ziplock_amount_usd, 0)) ziplock_amount_usd
		, SUM( COALESCE (zlaunch_amount_usd, 0)) zlaunch_amount_usd
		, SUM( COALESCE (CASE WHEN zipup_subscribed_at IS NOT NULL AND created_at >= DATE_TRUNC('day', zipup_subscribed_at) AND zipup_coin = TRUE
					THEN
						(CASE 	WHEN created_at <= '2021-09-02 00:00:00' THEN COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0)
								WHEN created_at > '2021-09-02 00:00:00' THEN COALESCE (z_wallet_amount_usd, 0) END)
					END, 0)) AS zipup_subscribed_usd
		, SUM( COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0) 
					+ COALESCE (ziplock_amount_usd, 0) + COALESCE(zlaunch_amount_usd, 0) ) total_aum
	FROM 
		base 
	WHERE is_asset_manager = FALSE AND is_nominee = FALSE
	GROUP BY 
		1,2,3
	ORDER BY 
		1  
)	, total_aum_plaung AS (
		SELECT
			created_at
			, ap_account_id
			, signup_hostcountry
--			, SUM(total_aum) total_aum
			, SUM(COALESCE (zipup_subscribed_usd, 0) + COALESCE (ziplock_amount_usd, 0)) total_aum
		FROM aum_snapshot
		GROUP BY 1,2,3
	UNION ALL 
		SELECT 
			DATE_TRUNC('month', thour)::DATE created_at 
			, 111111111 ap_account_id 
			, 'ID' signup_hostcountry	
			, SUM( COALESCE (usd_amount, 0)) total_aum 
		FROM plaung_aum 
		WHERE rank_ = 1 --AND symbol <> 'IDR'
		GROUP BY 1,2,3
---- rank AUM to get top 50 users, count account id to get total user and calculate user attribution
)	, rank_user AS (
	SELECT
		*
		, ROW_NUMBER() OVER(PARTITION BY created_at ORDER BY total_aum DESC) rank_ 
		, 1.0/ COUNT(ap_account_id) OVER(PARTITION BY created_at) user_attribution
        , 1.0 / cumulative_register register_attribution
        , CASE WHEN cumulative_verify = 0 THEN 0
                ELSE 1.0 / cumulative_verify END verify_attribution
    FROM total_aum_plaung t 
        LEFT JOIN user_base u 
        ON t.created_at = u.register_month      
	WHERE created_at >= '2022-06-01'
---- calculate cumulative attribution of user
)	, cum_attribute AS (
	SELECT 
		*
		, SUM(user_attribution) OVER(PARTITION BY created_at ORDER BY total_aum DESC) cumulative_attribution
        , SUM(register_attribution) OVER(PARTITION BY created_at ORDER BY total_aum DESC) cumulative_register_attribution
        , SUM(verify_attribution) OVER(PARTITION BY created_at ORDER BY total_aum DESC) cumulative_verify_attribution
	FROM rank_user
---- SUM AUM to get result for Investor Deck	
)
SELECT
	created_at 
	, signup_hostcountry
	, SUM( total_aum) total_aum
	, SUM( CASE WHEN rank_ <= 50 THEN total_aum END) AS top50_usd_amount
    , SUM( CASE WHEN cumulative_attribution <= 0.01 THEN total_aum END) AS top1p_usd_amount
    , SUM( CASE WHEN cumulative_attribution <= 0.001 THEN total_aum END) AS top01p_usd_amount
    , SUM( CASE WHEN cumulative_attribution > 0.001 AND cumulative_attribution <= 0.005 THEN total_aum END) AS top05p_usd_amount
	, SUM( CASE WHEN cumulative_attribution > 0.005 AND cumulative_attribution <= 0.01 THEN total_aum END) AS top05to1p_usd_amount
    , SUM( CASE WHEN cumulative_attribution > 0.01 AND cumulative_attribution <= 0.05 THEN total_aum END) AS top2to5p_usd_amount
	, SUM( CASE WHEN cumulative_attribution > 0.05 AND cumulative_attribution <= 0.1 THEN total_aum END) AS top5to10p_usd_amount
	, SUM( CASE WHEN cumulative_attribution > 0.1 AND cumulative_attribution <= 0.2 THEN total_aum END) AS top10to20p_usd_amount
	, SUM( CASE WHEN cumulative_attribution > 0.2 AND cumulative_attribution <= 0.5 THEN total_aum END) AS top20to50p_usd_amount
	, SUM( CASE WHEN cumulative_attribution > 0.5 AND cumulative_attribution <= 0.8 THEN total_aum END) AS top50to80p_usd_amount
	, SUM( CASE WHEN cumulative_attribution > 0.8 THEN total_aum END) AS top80to100p_usd_amount
--	, SUM( CASE WHEN cumulative_register_attribution <= 0.01 THEN total_aum END) AS top1p_reg_usd_amount
--	, SUM( CASE WHEN cumulative_register_attribution <= 0.001 THEN total_aum END) AS top01p_reg_usd_amount
--	, SUM( CASE WHEN cumulative_register_attribution > 0.001 AND cumulative_register_attribution <= 0.005 THEN total_aum END) AS top05p_reg_usd_amount
--	, SUM( CASE WHEN cumulative_register_attribution > 0.005 AND cumulative_register_attribution <= 0.01 THEN total_aum END) AS top1p_reg_usd_amount
--	, SUM( CASE WHEN cumulative_register_attribution > 0.01 AND cumulative_register_attribution <= 0.05 THEN total_aum END) AS top2to5p_reg_usd_amount
--	, SUM( CASE WHEN cumulative_register_attribution > 0.05 AND cumulative_register_attribution <= 0.1 THEN total_aum END) AS top5to10p_reg_usd_amount
--	, SUM( CASE WHEN cumulative_register_attribution > 0.1 AND cumulative_register_attribution <= 0.2 THEN total_aum END) AS top10to20p_reg_usd_amount
--	, SUM( CASE WHEN cumulative_register_attribution > 0.2 AND cumulative_register_attribution <= 0.5 THEN total_aum END) AS top20to50p_reg_usd_amount
--	, SUM( CASE WHEN cumulative_register_attribution > 0.5 AND cumulative_register_attribution <= 0.8 THEN total_aum END) AS top50to80p_reg_usd_amount
--	, SUM( CASE WHEN cumulative_register_attribution > 0.8 THEN total_aum END) AS top80to100p_reg_usd_amount
--	, SUM( CASE WHEN cumulative_verify_attribution <= 0.01 THEN total_aum END) AS top1p_ver_usd_amount
--	, SUM( CASE WHEN cumulative_verify_attribution <= 0.001 THEN total_aum END) AS top01p_ver_usd_amount
--	, SUM( CASE WHEN cumulative_verify_attribution > 0.001 AND cumulative_verify_attribution <= 0.005 THEN total_aum END) AS top05p_ver_usd_amount
--	, SUM( CASE WHEN cumulative_verify_attribution > 0.005 AND cumulative_verify_attribution <= 0.01 THEN total_aum END) AS top1p_ver_usd_amount
--	, SUM( CASE WHEN cumulative_verify_attribution > 0.01 AND cumulative_verify_attribution <= 0.05 THEN total_aum END) AS top2to5p_ver_usd_amount
--	, SUM( CASE WHEN cumulative_verify_attribution > 0.05 AND cumulative_verify_attribution <= 0.1 THEN total_aum END) AS top5to10p_ver_usd_amount
--	, SUM( CASE WHEN cumulative_verify_attribution > 0.1 AND cumulative_verify_attribution <= 0.2 THEN total_aum END) AS top10to20p_ver_usd_amount
--	, SUM( CASE WHEN cumulative_verify_attribution > 0.2 AND cumulative_verify_attribution <= 0.5 THEN total_aum END) AS top20to50p_ver_usd_amount
--	, SUM( CASE WHEN cumulative_verify_attribution > 0.5 AND cumulative_verify_attribution <= 0.8 THEN total_aum END) AS top50to80p_ver_usd_amount
--	, SUM( CASE WHEN cumulative_verify_attribution > 0.8 THEN total_aum END) AS top80to100p_ver_usd_amount
FROM cum_attribute
GROUP BY 1,2
;



