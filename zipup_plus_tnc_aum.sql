
WITH zipup_tnc AS (
	SELECT DISTINCT um.ap_account_id  
	FROM zip_up_service_tnc.acceptances a
		LEFT JOIN analytics.users_master um 
			ON a.user_id = um.user_id 
	WHERE accepted_at >= '2022/04/07 00:00:00'
)	, base AS (
    SELECT 
        a.created_at 
        , CASE WHEN u.signup_hostcountry IS NULL THEN 'unknown' ELSE u.signup_hostcountry END AS signup_hostcountry
        , a.ap_account_id 
        , pii.email , pii.mobile_number 
        , CASE WHEN a.ap_account_id = 496001 THEN TRUE ELSE FALSE END AS is_asset_manager
        , a.symbol 
        , u.zipup_subscribed_at 
        , u.is_zipup_subscribed 
        , trade_wallet_amount
        , z_wallet_amount
        , ziplock_amount
        , r.price usd_rate 
        , CASE  
            WHEN r.product_type = 1 THEN trade_wallet_amount * 1/r.price 
            WHEN r.product_type = 2 THEN trade_wallet_amount * r.price 
            END AS trade_wallet_amount_usd
        , z_wallet_amount * r.price z_wallet_amount_usd
        , ziplock_amount * r.price ziplock_amount_usd
    FROM 
        analytics.wallets_balance_eod a 
        LEFT JOIN 
        	zipup_tnc zt 
        	ON a.ap_account_id = zt.ap_account_id
        LEFT JOIN 
            analytics.users_master u 
            ON a.ap_account_id = u.ap_account_id 
	    LEFT JOIN 
	    	analytics_pii.users_pii pii
	    	ON u.user_id = pii.user_id 
        LEFT JOIN 
            analytics.rates_master r 
            ON a.symbol = r.product_1_symbol 
            AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', r.created_at)
    WHERE 
        (a.created_at = DATE_TRUNC('day', NOW()) - '2 day'::INTERVAL)
        AND u.signup_hostcountry IN ('TH')
        AND a.symbol NOT IN ('TST1','TST2')
        AND zt.ap_account_id IS NULL
--        AND u.user_id not in (select DISTINCT user_id from zip_up_service_tnc.acceptances where accepted_at >= '2022/04/07 00:00:00')
    ORDER BY 1 DESC 
)
--, aum_snapshot AS (
    SELECT 
        DATE_TRUNC('day', created_at)::DATE AS created_at
        , signup_hostcountry
        , ap_account_id
        , email, mobile_number
        , is_zipup_subscribed
        --, symbol
        --, SUM( COALESCE (trade_wallet_amount, 0)) trade_wallet_amount
        --, SUM( COALESCE (z_wallet_amount, 0)) z_wallet_amount
        --, SUM( COALESCE (ziplock_amount, 0)) ziplock_amount
        , SUM( COALESCE (trade_wallet_amount_usd, 0)) trade_wallet_amount_usd
        , SUM( COALESCE (z_wallet_amount_usd, 0)) z_wallet_amount_usd
        , SUM( COALESCE (ziplock_amount_usd, 0)) ziplock_amount_usd
        , SUM( COALESCE (CASE WHEN is_zipup_subscribed = TRUE AND created_at >= DATE_TRUNC('day', zipup_subscribed_at) AND symbol IN ('BTC', 'USDT', 'USDC', 'GOLD', 'LTC', 'ETH', 'ZMT')
                    THEN
                        (CASE   WHEN created_at <= '2021-09-02 00:00:00' THEN COALESCE (trade_wallet_amount_usd, 0) + COALESCE (z_wallet_amount_usd, 0)
                                WHEN created_at > '2021-09-02 00:00:00' THEN COALESCE (z_wallet_amount_usd, 0) END)
                    END, 0)) AS zwallet_subscribed_usd
    FROM 
        base 
    WHERE 
        is_asset_manager = FALSE 
    GROUP BY 
        1,2,3,4,5,6
    ORDER BY 
        1 
;

