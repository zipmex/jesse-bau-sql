--ACQUISITION TEMPLATE
-----------------------------------------------------------------------------------------------------------------------------BASE QUERY -------------------------------------------------------------------------------------------------------------
--NOTE:
--$1 = IDR 14500 
--$1 = THB 33
--$1 = AUD 1.4
--$1 = SGD 1.37

SELECT 
	rm.created_at::DATE 
	, product_1_symbol 
	, product_2_symbol 
	, price 
FROM analytics.rates_master rm 
WHERE product_type = 1
ORDER BY 1 DESC 

WITH 
"user_" AS ------------------------------------------------------------------------------------------------------------------------------USER FUNNEL
(
	SELECT 
		u.ap_account_id 
		,a.ap_account_id "referring_account_id"
		,u.user_id
		,u.referring_user_id
		,b.email 
		,c.email "referring_email"
		,u.signup_hostcountry 
		,CASE WHEN u.created_at + INTERVAL '7h' >= '2021-12-01' THEN 'new_user' ELSE 'existing_user 'END "user_status" ------------------CHANGE THE INTERVAL
		,u.is_verified 
		,CASE 	
			WHEN c.email LIKE ('%campaigns%') THEN 'campaign' 
			WHEN c.email IS NULL THEN 'organic'
			ELSE 'friend_referral' 
	 	 END "reff_source"
	 	,d.source_raw 
		,u.invitation_code 
		,date(u.created_at + interval '7h') "register_date"
		,date(u.verification_approved_at + INTERVAL '7h') "verified_at"
	FROM 
		analytics.users_master u
	LEFT JOIN 
	    analytics_pii.users_pii b ON u.user_id = b.user_id 
	LEFT JOIN 
	    	analytics.users_master a ON a.user_id = u.referring_user_id
	LEFT JOIN 
	    analytics_pii.users_pii c ON a.user_id = c.user_id 
	LEFT JOIN 
		analytics.registration_channel_master d ON u.user_id = d.user_id 
	WHERE 
	   	u.signup_hostcountry = 'TH'   -------------------CHANGE DESIRED COUNTRY (ID,TH,Global,AU)
	AND 
	    date(u.created_at + interval '7h') >= '2021-12-01' AND date(u.created_at + interval '7h') <= '2021-12-31' ----------INSERT DATE OF CAMPAIGN AND -+ THE INTERVAL
--	AND 
--	    u.invitation_code = 'REVOLUTION' ---------------------------INSERT invitation code IF ANY
)
, "deposit_" AS -------------------------------FIRST DEPOSIT METRIC
(
	SELECT
		 date_trunc('day',a.created_at + interval '7h')::DATE "first_deposit_at" 
		,ROW_NUMBER () OVER (PARTITION BY a.ap_account_id order by date(a.created_at + INTERVAL '7h') ) AS "occurance" 
		,a.ap_account_id 
		,round(a.amount_usd) "deposit_usd"
    FROM 
    	analytics.deposit_tickets_master a
    WHERE 
    	date(a.created_at + interval '7h') >= '2021-12-01' AND date(a.created_at + interval '7h') <= '2021-12-31'
--    AND 
--    	a.product_symbol = 'IDR' -----------------------CHANGE INTO DESIRED DEPOSIT SYMBOL
--    AND 
--      a.amount_usd >= 50000 -----------------------CHANGE THIS IF THERE IS LIMIT AMOUNT OF DEPOSIT
    order by 1 ASC
 )
 , "first_deposit" AS 
 (
 	SELECT
    	a.*
 	FROM
    	deposit_ a 
	WHERE 
    	a.occurance = 1 
)
 , "deposit_master" AS 
(
	SELECT
		a.ap_account_id 
		,max(date(a.created_at + interval '7h')):: DATE "last_deposit"
		,round(sum(a.amount_usd)) "deposit_usd"
    FROM 
    	analytics.deposit_tickets_master a
    WHERE 
   		date(a.created_at + interval '7h') >= '2021-12-01' AND date(a.created_at + interval '7h') <= '2021-12-31'
    GROUP BY 1
    ORDER BY 2 ASC
 )
 , trade_ AS --------------TRADE ELIGIBILITY METRIC
(
	SELECT 
	    date_trunc('day',a.created_at + interval '7h') ::DATE "tday"
		,a.ap_account_id 
		,ROW_NUMBER () OVER (PARTITION BY a.ap_account_id order by date(a.created_at + INTERVAL '7h') ) AS "occurance" 
		,round(a.amount_usd) "trade_amount"
    FROM 
    	analytics.trades_master a
    WHERE 
        round(a.amount_usd) >= 499.9 ----------------------MINIMUM REQUIREMENT OF TRADING 
    AND 
      	date(a.created_at + interval '7h') >= '2021-12-01' AND date(a.created_at + interval '7h') <= '2021-12-31' 
    ORDER BY 1 ASC 
)
, "trade_req" as 
(
    SELECT
        a.tday "pass_time"
        ,a.ap_account_id
        ,a.trade_amount
        ,TRUE "trade_req"
    FROM 
        trade_ a 
    WHERE 
        a.occurance = 1
)
, trade_master  AS ---------------------------------------------------------------------------------------------------------------TRADE VOLUME METRIC
(
	SELECT 
		a.ap_account_id 
		,count(a.order_id) "t_frequency"
		,round(sum(a.amount_usd)) "trade_volume_usd" 
		,ARRAY_AGG(DISTINCT a.product_1_symbol ORDER BY a.product_1_symbol) AS traded_symbol 
	FROM
		analytics.trades_master a
	WHERE 
		date(a.created_at + interval '7h') >= '2021-12-01' AND date(a.created_at + interval '7h') <= '2021-12-31'
	GROUP BY 
		1
)
 , "withdraw_master" AS -----------------------------------------------------------------------------------------------------------WITHDRAW METRIC
(
	SELECT
		a.ap_account_id 
		,max(date(a.created_at + interval '7h')):: DATE "last_withdraw"
		,round(sum(a.amount_usd)) "withdraw_usd"
    FROM 
    	analytics.withdraw_tickets_master a
    WHERE 
   		date(a.created_at + interval '7h') >= '2021-12-01' AND date(a.created_at + interval '7h') <= '2021-12-31'
    GROUP BY 1
    ORDER BY 2 ASC
 )
, "lock_master" AS ---------------------------------------------------------------------------------------------------------------USER LOCK METRIC
 (
	SELECT 
		date(locked_at + INTERVAL '7h') "locked_at" 
		,a.user_id 
		,CASE WHEN product_id IN ('usdc.global','usdc.th','usdt.global','usdt.th') 
	 	THEN UPPER(LEFT(a.product_id,4)) 
	 	ELSE UPPER(LEFT(a.product_id,3)) 
	 	END "symbol" 
		,a.amount 
	,ROW_NUMBER () OVER (PARTITION BY a.user_id order by date(a.locked_at + INTERVAL '7h') ) AS "occurance" 
	FROM 
		zip_lock_service_public.lock_transactions a
	WHERE 
		a.status = 'completed'
--	AND 
--		a.user_id = '01ERBV3VEV8278JAPSADVM68VG'
	ORDER BY 2,1 ASC 
)
, "first_lock" AS 
(
	SELECT 
		 a.locked_at 
		,a.user_id 
		,a.symbol
		,a.amount "quantity"
		,b.price 
		,a.amount * b.price "amount_usd"
	FROM 
		lock_master a 
	LEFT JOIN 
		analytics.rates_master b 
			ON a.locked_at = date_trunc('day', b.created_at + INTERVAL '7h')::DATE
			AND a.symbol = b.product_1_symbol 
	WHERE 
		a.occurance = 1
	ORDER BY 2,1 ASC 
---------------------------------COMBINE ALL QUERY ----------------------------
)	
, "output_list" AS 
(
	SELECT
		 a.ap_account_id 
		,a.referring_account_id
		,a.user_id 
		,a.referring_user_id 
		,a.email 
		,a.referring_email
		,a.invitation_code 
		,a.signup_hostcountry 
		,a.is_verified 
		,a.user_status
		,a.reff_source
		,a.source_raw
		,f.trade_req
		,b.deposit_usd "first_deposit_amount"
		,c.deposit_usd "total_deposit"
		,d.withdraw_usd "total_withdraw"
		,g.trade_volume_usd
		,e.symbol "first_lock_symbol"
		,e.amount_usd "locked_amount"
		,a.register_date
		,a.verified_at
		,b.first_deposit_at
		,f.pass_time
		,e.locked_at
	FROM 
		user_ a 
	LEFT JOIN 
		first_deposit b ON a.ap_account_id = b.ap_account_id 
	LEFT JOIN 
		deposit_master c ON a.ap_account_id = c.ap_account_id 
	LEFT JOIN 
		withdraw_master d ON a.ap_account_id = d.ap_account_id
	LEFT JOIN 
		first_lock e ON a.user_id = e.user_id 
	LEFT JOIN 
		trade_req f ON a.ap_account_id = f.ap_account_id 
	LEFT JOIN 
		trade_master g ON a.ap_account_id = g.ap_account_id
)
--------------------------------------------------FINAL OUTPUT------------------------------------
SELECT
    a.*
FROM
    output_list a 
;

