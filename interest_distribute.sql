WITH
    var_account_id AS (VALUES (58183)),
    var_user_id AS (VALUES ('01ER7CRCW365BF7VPK87C09VEG'),('01F67663GD1K5PT8HE2GGMD3RM'))
    , 
query AS (
	WITH ziplock_interest AS (
	-- ziplock interest in Z wallet
		SELECT 
			id.distributed_at "date"
			, up.ap_account_id 
			, up.email 
			, UPPER(SPLIT_PART(id.product_id,'.',1)) product
			, id.amount 
			, 'ziplock' classification
		FROM 
		-- ziplock interest distribution
			zip_lock_service_public.interest_distributions id 
		-- get email and account id
			LEFT JOIN analytics_pii.users_pii up 
				ON id.user_id = up.user_id 
		WHERE 
		-- designated user id 
			id.user_id IN (TABLE var_user_id)		
		--	id.user_id IN ('01F8MXKFJ10FND0Y1R2DZN63VB','01F3N6337JVA3Z3RJA8W4PFWVF')
			-- interest paidout period
			AND distributed_at >= '2021-01-01'
			AND distributed_at < DATE_TRUNC('day', NOW())
		ORDER BY 1
	)	, zipup_interest AS (
	-- zipup interest in Z wallet
		SELECT 
			id.distributed_at "date"
			, up.ap_account_id 
			, up.email 
			, UPPER(SPLIT_PART(id.product_id,'.',1)) product
			, id.amount 
			, 'zipup' classification
		FROM 
		-- zipup interest distribution
			zip_up_service_public.interest_distributions id 
		-- get email and account id
			LEFT JOIN analytics_pii.users_pii up 
				ON id.user_id = up.user_id 
		WHERE 
		-- designated user id 
			id.user_id IN (TABLE var_user_id)		
		--	id.user_id IN ('01F8MXKFJ10FND0Y1R2DZN63VB','01F3N6337JVA3Z3RJA8W4PFWVF')
			-- interest paidout period
			AND distributed_at >= '2021-01-01' 
			AND distributed_at < DATE_TRUNC('day', NOW())
		ORDER BY 1
	)	, interest_be4 AS (
	-- zipup / ziplock interest before double wallet
		SELECT 
			created_at::DATE 
			, receiver_ap_account_id 
			, up.email 
			, product_symbol 
			, SUM(amount) interest_amount
			, 'trade_wallet' transfer_subgroup 
		FROM 
			analytics.transfers_master tm 
		-- get email and account id
			LEFT JOIN analytics_pii.users_pii up 
				ON tm.receiver_ap_account_id = up.ap_account_id 
		WHERE 
		-- designated ap_account_id 
			receiver_ap_account_id IN (TABLE var_account_id)		
		--	receiver_ap_account_id IN (324715,225488)
		-- filter interest distribution only 
			AND transfer_group = 'interest'
			-- interest paidout period
			AND tm.created_at >= '2021-01-01'
		GROUP BY 
			1,2,3,4,6
	),	z_launch_base AS (
		SELECT 
			lv.created_at::DATE 
			, account_id 
			, up.email 
			, up.ap_account_id 
			, UPPER(SPLIT_PART(product_id,'.',1)) symbol
			, SUM(credit) credit 
			, SUM(debit) debit 
		FROM 
		-- all transactions in z wallets
			asset_manager_public.ledgers_v2 lv
		-- get email and account id 
			LEFT JOIN analytics_pii.users_pii up 
				ON lv.account_id = up.user_id 
		WHERE 
		-- zlaunch transactions only 
			ref_caller = 'z_launch'
		-- zlaunch reward filter
			AND ref_action = 'distribute_reward'
		-- designated user id 
			AND account_id IN (TABLE var_user_id)
		--	AND lv.account_id IN ('01F8MXKFJ10FND0Y1R2DZN63VB','01F3N6337JVA3Z3RJA8W4PFWVF')
		GROUP BY 1,2,3,4,5
	),	z_launch_reward AS (
		SELECT 
			created_at 
			, ap_account_id 
			, email
			, symbol
			, credit - debit amount
			, 'z_launch_reward' subgroup
		FROM 
			z_launch_base
	)
	SELECT * FROM zipup_interest
		UNION ALL
	SELECT * FROM ziplock_interest
		UNION ALL
	SELECT * FROM interest_be4
--		UNION ALL
--	SELECT * FROM z_launch_reward
)
SELECT * FROM query 
ORDER BY 1
;



-- z launch rewards
WITH
    var_account_id AS (VALUES (123)),
    var_user_id AS (VALUES ('01EC47XTNXDQ33612DE4MC2XSX'))
    , 
query AS (
	WITH z_launch_reward AS (
		SELECT 
			lv.created_at::DATE 
			, account_id 
			, up.email 
			, up.ap_account_id 
			, UPPER(SPLIT_PART(product_id,'.',1)) symbol
			, SUM(credit) credit 
			, SUM(debit) debit 
		FROM 
			asset_manager_public.ledgers_v2 lv
			LEFT JOIN analytics_pii.users_pii up 
				ON lv.account_id = up.user_id 
		WHERE 
			ref_caller = 'z_launch'
			AND ref_action = 'distribute_reward'
			AND account_id IN (TABLE var_user_id)
--			AND lv.account_id IN ('01F8MXKFJ10FND0Y1R2DZN63VB','01F3N6337JVA3Z3RJA8W4PFWVF')
		GROUP BY 1,2,3,4,5
	)
	SELECT 
	--	created_at 
	--	ap_account_id 
		email
		, symbol
		, 'z_launch_reward' subgroup
		, SUM(credit) - SUM(debit) amount
	FROM 
		z_launch_reward
	GROUP BY 1,2,3
)
SELECT * FROM query
;


WITH
  var_user_id AS (VALUES('01EC47XTNXDQ33612DE4MC2XSX'))
  , zlaunch_base AS 
		(
		-- all z launch transaction (lock, unlock, released)
			select 
			--*, 
		--	 DATE_TRUNC('day', inserted_at) created_at
 			up.email
 			, UPPER(SPLIT_PART(product_id,'.',1)) symbol
 			, 'zlaunch' as program
 			, SUM(amount) amount
			FROM 
				z_launch_service_public.reward_distributions rt
			left join analytics_pii.users_pii up 
  			on rt.user_id=up.user_id
		  	WHERE
  			rt.user_id = (table var_user_id)
  		-- interest paidout period
--  		[[and inserted_at>={{from_date}}]]
  		and inserted_at <DATE_TRUNC('day',NOW())
  		GROUP BY 1,2,3
		)
		select * from zlaunch_base
;



-- airdrop / reward distribution from campaigns
WITH
    var_account_id AS (VALUES (775459),(531833)),
    var_user_id AS (VALUES ('01FNXR10VDSHZJ6TK1GZKM8JHV'),('01FCTS5GW751FCJJZR2GX1FXEW')),
query AS (
	SELECT 
		created_at::DATE 
		, receiver_ap_account_id ap_account_id
		, up.email 
		, product_symbol symbol
		, notes reward_note
		, SUM(amount) reward_amount
	FROM 
		analytics.transfers_master tm 
	-- get email and account id
		LEFT JOIN analytics_pii.users_pii up 
			ON tm.receiver_ap_account_id = up.ap_account_id 
	WHERE 
	-- designated ap_account_id 
		receiver_ap_account_id IN (TABLE var_account_id)		
	-- filter interest distribution only 
		AND transfer_subgroup IN ('other')
		-- interest paidout period
		AND tm.created_at >= '2021-01-01'
	GROUP BY 
		1,2,3,4,5
)
SELECT * FROM query 
;





