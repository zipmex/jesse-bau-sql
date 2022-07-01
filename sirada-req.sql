-- trade volume by order type (limit/ market)
select t.created_at::date as datadate 
	, extract(year from t.created_at) as year_
--  , extract(month from t.created_at) as month_ -- month = 1	
    , to_char(date(t.created_at), 'Mon') as datemonth -- month = Jan
	, u.user_id 
--	, t.account_id , u.account_id , t.account_id  
	, t.signup_hostcountry as sigup_country
	, o.side as order_detail
	, o.order_type 
	, u.user_segment 
--	, o.order_id 
--	, o.order_state 
	, SUM(t.quantity) as crypto_vol 
--	, o.quantity_executed as crypto_2 -- verify vol if 2 sources match
	, SUM(t.amount_base_fiat) as trade_amt_base 
--	, o.gross_value_executed as fiat_2 -- verify value if 2 sources match
	, SUM(t.amount_usd) as trade_amt_usd 
from analytics.trades_master t 
left join analytics.users_master u 
	on t.account_id = u.account_id 
left join public.orders o 
	on t.order_id = o.order_id 
where t.signup_hostcountry in ('AU','ID','TH','global') 
and date(t.created_at) >= '2020-01-01'
and o.order_type not in ('BlockTrade')
and o.order_state in ('Working', 'FullyExecuted')
and t.amount_usd is not null 
and u.account_id not in ('0', '37807', '37955', '38121', '38260', '38262', '38263', '40683', '40706')
group by 1,2,3,4,5,6,7,8,9,10,11 
order by 1 	desc 

select * from apex.user_configs uc 



select MONTH(FROM_UNIXTIME(((o.time_stamp - 621355968000000000) / 10000000)) + interval 7 hour) as month_
	, o.account_id 
-- 	, s.user_id , s.email_address 
	, Case when o.order_type = 1 then 'Market'
			when o.order_type = 2 then 'Limit'
			end as order_type 
	, case when o.side = 0 then 'Buy'
			when o.side = 1 then 'Sell'
			end as side
	, ROUND(SUM(o.executed_quantity),2) as coin_vol
	, ROUND(SUM(o.gross_value_executed),2) as fiat_vol
from apex.oms_orders o 
-- 	left join apex.users s 
-- 		on o.account_id = s.account_id 
where  o.state in ('1','5') -- s.email_verified = 1 
and DATE(FROM_UNIXTIME(((o.time_stamp - 621355968000000000) / 10000000)) + interval 7 hour) >= '2021-01-01'	
and o.account_id not in ('0', '37807', '37955', '38121', '38260', '38262', '38263', '40683', '40706')
group by 1,2,3,4



select order_id 
	, trade_id 
	, SUM(quantity)
	, SUM(value) 
from apex.oms_trades ot 
where (FROM_UNIXTIME(((time_stamp - 621355968000000000) / 10000000)) + interval 7 hour) > '2021-05-01'
group by 1,2
limit 100 

select MONTH(FROM_UNIXTIME(((time_stamp - 621355968000000000) / 10000000)) + interval 7 hour) as time_stamp_
	, DATE(FROM_UNIXTIME(((receive_time_tick - 621355968000000000) / 10000000)) + interval 7 hour) as received_time
	, FROM_UNIXTIME(((last_updated_time_ticks - 621355968000000000) / 10000000)) + interval 7 hour as last_updated
	, order_id 	, order_type 	, state 	, side  , COUNT(*) 
from apex.oms_orders 
where order_id = '452135717'
group by 1,2,3,4,5,6,7
limit 100

-- average number of ERC-20 tokens wallets held by a user
select account_id 
 	, asset_states ->> '$[*].AssetId'
-- 	, JSON_EXTRACT(asset_states ,'$.v.AssetID') asset_id 
-- 	, JSON_DEPTH(asset_states)
-- 	, JSON_LENGTH(asset_states)
-- 	, JSON_TYPE(asset_states)
 	, JSON_PRETTY(asset_states)
-- 	, count(*)
from apex.asset_manager_account_states amas 
where account_id = '38649'
-- group by 1,2,3,4
limit 10
-- where account_id = '143639' 

SELECT AssetId, Holds -- , Balances, AccountId, TotalReceived 
FROM apex.asset_manager_account_states amas ,JSON_TABLE(asset_states, "$" COLUMNS (AssetId char(100) PATH "$.AssetId", Holds char(100))) -- PATH "$.Holds", NESTED PATH "$.Balances[*]" COLUMNS (AccountId FOR ORDINALITY, TotalReceived char(20) PATH "$.TotalReceived"))) AS jst 
WHERE AccountId = '38649'