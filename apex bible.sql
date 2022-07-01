apex.oms_orders 
apex.oms_trades

TRADE_ID IS INDEXED SO FASTER TO FILTER BY TRADE_ID 

state: 	1 = Working
		2 = Rejected
		3 = Canceled
		5 = FullyExecuted
		
side: 	1 = Sell
		0 = Buy
		
order_type:
		1 = Market
		2 = Limit
		4 = StopLimit
		7 = BlockTrade
		
convert time_sticks in apex (637563414221078007 is not EPOCH) to datetime_ and hours:
 FROM_UNIXTIME(((*time_stick* - 621355968000000000) / 10000000)) -- GMT 0
 to GMT +7 (BKK time_):
  DATE(FROM_UNIXTIME(((*time_stick* - 621355968000000000) / 10000000)) + interval 7 hour)
  MONTH(FROM_UNIXTIME(((*time_stick* - 621355968000000000) / 10000000)) + interval 7 hour)

  637554240000000000 = '2021-05-01 00:00:00'
  apex.instruments :: product symbol/ product id 

SELECT tick_to_timestamp(637694511999990000)


FROM_UNIXTIME((637554240000000000 - 621355968000000000) / 10000000) 

  
SELECT order_type 
	, count(*)
FROM apex.oms_trades ot 
group by 1 
limit 10

  
select config_value 
	, count(*)
	from apex.user_configs c 
	where config_id = 'signupHostname'
	group by 1

with trades_sum as (
select t.account_id
	, DATE(FROM_UNIXTIME(((t.trade_time - 621355968000000000) / 10000000)) + interval 7 hour) trade_time 
	, DATE(FROM_UNIXTIME(((t.time_stamp - 621355968000000000) / 10000000)) + interval 7 hour) time_stamp 
	, t.trade_id
	, i.product_1_id 
	, i.symbol 
	, t.order_id
	, t.price
	, t.quantity 
	, t.value 
	, case when t.order_type = 1 then 'Market' 
			when t.order_type = 2 then 'Limit'
			end as order_type 
	, case when t.side = 0 then 'Buy'
			when t.side = 1 then 'Sell'
			end as side 
from apex.oms_trades t 
left join apex.instruments i on t.instrument_id = i.instrument_id and i.is_disabled = 0 
left join apex.oms_orders_states s on t.order_id = s.order_id 
where t.is_block_trade = 0 
and t.account_id not in (0, 27308,48870,48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63312, 63313, 161347)
and s.state = 5
-- and i.product_1_id IN (16,50)
), user_country AS 
				(	SELECT user_id
				, CASE WHEN config_id = 'signupHostname' 
				THEN (CASE WHEN config_value IN ('sg.zipmex.com', 'exchange.zipmex.com', 'trade.zipmex.com') THEN 'global'
							WHEN config_value IN ('au.zipmex.com','trade.zipmex.com.au') THEN 'AU'
							WHEN config_value IN ('id.zipmex.com','trade.zipmex.com.id') THEN 'ID'
							WHEN config_value IN ('th.zipmex.com','trade.zipmex.co.th') THEN 'TH'
							WHEN config_value IN ('trade.xbullion.io') THEN 'xbullion'
							WHEN config_value IN ('global-staging.zipmex.com', 'localhost') THEN 'test'
							ELSE 'error' END) 
							END AS signup_hostcountry
				FROM apex.user_configs )
select t.account_id 
	, u.email_address 
	, c.signup_hostcountry 
	, CASE WHEN u.user_id IN (1368, 1371, 1430, 2913, 13264, 22579) THEN '0_resellers'
			WHEN u.user_id IN (27448, 37813, 37961, 38127, 38266, 38268, 38269, 40690, 40713, 44062) THEN '0_market_maker'
			WHEN u.user_id IN (9, 12, 13, 24, 219, 243, 701, 865, 1286, 5882, 6072, 6876, 8112, 11043, 12017, 37281, 38390, 38407, 39837) THEN '0_zipmex_staff'
			ELSE NULL
			END "user_segment"	
	, t.trade_time 
 	, t.product_1_id , t.symbol -- , t.order_id 
	, t.order_type 
	, t.side 
	, ROUND(SUM(t.quantity),2) quantity 
	, ROUND(SUM(t.value),2) quantity 	
from trades_sum t 
LEFT JOIN apex.users u ON t.account_id = u.account_id 
LEFT JOIN user_country c ON u.user_id = c.user_id  
where trade_time >= '2021-05-01'
AND c.signup_hostcountry IN ('AU','TH','ID','global')
group by 1,2,3,4,5,6,7,8,9 



--- JOSN TABLE
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

-- trade id 
select * 
FROM apex.oms_trades t 
where trade_id >= 12984877 
-- 12798278 -- 15298308 -- 14798308 -- 14298308 -- 13798308 -- 13298308 -- 12798308 -- may 1
and trade_id < 13028815 
-- 12798308 -- 15798308 -- 15298308 -- 14798308 -- 14298308 -- 13798308 -- 13298308 -- 13733972 may 9
and t.is_block_trade = 0 
and t.account_id not in (0, 27308,48870,48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63312, 63313, 161347)
-- and t.remaining_quantity = 0 
-- and order_id = 559804770 
-- order by trade_id 


select 12798308 + 500000
	, 12798308 + 1000000
	, 12798308 + 1500000
	, 12798308 + 2000000
	, 12798308 + 2500000
	, 12798308 + 3000000
	
	
SELECT *
FROM apex.users u 
WHERE email_address LIKE 'lecongthinh%'