--Top 100 ZMT sellers past 14 days; no. of zmt sales, no. of zmt buy, zmt balance, name, e-mail address, phone number --> I want to run (1) campaign/news targeting this group
with balance_ as
(
select *
	, row_number() over(partition by account_id order by created_at desc) as rank_
from public.accounts_positions_daily a
where product_id in ('16','50')
)
select  u.first_name 
	, u.last_name 
	, u.account_id 
	, u.email 
	, u.mobile_number 
--	, t.side 
	, b.amount as zmt_balance
	, SUM(round(t.quantity,1)) as zmt_sell 
	, c.zmt_buy
from analytics.trades_master t
	left join analytics.users_master u on t.ap_account_id = u.account_id
	left join balance_ b on t.ap_account_id = b.account_id and b.rank_ = 1
	join (select account_id , sum(round(quantity,1)) as zmt_buy from analytics.trades_master t 
	where side = 'Buy' and product_1_id in ('16','50') and created_at between NOW()::DATE-EXTRACT(DOW FROM NOW())::INTEGER-14 AND NOW() group by 1) c 
	on t.ap_account_id = c.account_id 
where t.product_1_id in ('16','50')
--and date(t.created_at) >= '2021-04-26' and date(t.created_at) <= NOW()
and t.side = 'Sell'
and t.created_at between NOW()::DATE-EXTRACT(DOW FROM NOW())::INTEGER-14 AND NOW()
group by 1,2,3,4,5,6,8
order by 7 desc limit 100

----------- extract the raw data of our users (passed KYC) -Name -E-mail address -Age / Age group -Date of Birth -Occupation -Gender -Present address -Registered address
--Trade volume (as of 9 May) = sell + buy in crypto
-- ZMT stacked volume (as of 9 May) zmt staked (lock) 
-- AUM (as of 9 May)
with "date_series" AS
(
	SELECT
		DISTINCT
		 date as date_
		,u.user_id
	FROM 
		GENERATE_SERIES('2020-01-01'::DATE, NOW()::DATE, '1 day') "date"
	CROSS JOIN
		(SELECT DISTINCT user_id FROM oms_data.user_app_public.zip_crew_stakes) u
	ORDER BY
		1 ASC
), zmt_staked as 
(
SELECT
	d.date_
	,u.account_id
	,u.user_id
	,u.email
	,SUM(s.amount) "zmt_staked_amount"
	,SUM(s.amount* c.price) "zmt_staked_usd_amount"
FROM
	date_series d
LEFT JOIN
	oms_data.user_app_public.zip_crew_stakes s
	ON d.user_id = s.user_id
	AND d.date_ >= DATE_TRUNC('day', s.staked_at)
	AND d.date_ < COALESCE(DATE_TRUNC('day', s.released_at), NOW())
LEFT JOIN
	oms_data.analytics.users_master u
	ON s.user_id = u.user_id
LEFT JOIN
	oms_data.mysql_replica_apex.products p
	ON s.product_id = p.product_id
-- join crypto usd prices
LEFT JOIN
	oms_data.public.prices_eod_gmt0 c
	ON p.symbol = c.product_1_symbol
	AND c.product_2_symbol = 'USD'
	AND d.date_ = DATE_TRUNC('day', c.actual_timestamp)
	AND p."type" = 2
WHERE
	u.account_id IS NOT null 
GROUP by 1, 2, 3,4
), aum_eod as 
(
select --a.balanced_at::date as datadate
	DATE_TRUNC('day', a.balanced_at) datadate --+ INTERVAL '1 MONTH - 1 day' "month"
	,a.account_id
	,u.email
	,u.user_id 
	,COALESCE(SUM(ROUND(CASE 	WHEN p.product_id = 6 THEN a.total_balance * 1
					WHEN p.type = 2 THEN a.total_balance * c.price
					WHEN p.type = 1 THEN a.total_balance / e.exchange_rate
					ELSE 0
	END, 2)), 0) "usd_amount"
FROM
	oms_data.data_imports.account_balance_eod_gmt0 a
LEFT JOIN
	oms_data.mysql_replica_apex.products p
	ON a.product_id = p.product_id
LEFT JOIN
	oms_data.analytics.users_master	u
	ON a.account_id = u.account_id
-- join crypto usd prices
LEFT JOIN
	oms_data.public.prices_eod_gmt0 c
	ON p.symbol = c.product_1_symbol
	AND c.product_2_symbol = 'USD'
	-- if you want rate from specific date replace DATE_TRUNC('day', a.balanced_at)
	AND DATE_TRUNC('day', a.balanced_at) = DATE_TRUNC('day', c.actual_timestamp)
	AND p."type" = 2
LEFT JOIN
	oms_data.public.exchange_rates e
	ON p.symbol = e.product_2_symbol
	AND e.product_1_symbol = 'USD'
	-- if you want rate from specific date replace DATE_TRUNC('day', a.balanced_at)
	AND DATE_TRUNC('day', a.balanced_at) = DATE_TRUNC('day', e.created_at)
	AND e."source" = 'coinmarketcap'
	AND p."type" = 1
where 
	 u.signup_hostcountry IN ('TH','AU','ID','global')
	AND a.account_id NOT IN (0 , 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63312, 63313, 161347)
GROUP BY
	1, 2, 3,4
ORDER BY
	1 desc
), aum_tot as 
(
select z.date_
	, z.account_id
	, z.user_id
	, a.email 
	, z.zmt_staked_amount
	, z.zmt_staked_usd_amount
	, a.usd_amount
	, SUM(z.zmt_staked_usd_amount + a.usd_amount) as aum_amount
from zmt_staked z 
	left join aum_eod a 
	on z.account_id = a.account_id 
	and z.date_ = a.datadate 
--where z.date_ = '2021-05-09'
group by 1,2,3,4,5,6,7
)
, user_info as (
select u.account_id 
	, u.user_id 
	, u.first_name 
	, u.last_name 
	, u.email 
	, u.age 
	, u.dob::date  
	, u.gender 
	, s.survey ->> 'total_estimate_monthly_income' income_level
	, CASE WHEN p.info ->> 'occupation' IN ('ค้าขายทองคำ',	'ค้าขายจิวเวลรี',	'ค้าขายเพชร พลอย ทอง นาก เงิน หรือ อัญมณี เก่า',	'ค้าขายวัตถุโบราณ / ศิลปวัตถุ',	'ค้าขาย พระเครื่อง พระบูชา เก่า',	'ค้าขาย แสตมป์ เหรียญ ธนบัตร เก่า',	'ค้าขายนาฬิกา / เครื่องหนัง มือสอง',	'ค้าขาย รถยนต์ รถจักรยานยนต์ มือสอง',	'ค้าขายชิ้นส่วนอะไหล่รถยนต์ รถจักรยานยนต์ ยางรถยนต์และรถจักรยานยนต์ ที่ใช้แล้ว',	'ค้าขายกระดาษ พลาสติก เศษเหล็ก ขวด สเตนเลส ที่ใช้แล้ว',	'ค้าขาย โทรศัพท์เคลื่อนที่ / กล้องถ่ายรูป / เครื่องดนตรี / เครื่องเสียง / เครื่องใช้ไฟฟ้า ที่ใช้แล้ว',	'ค้าขายไม้เรือนเก่า',	'ค้าขายเครื่องใช้สำนักงาน / เฟอร์นิเจอร์ ที่ใช้แล้ว',	'โรงรับจำนำ / ค้าขายของหลุดจำนำ',	'ค้าขาย จักรเย็บผ้า / เครื่องจักร เก่า',	'รับแลกเปลี่ยนเงินตราต่างประเทศ',	'ให้บริการโอนและรับโอนเงิน ทั้งในประเทศและต่างประเทศ',	'ธุรกิจบ่อนคาสิโน หรือบ่อนการพนัน',	'ผับ / บาร์ / คาราโอเกะ / เธค / ร้านเหล้า',	'สถานอาบ อบ นวด / เล้าจน์',	'ค้าขายอาวุธปืน / อาวุธที่ใช้ในการศึกสงคราม / หรือยุทธภัณฑ์อื่น ๆ',	'นายหน้าจัดหางาน ทั้งรับคนเข้ามาทำงาน และส่งคนออกไปทำงานต่างประเทศ',	'ธุรกิจนำเที่ยว / บริษัททัวร์',	'ส.ส. / ส.ว. / คณะรัฐบาล',	'นายกเทศมนตรี / รองนายกฯ / ที่ปรึกษาหรือเลขานุการนายกฯ / ปลัด  / สมาชิกสภา องค์การบริหารส่วนจังหวัด',	'นายกเทศมนตรี / รองนายกฯ / ที่ปรึกษาหรือเลขานุการนายกฯ / ปลัด / สมาชิกสภา  เทศบาลเมือง') THEN p.info ->> 'occupation' ELSE 'Other' END AS occupation
	, COALESCE(p.info ->> 'permanent_address',p.info ->> 'address_in_id_card',p.info ->> 'present_address',p.info ->> 'work_address','0') AS reg_address
	, COALESCE(p.info ->> 'permanent_address_province',p.info ->> 'address_in_id_card_province',p.info ->> 'present_address_province',p.info ->> 'work_address_province','0') AS reg_province
	, COALESCE(p.info ->> 'permanent_address_postal_code',p.info ->> 'address_in_id_card_postal_code',p.info ->> 'present_address_postal_code',p.info ->> 'work_address_postal_code','0') AS post_code
from analytics.users_master u 
	left join user_app_public.personal_infos p 
		on u.user_id = p.user_id 
		and p.archived_at is null 
	left join user_app_public.suitability_surveys s 
		on u.user_id = s.user_id 
		and s.archived_at is null 
where u.is_verified = true 
and u.signup_hostcountry in ('TH')
and u.account_id not in ('0', '37807', '37955', '38121', '38260', '38262', '38263', '40683', '40706')
)
select u.first_name 
	, u.last_name 
	, u.email 
	, u.age 
	, u.dob , u.occupation , u.gender , u.income_level, u.reg_address, u.reg_province , u.post_code 
	, t.product_1_symbol 
	, SUM(t.quantity) as trade_vol
	, ROUND(CAST(a.zmt_staked_amount AS INT),2) AS zmt_staked_amount  
	, ROUND(CAST(a.aum_amount AS INT),2) AS aum_usd  
from user_info u
	left join analytics.trades_master t
		on u.account_id = t.ap_account_id 
		and t.created_at < '2021-05-10'
	left join aum_tot a 
		on u.account_id = a.account_id 
		and a.date_ = '2021-05-09'
--where u.email in ('lecongthinh.255@gmail.com','wdamekader@gmail.com')
group by 1,2,3,4,5,6,7,8,9,10,11,12,14,15 



-------- ZIP CREW TIER v.2 ---- by zip staked balance end of month -----------------
WITH daily_user_balance AS ( ----- AUM FROM trade wallet 
SELECT created_at, account_id , sum(zmt_amount) AS zmt_amount , sum(zmt_usd_amount) AS zmt_usd_amount, sum(non_zmt_usd_amount) AS non_zmt_usd, avg(price) AS zmt_usd
FROM (
	SELECT date_trunc('day',a.created_at) AS created_at ,a.account_id , a.product_id, p.symbol
		, amount , c.average_high_low , g.mid_price , z.price, 1/e.exchange_rate as exchange_rate
		, SUM( CASE WHEN a.product_id IN (16,50) THEN a.amount END) zmt_amount , SUM( CASE WHEN a.product_id NOT IN (16,50) THEN a.amount END) non_zmt_amount 
		, SUM( CASE WHEN a.product_id IN (16,50) THEN a.amount * COALESCE(c.average_high_low, g.mid_price, z.price, 1/e.exchange_rate) END) zmt_usd_amount
		, SUM( CASE WHEN a.product_id NOT IN (16,50) THEN a.amount * COALESCE(c.average_high_low, g.mid_price, z.price, 1/e.exchange_rate) END) non_zmt_usd_amount 
	FROM oms_data.public.accounts_positions_daily a
		LEFT JOIN oms_data.mysql_replica_apex.products p
			ON a.product_id = p.product_id
		LEFT JOIN oms_data.public.cryptocurrency_prices c
		    ON CONCAT(p.symbol, 'USD') = c.instrument_symbol
		    AND DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', c.created_at)
		LEFT JOIN oms_data.public.daily_closing_gold_prices g
			ON DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', g.created_at)
			AND a.product_id IN (15, 35)
		LEFT JOIN oms_data.public.daily_ap_prices z
			ON DATE_TRUNC('day', a.created_at) = DATE_TRUNC('day', z.created_at)
			AND z.instrument_symbol  = 'ZMTUSD'
			AND a.product_id in (16, 50)
		LEFT JOIN public.exchange_rates e
			ON date_trunc('day', e.created_at) = date_trunc('day', a.created_at)
			AND e.product_2_symbol  = p.symbol
			AND e.source = 'coinmarketcap'
	WHERE a.created_at >='2019-01-01 00:00:00' AND a.created_at < NOW()::date  --<<<<<<<<CHANGE DATE HERE
	AND a.account_id NOT IN (0, 27308, 48870, 48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63312, 63313, 161347)
	AND a.product_id IN (1, 2, 3, 14, 15, 25, 26, 27, 30, 33, 34, 35, 16, 50) 
	GROUP BY 1,2,3,4,5,6,7,8,9 
	ORDER BY 1 DESC 
	) a
GROUP BY 1,2
), aum_bom AS (  
---- AUM BY beginning OF the month
	SELECT *
	FROM daily_user_balance 
	WHERE created_at = DATE_TRUNC('month',NOW()) -- beginning OF month
	),monthly_user_balance AS (
		SELECT date_trunc ('month', created_at) created_at
		, account_id 
		, COUNT(account_id) account_id_c
		, COALESCE(SUM(non_zmt_usd),0) + COALESCE(SUM(zmt_usd_amount),0) usd_amount
		, AVG(zmt_usd) AS zmt_usd 
		FROM daily_user_balance
		GROUP BY 1,2
	),asset_holding AS ( 
	--calculating balance by end of month or MTD
		SELECT a.created_at , a.account_id 
		, COALESCE(y.non_zmt_usd,0) + COALESCE(y.zmt_usd_amount,0) aum_balance
		, y.zmt_amount , y.non_zmt_usd aum_no_zmt
		, sum(l1y.account_id_c) account_id_c_l1y
		, sum(l1y.usd_amount) usd_amount_l1y
		, sum(l1y.usd_amount) / sum(l1y.account_id_c) yearly_balance 
		FROM monthly_user_balance a
		LEFT JOIN monthly_user_balance l1y ON l1y.account_id = a.account_id
			AND l1y.created_at <a.created_at
			AND l1y.created_at >= a.created_at - interval '1 year'
		LEFT JOIN aum_bom y ON a.account_id = y.account_id  
		WHERE a.created_at = date_trunc('month', now()) -- beginning of month 
		GROUP BY 1,2,3,4,5
		), staked_bom AS ( 
		----- zmt staked ON 1st OF the MONTH 
			SELECT
				d.month ,u.ap_account_id account_id ,u.signup_hostcountry
				,SUM(s.amount) "zmt_staked_amount"
				,SUM(s.amount* c.price) "zmt_staked_usd_amount"
			FROM (
				SELECT DISTINCT date(DATE_TRUNC('month', date)) "month"
					,u.user_id
				FROM  GENERATE_SERIES('2020-12-01'::DATE, NOW()::DATE, '1 month') "date"
				CROSS JOIN (SELECT DISTINCT user_id FROM oms_data.user_app_public.zip_crew_stakes) u
				ORDER BY 1 ASC
				) d --date_series
			LEFT JOIN oms_data.user_app_public.zip_crew_stakes s
				ON d.user_id = s.user_id
				AND DATE_TRUNC('day', d.month) >= DATE_TRUNC('day', s.staked_at)
				AND DATE_TRUNC('day', d.month) < COALESCE(DATE_TRUNC('day', s.released_at), NOW()) 
			LEFT JOIN oms_data.analytics.users_master u
				ON s.user_id = u.user_id
			LEFT JOIN oms_data.mysql_replica_apex.products p
				ON s.product_id = p.product_id
			-- join crypto usd prices
			LEFT JOIN oms_data.public.prices_eod_gmt0 c
				ON p.symbol = c.product_1_symbol
				AND c.product_2_symbol = 'USD'
				AND d.month = DATE_TRUNC('day', c.actual_timestamp)
				AND p."type" = 2
			WHERE u.ap_account_id IS NOT NULL 
			AND d.month = DATE_TRUNC('month', NOW()) ----<<<<<< 1st OF CURRENT MONTH 
--			AND u.is_zipup_subscribed = TRUE -- zipup users only 
--			AND d.month >= date_trunc('month', u.zipup_subscribed_at) -- zip lock balance starting after subcribed to zipup
			AND u.ap_account_id NOT IN (0, 27308,48870,48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63611, 63312, 63313, 161347)
			AND u.signup_hostcountry IN ('TH','ID','AU','global')
			GROUP BY 1,2,3
		), base AS ( 
		---- base for calculate released ZMT
		SELECT DATE_TRUNC('month',s.staked_at) AS eligible_month 
			, coalesce(DATE_TRUNC('month',s.released_at), DATE_TRUNC('month',s.releasing_at)) as crew_expired_month 
			, DATE_TRUNC('month',s.released_at) released_month 
		--	, DATE_TRUNC('week',s.released_at) as already_released_date  
			, u.ap_account_id account_id 
			, SUM(s.amount) AS zmt_amount 
			, c.price -- zmt price as of today 
			, SUM(s.amount * c.price) AS zmt_usd_amount 
		FROM oms_data.user_app_public.zip_crew_stakes s 
			LEFT JOIN analytics.users_master u ON s.user_id = u.user_id 
			LEFT JOIN oms_data.mysql_replica_apex.products p ON s.product_id = p.product_id
			LEFT JOIN oms_data.public.prices_eod_gmt0 c -- join crypto usd prices
			ON p.symbol = c.product_1_symbol
			AND c.product_2_symbol = 'USD'
			AND date_trunc('month',NOW()) = DATE_TRUNC('day', c.actual_timestamp)
			AND p."type" = 2
		WHERE s.staked_at <= DATE_TRUNC('month', NOW()) ----- total zmt staked AS OF beginning OF the month
		GROUP BY 1,2,3,4,6
		ORDER BY 4,1 
		), staked_cum AS ( 
		---- cumulative ZMT stake
		SELECT b.account_id
			, SUM(b.zmt_amount) total_zmt_staked 
			, b.price AS zmtusd_rate 
			, SUM(b.zmt_usd_amount) total_zmt_staked_usd 
			, s.zmt_staked_amount zmt_staked_balance
			, s.zmt_staked_usd_amount zmt_staked_usd_balance
		FROM base b 
		LEFT JOIN staked_bom s ON b.account_id = s.account_id 
		GROUP BY 1,3,5,6
		ORDER BY 2,1
	), zmt_released AS ( 
	---- ZMT release for this month, next month, after next month
		SELECT account_id 
			, SUM(CASE WHEN crew_expired_month = date_trunc('month',now()) THEN zmt_amount END) AS zmt_releasing_this_month
			, SUM(CASE WHEN crew_expired_month = date_trunc('month',now()) + interval '1 month' THEN zmt_amount END) AS zmt_releasing_next_month
			, SUM(CASE WHEN crew_expired_month = date_trunc('month',now()) + interval '2 month' THEN zmt_amount END) AS zmt_releasing_after_next_month
		FROM base 
		GROUP BY 1
		), weekly_trade_summary AS (
		----- trade volume by weekly, last month, last 3 month, last 1 year 
			SELECT
				t.ap_account_id 
--				, DATE_TRUNC('month', t.created_at) "month"
--				, COALESCE(SUM(t.amount_usd), 0) "mtd_trade_usd" 
--				, SUM(CASE WHEN t.side = 'Buy' THEN amount_usd END) AS buy_vol 
				, COALESCE(m1.buy_vol,0) l1month_buy_vol
				, COALESCE(w.wtd_trade_usd, 0) wtd_trade_usd 
				, COALESCE(w1.lweek_trade_usd, 0) lweek_trade_usd 
				, COALESCE(m1.l1month_trade_usd, 0) l1month_trade_usd 
				, COALESCE(q.l3m_trade_usd, 0) l3m_trade_usd 
				, COALESCE(y.l365_trade_usd, 0) l365_trade_usd 
				, COUNT(t.trade_id) count_trade 
			FROM oms_data.analytics.trades_master t
			LEFT JOIN ( SELECT ap_account_id ,  COALESCE(SUM(amount_usd), 0) "l1month_trade_usd"
						, SUM(CASE WHEN side = 'Buy' THEN amount_usd END) AS buy_vol
						FROM analytics.trades_master 
						WHERE date_trunc('month',created_at) = date_trunc('month', now()) - '1 month'::interval -- LAST 2 MONTH 
						GROUP BY 1) m1  
						on t.ap_account_id = m1.ap_account_id
		LEFT JOIN ( SELECT ap_account_id ,  COALESCE(SUM(amount_usd), 0) "l3m_trade_usd"
						, SUM(CASE WHEN side = 'Buy' THEN amount_usd END) AS buy_vol
						FROM analytics.trades_master 
						WHERE date_trunc('month',created_at) >= date_trunc('month', now()) - '3 month'::interval -- LAST 3 MONTH 
						AND date_trunc('month',created_at) < date_trunc('month', now()) -- LAST MONTH 
						GROUP BY 1) q  
						on t.ap_account_id = q.ap_account_id
			LEFT JOIN ( SELECT ap_account_id ,  COALESCE(SUM(amount_usd), 0) "l365_trade_usd"
						, SUM(CASE WHEN side = 'Buy' THEN amount_usd END) AS buy_vol
						FROM analytics.trades_master 
						WHERE date_trunc('month',created_at) >= date_trunc('month', now()) - '12 month'::interval -- LAST 12 MONTH 
						AND date_trunc('month',created_at) < date_trunc('month', now()) -- LAST MONTH 
						GROUP BY 1) y   
						on t.ap_account_id = y.ap_account_id
			LEFT JOIN ( SELECT ap_account_id ,  COALESCE(SUM(amount_usd), 0) "wtd_trade_usd"
						FROM analytics.trades_master 
						WHERE date_trunc('day',created_at) >= date_trunc('month', now()) - '6 day'::interval -- week 4 OF LAST MONTH 
						AND date_trunc('day',created_at) <= date_trunc('month', now()) 
						GROUP BY 1) w 
						ON t.ap_account_id = w.ap_account_id
			LEFT JOIN ( SELECT ap_account_id ,  COALESCE(SUM(amount_usd), 0) "lweek_trade_usd"
						FROM analytics.trades_master 
						WHERE date_trunc('day',created_at) >= date_trunc('month', now()) - '13 day'::interval -- beginning of week 3 OF LAST MONTH 
						AND date_trunc('day',created_at) < date_trunc('month', now()) - '6 day'::interval -- till end of week 3 OF LAST MONTH 
						GROUP BY 1) w1 
						ON t.ap_account_id = w1.ap_account_id
			WHERE	
				t.ap_account_id NOT IN ('0','186','187','869','870','1356','1357','4344','18866','25041','25224','25225','25226','25227'
				,'38263','38262','38260','38121','37955','40706','40683','37807','48948','48870','44679','49659','49658','52018','52019','44057','161347') 
			GROUP BY 1,2,3,4,5,6,7
)
SELECT u.ap_account_id , u.signup_hostcountry , u.first_name , u.last_name , u.mobile_number , u.email , u.gender , u.dob 
--	, COALESCE(i.info ->> 'permanent_address',i.info ->> 'address_in_id_card',i.info ->> 'present_address',i.info ->> 'work_address') AS address 
	, i.info ->> 'present_address' present_address , i.info ->> 'present_address_district' present_address_district , i.info ->> 'present_address_sub_district' present_address_sub_district
	, i.info ->> 'present_address_province' present_address_province , i.info ->> 'present_address_postal_code' present_address_postal_code
	, i.info ->> 'occupation' occupation 
	, u.is_zipup_subscribed , z.auto_restake_enabled 
	, COUNT(re.invited_user_id) number_of_referral
	, c.zmtusd_rate , c.total_zmt_staked , c.zmt_staked_balance 
	, a.zmt_amount zmt_unstaked , a.aum_no_zmt aum_balance_no_zmt , a.aum_balance  
--	, a.account_id_c_l1y , a.usd_amount_l1y 
	, a.yearly_balance yearly_aum 
	, COALESCE(w.l1month_trade_usd,0) l1m_trade_vol 
	, COALESCE(w.l3m_trade_usd,0) l3m_trade_usd 
	, COALESCE(w.l365_trade_usd,0) l365_trade_usd 
--	, CASE WHEN w.lweek_trade_usd = 0 THEN 1 ELSE w.wtd_trade_usd / w.lweek_trade_usd END AS trade_vol_wow
--	, CASE WHEN w.lmonth_trade_usd = 0 THEN 1 ELSE w.mtd_trade_usd / w.lmonth_trade_usd END AS trade_vol_mom
--	, w.mtd_trade_usd , w.buy_vol 
	, ROUND(cast(zmt_releasing_this_month AS numeric),4) zmt_releasing_this_month
	, ROUND(cast(zmt_releasing_next_month AS numeric),4) zmt_releasing_next_month 
	, ROUND(cast(zmt_releasing_after_next_month AS numeric),4) zmt_releasing_after_next_month
FROM asset_holding a  
	LEFT JOIN  analytics.users_master u ON  a.account_id = u.ap_account_id  ---- country, email, names, dob, gender...
	LEFT JOIN  user_app_public.personal_infos i ON  u.user_id = i.user_id AND  i.archived_at IS NULL ---- FOR occupation, address
	LEFT JOIN  referral_service.referral_status re ON  u.user_id = re.referring_user_id ---- referral info
	LEFT JOIN  zmt_released r ON  a.account_id = r.account_id 
	LEFT JOIN  staked_cum c ON  c.account_id = a.account_id 
	LEFT JOIN  weekly_trade_summary w ON  a.account_id = w.ap_account_id 
	LEFT JOIN oms_data.user_app_public.zip_crew_subscriptions z ON u.user_id = z.user_id  ---- restake auto-enabled
WHERE u.ap_account_id NOT IN (0, 27308, 48870, 48948, 37807 , 37955 , 38121 , 38260 , 38262 , 38263 , 40683 , 40706, 63312, 63313, 161347) 
AND u.signup_hostcountry IN ('AU', 'global', 'ID', 'TH') 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,18,19,20,21,22,23,24,25,26,27,28,29,30
ORDER BY 1 


SELECT 
	*
FROM user_app_public.personal_infos



Address (present)
District (Present)
Sub District (Present)
Province (Present)
Postal code (Present)


SECTION 1: number OF users IN EACH zip crew tiers
oms_data.analytics.wallets_balance_eod -- wallets balance : ZMT LOCKED + total AUM
oms_data.analytics.trades_master -- trade volume
oms_data.user_app_public.personal_infos -- USER info: addresses, occupation
oms_data.asset_manager_public.ledgers -- ZMT released 

SECTION 2: aum/ trade IN a SPECIFIC PERIOD 








