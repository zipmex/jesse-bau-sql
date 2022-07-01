-- V.2 -- zmt staked daily and bonus amount base on tier
WITH date_serie AS 
(
	SELECT DISTINCT 
			DATE_TRUNC('day', p.created_at) created_at
			, u.account_id user_id 
	--		, s.service_id 
	FROM  analytics.period_master p 
		CROSS JOIN (SELECT DISTINCT account_id FROM asset_manager_public.ledgers) u
	--	CROSS JOIN (SELECT DISTINCT service_id FROM asset_manager_public.ledgers WHERE service_id IN ('main_wallet','zip_lock')) s
	WHERE 
		p."period" = 'day'
		AND DATE_TRUNC('day', p.created_at) >= '2021-08-23 00:00:00'
		AND DATE_TRUNC('day', p.created_at) <= '2021-09-30 00:00:00'
		AND u.account_id IN ('01EYKWGFG45WE55BKWHGX1KE2R',	'01EYNHTGDQE3AZC5AXXB1F8YFZ',	'01EYFG0F1T3N4H3J593FC9SV5F',	'01F7HX085KCH820TPRFJNMYHBR',	'01EVAT05GVJXHP626AQ9PKJHG0',	'01ETDB2YQPCZHPZPMGF5ZE7SES',	'01EPAYEA8XACDDA33SFE4W7J2J',	'01EYCTT3BNFF8PVVNN7D32QMPC',	'01EWEDTK0A16QR8Q425F3FQMN4',	'01EVE72JYVV3JZDP9W6G1WYP2F',	'01EYQ0EJJKRXJB5PR3H7BEX17W',	'01ETX055M2V7AZRBADB22670MD',	'01EVB96Q2ETDKF34BBC4SYGGD2',	'01EDNN7GK8Z4EM32J8RFVEJ5NB',	'01F093QE5RZ0Z13V4PCKX65R77',	'01ES5YBVD83PH8FYP6VAR1GWHZ',	'01EXV32591K43RS2HND3Z81Q49',	'01EGQ9T8EQ88RC1MNKVJW43E3X',	'01EX79JDBVFXK8QTA6NDAB3XCF',	'01EMTT7AYSS6YN3K5WR7MKR0FN',	'01EZ55TGABDHV5REG4C469ZHG9',	'01ERX9Z6PJWR08PPSMW5VR7D9M',	'01EN0AF7Q89A3DTX9DEE4AMA8B',	'01EXHRZ4ECR1RSFBRHAD3A04TG',	'01EQDBZ422HTW9WJESXHKX73W5',	'01EYFGMHYM2A8BGQFYN390SGVC',	'01ETYQ39VT3M14ZV98AMJDXYEV',	'01EXGV26SVR05BN8HYRQFKD9QC',	'01F649359BA21Z1VTCX7BCN9DG',	'01EZJ9ZHKRN82PZDP9B6PCWVSH',	'01F2PACJCTV9JGZNVCJ894YT2Q',	'01EVN7AS2KCQ3EJXY2HCNK8TQF',	'01EY7E78NJZX11M2C5CCPAD8N2',	'01EV3Y0VEYKR7HMF2BR0WS5TDA',	'01EV8RKHSATJ8V14AHF3064Q4N',	'01EMH1JB18NDSX6MWJHBKYRZ11',	'01EWX4Y42E1CXV3FEKNF08NVDW',	'01EZ2QA2QXR2Z8HMZEBVCKW3XE',	'01EVJV297VH9PGJ0ZFMDZ4XKM6',	'01EXYPHXVZT38DVTSH0RB0VN84',	'01EV1EM8QJ1T3JQ1EE3GVEMVJ5',	'01EWFRK4RV63CRKH1MQS4F21PW',	'01EYAM5W5RXV9MP80CSDF0RE59',	'01EQJG64MEH3RFNAQ2AQQEKZAC',	'01ERSGQY6DW20FC1ZNQSVSEJQ6',	'01F00QSY436QBVXGKTXGCB0WC8',	'01EST1H99PSP0MPQ7JWK5FFVAF',	'01EXSE9KNKYZ2D7T7ACXDQG36N',	'01EHX0NA969HD7Z45S1DTH33Z1',	'01ET7TKF9KM9TCYZT276SGP1WG',	'01EX0PGBMG0APDPB0WSEWHEX36',	'01EVSKATG90MZ2RWTP596A04ZF',	'01EY2ADG3Y2YC4FCAQB17P2VYZ',	'01ERKG6TA8B859EMD6NNJ52R8H',	'01EN2RBVEA4KBDHGCGPQQA7ERR',	'01EW232ADGJQK8EG9FSYEMN5DG',	'01ERMX14E528RT477C7JD2T64W',	'01ECSBFD7WNZ6CP4P72SYVGT6Q',	'01ESWR1VJBPZ6JB4KSR149TRY3',	'01F05Y70PFPBA2VB0HARG4738V',	'01EYG3XKTSYFGNVQH7E2XNP6DH',	'01EDNPQ1YKY4N5RJC0SGMAYG7C',	'01EWAFHA9GAZHEZ9GSRKC4BTGK',	'01EWN70SJ4QGYXT8JP33G4RDDH',	'01F3GCTXQRTHTCSFVAYW6Z4DRR',	'01ES5CCS9JV6QPTG36ZCB0CXZM',	'01F0GEZEP6Z358JEW353N7D3V9',	'01EX95JND9BT9CM9F8FQ3GA94Z',	'01EFNEXA4Q4H63WEWCTCPT80WR',	'01EWN71JTA6CEN2PCC37A9RKQD',	'01ESR35WZEV89HD289VKM89RJB',	'01ET6WYYHM457FAYHZGXY09GQW',	'01EWQWGAGMTKMHMSRD5M0MQY56',	'01ET1NJ0EE4XZ1Q83RKGA4BT0E',	'01EY3NJ6ABJGYJNRVVSC3TS3CK',	'01F34N6APTE51TGA4E2ABAY0DN',	'01F58M9JZKNQZMMPRC2E9VGTSP',	'01EVVN96TZ2QMFXC9M2RTK8DG5',	'01F185FRHM9C6EB1QXMQENJ09A',	'01EQ3K5FCY4FGF6E5FKVP6KAMR',	'01EWF2J5G3Y16MDNGZ5PB62H65',	'01EY8A9K6PBMFQRG6JFXMCVCM8',	'01ETYTPR2V2HPQSED1F3QXFTEW',	'01EF5RQ2VN6JWH32HR2PQH651S',	'01EMD7B3RRQ7JCF1AY393D5KPZ',	'01F4KS6NP59QCF1WMWSN3KMZ20',	'01EHY1062BBEGJRTYN51D4Y1HH',	'01EVBY9ESGHQ59HM8AS1J5DE97',	'01EJG5HQSCDTFP0DA98SP1935N',	'01ERWATW2JH3CBY62PP86YK2PH',	'01EVBE4WAPTT0WNJ0RPBJ1VRB7',	'01EWC336KXEFM27FE1YTMTQ4Z6',	'01EV3RDBN7DXS8TZ8YJ3HCMP2S',	'01EXT9DEKW956YR8VSG0ANVG0J',	'01EW0B296WAD78BCQ1KPF673NF',	'01EV3R9SJRK9VNB1DH3JV3PHP2',	'01EVV5698FB807Y7N5VPBRF4YK',	'01EVTP0VNARFEAQVQ2V75KP4DV',	'01EVZY18AS7JWJMG9NT355J2BR',	'01F0N3665PDWAWC5GRD2VT4ANB',	'01EY7SPS48X7MX1K4BKHESBM3M',	'01EYXGW1WNN96RWB2CJTNYYQG2',	'01ED4Z89SG6VQTQ30NHHB3G9ZP',	'01EEHVGQ0CVA3ZE93509CVFFGK',	'01ESTKEZTF9XXQGF66461J9758',	'01ENQXR3QPVEM0R8F8WGK415MF',	'01EPG7P1YP77RGWBTM27EES5M7',	'01EVKHDBAXYECKY9AMY90E3D7P',	'01EWAZGGSFKH054J5SY1NR1PZS',	'01EYG38GRBCE3WVQJCEDE18X6Q',	'01ETYN0G663VAHZ34CQ4JBMW7S',	'01ETV9EZNQTY4RDP9SKFJXJ54K',	'01EP1XR0XZ7E1B4V2ZX5M5P9A8',	'01ER7CRCW365BF7VPK87C09VEG',	'01ESQV05P0XEP7PGYD27BXBD8Z',	'01EPM4C08M91FC90KJ0QERNHRN',	'01ESXE7J2YT175363A0K1H6CMD')
--		AND u.email IN ('addy.phd@gmail.com',	'anna.phian@gmail.com',	'anusak_bas@hotmail.co.th',	'aong1601@gmail.com',	'apollo.14@live.com',	'arad.ptg@gmail.com',	'aru2.wanchai@gmail.com',	'auraurvararat@gmail.com',	'berserk.ta@gmail.com',	'b.jirapa@gmail.com',	'boontriga@hotmail.com',	'chidchai23@gmail.com',	'chitharnant@panjabhat.com',	'chontisa.p@gmail.com',	'cocoapowerr@gmail.com',	'codename_opzzz@hotmail.com',	'cokokung@gmail.com',	'c.salinthorn@gmail.com',	'demoncie@yahoo.com',	'dentosoraya@gmail.com',	'dr.kung888@gmail.com',	'electrica3970@gmail.com',	'fookfcnfcn@gmail.com',	'harinwan@hotmail.com',	'icepop560@gmail.com',	'invest@gofive.co.th',	'ja.anantarit@gmail.com',	'jear009@gmail.com',	'jirawit@gmail.com',	'joemarlborohouse@gmail.com',	'jutiphan@hotmail.com',	'kchutipong17@gmail.com',	'kittichet14@gmail.com',	'kitti.hvn@gmail.com',	'koongwannabe@gmail.com',	'lkritsada@gmail.com',	'maibusinesshome@gmail.com',	'meesurin@gmail.com',	'mgb1610@ymail.com',	'milabissy4@gmail.com',	'm.p.jitrin@gmail.com',	'mr.pipat@gmail.com',	'mrsatorn@gmail.com',	'namzomvip@gmail.com',	'naniinan@hotmail.co.th',	'nantharat.sri@gmail.com',	'nataumi@gmail.com',	'natt_ruang@yahoo.com',	'nawanwatsinseubpol1@gmail.com',	'nopanitjinny@gmail.com',	'oat-14@windowslive.com',	'ooiill24@gmail.com',	'panidavong@gmail.com',	'papetch@gmail.com',	'passawamaneephan@gmail.com',	'pasupply@hotmail.com',	'peerawat.kun@gmail.com',	'pissanuwat22@gmail.com',	'piyasil.p@hotmail.com',	'polkrit.r@gmail.com',	'poonawanakorn@gmail.com',	'ppc.naris@gmail.com',	'ps_2700@hotmail.com',	'pusimplyme@gmail.com',	'reideen.man@gmail.com',	'rongfred@gmail.com',	'rudpao1997@gmail.com',	'santawit_jitsomboon@hotmail.com',	'savingintrend@gmail.com',	'sawaporn_c@hotmail.com',	'siczones.eu5.org@gmail.com',	'sky_2516@hotmail.com',	'srimasittikul@gmail.com',	'sspptt1958@gmail.com',	'supawadee.sara@gmail.com',	'suthbhumi@gmail.com',	'tanapum@hotmail.com',	'te_051091@hotmail.com',	'test1.coin@hotmail.com',	'thanisakp@hotmail.com',	'thaniya.thaithanee@gmail.com',	'tickkv@gmail.com',	't.n08@hotmail.com',	'tnt16.arch69@gmail.com',	'tum_sailom@hotmail.com',	'tunyathon@gmail.com',	'udomjate@yahoo.com',	'upuptang@gmail.com',	'vidpong@gmail.com',	'vipadatam@gmail.com',	'vtrangka+1@icloud.com',	'wanapayod@gmail.com',	'waritbuzz@gmail.com',	'wijittudon@gmail.com',	'worrawit.sur@hotmail.com',	'wpzm995975@gmail.com',	'zatarn.tonixc@hotmail.com',	'anan.pracharktam@gmail.com',	'jangbtv@gmail.com',	'bigbird_p@hotmail.com',	'ibaaoth8@gmail.com',	'kh@thaitanium.biz',	'montree.somroop@me.com',	'pinya.pan@gmail.com',	'sabadahap1168@hotmail.com',	'sorranat.tas@gmail.com',	'ss.wern@gmail.com',	'suchizx@hotmail.com',	'suttikeat35@hotmail.com',	'tlmtp27@gmail.com',	'lub.pui@hotmail.com',	'peerasak.si.plastic@gmail.com',	'kuengm@gmail.com',	'me_chatchi@hotmail.com',	'emchitshop@hotmail.com',	'boykung195@gmail.com',	'amornthep44@gmail.com')
------ TEST ACCOUNT HERE Pluang id 01EPB97EP6PPTB070VPZ445111
)
	, zmt_stake AS 
(
	SELECT 
		d.created_at
		, d.user_id
		, u.ap_account_id
		, SPLIT_PART(l.product_id,'.',1) symbol
		, SUM( CASE WHEN l.service_id = 'zip_lock' THEN COALESCE (credit,0) - COALESCE (debit,0) END) ziplock_amount  
	FROM date_serie d 
		LEFT JOIN 
			asset_manager_public.ledgers l 
			ON d.user_id = l.account_id 
		--	AND d.service_id = l.service_id 
			AND d.created_at >= DATE_TRUNC('day', l.updated_at + '7 hour'::INTERVAL)
		LEFT JOIN
			warehouse.analytics.users_master u
			ON l.account_id = u.user_id
	WHERE
		SPLIT_PART(l.product_id,'.',1) = 'zmt'
		AND u.ap_account_id IS NOT NULL 
	GROUP BY 
		1,2,3,4
)
	, vip_tier AS 
(	-- calculate daily vip tier using zmt lock balance
		SELECT 
			created_at , ap_account_id , user_id , symbol , COALESCE (ziplock_amount,0) zmt_stake_balance
			, CASE WHEN ziplock_amount >= 100 AND ziplock_amount < 1000 THEN 'vip1'
					WHEN ziplock_amount >= 1000 AND ziplock_amount < 5000 THEN 'vip2'
					WHEN ziplock_amount >= 5000 AND ziplock_amount < 20000 THEN 'vip3'
					WHEN ziplock_amount >= 20000 THEN 'vip4'
					ELSE 'no_tier' END AS vip_tier
			, CASE WHEN ziplock_amount >= 100 AND ziplock_amount < 20000 THEN 'ZipMember'
					WHEN ziplock_amount >= 20000 THEN 'ZipCrew'
					ELSE 'ZipStarter' END AS zip_tier
		FROM 
			zmt_stake			
)
	, lock_balance AS (
	SELECT 
		d.created_at 
--		, d.user_id  
		, u.ap_account_id 
		, u.signup_hostcountry 
		, u.email 
		, zmt_stake_balance 
		, zip_tier
		, vip_tier
		, UPPER(SPLIT_PART(l.product_id,'.',1)) symbol 
--		, COALESCE(SUM( CASE WHEN l.service_id = 'main_wallet' THEN credit - debit END),0) AS wallet_balance 
--		, COALESCE(SUM( CASE WHEN l.ref_action = 'deposit' AND d.created_at = DATE_TRUNC('day', l.updated_at) THEN credit END),0) AS transfer_in  
--		, COALESCE(SUM( CASE WHEN l.ref_action = 'withdraw' AND d.created_at = DATE_TRUNC('day', l.updated_at) THEN debit END),0) AS transfer_out  
		, COALESCE(SUM( CASE WHEN l.service_id = 'zip_lock' THEN credit - debit END),0) AS lock_balance 
		, COALESCE(SUM( CASE WHEN l.credit > 0 AND l.service_id = 'zip_lock' AND d.created_at = DATE_TRUNC('day', l.updated_at) THEN credit - debit END),0) AS new_lock_amount 
		, COUNT(CASE WHEN l.credit > 0 AND l.service_id = 'zip_lock' AND d.created_at = DATE_TRUNC('day', l.updated_at) THEN l.account_id END) lock_count 
	FROM 
		date_serie d
		LEFT JOIN 
			asset_manager_public.ledgers l 
			ON d.user_id = l.account_id 
		--	AND d.service_id = l.service_id 
			AND DATE_TRUNC('day', d.created_at) >= DATE_TRUNC('day', l.updated_at) 
		LEFT JOIN 
			vip_tier s 
			ON d.user_id = s.user_id
			AND d.created_at = s.created_at
		LEFT JOIN 
			analytics.users_master u 
			ON l.account_id = u.user_id 
	WHERE 
		UPPER(SPLIT_PART(l.product_id,'.',1)) = 'BTC'
		AND u.ap_account_id IS NOT NULL
	GROUP BY 1,2,3,4,5,6,7,8
)	, user_tier AS (
	SELECT 
		ap_account_id
		, signup_hostcountry 
		, email 
		, zip_tier
		, vip_tier
		, SUM( CASE WHEN zip_tier = 'ZipCrew' AND lock_balance > 0 THEN COALESCE(lock_balance, 0) END) total_btc_lock_amount
--		, SUM( COALESCE(lock_balance, 0) ) total_btc_lock_amount
		, COUNT( CASE WHEN zip_tier = 'ZipCrew' AND lock_balance > 0 THEN ap_account_id END) zipcrew_day
--		, SUM(lock_count) OVER(PARTITION BY ap_account_id ORDER BY created_at) total_lock_count 
--		, SUM(new_lock_amount) OVER(PARTITION BY ap_account_id ORDER BY created_at) total_new_lock_amount 
	FROM 
		lock_balance  
	WHERE zip_tier = 'ZipCrew'
	GROUP BY 1,2,3,4,5
	)--	, bonus_calc AS (
	SELECT 
		*
		, total_btc_lock_amount/ zipcrew_day::float avg_btc_lock_amount
		, (total_btc_lock_amount/ zipcrew_day::float) * (zipcrew_day::float/365) * 0.05 bonus_amount
	--	, CASE WHEN total_lock_count = 0 THEN 0 ELSE total_new_lock_amount/ total_lock_count::FLOAT END AS avg_lock_amount
		-- bonus amount calculated by user tier in a daily basis 
	--	, CASE WHEN zip_tier = 'ZipCrew' AND total_lock_count >= 1 THEN (total_new_lock_amount/ total_lock_count::FLOAT) * (0.05/365)		---- CHANGE incentive RATE FOR ZIP_CREW
			--	WHEN zip_tier = 'ZipMember' AND total_lock_count >= 1  THEN (total_new_lock_amount/ total_lock_count::FLOAT) * (0.02/365)	---- CHANGE incentive RATE FOR ZIP_MEMBER
	--			ELSE 0 END AS cumulative_bonus_amount
	FROM 
		user_tier 
	ORDER BY 1 
;

