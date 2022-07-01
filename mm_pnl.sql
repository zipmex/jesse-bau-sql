WITH sum_pnl AS (
SELECT 
	"date" 
--	DATE_TRUNC('week', "date") weekly
--	DATE_TRUNC('month', "date") monthly
	, SUM(pnl) sum_pnl
FROM mm_prod_public.reports_bucket rb 
-- exclude Feb 23rd as lots of ZipStocks trades have been reverted
WHERE "date" <> '2022-02-23'
GROUP BY 1
)
SELECT 
	MAX(sum_pnl) positive_pnl
	, MIN(sum_pnl) drawdown_pnl
FROM sum_pnl