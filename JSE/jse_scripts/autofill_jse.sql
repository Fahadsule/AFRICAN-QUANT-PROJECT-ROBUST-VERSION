-- First SQL statement (this one is fine)
UPDATE jse_indices_daily_ohlcv
SET index_name = CASE ticker
    WHEN '^J200.JO' THEN 'jse_sa_top_40_index'
    WHEN '^J201.JO' THEN 'jse_sa_midcap_index'
    WHEN '^J202.JO' THEN 'jse_sa_smallcap_index'
    WHEN '^J203.JO' THEN 'jse_sa_allshare_index'
    WHEN '^J204.JO' THEN 'jse_sa_fledging_index'
    WHEN '^J205.JO' THEN 'jse_sa_largecap_index'
END
WHERE index_name IS NULL;

-- FIXED: create a temp table of ticker → one company_name using aggregation
CREATE TEMP TABLE temp_ticker_map AS
SELECT 
    ticker, 
    MAX(company_name) AS company_name  -- or MIN(), or any other aggregate
FROM jse_sa_daily_ohlcv
WHERE company_name IS NOT NULL
GROUP BY ticker;

-- then update using just the temp table
UPDATE jse_sa_daily_ohlcv AS target
SET company_name = (
    SELECT company_name
    FROM temp_ticker_map
    WHERE temp_ticker_map.ticker = target.ticker
)
WHERE company_name IS NULL;

-- FIXED: Use more efficient approach for sector/industry
DROP TABLE IF EXISTS temp_ticker_sector_industry;

-- First, create a table with one record per ticker with non-null sector/industry
CREATE TEMP TABLE temp_ticker_sector_industry AS
SELECT DISTINCT ON (ticker)
    ticker,
    sector,
    industry
FROM jse_sa_daily_ohlcv
WHERE (sector IS NOT NULL OR industry IS NOT NULL)
ORDER BY ticker, 
    CASE WHEN sector IS NOT NULL AND industry IS NOT NULL THEN 1 ELSE 2 END,
    trade_date DESC;  -- if you want the latest record

-- Update using the temp table
UPDATE jse_sa_daily_ohlcv AS target
SET
    sector = COALESCE(target.sector, temp.sector),
    industry = COALESCE(target.industry, temp.industry)
FROM temp_ticker_sector_industry AS temp
WHERE target.ticker = temp.ticker
    AND (target.sector IS NULL OR target.industry IS NULL);