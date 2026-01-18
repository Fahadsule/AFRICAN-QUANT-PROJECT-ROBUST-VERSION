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

-- create a temp table of ticker → one company_name
CREATE TEMP TABLE temp_ticker_map AS
SELECT ticker, company_name
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

DROP TABLE IF EXISTS temp_ticker_sector_industry;

CREATE TEMP TABLE temp_ticker_sector_industry AS
SELECT
    ticker,
    MAX(sector)   AS sector,
    MAX(industry) AS industry
FROM jse_sa_daily_ohlcv
WHERE sector IS NOT NULL
   OR industry IS NOT NULL
GROUP BY ticker;

UPDATE jse_sa_daily_ohlcv
SET
    sector = COALESCE(
        sector,
        (SELECT t.sector FROM temp_ticker_sector_industry t
         WHERE t.ticker = jse_sa_daily_ohlcv.ticker)
    ),
    industry = COALESCE(
        industry,
        (SELECT t.industry FROM temp_ticker_sector_industry t
         WHERE t.ticker = jse_sa_daily_ohlcv.ticker)
    )
WHERE sector IS NULL OR industry IS NULL;

