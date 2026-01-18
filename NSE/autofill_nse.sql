UPDATE nse_ke_daily_ohlcv AS t
SET industry = (
    SELECT t2.industry
    FROM nse_ke_daily_ohlcv t2
    WHERE t2.ticker = t.ticker
      AND t2.industry IS NOT NULL
    ORDER BY t2.trade_date DESC
    LIMIT 1
)
WHERE t.industry IS NULL;
