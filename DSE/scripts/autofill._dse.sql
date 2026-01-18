UPDATE dse_tz_daily_ohlcv AS target
SET
    industry = (
        SELECT industry
        FROM dse_tz_daily_ohlcv
        WHERE ticker = target.ticker AND industry IS NOT NULL
        ORDER BY trade_date DESC
        LIMIT 1
    ),
    shares_in_issue = (
        SELECT shares_in_issue
        FROM dse_tz_daily_ohlcv
        WHERE ticker = target.ticker AND shares_in_issue IS NOT NULL
        ORDER BY trade_date DESC
        LIMIT 1
    )
WHERE industry IS NULL OR shares_in_issue IS NULL;
