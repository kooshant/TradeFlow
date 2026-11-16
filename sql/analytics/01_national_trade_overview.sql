SELECT
    refYear AS trade_year,
    reporterISO AS reporter_country,
    reporterDesc AS reporter_name,

    SUM(CASE WHEN LOWER(trade_flow) = 'export'
        THEN total_trade_value_usd ELSE 0 END) AS total_exports_usd,

    SUM(CASE WHEN LOWER(trade_flow) = 'import'
        THEN total_trade_value_usd ELSE 0 END) AS total_imports_usd,

    SUM(CASE WHEN LOWER(trade_flow) = 'export'
        THEN record_count ELSE 0 END) AS export_record_count,

    SUM(CASE WHEN LOWER(trade_flow) = 'import'
        THEN record_count ELSE 0 END) AS import_record_count,

    MAX(CASE WHEN LOWER(trade_flow) = 'export'
        THEN trade_value_basis END) AS export_value_basis,

    MAX(CASE WHEN LOWER(trade_flow) = 'import'
        THEN trade_value_basis END) AS import_value_basis,

    SUM(CASE WHEN LOWER(trade_flow) = 'export'
        THEN total_trade_value_usd ELSE 0 END)
    -
    SUM(CASE WHEN LOWER(trade_flow) = 'import'
        THEN total_trade_value_usd ELSE 0 END)
        AS indicative_trade_balance_usd

FROM read_parquet(
    'data/processed/delta/gold/national_trade_flow_summary_2023/*.parquet'
)

WHERE reporterISO IS NOT NULL
GROUP BY refYear, reporterISO, reporterDesc
ORDER BY trade_year;
