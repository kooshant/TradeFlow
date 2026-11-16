-- TradeFlow: Top 10 Export and Import Products
-- Source: Gold product trade analytics
-- Data: Actual UN Comtrade India 2023 data

SELECT
    refYear AS trade_year,
    reporterISO AS reporter_country,
    trade_flow,
    cmdCode AS hs_product_code,
    commodity_description,
    hs_chapter,
    product_trade_value_usd,
    total_flow_trade_value_usd,
    trade_value_share_pct,
    product_rank

FROM read_parquet(
    'data/processed/delta/gold/product_trade_analytics_2023/*.parquet'
)

WHERE reporterISO IS NOT NULL
  AND LOWER(trade_flow) IN ('export', 'import')
  AND product_rank <= 10

ORDER BY
    LOWER(trade_flow),
    product_rank;
