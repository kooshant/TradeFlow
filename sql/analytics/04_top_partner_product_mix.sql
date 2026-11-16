-- TradeFlow: Top Partner Product Mix
-- Source: Gold import partner analytics
-- Analysis: Top 5 partners and top 5 products per partner

SELECT
    refYear AS trade_year,
    reporterISO AS reporter_country,
    partner2Code AS partner_code,
    partner2Desc AS partner_country,
    partner_rank,
    cmdCode AS hs_product_code,
    commodity_description,
    partner_product_import_value_usd,
    product_share_within_partner_pct,
    product_rank_within_partner

FROM read_parquet(
    'data/processed/delta/gold/import_partner_analytics_2023/*.parquet'
)

WHERE reporterISO IS NOT NULL
  AND partner2Desc IS NOT NULL
  AND LOWER(trade_flow) = 'import'
  AND partner_rank <= 5
  AND product_rank_within_partner <= 5

ORDER BY
    partner_rank,
    product_rank_within_partner;
