-- TradeFlow: Import Partner Concentration
-- Source: Gold import partner analytics
-- Data: Actual UN Comtrade India 2023 data

WITH partner_summary AS (
    SELECT
        refYear AS trade_year,
        reporterISO AS reporter_country,
        partner2Code AS partner_code,
        partner2Desc AS partner_country,

        MAX(partner_total_import_value_usd)
            AS total_import_value_usd,

        MAX(partner_import_share_pct)
            AS import_share_pct,

        MAX(partner_rank)
            AS partner_rank

    FROM read_parquet(
        'data/processed/delta/gold/import_partner_analytics_2023/*.parquet'
    )

    WHERE reporterISO IS NOT NULL
      AND partner2Desc IS NOT NULL
      AND LOWER(trade_flow) = 'import'

    GROUP BY
        refYear,
        reporterISO,
        partner2Code,
        partner2Desc
),

ranked_partners AS (
    SELECT
        *,
        SUM(import_share_pct) OVER (
            PARTITION BY trade_year, reporter_country
            ORDER BY partner_rank
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_import_share_pct

    FROM partner_summary
)

SELECT
    trade_year,
    reporter_country,
    partner_code,
    partner_country,
    total_import_value_usd,
    ROUND(import_share_pct, 2) AS import_share_pct,
    partner_rank,
    ROUND(cumulative_import_share_pct, 2)
        AS cumulative_import_share_pct

FROM ranked_partners

ORDER BY partner_rank;
