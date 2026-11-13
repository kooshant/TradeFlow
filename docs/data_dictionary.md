cat > docs/data_dictionary.md <<'EOF'
# TradeFlow — Data Dictionary

## 1. Overview

This document describes the 53 columns in the TradeFlow Silver Layer dataset.

**Dataset:** `india_trade_2023_silver.csv`

**Source:** UN Comtrade

**Reporter:** India

**Reference Year:** 2023

**Classification:** HS 2022

**Processing Level:** Six-digit HS commodity detail, with other hierarchy levels retained in Silver.

## 2. Trade Identification and Reporting Period

| Field | Description |
|---|---|
| typeCode | Type of trade data record |
| freqCode | Frequency of reporting |
| refPeriodId | Reference period identifier |
| refYear | Reference year |
| refMonth | Reference month |
| period | Reporting period |
| flowCode | Trade flow code, such as Export or Import |
| flowDesc | Description of the trade flow |
| trade_flow | TradeFlow-standardized trade flow |

## 3. Reporter and Partner Information

| Field | Description |
|---|---|
| reporterCode | UN Comtrade code of the reporting country |
| reporterISO | ISO code of the reporting country |
| reporterDesc | Reporting country name |
| partnerCode | Primary partner area code |
| partnerISO | Primary partner ISO code |
| partnerDesc | Primary partner area description |
| partner2Code | Secondary partner area code |
| partner2ISO | Secondary partner ISO code |
| partner2Desc | Secondary partner area description |

**Important:** `partner2Code` represents the secondary partner / country of consignment in this dataset. It must not automatically be interpreted as country of origin.

## 4. Commodity Classification

| Field | Description |
|---|---|
| classificationCode | Commodity classification used by the source |
| classificationSearchCode | Classification code used for searching |
| isOriginalClassification | Indicates original classification status |
| cmdCode | Commodity / HS code |
| cmdDesc | Source commodity description |
| aggrLevel | Commodity aggregation level |
| isLeaf | Indicates whether the commodity code is a leaf-level item |
| customsCode | Customs procedure code |
| customsDesc | Customs procedure description |

## 5. Additional Trade Dimensions

| Field | Description |
|---|---|
| mosCode | Mode of supply code |
| motCode | Mode of transport code |
| motDesc | Mode of transport description |
## 6. Quantity and Weight Measurements

| Field | Description |
|---|---|
| qtyUnitCode | Primary quantity unit code |
| qtyUnitAbbr | Primary quantity unit abbreviation |
| qty | Reported primary quantity |
| isQtyEstimated | Indicates estimated primary quantity |
| altQtyUnitCode | Alternative quantity unit code |
| altQtyUnitAbbr | Alternative quantity unit abbreviation |
| altQty | Alternative quantity value |
| isAltQtyEstimated | Indicates estimated alternative quantity |
| netWgt | Net weight of the commodity |
| isNetWgtEstimated | Indicates estimated net weight |
| grossWgt | Gross weight of the commodity |
| isGrossWgtEstimated | Indicates estimated gross weight |

## 7. Trade Valuation

| Field | Description |
|---|---|
| cifvalue | Trade value using CIF valuation |
| fobvalue | Trade value using FOB valuation |
| primaryValue | Primary trade value reported by UN Comtrade |
| trade_value_usd | TradeFlow-standardized trade value in USD |
| trade_value_basis | Valuation basis associated with standardized trade value |

### Valuation Notes

- Exports use FOB valuation.
- Imports use CIF valuation.
- trade_value_usd is used for standardized analytics.
- FOB and CIF values have different valuation bases.

## 8. Data Quality and Reporting Flags

| Field | Description |
|---|---|
| legacyEstimationFlag | Legacy estimation indicator supplied by the source |
| isReported | Indicates whether the record is reported |
| isAggregate | Indicates whether the record is an aggregate |
| isGrossWgtEstimated | Indicates whether gross weight is estimated |

## 9. TradeFlow-Derived Fields

| Field | Description |
|---|---|
| hs_code_length | Length of the commodity code |
| hs_level | Derived HS hierarchy level |
| is_annual_period | Indicates whether the record represents an annual reporting period |

## 10. Data Interpretation Rules

1. Use trade_flow to distinguish imports and exports.
2. Use trade_value_usd for standardized trade-value calculations.
3. Preserve trade_value_basis when interpreting trade values.
4. Do not combine World aggregate records with partner-level records.
5. Interpret partner2Code as the secondary partner / country of consignment, not confirmed country of origin.
6. Do not aggregate quantities across different measurement units without normalization.
7. Treat HS hierarchy levels as alternate representations; do not add chapter, heading and subheading totals together.

## 11. Dataset Scope and Limitations

- Current reporting year: 2023.
- Reporter: India.
- Commodity classification: HS 2022.
- Current analytics focus: six-digit HS commodity detail.
- The current dataset does not provide multi-year trend analysis.
- Some source fields may be null or estimated depending on the original UN Comtrade record.
