# TradeFlow – Global Trade Intelligence Platform

## Overview

TradeFlow is an advanced data engineering platform that collects,
processes and analyses public international trade data.

The initial focus is on India's imports and exports.

## Problem Statement

International trade data is available across multiple public sources.
However, analysing it consistently across countries, products and time
periods requires reliable data pipelines and standardisation.

TradeFlow aims to solve this problem through automated batch data
pipelines and analytics-ready datasets.

## Key Objectives

- Analyse India's imports and exports.
- Track trade trends across countries and products.
- Identify supplier concentration and market opportunities.
- Build reliable and repeatable batch pipelines.
- Process large datasets using Apache Spark.
- Create analytics-ready datasets.

## Technology Stack

- Python
- SQL
- Apache Spark / PySpark
- Databricks
- Delta Lake
- AWS
- Git and GitHub
- Docker

## Data Sources

- India Department of Commerce – TradeStat
- United Nations – UN Comtrade

## Planned Architecture

Public Data Sources
        |
        v
Python Data Ingestion
        |
        v
Bronze Layer – Raw Data
        |
        v
Silver Layer – Cleaned Data
        |
        v
Gold Layer – Analytics-ready Data
        |
        v
SQL Analytics and Dashboard

## Project Status

Phase 1: Environment Setup and Python Foundations

Currently under development.
