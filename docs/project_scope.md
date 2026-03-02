# Risk/Markets Lakehouse: Project Scope

## 1) Project title
Risk/Markets Lakehouse: Daily market, FX and macro pipeline

## 2) Business goal
This project builds an end-to-end data engineering pipeline for risk and market monitoring. It ingests daily market prices, FX rates, and macroeconomic indicators into a layered architecture (raw, silver, curated). The curated layer supports analytics on daily returns, rolling volatility, drawdowns, correlations, and macro-market relationships. The goal is to simulate a production-style finance data platform for investment and risk analysis.

## 3) Asset universe (8)
- SPY
- QQQ
- IWM
- EFA
- TLT
- IEF
- GLD
- VNQ

## 4) FX scope (3)
- EUR/USD
- EUR/GBP
- USD/JPY

## 5) Macro series (5)
- CPIAUCSL (US CPI)
- FEDFUNDS (Fed Funds Rate)
- UNRATE (Unemployment Rate)
- DGS10 (10Y Treasury Rate)
- VIXCLS (VIX)

## 6) Core analytics (curated layer)
- daily return
- 20-day rolling volatility
- drawdown
- 50-day moving average
- 200-day moving average
- rolling correlation (selected pairs)
- macro overlay by date

## 7) Target dashboard questions
- Which assets have the highest recent volatility?
- How do equities behave when rates rise?
- How does gold perform during equity drawdowns?
- What is the rolling correlation between SPY and TLT?
- How do market regimes change when VIX spikes?