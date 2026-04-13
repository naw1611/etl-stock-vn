# ETL Pipeline — Vietnamese Stock Market

## Overview
An automated ETL pipeline that fetches daily stock data
for major Vietnamese banking/finance stocks, performs
technical analysis, and loads results into SQL Server.

## Architecture
Yahoo Finance API → Python (Pandas) → SQL Server
Automated daily via Windows Task Scheduler at 18:00 ICT.

## Tech Stack
- Python 3.13 · Pandas · yfinance · SQLAlchemy · pyodbc
- SQL Server 2021 · Windows Task Scheduler

## Features
- Fetches OHLCV data for 5 banking stocks (VCB, HPG, VIC, MBB, TCB)
- Calculates MA5, MA20, daily % price change
- Upsert logic prevents duplicate records
- Retry mechanism for API failures
- CSV backup on every run

## How to Run
pip install -r requirements.txt
python etl.py

## Database Schema
Table: stock_daily
- symbol (NVARCHAR) · trade_date (DATE) — UNIQUE together
- open/high/low/close_price (FLOAT) · volume (BIGINT)
- pct_change · ma5 · ma20 · vol_ma5 (FLOAT)

## Demo
[Screenshot của Task Scheduler]
[Screenshot của SSMS với data]