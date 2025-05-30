# ADI Stock Historical Data ETL Project

## Overview
This project extracts, transforms, and loads (ETL) historical stock price data for Analog Devices Inc. (ticker: ADI) into a SQLite database.
It also automates daily updates and provides tools for analysis and visualization.

## Features
- Download full historical daily stock price data using Yahoo Finance API.
- Clean and transform data (select key columns, remove missing data).
- Load data into a local SQLite database.
- Automate daily incremental updates.
- Basic data visualization using matplotlib.
- Sample SQL queries for common stock analysis.
