# EODHD_API

A Python-based tool for interacting with the EODHD API to retrieve end-of-day (EOD) and intraday market data.

## Overview

This library simplifies fetching financial market data using the EODHD API. Whether you're analyzing daily trends or exploring high-frequency intraday movements, this tool provides a straightforward way to retrieve and process the data.

---

## Features

- Fetch **end-of-day (EOD)** market data for multiple tickers.
- Retrieve **intraday data** for single or multiple tickers.
- Supports specifying time zones for accurate data alignment.
- Customizable request parameters such as period, interval, and date range.

---

## Installation

To get started, clone this repository and install the required dependencies:

```bash
git clone https://github.com/yourusername/EODHD_API.git
cd EODHD_API
```

---

## Usage
Below are examples of how to use the library:

Initialize the Extractor
Start by initializing the StockDataEngine with your API token, the path to exchange data, and your desired time zone.

```python
from eodhd_api import StockDataEngine

eodhd = StockDataEngine(
    api_token="YOUR_API_TOKEN",
    exchanges_path="path/to/exchanges",
    timezone="Mexico_City"
)
```

---

## Download End-of-Day (EOD) Data for Multiple Tickers
Retrieve daily market data for a list of tickers within a specified date range:

```python
eod_data = eodhd.download_data(
    tickers=["AAPL", "MFRISCOA-1.MX", "LOGN.SW"],
    exchange_code="US",
    period="1d",
    start="2024-01-01",
    end="2024-11-25"
)
```
Even if we want to chain column operator in the request we could do the following:
```python
eod_data = eodhd.download_data(
    tickers=["AAPL", "MFRISCOA-1.MX", "LOGN.SW"],
    exchange_code="US",
    period="1d",
    start="2024-01-01",
    end="2024-11-25"
)["Adj Close"]
```

---

## Download Intraday Data for a Single Ticker
Fetch high-frequency intraday data for a single ticker, specifying the desired interval and time range:

```python
intraday_data = eodhd.download_data(
    tickers="AAPL",
    exchange_code="US",
    interval="1m",
    start="2024-01-01 09:30:00",
    end="2024-01-02 16:00:00"
)
```
