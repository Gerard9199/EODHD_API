from .StockDataEngine import StockDataEngine

# Initialize the extractor with your API token and path to exchanges data
eodhd = StockDataEngine(api_token="your_api_token", exchanges_path="exchanges.parquet")

# Download EOD data for multiple tickers
eod_data = eodhd.download_data(
    tickers=["AAPL", "TSLA"],
    exchange_code="US",
    period="1d",
    start="2024-01-01",
    end="2024-11-25"
)["Adj Close"]

# Download intraday data for a single ticker
intraday_data = eodhd.download_data(
    tickers="AAPL",
    exchange_code="US",
    interval="1m",
    start="2024-01-01 09:30:00",
    end="2024-01-02 16:00:00"
)

# Get tickers from an exchange
tickers_list = eodhd.get_tickers(exchange_code="US", delisted=True)
