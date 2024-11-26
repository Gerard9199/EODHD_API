import pandas as pd
import requests
from datetime import datetime
from pandas.tseries.offsets import BDay
from io import StringIO

class StockDataEngine:
    def __init__(self, api_token, exchanges_path):
        """
        Initializes the StockDataExtractor with the provided API token and path to the exchanges data.

        Parameters:
        - api_token (str): Your API token for authentication.
        - exchanges_path (str): Path to the parquet file containing exchange data.
        """
        self.api_token = api_token
        self.exchanges = pd.read_parquet(exchanges_path)
        self.api_requests_today = 0
        self.daily_rate_limit = 100000
        self.minute_request_limit = 1000
        self.last_api_requests_date = None
        self.user_info = {}
        self.get_user_info()

    def get_user_info(self):
        """
        Fetches the user information from the API and updates the instance variables accordingly.
        """
        url = f"https://eodhd.com/api/user?api_token={self.api_token}&fmt=json"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            self.daily_rate_limit = int(data.get("dailyRateLimit", 100000))
            self.api_requests_today = int(data.get("apiRequests", 0))
            self.last_api_requests_date = data.get("apiRequestsDate", None)
            self.user_info = data
        else:
            print("Error fetching user info")

    def _check_available_calls(self, required_calls):
        """
        Checks if the required number of API calls are available.

        Parameters:
        - required_calls (int): The number of API calls required.

        Returns:
        - bool: True if enough calls are available, False otherwise.
        """
        self.get_user_info()  # Update api_requests_today
        calls_remaining = self.daily_rate_limit - self.api_requests_today
        if calls_remaining >= required_calls:
            return True
        else:
            print(f"Not enough API calls remaining. Required: {required_calls}, Available: {calls_remaining}")
            return False

    def _make_api_request(self, url, cost_of_calls, request_type, fmt):
        """
        Makes the API request(s) and processes the response.

        Parameters:
        - url (str or list): The API URL(s) to request.
        - cost_of_calls (int): The cost of API calls.
        - request_type (str): The type of request ('data' or 'tickers').
        - fmt (str): The format of the response ('csv' or 'json').

        Returns:
        - pd.DataFrame: The processed data.
        """
        # If the URL is a list with fewer than 2 elements or is a string, and the request_type is 'ticker' or 'data'
        if (isinstance(url, list) and len(url) < 2) or isinstance(url, str):
            if isinstance(url, list) and len(url) < 2:
                url = url[0]

            # Make the request
            response = requests.get(url)
            self.get_user_info()
            if response.status_code == 200:
                self.api_requests_today += cost_of_calls
                # Parse and return data
                if fmt == "json":
                    data = pd.DataFrame(response.json())
                elif fmt == "csv":
                    data = pd.read_csv(StringIO(response.text))
                else:
                    data = response.text
                return data
            else:
                print(f"Error fetching {request_type}: {response.status_code}")
                return pd.DataFrame()

        # In case the URL is a list with more than one element and the request_type is 'data'
        elif isinstance(url, list) and len(url) > 1 and request_type == "data":

            data_frames = []
            for single_url in url:
                # Extract the ticker from the URL to label
                if "/eod/" in single_url:
                    symbol = single_url.split("/eod/")[1].split("?")[0]
                elif "/intraday/" in single_url:
                    symbol = single_url.split("/intraday/")[1].split("?")[0]
                else:
                    symbol = "Unknown"

                response = requests.get(single_url)
                self.get_user_info()
                if response.status_code == 200:
                    self.api_requests_today += cost_of_calls
                    # Parse and process data
                    if fmt == "json":
                        df = pd.DataFrame(response.json())
                    elif fmt == "csv":
                        df = pd.read_csv(StringIO(response.text))
                    else:
                        df = response.text  # Not expected

                    # Check if 'Adjusted_close' is in the columns
                    if "Adjusted_close" not in df.columns:
                        # Intraday data
                        df["Adjusted_close"] = df["Close"]
                        intraday_cols = ["Datetime", "Open", "High", "Low", "Close", "Adjusted_close", "Volume"]
                        df = df[intraday_cols]
                        # Rename general columns
                        general_cols = ["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"]
                        df.columns = general_cols
                    else:
                        # EOD data
                        eod_cols = ["Date", "Open", "High", "Low", "Close", "Adjusted_close", "Volume"]
                        df = df[eod_cols]
                        # Rename general columns
                        general_cols = ["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"]
                        df.columns = general_cols

                    # Add 'Price' column equal to 'Adj Close'
                    df["Price"] = df["Adj Close"]
                    # Reorder columns
                    df = df[["Date", "Price", "Adj Close", "Close", "High", "Low", "Open", "Volume"]]

                    # Set index
                    df.set_index("Date", inplace=True)

                    # Add DataFrame to the list
                    data_frames.append((symbol, df))
                else:
                    print(f"Error fetching data for {symbol}: {response.status_code}")
                    continue

            # Concatenate DataFrames into a MultiIndex with the required format
            if data_frames:
                # Create a list of all tickers
                tickers = [symbol for symbol, _ in data_frames]
                # Create a list of all DataFrames
                dfs = [df for _, df in data_frames]

                # Concatenate the DataFrames along the columns
                df_combined = pd.concat(dfs, axis=1, keys=tickers)

                # Rearrange the levels of the MultiIndex
                # Swap levels so that the first level is the column names and the second level is the tickers
                df_combined.columns = df_combined.columns.swaplevel(0, 1)

                # Sort the column levels according to the desired format
                desired_order = ["Price", "Adj Close", "Close", "High", "Low", "Open", "Volume"]
                df_combined = df_combined.reindex(columns=desired_order, level=0)

                return df_combined
            else:
                return pd.DataFrame()

        else:
            print("Invalid parameters for _make_api_request.")
            return pd.DataFrame()

    def _format_timestamp(self, start, end, eod_data=True, interval=None, max_periods=None):
        """
        Formats the start and end timestamps based on the data type (EOD or intraday).

        Parameters:
        - start (str): The start date/time as a string.
        - end (str): The end date/time as a string.
        - eod_data (bool): True if EOD data, False for intraday data.
        - interval (str): The interval for intraday data.
        - max_periods (dict): A mapping of intervals to maximum periods.

        Returns:
        - tuple: Formatted start and end timestamps.
        """
        if eod_data:
            # Convert dates to 'YYYY-MM-DD' format
            if end is None:
                end = datetime.utcnow().strftime("%Y-%m-%d")
            else:
                end = datetime.strptime(end, "%Y-%m-%d").strftime("%Y-%m-%d")

            if start is None:
                start = (datetime.strptime(end, "%Y-%m-%d") - BDay(252)).strftime("%Y-%m-%d")
            else:
                start = datetime.strptime(start, "%Y-%m-%d").strftime("%Y-%m-%d")

        else:
            max_days = max_periods[interval]

            # Convert start and end from string to datetime
            if end is None:
                end = datetime.utcnow()
            else:
                end = datetime.strptime(end, "%Y-%m-%d %H:%M:%S")

            if start is None:
                start = end - BDay(max_days)
            else:
                start = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")

        return (start, end)

    def _delta_days_batch(self, start, end, interval, max_periods):
        """
        Generates batches of date ranges that comply with the maximum allowed days per interval.

        Parameters:
        - start (datetime): The start datetime.
        - end (datetime): The end datetime.
        - interval (str): The interval for intraday data.
        - max_periods (dict): A mapping of intervals to maximum periods.

        Returns:
        - list: A list of tuples containing UNIX timestamps for start and end dates.
        """
        # Get max days per interval
        max_days = max_periods[interval]

        # Adjust date range if it exceeds maximum period
        current_start = start

        # List to store all date ranges
        date_ranges = []
        while current_start < end:
            # Calculate current end date by adding max_days
            current_end = current_start + BDay(max_days)
            # Ensure current_end does not exceed the overall end date
            if current_end > end:
                current_end = end

            # Convert dates to UNIX timestamps
            from_unix = int(current_start.timestamp())
            to_unix = int(current_end.timestamp())

            date_ranges.append((from_unix, to_unix))
            # Update current_start for the next batch
            current_start = current_end

        return date_ranges

    def _get_rest_url(self, tickers=None, exchange_code=None, period=None, interval=None, start=None, end=None, max_periods=None,
                      fmt=None, order=None, eod_data=None, delisted_param=None, request_type=None):
        """
        Constructs the REST API URLs based on the parameters.

        Parameters:
        - tickers (str or list): The ticker symbol(s).
        - exchange_code (str): The exchange code.
        - period (str): The period for EOD data.
        - interval (str): The interval for intraday data.
        - start (str or datetime): The start date/time.
        - end (str or datetime): The end date/time.
        - max_periods (dict): A mapping of intervals to maximum periods.
        - fmt (str): The format of the response ('csv' or 'json').
        - order (str): The order of the data ('a' or 'd').
        - eod_data (bool): True if EOD data, False for intraday data.
        - delisted_param (str): '1' or '0' to include delisted tickers.
        - request_type (str): The type of request ('data' or 'tickers').

        Returns:
        - str or list: The constructed URL(s).
        """
        if request_type == "data":
            url = []
            if eod_data:
                if isinstance(tickers, list) and len(tickers) > 1:
                    for ticker in tickers:
                        symbol = f"{ticker}.{exchange_code}"
                        url_ = (
                            f"https://eodhd.com/api/eod/{symbol}"
                            f"?api_token={self.api_token}&fmt={fmt}&period={period}&from={start}&to={end}&order={order}"
                        )
                        url.append(url_)
                else:
                    if isinstance(tickers, list):
                        tickers = tickers[0]

                    symbol = f"{tickers}.{exchange_code}"
                    url_ = (
                        f"https://eodhd.com/api/eod/{symbol}"
                        f"?api_token={self.api_token}&fmt={fmt}&period={period}&from={start}&to={end}&order={order}"
                    )
                    url.append(url_)

            else:
                date_ranges = self._delta_days_batch(start=start, end=end, interval=interval, max_periods=max_periods)

                if isinstance(tickers, list) and len(tickers) > 1:
                    for ticker in tickers:
                        for dates in date_ranges:
                            (start_unix, end_unix) = dates
                            symbol = f"{ticker}.{exchange_code}"
                            url_ = (
                                f"https://eodhd.com/api/intraday/{symbol}"
                                f"?api_token={self.api_token}&fmt={fmt}&interval={interval}&from={start_unix}&to={end_unix}"
                            )
                            url.append(url_)
                else:
                    if isinstance(tickers, list):
                        tickers = tickers[0]

                    for dates in date_ranges:
                        (start_unix, end_unix) = dates
                        symbol = f"{tickers}.{exchange_code}"
                        url_ = (
                            f"https://eodhd.com/api/intraday/{symbol}"
                            f"?api_token={self.api_token}&fmt={fmt}&interval={interval}&from={start_unix}&to={end_unix}"
                        )
                        url.append(url_)

        elif request_type == "tickers":
            url = (
                f"https://eodhd.com/api/exchange-symbol-list/{exchange_code}"
                f"?api_token={self.api_token}&fmt={fmt}&delisted={delisted_param}"
            )

        else:
            raise ValueError("Invalid request type.")

        return url

    def download_data(self, tickers, exchange_code="US", period=None, interval=None, start=None, end=None, fmt="csv", order="a", request_type="data"):
        """
        Downloads data for the specified tickers and parameters.

        Parameters:
        - tickers (str or list): The ticker symbol(s).
        - exchange_code (str): The exchange code.
        - period (str): The period for EOD data ('1d', '1wk', '1mo').
        - interval (str): The interval for intraday data ('1m', '5m', '1h').
        - start (str): The start date/time.
        - end (str): The end date/time.
        - fmt (str): The format of the response ('csv' or 'json').
        - order (str): The order of the data ('a' or 'd').
        - request_type (str): The type of request ('data').

        Returns:
        - pd.DataFrame: The downloaded data.
        """
        # Map available periods
        periods_available = {"1d": "d", "1wk": "w", "1mo": "m"}

        # Map intervals to maximum periods in days
        max_periods = {"1m": 120, "5m": 600, "1h": 7200}

        if (period or interval) is None:
            raise ValueError("You must pass interval or period.")

        if period is not None and interval is None:
            if period not in periods_available.keys():
                raise ValueError("Invalid period. Allowed periods are '1d', '1wk', '1mo'.")
            else:
                required_calls = 1
                eod_data = True  # End of Day Data
                period = periods_available[period]

        elif interval is not None and period is None:
            if interval not in max_periods.keys():
                raise ValueError("Invalid interval. Allowed intervals are '1m', '5m', '1h'.")
            else:
                required_calls = 5
                eod_data = False  # Intraday Data

        else:
            raise ValueError("Specify either period or interval, not both.")

        # Format start and end dates
        start_formatted, end_formatted = self._format_timestamp(start=start, end=end, eod_data=eod_data, interval=interval, max_periods=max_periods)

        url = self._get_rest_url(
            tickers=tickers,
            exchange_code=exchange_code,
            period=period,
            interval=interval,
            start=start_formatted,
            end=end_formatted,
            max_periods=max_periods,
            fmt=fmt,
            order=order,
            eod_data=eod_data,
            request_type=request_type
        )

        cost_of_calls = len(url) * required_calls
        # Check API call availability
        if not self._check_available_calls(required_calls=cost_of_calls):
            print("Not enough API calls available for data request.")
            return pd.DataFrame()

        data = self._make_api_request(url=url, cost_of_calls=cost_of_calls, request_type=request_type, fmt=fmt)

        return data

    def get_tickers(self, exchange_code, delisted=False, fmt="json", request_type="tickers"):
        """
        Retrieves the list of tickers for the specified exchange.

        Parameters:
        - exchange_code (str): The exchange code.
        - delisted (bool): True to include delisted tickers.
        - fmt (str): The format of the response ('csv' or 'json').
        - request_type (str): The type of request ('tickers').

        Returns:
        - pd.DataFrame: The list of tickers.
        """
        # Build the request URL
        delisted_param = "1" if delisted else "0"

        url = self._get_rest_url(exchange_code=exchange_code, fmt=fmt, delisted_param=delisted_param, request_type=request_type)

        # Check API call availability
        required_calls = 1
        cost_of_calls = required_calls
        if not self._check_available_calls(required_calls=cost_of_calls):
            print("Not enough API calls available for getting tickers.")
            return pd.DataFrame()

        tickers = self._make_api_request(url=url, cost_of_calls=cost_of_calls, request_type=request_type, fmt=fmt)

        return tickers
