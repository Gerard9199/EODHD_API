import pandas as pd
import requests
from datetime import datetime
from pandas.tseries.offsets import BDay
from pytz import timezone as tz
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time

class StockDataEngine:
    def __init__(self, api_token, exchanges_path, timezone="Mexico_City"):
        """
        Initializes the StockDataExtractor with the provided API token and path to the exchanges data.

        Parameters:
        - api_token (str): Your API token for authentication.
        - exchanges_path (str): Path to the parquet file containing exchange data.
        """
        self.api_token = api_token
        self.exchanges = pd.read_parquet(exchanges_path)
        self.exchange_codes = set(self.exchanges["Code"].tolist())
        self.timezone = self._get_timezone(timezone)
        self.api_requests_today = 0
        self.daily_rate_limit = 100000
        self.minute_request_limit = 1000  # Max requests per minute
        self.calls_remaining = self.minute_request_limit
        self.last_api_requests_date = None
        self.user_info = {}
        self.lock = threading.Lock()
        self.rate_limit_lock = threading.Lock()
        self.rate_limit_reset_time = time.time() + 60  # Reset every 60 seconds
        self.rate_limit_semaphore = threading.Semaphore(self.minute_request_limit)
        self.get_user_info()

    def _get_timezone(self, timezone):
        timezones = {
            "Mexico_City":tz('America/Mexico_City'),
            "New_York":tz('America/New_York'),
            "Londres":tz('Europe/London'),
            "Paris":tz('Europe/Paris'),
            "Tokyo":tz('Asia/Tokyo'),
            "Shanghai":tz('Asia/Shanghai'),
        }

        market_schedule = {
            "Mexico_City":{"Open":"08:30:00", "Close":"15:00:00"},
            "New_York":{"Open":"09:30:00", "Close":"16:00:00"},
            "Londres":{"Open":"08:00:00", "Close":"16:30:00"},
            "Paris":{"Open":"09:00:00", "Close":"17:30:00"},
            "Tokyo":{"Open":"09:00:00", "Close":"15:00:00"},
            "Shanghai":{"Open":"09:30:00", "Close":"15:00:00"},
        }

        self.available_timezones = list(timezones.keys())
        self.market_schedule = market_schedule
        
        if timezone not in timezones.keys():
            raise ValueError("Invalid timezone. Availables: Mexico_City, New_York, Londres, Paris, Tokyo, Shangai.")

        return timezones[timezone]

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
            self.calls_remaining = self.daily_rate_limit - self.api_requests_today
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
        if self.calls_remaining >= required_calls:
            return True
        else:
            print(f"Not enough API calls remaining. Required: {required_calls}, Available: {self.calls_remaining}")
            return False

    def _check_tickers_code(self, ticker, exchange_code):
        """
        Checks if the given ticker has a exchange code. If not will assume the default code.
    
        Parameters:
        - ticker (str): The stock ticker in the format 'TICKER.EXCHANGE_CODE' or 'TICKER'.
        - exchange_code (str): The exchange code in the format 'CODE'.
    
        Returns:
        - str: The symbol.
        """
        if "." in ticker: return ticker
        else: return f"{ticker}.{exchange_code}"

    def _filter_tickers(self, tickers):
        """
        Processes the tickers to filter out those with invalid exchange codes.
    
        Parameters:
        - tickers (str or list): The ticker symbol(s).
    
        Returns:
        - tuple: (valid_tickers, invalid_tickers)
            - valid_tickers: list of tickers without '.' and tickers with '.' where the exchange code exists.
            - invalid_tickers: list of tickers where the exchange code does not exist in self.exchange_codes.
        """
        # Ensure tickers is a list
        if isinstance(tickers, str):
            tickers = [tickers]
    
        valid_tickers = []
        invalid_tickers = []
    
        for ticker in tickers:
            if "." in ticker:
                parts = ticker.split(".")
                if len(parts) == 2:
                    code = parts[1]
                    if code in self.exchange_codes:
                        valid_tickers.append(ticker)
                    else:
                        invalid_tickers.append(ticker)
                else:
                    # Invalid format, consider it invalid
                    invalid_tickers.append(ticker)
            else:
                # No exchange code, will be appended later
                valid_tickers.append(ticker)
    
        return valid_tickers, invalid_tickers

    def _extract_url_symbol(self, single_url):
        # Extract the ticker from the URL to label
        if "/eod/" in single_url:
            symbol = single_url.split("/eod/")[1].split("?")[0]
        elif "/intraday/" in single_url:
            symbol = single_url.split("/intraday/")[1].split("?")[0]
        else:
            symbol = "Unknown"

        return symbol

    def _print_progress(self, iteration, total):
        """
        Prints the progress bar in the format:
        [*********************100%**********************]  1 of 2 completed
    
        Parameters:
        - iteration (int): Current iteration number.
        - total (int): Total number of iterations.
        """
        percent = int(100 * iteration / total)
        bar_length = 50  # Total length of the bar
        percent_text = f"{percent}%"
        percent_len = len(percent_text)
    
        # Calculate left and right lengths
        left_length = (bar_length - percent_len) // 2
        right_length = bar_length - left_length - percent_len
    
        # Compute the number of filled positions
        filled_total = int(bar_length * iteration // total)
        filled_left = min(filled_total, left_length)
        filled_right = max(0, filled_total - left_length - percent_len)
    
        # Build the bar segments
        bar_left = '*' * filled_left + '-' * (left_length - filled_left)
        bar_right = '*' * filled_right + '-' * (right_length - filled_right)
        bar = bar_left + percent_text + bar_right
    
        # Print the progress bar
        print(f"\r[{bar}]  {iteration} of {total} completed", end='')

    def _acquire_rate_limit(self):
        """
        Manages rate limiting by acquiring a semaphore slot and resetting it every minute.
        """
        while True:
            current_time = time.time()
            with self.rate_limit_lock:
                if current_time >= self.rate_limit_reset_time:
                    # Reset the semaphore and the timer
                    self.rate_limit_semaphore = threading.Semaphore(self.minute_request_limit)
                    self.rate_limit_reset_time = current_time + 60
                    self.calls_remaining = self.minute_request_limit

            # Try to acquire the semaphore
            acquired = self.rate_limit_semaphore.acquire(blocking=False)
            if acquired:
                break
            else:
                # Wait a bit before trying again
                time.sleep(0.05)

    def _fetch_data(self, single_url, fmt, cost_of_calls):
        """
        Fetches data for a single URL with rate limiting.

        Parameters:
        - single_url (str): The API URL to request.
        - fmt (str): The format of the response ('csv' or 'json').
        - cost_of_calls (int): The cost of API calls.

        Returns:
        - pd.DataFrame: The data fetched from the API.
        """
        # Acquire the rate limit semaphore
        self._acquire_rate_limit()

        try:
            response = requests.get(single_url)
            if response.status_code == 200:
                with self.lock:
                    self.api_requests_today += cost_of_calls
                    self.calls_remaining -= cost_of_calls
                # Parse and return data
                if fmt == "json":
                    df = pd.DataFrame(response.json())
                elif fmt == "csv":
                    df = pd.read_csv(StringIO(response.text))
                else:
                    df = response.text
                return df
        except Exception as e:
            # Log the error and return an empty DataFrame
            print(f"Error fetching data from {single_url}: {e}")
            return pd.DataFrame()
            
        finally:
            # Release the semaphore
            self.rate_limit_semaphore.release()

    def _process_dataframe(self, df):
        """
        Processes the DataFrame to ensure consistent formatting.
    
        Parameters:
        - df (pd.DataFrame): The DataFrame to process.
    
        Returns:
        - pd.DataFrame: The processed DataFrame.
        """
        # Check if 'Adjusted_close' is in the columns
        if "Adjusted_close" not in df.columns and "Close" in df.columns:
            # Intraday data
            df["Adjusted_close"] = df["Close"]
            intraday_cols = ["Datetime", "Open", "High", "Low", "Close", "Adjusted_close", "Volume"]
            df = df[intraday_cols]
            # Rename general columns
            general_cols = ["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"]
            df.columns = general_cols
        elif "Adjusted_close" in df.columns:
            # EOD data
            eod_cols = ["Date", "Open", "High", "Low", "Close", "Adjusted_close", "Volume"]
            df = df[eod_cols]
            # Rename general columns
            general_cols = ["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"]
            df.columns = general_cols
        else:
            # If required columns are missing, return an empty DataFrame
            return pd.DataFrame()
    
        # Reorder columns
        df = df[["Date", "Adj Close", "Close", "High", "Low", "Open", "Volume"]]
    
        # Set index
        df.set_index("Date", inplace=True)
    
        return df

    def _make_api_request(self, url, cost_of_calls, request_type, fmt):
        """
        Makes the API request(s) and processes the response concurrently.
    
        Parameters:
        - url (str or list): The API URL(s) to request.
        - cost_of_calls (int): The cost of API calls.
        - request_type (str): The type of request ('data' or 'tickers').
        - fmt (str): The format of the response ('csv' or 'json').
    
        Returns:
        - tuple: (empty_stocks, data)
            - empty_stocks: List of tickers that returned empty data.
            - data: The processed DataFrame.
        """
        empty_stocks = []
    
        # If the URL is a single string or a list with one element
        if ((isinstance(url, list) and len(url) < 2) or isinstance(url, str)) and (request_type == "ticker" or request_type == "data"):
            if isinstance(url, list):
                url = url[0]
    
            # Make the request
            response = requests.get(url)
            symbol = self._extract_url_symbol(url)
            self.get_user_info()
            if response.status_code == 200:
                self.api_requests_today += cost_of_calls
                # Parse and return data
                if fmt == "json":
                    df = pd.DataFrame(response.json())
                elif fmt == "csv":
                    df = pd.read_csv(StringIO(response.text))
                else:
                    df = response.text
    
                if df.empty:
                    empty_stocks.append(symbol)
                    return (empty_stocks, pd.DataFrame())
    
                # Process DataFrame
                df = self._process_dataframe(df)
                return (empty_stocks, df)
            else:
                empty_stocks.append(symbol)
                print(f"Error fetching {symbol} {request_type}: {response.status_code}")
                return (empty_stocks, pd.DataFrame())
    
        # If the URL is a list with more than one element and the request_type is 'data'
        elif isinstance(url, list) and len(url) > 1 and request_type == "data":
            data_frames = []
            total_urls = len(url)
    
            max_workers = 16  # Limit the number of concurrent threads
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all the requests to the executor
                future_to_url = {executor.submit(self._fetch_data, single_url, fmt, cost_of_calls): single_url for single_url in url}

                for idx, future in enumerate(as_completed(future_to_url), start=1):
                    single_url = future_to_url[future]
                    symbol = self._extract_url_symbol(single_url)

                    try:
                        df = future.result()
                        if df.empty:
                            empty_stocks.append(symbol)
                        else:
                            # Process DataFrame
                            df = self._process_dataframe(df)
                            data_frames.append((symbol, df))
                    except Exception as e:
                        empty_stocks.append(symbol)
                        print(f"\nError fetching data for {symbol}: {e}")

                    # After processing each URL, print the progress
                    self._print_progress(idx, total_urls)
    
            # Concatenate DataFrames into a MultiIndex with the required format
            if data_frames:
                # Create a list of all tickers
                tickers = [symbol for symbol, _ in data_frames]
                # Create a list of all DataFrames
                dfs = [df for _, df in data_frames]
    
                # Concatenate the DataFrames along the columns
                df_combined = pd.concat(dfs, axis=1, keys=tickers)
    
                # Rearrange the levels of the MultiIndex
                df_combined.columns = df_combined.columns.swaplevel(0, 1)
    
                # Sort the column levels according to the desired format
                desired_order = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]
                df_combined = df_combined.reindex(columns=desired_order, level=0)
    
                return (empty_stocks, df_combined)
            else:
                return (empty_stocks, pd.DataFrame())
    
        else:
            print("Invalid parameters for _make_api_request.")
            return (empty_stocks, pd.DataFrame())

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
                end = datetime.utcnow().replace(tzinfo=self.timezone).strftime("%Y-%m-%d")
            else:
                if not isinstance(end, str):
                    end = end.strftime("%Y-%m-%d")
                end = datetime.strptime(end, "%Y-%m-%d").replace(tzinfo=self.timezone).strftime("%Y-%m-%d")

            if start is None:
                start = (datetime.strptime(end, "%Y-%m-%d") - BDay(252)).replace(tzinfo=self.timezone).strftime("%Y-%m-%d")
            else:
                if not isinstance(start, str):
                    start = start.strftime("%Y-%m-%d")
                start = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=self.timezone).strftime("%Y-%m-%d")

        else:
            max_days = max_periods[interval]

            # Convert start and end from string to datetime
            if end is None:
                end = datetime.utcnow().replace(tzinfo=self.timezone)
            else:
                if not isinstance(end, str):
                    end = end.strftime("%Y-%m-%d %H:%M:%S")
                end = datetime.strptime(end, "%Y-%m-%d %H:%M:%S").replace(tzinfo=self.timezone)

            if start is None:
                start = end - BDay(max_days)
            else:
                if not isinstance(start, str):
                    start = start.strftime("%Y-%m-%d %H:%M:%S")
                start = datetime.strptime(start, "%Y-%m-%d %H:%M:%S").replace(tzinfo=self.timezone)

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
                        symbol = self._check_tickers_code(ticker, exchange_code)
                        url_ = (
                            f"https://eodhd.com/api/eod/{symbol}"
                            f"?api_token={self.api_token}&fmt={fmt}&period={period}&from={start}&to={end}&order={order}"
                        )
                        url.append(url_)
                else:
                    if isinstance(tickers, list):
                        ticker = tickers[0]

                    symbol = self._check_tickers_code(ticker, exchange_code)
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
                            symbol = self._check_tickers_code(ticker, exchange_code)
                            url_ = (
                                f"https://eodhd.com/api/intraday/{symbol}"
                                f"?api_token={self.api_token}&fmt={fmt}&interval={interval}&from={start_unix}&to={end_unix}"
                            )
                            url.append(url_)
                else:
                    if isinstance(tickers, list):
                        ticker = tickers[0]

                    for dates in date_ranges:
                        (start_unix, end_unix) = dates
                        symbol = self._check_tickers_code(ticker, exchange_code)
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

        # Filter tickers to separate valid and invalid ones
        valid_tickers, invalid_tickers = self._filter_tickers(tickers)

        # Create urls for every valid ticker
        url = self._get_rest_url(
            tickers=valid_tickers,
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

        (empty_stocks, data) = self._make_api_request(url=url, cost_of_calls=cost_of_calls, request_type=request_type, fmt=fmt)

        if invalid_tickers:
            # Format the tickers into a string
            invalid_tickers_str = ', '.join(invalid_tickers)
            # Create the exception message
            invalid_exception_message = f"\n{len(invalid_tickers)} Failed download:\n{invalid_tickers}: Exception('{invalid_tickers_str}: The exchange code is not listed')"
            # Raise the exception with the formatted message
            print(invalid_exception_message)

        if empty_stocks:
            # Format the tickers into a string
            tickers_str = ', '.join(empty_stocks)
            # Create the exception message
            exception_message = f"\n{len(empty_stocks)} Failed download:\n{empty_stocks}: Exception('{tickers_str}: No data found, symbol may be delisted')"
            # Raise the exception with the formatted message
            print(exception_message)

        if data.empty:
            return pd.DataFrame()

        else:
            return data.sort_index()

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
