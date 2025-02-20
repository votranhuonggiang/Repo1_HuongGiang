import requests
import time
import json
import pandas as pd

class FetchData:
    def __init__(self, max_retries=5, delay=1):
        self.max_retries = max_retries
        self.delay = delay

    def fetch_yahoo_finance(self, symbol, start="2013-01-01", end="2025-02-12", interval="1d"):
        """
        Fetches all historical stock data from Yahoo Finance.
        Useful for global datasets (e.g., stocks in the UK).
        """
        period1 = int(pd.Timestamp(start).timestamp())
        period2 = int(pd.Timestamp(end).timestamp())

        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?period1={period1}&period2={period2}&interval={interval}"
        headers = {"User-Agent": "Mozilla/5.0"}

        session = requests.Session()
        session.headers.update(headers)

        for attempt in range(self.max_retries):
            response = session.get(url)
            if response.status_code == 200:
                break
            elif response.status_code == 429:
                time.sleep(5 * (attempt + 1))
            else:
                raise Exception(f"Error {response.status_code}: {response.reason}")

        data = response.json().get("chart", {}).get("result", [{}])[0]
        if "timestamp" not in data or "indicators" not in data:
            raise ValueError("Invalid response format")

        df = pd.DataFrame(data["indicators"]["quote"][0])
        df["time"] = pd.to_datetime(data["timestamp"], unit="s").normalize()
        df.set_index("time", inplace=True)

        return df.dropna().assign(index=symbol)

    def fetch_vietcap(self, symbol, timeframe="ONE_DAY", count_back=3850, to_timestamp=1739491200):
        """
        Fetches all historical stock data from Vietcap API.
        Useful for Vietnamese datasets (e.g., stocks in Vietnam).
        """
        url = "https://trading.vietcap.com.vn/api/chart/OHLCChart/gap-chart"
        
        payload = json.dumps({
            "timeFrame": timeframe,
            "symbols": [symbol],
            "countBack": count_back,
            "to": to_timestamp
        })
        
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        
        response = requests.post(url, headers=headers, data=payload)
        time.sleep(self.delay)
        
        if response.status_code != 200:
            raise Exception(f"Error {response.status_code}: {response.reason}")
        
        df = pd.DataFrame(response.json()).explode(['o', 'h', 'l', 'c', 'v', 't']).rename(columns={
            't': 'time', 'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'volume'
        })
        
        df['time'] = pd.to_datetime(df['time'], unit='s')
        df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].apply(pd.to_numeric, errors='coerce')
        
        return df.set_index('time')[['open', 'high', 'low', 'close', 'volume']].assign(index=symbol)

# Example usage
if __name__ == "__main__":
    fetcher = FetchData()

    sp500 = fetcher.fetch_yahoo_finance("^GSPC")
    print(sp500)

    vnindex = fetcher.fetch_vietcap('VNINDEX')
    print(vnindex)
``` ▋
