import requests
import json
import pandas as pd


class PolygonAllTickers:
    def __init__(self, api_key, active="true", response_limit=1000, market="stocks"):
        self.api_key = api_key
        self.active = active
        self.response_limit = response_limit
        self.market = market
        self._ticker_data = []
        self.base_query = f"https://api.polygon.io/v3/reference/tickers?market={market}&active={active}&sort=ticker&order=asc&limit={response_limit}"
        self.and_api_key = f"&apiKey={api_key}"

    def get_ticker_names(self):
        ticker_list = []
        for ticker_data in self._ticker_data:
            for tickers in ticker_data["results"]:
                ticker_list.append(tickers["ticker"])
        return ticker_list

    def get_company_names(self):
        company_list = []
        for ticker_data in self._ticker_data:
            for tickers in ticker_data["results"]:
                company_list.append(tickers["name"])
        return company_list

    def get_ticker_company_tuple(self):
        data = []
        for ticker_data in self._ticker_data:
            for ticks in ticker_data["results"]:
                data.append((ticks["ticker"], ticks["name"]))
        return data

    def fetch_tickers_data(self):
        query = self.base_query
        continue_fetching = True
        while continue_fetching:
            result = requests.get(query + self.and_api_key).json()
            self._ticker_data.append(result)
            print("Fetching")
            try:
                query = result["next_url"] + self.and_api_key
            except Exception as e:
                continue_fetching = False
                print("Fetched")
        return self


def update_ticker_list_file(api_key):
    tickers = PolygonAllTickers(api_key=api_key)
    tickers.fetch_tickers_data()
    df = pd.DataFrame(columns=["Name", "symbol"])
    df["Name"] = tickers.get_company_names()
    df["symbol"] = tickers.get_ticker_names()
    df.to_csv("../app_data/company_tickers.csv", index=False)


if __name__ == "__main__":
    api_key = "Zay2cQZwZfUTozLiLmyprY4Sr3uK27Vp"
    update_ticker_list_file(api_key)