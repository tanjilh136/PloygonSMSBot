import pandas as pd


class OTCList:
    def __init__(self):
        self.all_symbols = []
        self.all_company = []

    def get_all_symbols(self):
        df = pd.read_csv("../app_data/company_tickers.csv")
        self.all_symbols = list(df["symbol"])
        self.all_company = list(df["Name"])
        return self

    def get_symbols_endswith(self, ending_str):
        result = []
        ending_str = ending_str.lower()
        for symbol in self.all_symbols:
            if symbol.lower().endswith(ending_str):
                result.append(symbol)
        return result

    def is_symbol_exists(self, symbol):
        symbol = symbol.upper()
        if symbol in self.all_symbols:
            return True
        else:
            return False

    def get_symbols_ends_with_w_and_symbol_exists_without_w(self, ends_with="w"):
        result = []
        ends_with = ends_with.lower()
        possible_otc_list = self.get_symbols_endswith(ends_with)
        for possible_otc in possible_otc_list:
            without_ends_with = possible_otc[0:len(possible_otc) - 1]
            if self.is_symbol_exists(without_ends_with):
                result.append(possible_otc)
        return result

    def produce_otc(self):
        otc = OTCList()
        otc.get_all_symbols()
        existing_otc = pd.read_csv("../app_data/otc_ticks.csv")

        existing_otc_list = list(existing_otc["symbol"])
        otc_ends_with_ws = otc.get_symbols_endswith(".WS")
        otc_ends_with_w = otc.get_symbols_ends_with_w_and_symbol_exists_without_w("W")
        print(f"Existing: {len(existing_otc)}".center(100, " "))
        print(f"ENDS WITH .WS: {len(otc_ends_with_ws)}".center(100, " "))
        print(f"ENDS WITH .W and more: {len(otc_ends_with_w)}".center(100, " "))
        # Merge the arrays
        update_otc_symbols = [*existing_otc_list, *otc_ends_with_ws, *otc_ends_with_w]
        df = pd.DataFrame()
        df["otc_symbols"] = update_otc_symbols
        df.to_csv("../app_data/otc_symbols.csv", index=False)

    def get_all_symbols_without_otc(self):
        df = pd.read_csv("../app_data/otc_symbols.csv")
        otc_symbols = df["otc_symbols"]
        otc_symbols = list(otc_symbols)
        final_symbols = []
        final_company = []
        otc_found = []
        for i in range(0, len(self.all_symbols)):
            if self.all_symbols[i] not in otc_symbols:
                final_symbols.append(self.all_symbols[i])
                final_company.append(self.all_company[i])
            else:
                otc_found.append(self.all_symbols[i])
        print(f"OTC FOUND: {len(otc_found)}")
        print(otc_found)
        non_otc_csv = pd.DataFrame()
        non_otc_csv["Name"] = final_company
        non_otc_csv["symbol"] = final_symbols
        non_otc_csv.to_csv("../app_data/non_otc_symbols.csv", index=False)


if __name__ == "__main__":
    otc = OTCList()
    otc.get_all_symbols()
    otc.get_all_symbols_without_otc()
