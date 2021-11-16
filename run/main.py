from run.stock_bot import StockBot


if __name__ == "__main__":
    bot = StockBot()
    bot.start(start_time_iso_utc="now", start_date_iso_utc="2021-06-03", count_24_hr_at_iso_utc_time="19:59:59")  # Los angelos time 12:59:59
