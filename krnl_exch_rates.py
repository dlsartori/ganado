import json
import requests
from krnl_config import krnl_logger

# Python program to get the real-time currency exchange rate
# https://www.geeksforgeeks.org/python-get-the-real-time-currency-exchange-rate/
# https://www.alphavantage.co/documentation/
# IMPORTANT: API KEY for alphaadvantage access is: 6ZSOD2KSZ8F7P275.
# TODO: MAX FREE requests per day at alphaadvantage: 500 or roughly 1 every 3 minutes.

API_KEY = "6ZSOD2KSZ8F7P275"
# Function to get real time currency exchange
def RealTimeExchangeRate(from_cur, to_cur, apikey):
    # base_url variable store base url
    base_url = r"https://www.alphavantage.co/query?function=CURRENCY_EXCHANGE_RATE"

    # main_url variable store complete url
    main_url = base_url + "&from_currency=" + from_cur + "&to_currency=" + to_cur + "&apikey=" + apikey

    # get method of requests module. Return response object
    req_ob = requests.get(main_url)

    # json method return json format data into python dictionary data type. Result contains list of nested dictionaries
    result = req_ob.json()

    print(" Result before parsing the json data :\n", result)
    print("\n After parsing : \n Realtime Currency Exchange Rate for",
          result["Realtime Currency Exchange Rate"]
          ["2. From_Currency Name"], "TO",
          result["Realtime Currency Exchange Rate"]
          ["4. To_Currency Name"], "is",
          result["Realtime Currency Exchange Rate"]
          ['5. Exchange Rate'], to_cur)

    return result["Realtime Currency Exchange Rate"]['5. Exchange Rate']





if __name__ == "__main__":
    from_currency = "USD"
    to_currency = "ARS"
    api_key = API_KEY

    # function calling
    try:
        res = RealTimeExchangeRate(from_currency, to_currency, api_key)
        print(f'\nExchange rate {from_currency} to {to_currency} : {res}')
    except(KeyError, ValueError, TypeError) as e:
        krnl_logger.info(f'ERR_INP_Currency exchange input error: {e}')


