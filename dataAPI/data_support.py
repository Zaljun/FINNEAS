import requests
import websocket

ALPACA_HOST = 'https://data.alpaca.markets'
alpaca_session = requests.Session()
alpaca_session.headers["APCA-API-KEY-ID"] = 'PK9SPEPNIYHXF6MH3PQI'
alpaca_session.headers["APCA-API-SECRET-KEY"] = 'zMU0UivOioKFg8WTFKMk7mAPcrVbqvTm6FNeVAWJ'

ALPHAV_HOST = 'https://www.alphavantage.co/query'
alphav_session = requests.Session()
alphav_session.params["apikey"] = '29GR5RNIYPUF47T1'

NEWSAPI_HOST = 'https://newsapi.org/v2'
newsapi_session = requests.Session()
newsapi_session.params["apiKey"] = 'd65c94e8083e4fa8b1f1a8d3a2ab8ebe'


options = {
    'alpaca':alpaca_session.get,
    'newsapi':newsapi_session.get,
    'alphav':alphav_session.get
}


def _get_data(API, url, params=None):
    response = options[API](url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        return response.raise_for_status()



# General financial data

def get_historical(request):
    '''
    Returns historical open, high, low, close (for charting you use the closing price).
    Timespan can be either of 1min, 15mins, hour, day.
    Limit is maximum number of data points.
    request = {
        'symbol':'AAPL',
        'timespan':'day',
        'from':'2020-01-01',
        'to':'2019-02-01'
    }
    '''
    url     = f"{ALPACA_HOST}/v1/bars/{request['timespan']}"
    params  = {
        'symbols':request['symbol'],
        'limit': request['limit'],
        'start':request['from'],
        'end':request['to']
    }
    return _get_data('alpaca', url, params)


def get_company_overview(request):
    '''
    Returns company data like description, exchange it trades at, market cap, etc
    request = {
        'symbol':'AAPL'
    }
    '''
    url     = ALPHAV_HOST
    params  = {
        'function':'OVERVIEW',
        'symbol':request['symbol']
    }
    return _get_data('alphav', url, params)


def get_last_price(request):
    '''
    Returns last price
    request = {
        'symbol':'AAPL'
    }
    '''
    url     = f"{ALPACA_HOST}/v1/last/stocks/{request['symbol']}"
    return _get_data('alpaca', url)


def get_last_quote(request):
    '''
    Returns last bid and ask
    request = {
        'symbol':'AAPL'
    }
    '''
    url     = f"{ALPACA_HOST}/v1/last_quote/stocks/{request['symbol']}"
    return _get_data('alpaca', url)


# Financial Statements

def get_IS(request):
    '''
    Returns Income Statement
    request = {
        'symbol':'AAPL'
    }
    '''
    url     = ALPHAV_HOST
    params  = {
        'function':'INCOME_STATEMENT',
        'symbol':request['symbol']
    }
    return _get_data('alphav', url, params)


def get_BS(request):
    '''
    Returns Balance Sheet
    request = {
        'symbol':'AAPL'
    }
    '''
    url     = ALPHAV_HOST
    params  = {
        'function':'BALANCE_SHEET',
        'symbol':request['symbol']
    }
    return _get_data('alphav', url, params)


def get_CS(request):
    '''
    Returns Cash Flow Statement
    request = {
        'symbol':'AAPL'
    }
    '''
    url     = ALPHAV_HOST
    params  = {
        'function':'CASH_FLOW',
        'symbol':request['symbol']
    }
    return _get_data('alphav', url, params)

            

# News

def get_general_news(request):
    '''
    Returns general business news from the US.
    request = {
        'from':'2020-10-15T15:30:39'
    }
    '''
    url     = f"{NEWSAPI_HOST}/top-headlines"
    params  = {
        'country':'us',
        'category':'business',
        'from':request['from']
    }
    return _get_data('newsapi', url, params)

def get_company_news(request):
    '''
    Returns company news.
    request = {
        'symbol':'AAPL',
        'from':'2020-10-15T15:30:39'
    }
    '''
    url     = f"{NEWSAPI_HOST}/everything"
    params  = { 
        'language':'en',
        'q':request['symbol'],
        'from':request['from'],
        'sortBy':'relevance'
    }
    return _get_data('newsapi', url, params)


# Streaming
websocket.enableTrace(True)

class PriceStream:

    ALPACA_KEY      = 'PK9SPEPNIYHXF6MH3PQI'
    ALPACA_SECRET   = 'zMU0UivOioKFg8WTFKMk7mAPcrVbqvTm6FNeVAWJ'
    ALPACA_STR_HOST = "wss://data.alpaca.markets/stream"

    def __init__(self, initial_symbols):
        #self.currently_streaming = []
        '''
        initial_symbols = ['AAPL', 'MSFT']
        '''
        self.initial_symbols = str([f'T.{s}' for s in initial_symbols])
    
    def run(self):
        '''
        Starts the streaming with initial symbols.
        '''
        self.ws = websocket.WebSocketApp(self.ALPACA_STR_HOST)
        self.ws.on_message = self._on_message
        self.ws.on_open = self._on_open
        self.ws.on_close = self._on_close
        self.ws.run_forever()

    def _on_message(self, message):
        print(message)

    def _on_error(self, error):
        print(error)

    def _on_open(self):
        self.ws.send(f'{{"action":"authenticate","data":{{"key_id":{self.ALPACA_KEY},"secret_key":{self.ALPACA_SECRET}}}}}')
        self.ws.send(f'{{"action":"listen","data": {{"streams":{self.initial_symbols}}}}}')

    def _on_close(self):
        print('-closed-')

    def subscribe(self, request):
        '''
        Adds a new symbol to the streaming.
        request = {
            symbol:'AAPl'
        }
        '''
        self.ws.send(f'{{"action":"listen","data": {{ "streams": [T.{request["symbol"]}] }}}}')
        
    def unsubscribe(self, request):
        '''
        Removes a stock symbol from the streaming
        request = {
            symbol:'AAPl'
        }
        '''
        self.ws.send(f'{{"action":"unlisten","data": {{ "streams": [T.{request["symbol"]}] }}}}')
    



if __name__ == '__main__':
    import json

    # SAMPLE INPUTS

    # for get_historical(), timespan can be either of 1min, 15min, hour, day
    historical_req  = {
        'symbol':'AAPL',
        'timespan':'day', 
        'limit': '500',
        'from':'2020-01-01',
        'to':'2018-09-01'
    }

    # for get_company_news()
    company_news_req = { #fix: make symbol map to company name
        'symbol':'Apple',
        'from':'2020-10-16T19:30:00'
    }

    # for get_general_news()
    general_news_req = {
        'from':'2020-10-15T19:30:00',
    }

    # for functions that only need the symbol name 
    # get_company_overview(), get_last_price(), get_last_quote(),
    # get_IS(), get_BS(), get_CS()
    symbol          = {
        'symbol':'AAPL'
    }

    # API call
    data = get_CS(symbol)
    print(data)


    # Streaming
    # streamer = PriceStream(['AAPL','MSFT'])
    # streamer.run()


    # Write to json
    # json_object = json.dumps(data, indent = 4)
    # with open("data_samples/cash_flow.json", "w") as outfile: 
    #     outfile.write(json_object)
    # outfile.close()
