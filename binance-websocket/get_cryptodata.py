# coding=u8
import ccxt
import pandas as pd
import time
import sys
from datetime import datetime


exchange = ccxt.binance({'enableRateLimit': True})


hold = 30
limit = 1000


def download(symbol, from_timestamp, end_timestamp, timeframe):
    # set timeframe in msecs
    tf_multi = exchange.parse_timeframe(timeframe) * 1000

    data = []
    candle_no = (int(end_timestamp) - int(from_timestamp)) / tf_multi + 1
    print('downloading...')
    while from_timestamp < end_timestamp and (int(time.time()*1000) > from_timestamp):
        # from_timestamp 要小于现在时间
        try:
            print('from timestamp: %s' % from_timestamp)
            ohlcvs = exchange.fetch_ohlcv(symbol, timeframe, from_timestamp, limit)
            # within the from_timestamp > ohlcvs > end range
            if (ohlcvs[0][0] > end_timestamp) or (ohlcvs[-1][0] > end_timestamp):
                print("got a candle out of range! start:%s;  end:%s: end_timestamp:%s" % (ohlcvs[0][0], ohlcvs[-1][0], end_timestamp))
                break
            # ---------------------------------------------------------------------
            # 更新from_timestamp
            from_timestamp += len(ohlcvs) * tf_multi
            # 追加新获取到的data
            data += ohlcvs
            print(str(len(data)) + ' of ' + str(int(candle_no)) + ' candles loaded...')
        except (ccxt.ExchangeError, ccxt.AuthenticationError, ccxt.ExchangeNotAvailable, ccxt.RequestTimeout) as error:
            print('Got an error', type(error).__name__, error.args, ', retrying in', hold, 'seconds...')
            time.sleep(hold)
    return data


def save_data(data, filename):
    header = ['Time', 'Open', 'High', 'Low', 'Close', 'Volume']
    df = pd.DataFrame(data, columns=header)

    df.set_index('Time', inplace=True)
    print(df.head())
    df.to_csv(f'{filename}.csv')


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('请输入历史数据的起始日期，比如：2020-01-01')
        sys.exit()
    start_date = sys.argv[2]
    end_date = str(datetime.now())[:19]

    symbol = sys.argv[1]
    if not symbol:
        symbol = 'BTC/USDT'
    symbol_name = symbol.replace('/','')

    timeframe = sys.argv[3]
    if not timeframe:
        timeframe = '15m'

    from_timestamp = exchange.parse8601(f'{start_date} 00:00:00')
    end_timestamp = exchange.parse8601(f'{end_date} 00:00:00')

    data = download(symbol, from_timestamp, end_timestamp, timeframe)

    filename = f'{symbol_name}_{start_date}'
    save_data(data, filename)


# 使用示例：python3 cryptoData.py BTC/USDT 2020-01-01 15m