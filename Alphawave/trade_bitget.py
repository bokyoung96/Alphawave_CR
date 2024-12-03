import math
import ccxt
import time
from datetime import datetime, timedelta


def place_order(exchange, symbol, leverage, side, amount, target_time):

    exchange.set_position_mode(False, symbol)

    try:
        # 레버리지 설정
        market = exchange.market(symbol)
        exchange.set_leverage(leverage, symbol)

        # 타겟타임까지 기다리기
        while datetime.now() < target_time:
            time.sleep(0.1)
        
        params = {'reduceOnly': False}       # 마켓오더 생성
        order = exchange.create_order(
            symbol=symbol,
            type='market',
            side=side,
            amount=amount,
            params=params
        )

        return order

    except ccxt.BaseError as e:
        return None
    
import time

def close_position(exchange, symbol, side, delay_seconds):

    try:
        # 포지션 닫을 때까지 기다리기
        time.sleep(delay_seconds)

        # 닫을 포지션 정보 가지고 오기
        positions = exchange.fetch_positions([symbol])
        print(positions)
        position_side = 'long' if side == 'sell' else 'short' 
        position = positions[0]

        if not position:
            # print(f"No open {position_side} position found for {symbol}.")
            return None

        amount = position['contracts']  # 
        # print(f"Closing {position_side} position for {symbol}, amount: {amount}")

        # 포지션 닫는 오더 생성
        params = {
            'reduceOnly': True  # Ensure the order reduces the position
        }
        order = exchange.create_order(
            symbol=symbol,
            type='market',
            side=side,  # Side to close the position
            amount=amount,
            params=params
        )

        # print(f"Position closed successfully: {order}")
        return order

    except ccxt.BaseError as e:
        # print(f"An error occurred: {e}")
        return None


# API 설정
api_key = 'bg_bc97a1b614aa537633573841301705db'
secret_key = 'ddfef7c1e34657a982fdde7987ac4f91cd13dd28d8adffa624c8f29bd84d4782'
password = 'qkseltqnf12'  # 비트겟의 경우 패스워드도 필요함

# 비트겟 거래소 인스턴스 생성
exchange = ccxt.bitget({
    'apiKey': api_key,
    'secret': secret_key,
    'password': password,  
    'options': {'defaultType': 'swap'},  # 선물 트레이딩 옵션
})

balance = exchange.fetch_balance()

# 파라미터 입력
symbol = 'XVG/USDT:USDT'  
leverage = 1
side = 'buy'  # 'sell' for short
close_side = 'sell' if side == 'buy' else 'buy'

# 매매 개수 구하기
usdt_balance = balance['total']['USDT']
coin_price = exchange.fetch_ticker(symbol)['last']  
buffer = 0.9
amount = math.floor((usdt_balance / coin_price) * buffer)

# 타겟타임 입력
# now = datetime.now()
now = datetime.now() + timedelta(days=1)

target_time = now.replace(hour=0, minute=59, second=58, microsecond=0)

# 딜레이 타임 입력(초단위)
delay_seconds = 4

# 오더 생성하고 딜레이 타임 지나면 포지션 닫기
print(f"매수 목표 시간: {target_time}")

market_order = place_order(exchange, symbol, leverage, side, amount, target_time)
if market_order:
    print("매수 주문 완료:", market_order)
    close_order = close_position(exchange, symbol, close_side, delay_seconds=delay_seconds)
    if close_order:
        print("포지션 청산 완료", close_order)