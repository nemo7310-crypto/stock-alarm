import os
from pykrx import stock
import datetime
import requests

# 깃허브 금고(Secrets)에서 정보를 안전하게 가져옵니다
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID = os.environ.get('CHAT_ID')

def get_stocks():
    # 깃허브 서버 시간(UTC)을 한국 시간으로 변환 (+9시간)
    now = datetime.datetime.now() + datetime.timedelta(hours=9)
    today = now.strftime("%Y%m%d")
    
    try:
        # 전종목 거래대금 정보 가져오기
        df = stock.get_market_ohlcv_by_ticker(today, market="ALL")
        names = stock.get_market_ticker_name(today)

        # 거래대금 2,000억(200,000,000,000원) 이상 필터링
        threshold = 200_000_000_000
        target_df = df[df['거래대금'] >= threshold]

        if target_df.empty:
            return f"📢 [{today}] 거래대금 2,000억 상회 종목이 없습니다."

        msg = f"📢 [{today}] 거래대금 2천억 상회 종목\n"
        for ticker, row in target_df.iterrows():
            name = names.get(ticker, "알 수 없음")
            value_bn = int(row['거래대금'] / 100_000_000) # 억 단위 변환
            change_pct = row['등락률']
            msg += f"\n✅ {name}: {value_bn:,}억 ({change_pct:+.2f}%)"
        
        return msg
    except Exception as e:
        return f"📢 [{today}] 데이터를 조회할 수 없습니다. (휴장일 또는 데이터 집계 중)"

def send_msg(text):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print("설정된 토큰이나 ID가 없습니다. Secrets 설정을 확인하세요.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

    requests.post(url, data={"chat_id": CHAT_ID, "text": text})

if __name__ == "__main__":
    message = get_stocks()
    send_msg(message)
