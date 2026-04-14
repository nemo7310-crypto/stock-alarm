import os
import datetime
import requests
import FinanceDataReader as fdr

# 깃허브 Secrets에서 가져오기
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID        = os.environ.get('CHAT_ID')

# 스크리닝 조건
MIN_CHANGE_RATE   = 10.0                 # 등락률 +10% 이상
MIN_TRADING_VALUE = 200_000_000_000      # 거래대금 2,000억 이상
MAX_MARKET_CAP    = 30_000_000_000_000   # 시가총액 30조 이하


def get_stocks():
    now   = datetime.datetime.now() + datetime.timedelta(hours=9)
    today = now.strftime("%Y-%m-%d")

    try:
        # 코스피 + 코스닥 전종목 수집
        kospi  = fdr.StockListing('KOSPI')
        kospi["시장"] = "KOSPI"
        kosdaq = fdr.StockListing('KOSDAQ')
        kosdaq["시장"] = "KOSDAQ"

        import pandas as pd
        df = pd.concat([kospi, kosdaq], ignore_index=True)

        # 조건 필터링
        filtered = df[
            (df['ChagesRatio'] >= MIN_CHANGE_RATE) &
            (df['Amount']      >= MIN_TRADING_VALUE) &
            (df['Marcap']      >  0) &
            (df['Marcap']      <= MAX_MARKET_CAP)
        ].copy()

        # 등락률 내림차순 정렬
        filtered = filtered.sort_values('ChagesRatio', ascending=False)

        if filtered.empty:
            return (
                f"📢 [{today}] 조건 충족 종목 없음\n"
                f"(등락률≥10% / 거래대금≥2000억 / 시총≤30조)"
            )

        msg = f"🚀 [{today}] 코스피/코스닥 급등 스크리닝\n"
        msg += f"등락률≥10% | 거래대금≥2000억 | 시총≤30조\n"
        msg += f"총 {len(filtered)}종목\n"

        for _, row in filtered.head(20).iterrows():
            name       = row['Name']
            market     = row['시장']
            change_pct = row['ChagesRatio']
            value_bn   = int(row['Amount'] / 100_000_000)
            cap_jo     = row['Marcap'] / 1_000_000_000_000
            close      = int(row['Close'])

            msg += (
                f"\n✅ [{market}] {name}\n"
                f"   현재가: {close:,}원  등락률: +{change_pct:.2f}%\n"
                f"   거래대금: {value_bn:,}억  시총: {cap_jo:.1f}조"
            )

        return msg

    except Exception as e:
        return f"📢 [{today}] 오류 발생: {str(e)}"


def send_msg(text):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print("⚠️ TELEGRAM_TOKEN 또는 CHAT_ID가 없습니다. GitHub Secrets 확인하세요.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    requests.post(url, data={"chat_id": CHAT_ID, "text": text})


if __name__ == "__main__":
    print("스크립트 시작")
    message = get_stocks()
    print(f"메시지:\n{message}")
    send_msg(message)
    print("전송 완료")
