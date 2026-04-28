"""
github_new_high.py — 52주 신고가 근접 종목을 텔레그램으로 전송.

기존 급등주 스크립트와 동일 패턴 (단일 스크립트 / 환경변수 / fdr 사용).
GitHub Actions 매일 16:00 KST 자동 실행 → stock-alarm 레포에 배포.

원칙.md 충실:
  - 가격/등락률/시총 상한 필터 X
  - 단일 조건: 52주 최고 종가의 NEAR_HIGH_PCT% 이상
  - 시총·거래량 하한 = 유동성 가드 (책 철학과 무관)

환경변수 (GitHub Secrets):
  TELEGRAM_TOKEN
  CHAT_ID
"""
import os
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pandas as pd
import FinanceDataReader as fdr

# 깃허브 Secrets
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID        = os.environ.get('CHAT_ID')

# 신고가 기준
NEAR_HIGH_PCT     = 95.0                    # 52주 최고 종가의 95% 이상
LOOKBACK_DAYS     = 380                     # 약 1년 + 여유
MIN_MARKET_CAP    = 50_000_000_000          # 500억 (유동성 가드)
MIN_AVG_VOLUME    = 50_000                  # 5만주
TOP_N             = 30                      # 메시지 TOP N
WORKERS           = 10                      # 병렬 fetch


def fetch_52w(code: str):
    """일봉 ~1년 → 주봉(금) 종가 시계열 → 52주 최고 종가 비율 계산."""
    end = datetime.datetime.now()
    start = end - datetime.timedelta(days=LOOKBACK_DAYS)
    try:
        df = fdr.DataReader(code, start, end)
        if df is None or df.empty:
            return None
        df.columns = [c.lower() for c in df.columns]
        weekly = df.resample('W-FRI').agg({'close': 'last'}).dropna()
        recent = weekly.tail(52)
        if len(recent) < 26:
            return None
        high_52w   = float(recent['close'].max())
        last_close = float(recent['close'].iloc[-1])
        if high_52w <= 0:
            return None
        pct = last_close / high_52w * 100.0
        # 신고가가 언제 찍혔나 (몇 주 전)
        high_idx = recent['close'].idxmax()
        weeks_since = int((recent.index[-1] - high_idx).days // 7)
        return {
            'code':            code,
            'high_52w':        high_52w,
            'last_close':      last_close,
            'pct_of_high':     pct,
            'weeks_since':     weeks_since,
        }
    except Exception:
        return None


def get_stocks():
    now   = datetime.datetime.now() + datetime.timedelta(hours=9)
    today = now.strftime("%Y-%m-%d")

    try:
        # 코스피 + 코스닥 전종목
        kospi  = fdr.StockListing('KOSPI')
        kospi["시장"]  = "KOSPI"
        kosdaq = fdr.StockListing('KOSDAQ')
        kosdaq["시장"] = "KOSDAQ"
        df = pd.concat([kospi, kosdaq], ignore_index=True)

        # 유동성 프리필터 (시총·거래량) — 원칙 위반 X (가격/추세 필터 아님)
        df = df[
            (df['Marcap'].fillna(0) >= MIN_MARKET_CAP) &
            (df['Volume'].fillna(0) >= MIN_AVG_VOLUME)
        ].copy()

        # 각 종목 52주 신고가 비율 계산 (병렬)
        results = []
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futures = {ex.submit(fetch_52w, row['Code']): row for _, row in df.iterrows()}
            for fut in as_completed(futures):
                row = futures[fut]
                res = fut.result()
                if res is None:
                    continue
                if res['pct_of_high'] < NEAR_HIGH_PCT:
                    continue
                results.append({
                    'code':         row['Code'],
                    'name':         row['Name'],
                    'market':       row['시장'],
                    'marcap':       row['Marcap'],
                    'last_close':   res['last_close'],
                    'pct_of_high':  res['pct_of_high'],
                    'weeks_since':  res['weeks_since'],
                })

        if not results:
            return (
                f"📢 [{today}] 52주 신고가 근접 종목 없음\n"
                f"(기준: 52주 최고 종가의 {NEAR_HIGH_PCT:.0f}% 이상)"
            )

        # 신고가 비율 내림차순
        results.sort(key=lambda r: r['pct_of_high'], reverse=True)

        msg  = f"📈 [{today}] 52주 신고가 근접\n"
        msg += f"기준: 52주 최고 × {NEAR_HIGH_PCT:.0f}% 이상\n"
        msg += f"총 {len(results)}종목 (TOP {min(TOP_N, len(results))})\n"

        for r in results[:TOP_N]:
            cap_jo = r['marcap'] / 1_000_000_000_000
            cap_str = (
                f"{cap_jo:.1f}조" if cap_jo >= 1
                else f"{int(r['marcap'] / 100_000_000):,}억"
            )
            tag = "🔥신고가" if r['weeks_since'] == 0 else f"{r['weeks_since']}주전 고점"
            msg += (
                f"\n✅ [{r['market']}] {r['name']}\n"
                f"   현재가: {int(r['last_close']):,}원  "
                f"({r['pct_of_high']:.1f}%/52w)\n"
                f"   시총: {cap_str}  ·  {tag}"
            )
        return msg
    except Exception as e:
        return f"📢 [{today}] 오류 발생: {str(e)}"


def send_msg(text):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print("⚠️ TELEGRAM_TOKEN 또는 CHAT_ID 가 없습니다. GitHub Secrets 확인.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    # 4096자 한도 → 라인 경계로 분할 전송
    chunks = []
    remaining = text
    while remaining:
        if len(remaining) <= 3900:
            chunks.append(remaining)
            break
        cut = remaining.rfind("\n", 0, 3900)
        if cut == -1:
            cut = 3900
        chunks.append(remaining[:cut])
        remaining = remaining[cut:].lstrip("\n")
    for chunk in chunks:
        requests.post(url, data={"chat_id": CHAT_ID, "text": chunk})


if __name__ == "__main__":
    print("스크립트 시작")
    message = get_stocks()
    print(f"메시지:\n{message[:500]}...")
    send_msg(message)
    print("전송 완료")
