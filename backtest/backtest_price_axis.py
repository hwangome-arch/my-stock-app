# -*- coding: utf-8 -*-
"""
backtest_price_axis.py — 1단계 백테스트: 가격/거래량 기반 축 검증

────────────────────────────────────────────────────────────────────────
무엇을 검증하나
────────────────────────────────────────────────────────────────────────
inventory_manager.py의 AI 종합점수 8개 항목 중, 순수하게 "가격·거래량"만으로
계산되는 5개 항목(추세·거래량·모멘텀·패턴·리스크)만 뽑아서, 과거 시점에 그
점수가 높았던 종목이 실제로 이후 수익률도 좋았는지를 검증한다.

    가격축 점수(0~100) = (추세 + 거래량 + 모멘텀 + 패턴점수 + 리스크) / 500 * 100

⚠️ 왜 이 5개만?
    - 수급(외국인·기관 순매수)은 한국 시장 전용 데이터라 yfinance엔 없다.
      네이버 frgn.naver 페이지를 과거까지 긁어와야 하는데, 종목 수 × 3년치를
      다 모으려면 별도 파이프라인이 필요해서 이번 1단계에서는 뺐다.
    - 재무·밸류는 "그 시점에 실제로 공시돼 있었는지"(reporting lag)를 맞추지
      않으면 미래 정보가 과거 점수에 섞여 들어가는 룩어헤드 편향이 생긴다.
      이 코드베이스엔 시점별 재무 스냅샷이 없어서 이번 1단계에서는 뺐다.
    - 위 두 축은 별도 데이터 파이프라인이 준비되면 2단계로 이어서 붙인다.

⚠️ 아래 _lerp_score / calc_trend_score / calc_volume_score / calc_momentum_score /
calc_pattern_score / calc_risk_score 함수는 inventory_manager.py 원본과 "동일한
공식"이 되도록 그대로 복사했다. 앱에서 배점 공식을 수정하면 이 파일도 같이
수정해야 앱 점수와 백테스트 점수가 어긋나지 않는다. (더 좋은 방법은 두 파일이
공통으로 import하는 scoring_core.py로 분리하는 것 — 나중에 리팩터링 후보.)

⚠️ 리스크 점수 계산에서 debt(부채비율) 인자는 항상 None으로 넘긴다 — 과거
시점의 부채비율 스냅샷이 없기 때문이다. 즉 리스크 점수 중 "부채비율 감점"
부분만 이번 백테스트에서는 항상 0으로 계산된다(변동성·52주 고점대비 하락·
연속하락일 부분은 정상 반영). 결과 해석 시 참고할 것.

────────────────────────────────────────────────────────────────────────
사용법
────────────────────────────────────────────────────────────────────────
    pip install yfinance pandas numpy --break-system-packages
    python backtest_price_axis.py

⚠️ 이 스크립트는 yfinance로 실제 네트워크 요청을 보낸다. Claude가 코드를
생성한 샌드박스 환경은 금융 데이터 사이트 접근이 막혀 있어 여기서는 실행/검증을
못 했다 — 문법 오류만 확인했다(py_compile 통과). 사용자 로컬 환경 또는 실제
네트워크가 열린 서버에서 실행해야 한다.

CONFIG 섹션에서 종목 리스트·기간·리밸런스 방식을 조정할 수 있다. 기본으로는
KOSPI 대형주 위주 40종목 샘플이 들어있다. 앱의 스크리너 결과(종목코드 전체)를
CSV로 내보내서 "종목코드" 컬럼 하나짜리 universe.csv로 저장해두면, 이 스크립트가
그걸 자동으로 읽어서 하드코딩 리스트 대신 사용한다(더 넓은 유니버스로 확장하고
싶을 때 사용).
"""

import time
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# ════════════════════════════════════════════════════════════════════════
# CONFIG — 여기 값들을 조정해서 실험하면 된다
# ════════════════════════════════════════════════════════════════════════

# 기본 유니버스 (KOSPI 대형주 위주 40종목 샘플). universe.csv가 있으면 이 리스트
# 대신 거기서 읽는다.
DEFAULT_TICKERS = [
    "005930.KS", "000660.KS", "373220.KS", "207940.KS", "005380.KS",
    "051910.KS", "006400.KS", "035420.KS", "035720.KS", "105560.KS",
    "055550.KS", "012330.KS", "028260.KS", "066570.KS", "096770.KS",
    "017670.KS", "030200.KS", "003670.KS", "086790.KS", "015760.KS",
    "034730.KS", "018260.KS", "010130.KS", "011200.KS", "032830.KS",
    "005490.KS", "009150.KS", "090430.KS", "011070.KS", "004020.KS",
    "010950.KS", "047050.KS", "024110.KS", "086280.KS", "021240.KS",
    "000270.KS", "302440.KS", "042700.KS", "078930.KS", "251270.KS",
]

UNIVERSE_CSV = Path(__file__).parent / "universe.csv"

START_DATE = "2022-09-01"   # 백테스트 시작일 (리밸런스 날짜 후보 시작)
END_DATE = "2025-08-01"     # 백테스트 종료일 (여기서 최대 120거래일 전까지만 실제 평가에 쓰임)
PRICE_FETCH_BUFFER_DAYS = 400  # 점수 계산에 필요한 과거 데이터를 위해 START_DATE보다 더 앞서 받아올 여유분(캘린더일)

REBALANCE_EVERY_N_TRADING_DAYS = 20  # 약 월 1회 리밸런스 (거래일 기준)

FORWARD_HORIZONS = {"20거래일(약1개월)": 20, "60거래일(약3개월)": 60, "120거래일(약6개월)": 120}

SCORE_BUCKETS = [
    (80, 101, "80~100"),
    (70, 80, "70~79"),
    (60, 70, "60~69"),
    (50, 60, "50~59"),
    (40, 50, "40~49"),
    (0, 40, "0~39"),
]

CACHE_DIR = Path(__file__).parent / "price_cache"
OUTPUT_RAW_CSV = Path(__file__).parent / "backtest_raw_results.csv"
OUTPUT_SUMMARY_CSV = Path(__file__).parent / "backtest_bucket_summary.csv"

KOSPI_TICKER = "^KS11"

# ════════════════════════════════════════════════════════════════════════
# 점수 계산 함수 — inventory_manager.py 원본과 동일한 공식 (그대로 복사)
# ════════════════════════════════════════════════════════════════════════


def _lerp_score(x, points):
    if x is None:
        return points[0][1]
    if x <= points[0][0]:
        return float(points[0][1])
    if x >= points[-1][0]:
        return float(points[-1][1])
    for (x0, s0), (x1, s1) in zip(points, points[1:]):
        if x0 <= x <= x1:
            if x1 == x0:
                return float(s0)
            ratio = (x - x0) / (x1 - x0)
            return s0 + (s1 - s0) * ratio
    return float(points[-1][1])


def calc_trend_score(df_price):
    try:
        if df_price is None or df_price.empty:
            return 50.0
        closes = df_price["Close"].dropna()
        if len(closes) < 20:
            return 50.0

        ma5 = closes.tail(5).mean()
        ma20 = closes.tail(20).mean()
        ma60 = closes.tail(60).mean() if len(closes) >= 60 else ma20

        gap_short = (ma5 - ma20) / ma20 * 100 if ma20 else 0
        gap_long = (ma20 - ma60) / ma60 * 100 if ma60 else 0
        align_pct = gap_short + gap_long
        s_align = _lerp_score(align_pct, [(-8, 13), (0, 30), (4, 75), (10, 130)])

        recent20 = closes.tail(20)
        ma20_series = closes.rolling(20).mean().reindex(recent20.index)
        above_ratio = (recent20 > ma20_series).sum() / len(recent20)
        s_persist = _lerp_score(above_ratio, [(0, 0), (0.3, 10), (0.5, 20), (0.8, 50), (1.0, 70)])

        return round(max(0, min(200, s_align + s_persist)), 1)
    except Exception:
        return 50.0


def calc_volume_score(df_price, exclude_today=False):
    try:
        if df_price is None or df_price.empty or "Volume" not in df_price.columns:
            return 50.0

        has_close = "Close" in df_price.columns
        cols = ["Volume", "Close"] if has_close else ["Volume"]
        data = df_price[cols].copy()
        data = data.dropna(subset=["Volume"])
        if exclude_today and len(data) >= 6:
            data = data.iloc[:-1]
        if len(data) < 5:
            return 50.0

        vol = data["Volume"]
        avg20 = vol.tail(20).mean() if len(vol) >= 20 else vol.mean()
        if avg20 <= 0:
            return 50.0

        recent = vol.iloc[-1]
        spike_ratio = recent / avg20
        spike_raw = _lerp_score(spike_ratio, [
            (0.0, 0), (0.4, 6), (0.6, 12), (0.8, 18), (1.0, 24),
            (1.2, 30), (1.5, 36), (2.0, 48), (3.0, 60),
        ])

        avg5 = vol.tail(5).mean() if len(vol) >= 5 else recent
        sustain_ratio = avg5 / avg20
        sustain_raw = _lerp_score(sustain_ratio, [
            (0.0, 0), (0.5, 8), (0.8, 16), (1.0, 24), (1.3, 32), (1.8, 40),
        ])

        spike_mult = 1.0
        sustain_mult = 1.0
        if has_close:
            closes = data["Close"].dropna()
            if len(closes) >= 2:
                day_ret = (closes.iloc[-1] / closes.iloc[-2] - 1) * 100
                spike_mult = _lerp_score(day_ret, [
                    (-8, 0.1), (-3, 0.35), (0, 0.7), (1.5, 1.0), (8, 1.0),
                ])
            if len(closes) >= 6:
                trend_ret = (closes.iloc[-1] / closes.iloc[-6] - 1) * 100
                sustain_mult = _lerp_score(trend_ret, [
                    (-15, 0.1), (-5, 0.35), (0, 0.7), (3, 1.0), (15, 1.0),
                ])

        spike = spike_raw * spike_mult
        sustain = sustain_raw * sustain_mult

        return round(max(0.0, min(100.0, spike + sustain)), 1)
    except Exception:
        return 50.0


def calc_momentum_score(df_price, kospi_closes=None):
    try:
        if df_price is None or df_price.empty:
            return 50.0
        closes = df_price["Close"].dropna()
        if len(closes) < 6:
            return 50.0

        def ret_pct(n):
            if len(closes) < n + 1:
                return None
            return (closes.iloc[-1] / closes.iloc[-(n + 1)] - 1) * 100

        ret5, ret20, ret60 = ret_pct(5), ret_pct(20), ret_pct(60)

        s5 = 10.0 if ret5 is None else _lerp_score(ret5, [(-15, 0), (-5, 5), (0, 10), (3, 15), (8, 20)])
        s20 = 20.0 if ret20 is None else _lerp_score(ret20, [(-30, 0), (-10, 5), (0, 15), (5, 25), (10, 30), (20, 40)])
        s60 = 10.0 if ret60 is None else _lerp_score(ret60, [(-40, 0), (-15, 5), (0, 10), (10, 15), (20, 20)])

        rs_bonus = 10.0
        if ret20 is not None and kospi_closes is not None and len(kospi_closes) >= 21:
            kospi_ret20 = (kospi_closes[-1] / kospi_closes[-21] - 1) * 100
            diff = ret20 - kospi_ret20
            rs_bonus = _lerp_score(diff, [(-15, 0), (0, 10), (10, 20)])

        return round(max(0, min(100, s5 + s20 + s60 + rs_bonus)), 1)
    except Exception:
        return 50.0


def calc_pattern_score(df_price, volume_score):
    try:
        if df_price is None or df_price.empty or len(df_price) < 21:
            return 40.0
        closes = df_price["Close"].dropna()
        cur = closes.iloc[-1]
        prior20_high = closes.iloc[-21:-1].max()

        if prior20_high > 0:
            gap_to_high = (cur - prior20_high) / prior20_high * 100
            score_breakout = _lerp_score(gap_to_high, [(-10, 0), (0, 15), (3, 35), (8, 50)])
        else:
            score_breakout = 0.0

        score_volume = _lerp_score(volume_score, [(0, 0), (40, 10), (60, 30), (100, 30)])

        recent5 = closes.tail(6).diff().dropna()
        up_ratio = (recent5 > 0).sum() / len(recent5) if len(recent5) else 0.5
        score_updays = _lerp_score(up_ratio, [(0, 0), (0.5, 8), (0.7, 15), (1.0, 20)])

        return round(max(0, min(100, score_breakout + score_volume + score_updays)), 1)
    except Exception:
        return 40.0


def calc_risk_score(df_price, debt, drop_pct=None):
    try:
        penalty = 0.0
        closes = df_price["Close"].dropna() if df_price is not None and not df_price.empty else None

        if closes is not None and len(closes) >= 6:
            daily_ret = closes.tail(21).pct_change().dropna() * 100
            if len(daily_ret) >= 5:
                ret_std = daily_ret.std()
                penalty += _lerp_score(ret_std, [(0, 0), (1.0, 5), (1.5, 12), (2.5, 20), (4.0, 30)])

        if debt is not None and debt > 0:
            penalty += _lerp_score(debt, [(0, 0), (60, 0), (100, 8), (150, 15), (200, 20), (300, 20)])

        effective_drop_pct = drop_pct
        if effective_drop_pct is None or effective_drop_pct == 0.0:
            if closes is not None and len(closes) > 0:
                high52 = closes.max()
                if high52 > 0:
                    effective_drop_pct = (closes.iloc[-1] - high52) / high52 * 100

        if effective_drop_pct is None or effective_drop_pct == 0.0:
            penalty += 5.0
        else:
            penalty += _lerp_score(effective_drop_pct, [(-60, 20), (-40, 14), (-20, 6), (-10, 2), (0, 0)])

        if closes is not None and len(closes) >= 6:
            diffs = closes.tail(6).diff().dropna()
            down_streak = 0
            for d in diffs.iloc[::-1]:
                if d < 0:
                    down_streak += 1
                else:
                    break
            penalty += _lerp_score(down_streak, [(0, 0), (2, 3), (4, 7), (5, 10)])

        return -round(max(0, min(80, penalty)), 1)
    except Exception:
        return -15.0


def price_axis_score_100(df_price_hist, kospi_closes_hist=None):
    """5개 가격 기반 항목을 합산해 0~100 스케일로 환산한다.
    만점 합계 = 추세(200) + 거래량(100) + 모멘텀(100) + 패턴(100) + 리스크(0, 최저 -80) = 500(최저 -80)."""
    trend = calc_trend_score(df_price_hist)
    volume = calc_volume_score(df_price_hist, exclude_today=False)
    momentum = calc_momentum_score(df_price_hist, kospi_closes_hist)
    pattern = calc_pattern_score(df_price_hist, volume)
    risk = calc_risk_score(df_price_hist, debt=None, drop_pct=None)  # 부채비율 데이터 없음 → None

    raw_sum = trend + volume + momentum + pattern + risk
    score_100 = max(0.0, min(100.0, raw_sum / 500.0 * 100.0))
    return score_100, {"추세": trend, "거래량": volume, "모멘텀": momentum, "패턴점수": pattern, "리스크": risk}


# ════════════════════════════════════════════════════════════════════════
# 데이터 수집 (yfinance, 로컬 캐시 사용)
# ════════════════════════════════════════════════════════════════════════

def load_universe():
    if UNIVERSE_CSV.exists():
        df = pd.read_csv(UNIVERSE_CSV, dtype=str)
        col = "종목코드" if "종목코드" in df.columns else df.columns[0]
        codes = df[col].astype(str).str.zfill(6).unique().tolist()
        # 시장 구분(.KS/.KQ)이 없으면 기본 KOSPI(.KS)로 가정 — 코스닥 종목이 섞여 있으면
        # universe.csv에 '시장' 컬럼을 추가해서 이 함수를 직접 조정할 것.
        tickers = [f"{c}.KS" for c in codes]
        print(f"[universe] universe.csv에서 {len(tickers)}종목 로드")
        return tickers
    print(f"[universe] universe.csv 없음 → 기본 샘플 {len(DEFAULT_TICKERS)}종목 사용")
    return DEFAULT_TICKERS


def fetch_price_history(ticker, start, end, cache_dir=CACHE_DIR):
    import yfinance as yf

    cache_dir.mkdir(exist_ok=True)
    cache_path = cache_dir / f"{ticker.replace('^', 'IDX_')}.csv"
    if cache_path.exists():
        df = pd.read_csv(cache_path, index_col=0, parse_dates=True)
        if not df.empty:
            return df

    for attempt in range(3):
        try:
            df = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=False)
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            if not df.empty:
                df.to_csv(cache_path)
                return df
        except Exception as e:
            print(f"  [경고] {ticker} 다운로드 실패(시도 {attempt+1}/3): {e}")
            time.sleep(2)
    return pd.DataFrame()


# ════════════════════════════════════════════════════════════════════════
# 백테스트 루프
# ════════════════════════════════════════════════════════════════════════

def run_backtest():
    tickers = load_universe()
    fetch_start = (pd.Timestamp(START_DATE) - pd.Timedelta(days=PRICE_FETCH_BUFFER_DAYS)).strftime("%Y-%m-%d")
    fetch_end = (pd.Timestamp(END_DATE) + pd.Timedelta(days=200)).strftime("%Y-%m-%d")

    print(f"[1/3] 코스피 지수({KOSPI_TICKER}) 데이터 수집 중...")
    kospi_df = fetch_price_history(KOSPI_TICKER, fetch_start, fetch_end)
    kospi_closes_full = kospi_df["Close"].dropna() if not kospi_df.empty else pd.Series(dtype=float)

    print(f"[2/3] 종목 {len(tickers)}개 가격 데이터 수집 중... (캐시: {CACHE_DIR})")
    price_data = {}
    for i, t in enumerate(tickers, 1):
        df = fetch_price_history(t, fetch_start, fetch_end)
        if df.empty or len(df) < 100:
            print(f"  - [{i}/{len(tickers)}] {t}: 데이터 부족, 스킵")
            continue
        price_data[t] = df
        print(f"  - [{i}/{len(tickers)}] {t}: {len(df)}행 확보")

    print(f"[3/3] 리밸런스 시점별 점수 계산 + 이후 수익률 집계 중...")
    rows = []
    for ticker, df in price_data.items():
        df = df[["Open", "High", "Low", "Close", "Volume"]].dropna(subset=["Close"])
        idx = df.index

        # 백테스트 구간(START_DATE~END_DATE)에 해당하는 위치만 리밸런스 후보로 사용,
        # N거래일 간격으로 샘플링
        in_range = np.where((idx >= pd.Timestamp(START_DATE)) & (idx <= pd.Timestamp(END_DATE)))[0]
        if len(in_range) == 0:
            continue
        rebalance_positions = in_range[::REBALANCE_EVERY_N_TRADING_DAYS]

        for pos in rebalance_positions:
            if pos < 60:  # 점수 계산에 필요한 최소 과거 데이터(60거래일) 확보 안 되면 스킵
                continue
            hist = df.iloc[: pos + 1]  # ⚠️ pos 시점까지만 — 미래 데이터 절대 안 섞임
            date = idx[pos]

            kospi_hist = None
            if not kospi_closes_full.empty:
                kospi_hist = kospi_closes_full.loc[:date].tolist()

            score_100, sub = price_axis_score_100(hist, kospi_hist)

            price_now = df["Close"].iloc[pos]
            row = {"ticker": ticker, "date": date, "score_100": round(score_100, 1)}
            row.update({f"sub_{k}": v for k, v in sub.items()})

            for label, n in FORWARD_HORIZONS.items():
                future_pos = pos + n
                if future_pos < len(df):
                    price_future = df["Close"].iloc[future_pos]
                    row[label] = (price_future / price_now - 1) * 100
                else:
                    row[label] = np.nan

            rows.append(row)

    result_df = pd.DataFrame(rows)
    if result_df.empty:
        print("⚠️ 결과가 비어있습니다. 유니버스/기간 설정을 확인하세요.")
        return

    result_df.to_csv(OUTPUT_RAW_CSV, index=False, encoding="utf-8-sig")
    print(f"\n원본 결과 저장: {OUTPUT_RAW_CSV} ({len(result_df)}행)")

    # ── 버킷별 집계 ──
    def bucket_label(score):
        for lo, hi, label in SCORE_BUCKETS:
            if lo <= score < hi:
                return label
        return "N/A"

    result_df["bucket"] = result_df["score_100"].apply(bucket_label)
    bucket_order = [b[2] for b in SCORE_BUCKETS]

    summary_rows = []
    for bucket in bucket_order:
        sub = result_df[result_df["bucket"] == bucket]
        if sub.empty:
            continue
        row = {"점수구간": bucket, "표본수": len(sub)}
        for label in FORWARD_HORIZONS:
            row[f"{label} 평균수익률(%)"] = round(sub[label].mean(), 2)
            row[f"{label} 중앙값(%)"] = round(sub[label].median(), 2)
            row[f"{label} 승률(%)"] = round((sub[label] > 0).mean() * 100, 1)
        summary_rows.append(row)

    summary_df = pd.DataFrame(summary_rows)
    summary_df.to_csv(OUTPUT_SUMMARY_CSV, index=False, encoding="utf-8-sig")

    print("\n" + "=" * 70)
    print("📊 점수 구간별 이후 수익률 요약")
    print("=" * 70)
    print(summary_df.to_string(index=False))

    print("\n" + "=" * 70)
    print("📈 점수 ↔ 수익률 순위상관(Spearman) — 1에 가까울수록 '점수가 높을수록 수익률도 높다'는 관계가 강함")
    print("=" * 70)
    for label in FORWARD_HORIZONS:
        valid = result_df.dropna(subset=[label])
        if len(valid) >= 10:
            corr = valid["score_100"].corr(valid[label], method="spearman")
            print(f"  {label}: {corr:.3f}  (표본 {len(valid)}개)")

    print(f"\n요약 결과 저장: {OUTPUT_SUMMARY_CSV}")
    print("\n⚠️ 표본수가 적은 구간(특히 극단 구간)은 신뢰도가 낮으니 표본수도 같이 확인할 것.")
    print("⚠️ 이 결과는 가격/거래량 축만 반영한 것으로, 실제 앱의 AI 종합점수(1000점, 재무·밸류·수급 포함)와는 다르다.")


if __name__ == "__main__":
    run_backtest()
