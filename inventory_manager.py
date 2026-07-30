import os
import sys
import socket
import threading
import concurrent.futures
import psutil
import random
import time
import datetime
import re
import io
import html as html_lib
import hashlib
import hmac
import base64
import difflib
import numpy as np

# ── 전역 소켓 기본 타임아웃 ──────────────────────────────────────────────
# gspread(Google Sheets API)처럼 자체적으로 timeout 파라미터를 노출하지 않는
# 라이브러리가 있어서, requests/yfinance 호출마다 timeout=을 넣는 것만으로는
# 모든 네트워크 호출을 다 방어할 수 없었다. Python 소켓 레벨에서 기본 타임아웃을
# 걸어두면, 코드에서 개별적으로 timeout을 안 준 소켓 통신도(= 위 라이브러리들 포함)
# 이 값을 넘기면 예외를 던지고 빠져나온다. 각 함수는 이미 try/except로
# 감싸져 있어서, 이 타임아웃이 걸려도 앱이 멈추는 대신 "빈 결과"로 정상 진행된다.
socket.setdefaulttimeout(20)
# ────────────────────────────────────────────────────────────────────────

# ── 백그라운드 스레드(ThreadPoolExecutor)에서도 안전하게 쓸 수 있는 디버그 정보 저장소 ──
# st.session_state는 스크립트를 실행하는 메인 스레드 밖(예: 관심종목 페이지의 병렬 조회
# 워커 스레드)에서 접근하면 Streamlit 공식 문서상 지원되지 않으며, 실제로 이로 인해
# 스크립트 실행이 멈춰버리는(무한 로딩) 문제가 발생했다. 단순 dict 대입/조회는 CPython의
# GIL 덕분에 스레드에서 안전하므로, 디버그용 정보는 여기로 옮겨서 저장한다.
_DEBUG_STORE = {}

# 스크리너 데이터도 관심종목 병렬조회(백그라운드 스레드)에서 읽히므로, session_state와
# 별개로 여기에도 항상 최신값을 미러링해두고 스레드에서는 이쪽만 사용한다.
_SCREENER_DF_CACHE = {"df": None}

def _set_shared_screener_df(df):
    """스크리너 결과 df를 session_state(메인 스레드용)와 모듈 캐시(백그라운드 스레드용)에 동시 반영."""
    _SCREENER_DF_CACHE["df"] = df
    try:
        st.session_state['shared_screener_df'] = df
    except Exception:
        pass  # 백그라운드 스레드 등 session_state 접근이 불가능한 상황이면 모듈 캐시만 사용

# ==== 🚀 [테마 강제 고정 로직] ====
try:
    config_dir = ".streamlit"
    if not os.path.exists(config_dir):
        os.makedirs(config_dir)
    config_path = os.path.join(config_dir, "config.toml")
    
    theme_config = """[theme]
base="light"
primaryColor="#5A4EE5"
backgroundColor="#F8FAFC"
secondaryBackgroundColor="#0F141F"
textColor="#111827"
"""
    with open(config_path, "w", encoding="utf-8") as f:
        f.write(theme_config)
except Exception:
    pass
# ==================================

import streamlit as st
import pandas as pd
import requests
import gspread
from google.oauth2.service_account import Credentials

# =========================
# ⚙️ 페이지 설정
# =========================
st.set_page_config(page_title="Inventory Manager", page_icon="📦", layout="wide")

# =========================
# 🕸️ 데이터 처리 엔진
# =========================
def normalize_kr_code(code):
    return re.sub(r"\D", "", str(code)).zfill(6)[:6]

# ── 앱 전역 공유 스레드풀 ──────────────────────────────────────────────────
# 문제: 기존에는 대시보드/관심종목/스크리너 등에서 병렬조회가 필요할 때마다 매번
# 새 ThreadPoolExecutor를 만들고 shutdown(wait=False)로 버렸다. shutdown(wait=False)는
# "이 executor로 새 작업을 더 안 받겠다"는 뜻일 뿐, 이미 떠 있는 스레드가 실제로
# 끝나기를 기다리지 않는다. 그래서 사용자가 짧은 시간 안에 무거운 페이지(특히 대시보드,
# yfinance 4개 + 네이버 API)를 여러 번 왔다갔다 하면, 이전 방문에서 못 끝낸 스레드들이
# 계속 쌓이다가 결국 클라우드 프로세스가 열 수 있는 스레드/소켓 상한에 몰려서 이후
# 요청 자체를 못 받는(=탭 전환 시 멈춤) 상태가 됐다.
# 해결: st.cache_resource로 프로세스당(=앱 전체) 스레드풀을 "딱 하나"만 만들어 공유한다.
# max_workers를 고정해두면, 페이지를 아무리 빨리 왔다갔다 해도 동시에 살아있는 워커
# 스레드 수가 이 상한을 절대 넘지 않는다(넘치는 작업은 새 스레드를 만드는 대신 큐에서
# 대기했다가 순서대로 실행된다).
@st.cache_resource(show_spinner=False)
def get_shared_executor():
    return concurrent.futures.ThreadPoolExecutor(max_workers=32)

# ── 메인 스레드 직접 호출 보호용 헬퍼 ──────────────────────────────────────
# 문제: yfinance(내부적으로 curl_cffi 사용)에 timeout=8을 넘겨도, 클라우드 환경에서
# 상대 서버(야후 파이낸스)가 소켓을 붙잡아두는 경우 그 timeout이 실제로 지켜지지
# 않고 호출이 무한정 멈출 수 있다. 이런 호출이 메인 스크립트 실행 스레드에서 직접
# 일어나면, 스레드풀 보호와 무관하게 앱 전체가 그 자리에서 완전히 멈춰버린다
# (탭을 몇 번 안 눌렀는데 바로 먹통이 되는 현상은 대부분 이 패턴).
# 해결: 메인 스레드에서 직접 호출하는 대신 항상 스레드에서 실행시키고, 결과를
# 기다리는 시간 자체에 메인 스레드 쪽에서 강제 상한을 건다. 라이브러리의 자체
# timeout을 못 믿는 상황에 대한 이중 안전장치이며, 상한을 넘기면 그 스레드는
# 백그라운드에 남겨둔 채(cancel 시도만 하고) 메인 스레드는 즉시 다음 로직으로 넘어간다.
#
# ⚠️ 절대 get_shared_executor()를 재사용하면 안 된다! 이 함수(estimate_target_hit_
# probability 등)는 관심종목 프리페치처럼 "이미 공유 풀의 워커 스레드 안"에서 호출되는
# 경우가 있다. 그 상태에서 같은 공유 풀에 또 작업을 던지고 기다리면, 공유 풀의 워커가
# 전부 이런 식으로 서로를 기다리게 될 때 새로 던진 작업을 실행해 줄 워커가 하나도
# 안 남는 자기 자신을 기다리는 교착상태(deadlock)가 생길 수 있다. 그래서 이 안전장치
# 전용으로 완전히 분리된 별도의 작은 풀을 쓴다.
@st.cache_resource(show_spinner=False)
def get_yf_safety_executor():
    return concurrent.futures.ThreadPoolExecutor(max_workers=16)

def call_with_timeout(fn, timeout=10):
    future = get_yf_safety_executor().submit(fn)
    try:
        return future.result(timeout=timeout)
    except Exception:
        future.cancel()
        return None
# ────────────────────────────────────────────────────────────────────────
# ────────────────────────────────────────────────────────────────────────

# ── "겉 함수" 병렬 오케스트레이션 전용 풀 ──────────────────────────────────
# fetch_market_index_table/fetch_sparkline_data/fetch_investor_trend/
# fetch_sector_ranking 같은 함수들은 전부 내부적으로 run_parallel_safe를 통해
# get_shared_executor()에 또 작업을 던지고 기다린다. 이런 함수들 자체를 "여러 개
# 동시에" 실행하고 싶을 때(예: 대시보드가 4개를 한 번에 병렬로 미리 가져오기),
# 절대 그 겉 함수들도 get_shared_executor()에 던지면 안 된다 — 그러면 겉 작업이
# 공유 풀의 워커 하나를 차지한 채로 같은 풀에 안쪽 작업을 또 던지고 기다리게 되어,
# 관심종목 프리페치에서 겪었던 것과 똑같은 자기 자신을 기다리는 교착상태가 생길 수
# 있다. 그래서 이렇게 "안에서 공유 풀을 쓰는 함수를 여러 개 동시에 돌리는" 바깥쪽
# 오케스트레이션은 완전히 별도의 풀을 쓴다.
@st.cache_resource(show_spinner=False)
def get_orchestration_executor():
    return concurrent.futures.ThreadPoolExecutor(max_workers=8)

# ── 병렬 조회용 안전 실행 헬퍼 ──────────────────────────────────────────────
# 문제: concurrent.futures.as_completed(futures)를 타임아웃 없이 쓰면, 스레드
# 하나라도 응답이 안 오는 상태(클라우드 배포 환경에서 외부 API가 datacenter IP를
# 느리게/불안정하게 처리하는 경우 등)로 걸리면 메인 스크립트 실행 스레드가 그
# 자리에서 영원히 멈춘다(= Streamlit 프로세스 전체가 응답 없음 상태가 되어 브라우저
# 쪽에서 "Connection timed out"으로 나타남). 개별 requests/yfinance 호출에 걸린
# timeout만으로는 소켓이 완전히 멎어버리는 경우까지 막지 못한다.
# 해결: as_completed에 전체 상한 시간(overall_timeout)을 걸고, future.result()에도
# 개별 상한 시간을 걸어서, 무슨 일이 있어도 이 함수가 메인 스레드를 무한정 막지
# 않도록 한다. 상한을 넘긴 나머지 작업은 결과 없이 건너뛰고(다음 새로고침 때 캐시가
# 채워지며 자연히 채워짐), 아직 안 끝난 스레드는 shutdown(wait=False)로 기다리지
# 않고 그냥 백그라운드에 남겨둔 채 진행한다.
def run_parallel_safe(task_fn, items, max_workers=5, overall_timeout=15, per_result_timeout=8):
    """
    task_fn(item) -> (key, result) 형태의 함수를 items 각각에 대해 병렬 실행.
    반환: {key: result} dict (실패/타임아웃난 항목은 아예 키가 없음)

    ⚠️ max_workers는 더 이상 이 함수가 자체 executor를 만드는 데 쓰이지 않는다.
    앱 전역 공유 스레드풀(get_shared_executor)을 사용해서, 이 함수를 얼마나 자주
    호출하든 실제로 동시에 떠 있는 워커 스레드 수는 공유 풀의 max_workers(20)를
    절대 넘지 않는다.
    """
    results = {}
    if not items:
        return results
    executor = get_shared_executor()
    futures = {executor.submit(task_fn, item): item for item in items}
    try:
        for future in concurrent.futures.as_completed(futures, timeout=overall_timeout):
            try:
                k, entry = future.result(timeout=per_result_timeout)
                if entry is not None:
                    results[k] = entry
            except Exception:
                continue
    except concurrent.futures.TimeoutError:
        pass  # 전체 상한 초과 → 나머지는 건너뛰고 계속 진행
    finally:
        # 공유 스레드풀이므로 여기서 shutdown하지 않는다(앱 전체가 계속 재사용).
        # 아직 큐에서 시작도 못 한 future는 취소해서 불필요하게 스레드를 점유하지 않게 한다.
        # (이미 실행 중인 future는 cancel()이 안 먹지만, 그건 원래 자연히 끝나고 사라진다.)
        for f in futures:
            f.cancel()
    return results
# ────────────────────────────────────────────────────────────────────────

def run_with_progress(text, func, *args, **kwargs):
    pb = st.progress(0, text=f"🔄 {text}")
    for i in range(1, 85, 12):
        pb.progress(i, text=f"🔄 {text}")
        time.sleep(0.02)
    res = func(*args, **kwargs)
    pb.progress(100, text="✨ 완료!")
    time.sleep(0.3)
    pb.empty()
    return res

# ── 논블로킹 데이터 로딩 헬퍼 (탭 이동 멈춤 현상 대응) ───────────────────────
# 문제: 기존 방식은 concurrent.futures.as_completed(futures, timeout=15) 처럼
# 메인 스크립트 실행 스레드에서 최대 N초를 "한 번에 몰아서" 기다렸다. 이 N초
# 동안은 Streamlit 서버가 브라우저에서 온 새 상호작용(예: 사이드바 탭 클릭 → 새
# 스크립트 실행 요청)을 받아줄 수 없다. Streamlit은 스크립트가 st.* 호출 등으로
# "숨 쉬는" 타이밍에만 새 실행 요청을 확인하는데, 파이썬 블로킹 호출 도중에는
# 그 타이밍 자체가 오지 않기 때문이다. 그래서 사용자 입장에서는 "탭을 눌렀는데
# 몇 초~십몇 초간 반응이 없다가 뒤늦게 바뀐다"는 멈춤 현상으로 보인다.
#
# 해결: 무거운 조회는 지금처럼 공유 스레드풀에 던져두되(get_shared_executor /
# get_orchestration_executor는 그대로 재사용), 메인 스크립트는 그 결과를 절대
# 한 번에 몰아서 기다리지 않는다. 대신 st.fragment(run_every=poll_interval)로
# "다 됐는지"만 아주 짧은 간격(기본 0.4초)마다 확인하는 조각을 별도로 실행한다.
# 이 프래그먼트가 쉬는 그 짧은 간격마다 Streamlit이 사용자의 새 클릭을 정상적으로
# 받아 즉시 새 스크립트 실행으로 넘어갈 수 있다. 즉 탭 전환 시 최대 지연이
# (기존) 최대 수십 초 → (개선) 대략 poll_interval 수준으로 줄어든다.
# 작업(future)은 session_state에 보관되므로, 로딩 도중 다른 탭으로 갔다가 다시
# 돌아와도 이미 던져둔 작업이 그대로 이어서 진행되며 처음부터 다시 조회하지 않는다.
#
# ⚠️ st.fragment(run_every=...)는 Streamlit 1.37 이상이 필요하다. 그보다 낮은
# 버전이면 이 헬퍼 대신 기존 방식을 유지해야 한다 (streamlit --version으로 확인).
def render_async_multi(job_key, submit_fn, collect_fn, default_result,
                        spinner_text="데이터를 불러오는 중...",
                        poll_interval=0.4, overall_timeout=20):
    """
    job_key     : 이 로딩 작업을 구분하는 고유 문자열 키 (페이지마다 겹치지 않게 지정)
    submit_fn() : 인자 없이 호출하면 {"이름": future, ...} 형태의 dict를 반환해야 함
                  (예: lambda: {"indices": executor.submit(fetch_market_index_table), ...})
    collect_fn(futures_dict) : 완료된(또는 일부만 완료된) futures_dict를 받아
                  실제 결과 dict로 변환하는 함수. future.done()이 False인 항목은
                  건드리지 말고 default 값으로 채워서 반환할 것.
    default_result : 아직 하나도 준비 안 됐을 때 렌더링에 쓸 기본값
    반환값: (result, ready)
      - ready=False  → 아직 로딩 중. 호출부는 이 시점에 바로 return 해서
                        이후의 무거운 렌더링을 건너뛰어야 한다.
      - ready=True   → 완료(또는 상한시간 초과). result를 바로 사용하면 된다.
    """
    jobs = st.session_state.setdefault("_bg_jobs", {})
    job = jobs.get(job_key)
    if job is None:
        job = {"futures": submit_fn(), "started_at": time.time()}
        jobs[job_key] = job

    futures = job["futures"]
    all_done = all(f.done() for f in futures.values())
    timed_out = (time.time() - job["started_at"]) > overall_timeout

    if not all_done and not timed_out:
        @st.fragment(run_every=poll_interval)
        def _poll():
            if all(f.done() for f in futures.values()) or (time.time() - job["started_at"]) > overall_timeout:
                st.rerun()  # 준비 완료 → 프래그먼트가 아니라 앱 전체를 다시 그려서 실제 데이터를 반영
            else:
                st.info(f"🔄 {spinner_text}")
        _poll()
        return default_result, False

    # 다 끝났거나 상한 시간을 넘김 → 끝난 것만 회수, 안 끝난 항목은 collect_fn이
    # 알아서 기본값으로 채우도록 한다.
    result = collect_fn(futures)
    for f in futures.values():
        if not f.done():
            f.cancel()  # 이미 실행 중이면 취소는 안 되지만, 큐 대기중이었다면 자리를 비워준다
    jobs.pop(job_key, None)  # 다음 방문 때는 (캐시 TTL이 지났다면) 새로 조회
    return result, True

@st.cache_data(ttl=60, show_spinner=False)
def fetch_market_index_table():
    """
    코스피/코스닥: 네이버 m.stock API (실시간, 지연 없음)
    나스닥/환율/금/WTI: yfinance 1분봉 (기존 20분 지연 -> 수 분 이내로 개선)
    """
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

    naver_targets = {
        "kospi":  {"symbol": "KOSPI",  "name": "KOSPI",  "subtitle": "한국 코스피"},
        "kosdaq": {"symbol": "KOSDAQ", "name": "KOSDAQ", "subtitle": "한국 코스닥"},
    }
    yf_targets = {
        "nasdaq": {"symbol": "^IXIC", "name": "NASDAQ",   "subtitle": "미국 나스닥"},
        "usdkrw": {"symbol": "KRW=X", "name": "USD/KRW",  "subtitle": "원/달러 환율"},
        "gold":   {"symbol": "GC=F",  "name": "Gold",  "subtitle": "금 선물"},
        "wti":    {"symbol": "CL=F",  "name": "WTI Crude","subtitle": "서부텍사스산 원유"},
    }

    def get_naver(key, meta):
        try:
            url = f"https://m.stock.naver.com/api/index/{meta['symbol']}/basic"
            res = requests.get(url, headers=headers, timeout=6)
            data = res.json()
            price = float(str(data.get("closePrice", "0")).replace(",", ""))
            diff = float(str(data.get("compareToPreviousClosePrice", "0")).replace(",", ""))
            diff_pct = float(str(data.get("fluctuationsRatio", "0")).replace(",", ""))
            sign = "+" if diff >= 0 else ""
            vol_raw = data.get("accumulatedTradingVolume", None)
            vol = f"{int(str(vol_raw).replace(',', '')):,}" if vol_raw else "N/A"
            return key, {
                "name": meta["name"], "subtitle": meta["subtitle"],
                "value": f"{price:,.2f}",
                "change": f"{sign}{diff:,.2f}",
                "change_pct": f"{sign}{diff_pct:.2f}%",
                "status": "up" if diff > 0 else ("down" if diff < 0 else "neutral"),
                "volume": vol,
            }
        except Exception:
            return key, None

    def get_yfinance(key, meta):
        try:
            import yfinance as yf
            df = yf.Ticker(meta["symbol"]).history(period="2d", interval="1m", timeout=8)
            if df.empty:
                raise ValueError("empty")
            price = float(df["Close"].dropna().iloc[-1])
            today = df.index[-1].date()
            prev_df = df[df.index.date < today]["Close"].dropna()
            prev = float(prev_df.iloc[-1]) if not prev_df.empty else price
            diff = price - prev
            diff_pct = diff / prev * 100 if prev else 0
            sign = "+" if diff >= 0 else ""
            fmt = f"{price:,.1f}" if key == "usdkrw" else f"{price:,.2f}"
            return key, {
                "name": meta["name"], "subtitle": meta["subtitle"],
                "value": fmt,
                "change": f"{sign}{diff:,.2f}",
                "change_pct": f"{sign}{diff_pct:.2f}%",
                "status": "up" if diff > 0 else ("down" if diff < 0 else "neutral"),
                "volume": "N/A",
            }
        except Exception:
            return key, None

    all_tasks = (
        [(get_naver, k, m) for k, m in naver_targets.items()] +
        [(get_yfinance, k, m) for k, m in yf_targets.items()]
    )
    result = run_parallel_safe(
        lambda t: t[0](t[1], t[2]), all_tasks,
        max_workers=6, overall_timeout=12, per_result_timeout=6,
    )

    all_targets = {**naver_targets, **yf_targets}
    return {k: result.get(k, {"name": all_targets[k]["name"], "value": "-", "status": "neutral"})
            for k in all_targets.keys()}

@st.cache_data(ttl=86400, show_spinner=False)
def fetch_sparkline_data():
    """시장 지수 카드용 180일 스파크라인 데이터를 yfinance로 수집."""
    targets = {
        "kospi":  "^KS11",
        "kosdaq": "^KQ11",
        "nasdaq": "^IXIC",
        "usdkrw": "KRW=X",
        "gold":   "GC=F",
        "wti":    "CL=F",
    }

    def get_history(key, symbol):
        try:
            import yfinance as yf
            df = yf.Ticker(symbol).history(period="180d", interval="1d", timeout=8)
            if df.empty or "Close" not in df.columns:
                return key, []
            closes = df["Close"].dropna().tolist()
            return key, closes
        except:
            return key, []

    result = run_parallel_safe(
        lambda kv: get_history(kv[0], kv[1]), list(targets.items()),
        max_workers=6, overall_timeout=12, per_result_timeout=6,
    )
    # 실패/타임아웃난 종목은 빈 리스트로 채워서 카드가 항상 렌더링되게 함
    for k in targets.keys():
        result.setdefault(k, [])
    return result


def make_sparkline_svg(closes, status, width=160, height=52):
    """종가 리스트를 받아 인라인 SVG 스파크라인 문자열을 반환."""
    if not closes or len(closes) < 2:
        return ""
    mn, mx = min(closes), max(closes)
    rng = mx - mn if mx != mn else 1.0
    pad = 3
    def cx(i): return round(i / (len(closes) - 1) * width, 2)
    def cy(v): return round(pad + (1 - (v - mn) / rng) * (height - pad * 2), 2)
    pts = " ".join(f"{cx(i)},{cy(v)}" for i, v in enumerate(closes))
    color = "#DC2626" if status == "up" else ("#2563EB" if status == "down" else "#94A3B8")
    fill_color = color + "15"
    first_x, last_x = cx(0), cx(len(closes) - 1)
    poly_pts = f"{pts} {last_x},{height} {first_x},{height}"
    svg = (
        f'<svg viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg" '
        f'style="width:100%;height:{height}px;display:block;">'
        f'<polygon points="{poly_pts}" fill="{fill_color}" stroke="none"/>'
        f'<polyline points="{pts}" fill="none" stroke="{color}" stroke-width="1.8" '
        f'stroke-linejoin="round" stroke-linecap="round"/>'
        f'</svg>'
    )
    return svg


@st.cache_data(ttl=120, show_spinner=False)
def fetch_investor_trend():
    """코스피/코스닥 당일(최근 거래일) 투자자별(외국인/기관/개인) 순매수 동향을 억원 단위로 가져온다."""
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    markets = {"kospi": "01", "kosdaq": "02"}  # 01: 코스피, 02: 코스닥

    def get_data(key, sosok):
        try:
            today = datetime.datetime.now().strftime("%Y%m%d")
            url = f"https://finance.naver.com/sise/investorDealTrendDay.naver?bizdate={today}&sosok={sosok}"
            res = requests.get(url, headers=headers, timeout=7)
            res.encoding = res.apparent_encoding or 'euc-kr'
            dfs = pd.read_html(io.StringIO(res.text))

            target = None
            for df in dfs:
                cols = [str(c) for c in df.columns.to_flat_index()]
                if any('외국인' in c for c in cols) and any('기관' in c for c in cols):
                    target = df.dropna(how="all").reset_index(drop=True)
                    break
            if target is None or target.empty:
                return key, None

            target.columns = [str(c).strip() for c in target.columns.to_flat_index()]
            row = target.iloc[0]  # 가장 최근 거래일(맨 위 행)

            def pick(keyword):
                col = next((c for c in target.columns if keyword in c), None)
                if col is None:
                    return None
                try:
                    return float(str(row[col]).replace(',', '').strip())
                except Exception:
                    return None

            entry = {
                "foreign": pick("외국인"),
                "institution": pick("기관"),
                "individual": pick("개인"),
            }
            if all(v is None for v in entry.values()):
                return key, None
            return key, entry
        except Exception:
            return key, None

    result = run_parallel_safe(
        lambda kv: get_data(kv[0], kv[1]), list(markets.items()),
        max_workers=2, overall_timeout=15, per_result_timeout=8,
    )
    for k in markets.keys():
        result.setdefault(k, {"foreign": None, "institution": None, "individual": None})
    return result

@st.cache_data(ttl=1800, show_spinner=False)
def fetch_investor_trend_monthly(sosok):
    """코스피(01)/코스닥(02) 최근 한달 일별 투자자별 순매수 동향을 가져온다."""
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

    today = datetime.datetime.now()
    business_days = []
    for delta in range(45):
        date = today - datetime.timedelta(days=delta)
        if date.weekday() < 5:
            business_days.append(date)
        if len(business_days) >= 22:
            break

    def fetch_one(date):
        try:
            time.sleep(random.uniform(0.1, 0.2))
            date_str = date.strftime("%Y%m%d")
            url = f"https://finance.naver.com/sise/investorDealTrendDay.naver?bizdate={date_str}&sosok={sosok}"
            res = requests.get(url, headers=headers, timeout=5)
            res.encoding = res.apparent_encoding or 'euc-kr'
            dfs = pd.read_html(io.StringIO(res.text))
            target = None
            for df in dfs:
                cols = [str(c) for c in df.columns.to_flat_index()]
                if any('외국인' in c for c in cols) and any('기관' in c for c in cols):
                    target = df.dropna(how="all").reset_index(drop=True)
                    break
            if target is None or target.empty:
                return None
            target.columns = [str(c).strip() for c in target.columns.to_flat_index()]
            row = target.iloc[0]

            def pick(keyword):
                col = next((c for c in target.columns if keyword in c), None)
                if col is None: return None
                try: return float(str(row[col]).replace(',', '').strip())
                except: return None

            f_val = pick("외국인")
            i_val = pick("기관")
            p_val = pick("개인")
            if f_val is None and i_val is None and p_val is None:
                return None
            return {
                "날짜": date.strftime("%m/%d"),
                "외국인": f_val or 0.0,
                "기관":   i_val or 0.0,
                "개인":   p_val or 0.0,
            }
        except Exception:
            return None

    rows = []
    _executor = get_shared_executor()
    _futures = {_executor.submit(fetch_one, d): d for d in business_days}
    try:
        for future in concurrent.futures.as_completed(_futures, timeout=12):
            try:
                res = future.result(timeout=6)
                if res:
                    rows.append(res)
            except Exception:
                continue
    except concurrent.futures.TimeoutError:
        pass  # 전체 상한 초과 → 지금까지 모인 결과로 진행
    finally:
        for f in _futures:
            f.cancel()

    rows.sort(key=lambda r: r["날짜"])
    return rows

# =========================
# 📅 통화정책 회의 일정 (공식 확정 일정)
# =========================
FOMC_MEETING_DATES = [
    "2026-01-28", "2026-03-18", "2026-04-29", "2026-06-17",
    "2026-07-29", "2026-09-16", "2026-10-28", "2026-12-09",
]
BOK_MEETING_DATES = [
    "2026-01-15", "2026-02-26", "2026-04-10", "2026-05-28",
    "2026-07-16", "2026-08-27", "2026-10-22", "2026-11-26",
]

FED_RATE_HISTORY = [
    {"date": "2026-06-17", "range": "3.50~3.75%", "action": "인하 (-0.25%p)"},
    {"date": "2026-04-29", "range": "3.75~4.00%", "action": "동결"},
    {"date": "2026-03-18", "range": "3.75~4.00%", "action": "동결"},
    {"date": "2026-01-28", "range": "3.75~4.00%", "action": "인하 (-0.25%p)"},
    {"date": "2025-12-10", "range": "4.00~4.25%", "action": "동결"},
    {"date": "2025-10-29", "range": "4.00~4.25%", "action": "인하 (-0.25%p)"},
    {"date": "2025-09-17", "range": "4.25~4.50%", "action": "동결"},
    {"date": "2025-07-30", "range": "4.25~4.50%", "action": "동결"},
    {"date": "2025-06-18", "range": "4.25~4.50%", "action": "인하 (-0.25%p)"},
    {"date": "2025-04-30", "range": "4.50~4.75%", "action": "동결"},
]

BOK_RATE_HISTORY = [
    {"date": "2026-05-28", "rate": 2.50, "action": "동결"},
    {"date": "2026-04-10", "rate": 2.50, "action": "동결"},
    {"date": "2026-02-26", "rate": 2.50, "action": "동결"},
    {"date": "2026-01-15", "rate": 2.50, "action": "동결"},
    {"date": "2025-11-27", "rate": 2.50, "action": "동결"},
    {"date": "2025-10-16", "rate": 2.75, "action": "인하 (-0.25%p)"},
    {"date": "2025-08-28", "rate": 2.75, "action": "동결"},
    {"date": "2025-07-17", "rate": 3.00, "action": "인하 (-0.25%p)"},
    {"date": "2025-05-29", "rate": 3.00, "action": "동결"},
    {"date": "2025-04-17", "rate": 3.00, "action": "동결"},
    {"date": "2025-02-25", "rate": 3.00, "action": "인하 (-0.25%p)"},
    {"date": "2025-01-16", "rate": 3.25, "action": "동결"},
]

def next_meeting_label(date_list):
    today = datetime.datetime.now().date()
    dates = sorted(datetime.datetime.strptime(d, "%Y-%m-%d").date() for d in date_list)
    upcoming = [d for d in dates if d >= today]
    if not upcoming:
        return None
    d = upcoming[0]
    return f"{d.month}/{d.day}"

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_fed_rate_data():
    try:
        history = [
            {"date": h["date"], "range": h["range"], "action": h["action"]}
            for h in FED_RATE_HISTORY[:10]
        ]
        latest = FED_RATE_HISTORY[0]
        current = {
            "range": latest["range"],
            "date": latest["date"],
        }
        return {"current": current, "history": history}
    except Exception:
        return None

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_bok_rate_data():
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

    try:
        url = "https://m.stock.naver.com/api/index/IRR_BOK/basic"
        res = requests.get(url, headers=headers, timeout=8)
        data = res.json()
        rate_val = float(str(data.get("closePrice", "0")).replace(",", ""))
        date_str = str(data.get("localTradedAt", ""))[:10]
        dt = pd.to_datetime(date_str, errors="coerce")
        date_display = dt.strftime("%Y-%m-%d") if pd.notna(dt) else "최신"

        if rate_val > 0:
            history = [
                {"date": h["date"], "range": f"{h['rate']:.2f}%", "action": h["action"]}
                for h in BOK_RATE_HISTORY[:10]
            ]
            return {"current": {"rate": f"{rate_val:.2f}%", "date": date_display}, "history": history}
    except Exception:
        pass

    history = [
        {"date": h["date"], "range": f"{h['rate']:.2f}%", "action": h["action"]}
        for h in BOK_RATE_HISTORY[:10]
    ]
    latest = BOK_RATE_HISTORY[0]
    return {"current": {"rate": f"{latest['rate']:.2f}%", "date": latest["date"]}, "history": history}

def _stale_rate_warning(meeting_dates, history, label):
    """가장 최근에 지난 회의일이 이력 데이터(하드코딩)에 반영되어 있는지 확인.
    반영이 안 된 것으로 보이면 경고 문구를 반환하고, 정상이면 None을 반환."""
    if not history:
        return None
    today = datetime.datetime.now().date()
    past_dates = sorted(
        (d for d in (datetime.datetime.strptime(m, "%Y-%m-%d").date() for m in meeting_dates) if d <= today),
        reverse=True,
    )
    if not past_dates:
        return None
    latest_meeting = past_dates[0]
    try:
        latest_history_date = datetime.datetime.strptime(history[0]["date"], "%Y-%m-%d").date()
    except Exception:
        return None
    if latest_meeting > latest_history_date:
        return (
            f"⚠️ {label} {latest_meeting.strftime('%Y-%m-%d')} 회의 결과가 아직 코드에 반영되지 않은 것 같아요. "
            f"(현재 표시 중인 마지막 데이터: {latest_history_date.strftime('%Y-%m-%d')}) "
            f"FED_RATE_HISTORY / BOK_RATE_HISTORY 목록에 최신 결과를 추가해주세요."
        )
    return None

def render_rate_widget():
    fed = run_with_progress("연준 금리 데이터를 불러오는 중...", fetch_fed_rate_data)
    bok = run_with_progress("한국은행 금리 데이터를 불러오는 중...", fetch_bok_rate_data)

    fed_warning = _stale_rate_warning(FOMC_MEETING_DATES, FED_RATE_HISTORY, "연준(FOMC)")
    bok_warning = _stale_rate_warning(BOK_MEETING_DATES, BOK_RATE_HISTORY, "한국은행(금통위)")
    if fed_warning:
        st.warning(fed_warning)
    if bok_warning:
        st.warning(bok_warning)

    def build_help(history, value_key):
        if not history:
            return "최근 10건 변동 이력을 가져오지 못했습니다."
        lines = [f"- {h['date']} · {h[value_key]} ({h['action']})" for h in history[:10]]
        return "**최근 10건 변동 이력**\n\n" + "\n".join(lines)

    if fed:
        st.metric(
            label="🇺🇸 미국 기준금리 (FOMC)",
            value=fed["current"]["range"],
            help=build_help(fed["history"], "range") + f"\n\n_기준일: {fed['current']['date']}_",
        )
    else:
        st.metric(label="🇺🇸 미국 기준금리 (FOMC)", value="-", help="데이터를 불러오지 못했습니다.")
        
    if bok:
        st.metric(
            label="🇰🇷 한국 기준금리 (한국은행)",
            value=bok["current"]["rate"],
            help=build_help(bok["history"], "range") + f"\n\n_기준일: {bok['current']['date']}_",
        )
    else:
        st.metric(label="🇰🇷 한국 기준금리 (한국은행)", value="-", help="데이터를 불러오지 못했습니다.")

@st.cache_data(ttl=180, show_spinner=False)
def fetch_sector_ranking():
    sector_etfs = [
        ("반도체", "091160"), ("2차전지", "305720"), ("바이오", "244580"),   
        ("자동차", "091180"), ("금융", "091170"), ("철강/소재", "104530"),   
        ("에너지/화학", "117460"), ("IT·소프트웨어", "157490"),  
        ("조선", "139230"), ("미디어·통신", "266410")
    ]
    def get_data(name, code):
        try:
            time.sleep(random.uniform(0.1, 0.3))
            url = f"https://m.stock.naver.com/api/stock/{code}/basic"
            res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=5)
            data = res.json()
            pct = float(data.get('fluctuationsRatio', '0').replace(',', ''))
            return {"업종명": name, "등락률_num": round(pct, 2)}
        except: pass
        return None
        
    rows = []
    _executor = get_shared_executor()
    _futures = [_executor.submit(get_data, n, c) for n, c in sector_etfs]
    try:
        for future in concurrent.futures.as_completed(_futures, timeout=10):
            try:
                res = future.result(timeout=5)
                if res: rows.append(res)
            except Exception:
                continue
    except concurrent.futures.TimeoutError:
        pass
    finally:
        for f in _futures:
            f.cancel()

    if not rows: return pd.DataFrame()
    return pd.DataFrame(rows).sort_values("등락률_num", ascending=False).reset_index(drop=True)

@st.cache_data(ttl=600, show_spinner=False)
def fetch_dividend_ranking():
    _dbg = lambda msg: print(f"[DEBUG {datetime.datetime.now().strftime('%H:%M:%S')}] [배당] {msg}", file=sys.stderr, flush=True)
    _dbg("함수 시작")
    base_url = "https://finance.naver.com/sise/dividend_list.naver"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

    def fetch_page(page):
        try:
            _dbg(f"페이지 {page} 요청 시작")
            res = requests.get(f"{base_url}?page={page}", headers=headers, timeout=10)
            res.encoding = res.apparent_encoding or 'euc-kr'
            _dbg(f"페이지 {page} 응답 수신, read_html 파싱 시작")

            # ✅ 정규식으로 href 속성에서 종목코드 추출 후 딕셔너리에 저장
            code_matches = re.findall(r'href="/item/main\.naver\?code=(\d+)"[^>]*>(.*?)</a>', res.text)
            name_to_code = {re.sub(r'<[^>]+>', '', name).strip(): code for code, name in code_matches}

            dfs = pd.read_html(io.StringIO(res.text))
            _dbg(f"페이지 {page} read_html 파싱 완료 ({len(dfs)}개 테이블)")
            for df in dfs:
                if any('종목명' in str(c) for c in df.columns.to_flat_index()):
                    name_col = next((c for c in df.columns if '종목명' in str(c)), None)
                    if name_col:
                        page_df = df.dropna(subset=[name_col])
                        if not page_df.empty:
                            page_df['종목코드'] = page_df[name_col].map(name_to_code)
                            return page_df
        except Exception as e:
            _dbg(f"페이지 {page} 실패: {e}")
        return None

    try:
        import re as _re
        _dbg("첫 페이지(전체 페이지 수 파악) 요청 시작")
        res0 = requests.get(base_url, headers=headers, timeout=10)
        res0.encoding = res0.apparent_encoding or 'euc-kr'
        _dbg("첫 페이지 응답 수신")
        page_nums = [int(p) for p in _re.findall(r'[?&]page=(\d+)', res0.text)]
        max_page = max(page_nums) if page_nums else 10
        max_page = min(max_page, 15)
        _dbg(f"총 {max_page}페이지 병렬 조회 시작")

        all_pages = []
        _executor = get_shared_executor()
        _futures = {_executor.submit(fetch_page, p): p for p in range(1, max_page + 1)}
        try:
            for future in concurrent.futures.as_completed(_futures, timeout=12):
                try:
                    result = future.result(timeout=6)
                    if result is not None:
                        all_pages.append(result)
                except Exception:
                    continue
        except concurrent.futures.TimeoutError:
            _dbg("전체 12초 상한 초과 (일부 페이지 스킵)")
        finally:
            for f in _futures:
                f.cancel()

        _dbg(f"병렬 조회 종료, {len(all_pages)}개 페이지 확보, concat 시작")
        if not all_pages:
            return pd.DataFrame()
        result = pd.concat(all_pages, ignore_index=True)
        result = result.drop_duplicates()
        _dbg("함수 종료 (성공)")
        return result
    except Exception as e:
        _dbg(f"함수 종료 (예외): {e}")
        return pd.DataFrame()

# =========================
# 📱 FnGuide 모바일 페이지 기반 수집 (2026-07 기준)
#
# 배경: comp.fnguide.com(데스크톱) 은 봇으로 판단되는 요청에 대해
#       gicode 파라미터를 무시하고 항상 삼성전자(005930) 스냅샷을
#       돌려주는 것으로 확인됨(진단 완료). 반면 모바일 페이지
#       (m.comp.fnguide.com) 는 정상적으로 종목별 데이터를 반환하므로
#       이쪽을 1차 소스로 사용한다.
#         - company_01.asp (기업개요) : 여전히 차단됨 → 사용 안 함, 네이버로 대체
#         - company_02.asp (재무정보) : 정상 → 재무제표 소스로 사용
#         - company_03.asp (컨센서스) : 정상 → 투자의견/목표주가 소스로 사용
# =========================

_FN_MOBILE_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 '
                  '(KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
    'Referer': 'https://m.comp.fnguide.com/',
    'Accept-Language': 'ko-KR,ko;q=0.9',
}


def _parse_mobile_consensus(html_text):
    """m.comp.fnguide.com company_03.asp(컨센서스) 페이지 파싱."""
    result = {"opinion": "", "opinion_score": "", "target": "", "analyst_count": ""}

    score_match = re.search(r'<div class="msg step\d"[^>]*><span[^>]*>([\d.]+)</span></div>', html_text)
    if score_match:
        try:
            op_val = float(score_match.group(1))
            if op_val > 0:
                result["opinion_score"] = f"{op_val:.1f} / 5.0"
                if op_val >= 4.5:   result["opinion"] = "🔥 강력매수"
                elif op_val >= 3.5: result["opinion"] = "👍 매수"
                elif op_val >= 2.5: result["opinion"] = "✋ 중립"
                elif op_val >= 1.5: result["opinion"] = "👎 매도"
                else:               result["opinion"] = "💀 강력매도"
        except ValueError:
            pass

    target_match = re.search(r'<dt>목표주가</dt>\s*<dd>([\d,]+)</dd>', html_text)
    if target_match:
        tg_raw = re.sub(r'[^\d]', '', target_match.group(1))
        if tg_raw:
            result["target"] = f"{int(tg_raw):,} 원"

    count_match = re.search(r'<dd>\s*(\d+)\s*개의\s*증권사\s*의견\s*</dd>', html_text)
    if count_match:
        result["analyst_count"] = f"추정기관 {count_match.group(1)}곳"

    return result


_WISEREPORT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Referer': 'https://navercomp.wisereport.co.kr/',
    'Accept-Language': 'ko-KR,ko;q=0.9',
}


def _parse_wisereport_overview(html_text):
    """
    navercomp.wisereport.co.kr 기업현황(c1010001.aspx) 페이지의 실제 정적 HTML 구조:

        <div class="cmp_comment">
          <ul class="dot_cmp">
            <li class="dot_cmp" data-cd="005930">동사는 ...</li>
            <li class="dot_cmp" data-cd="005930">...</li>
          </ul>
        </div>

    를 그대로 파싱한다 (2026-07 실측 확인 완료, 브라우저 '페이지 소스 보기' 기준).
    """
    try:
        block_match = re.search(r'<div class="cmp_comment">(.*?)</ul>', html_text, re.DOTALL)
        if not block_match:
            return ""

        li_items = re.findall(r'<li class="dot_cmp"[^>]*>(.*?)</li>', block_match.group(1), re.DOTALL)
        lines = []
        for item in li_items:
            text = re.sub(r'<[^>]+>', '', item)
            text = html_lib.unescape(text)
            text = re.sub(r'\s+', ' ', text).strip()
            if text:
                lines.append(text)

        if not lines:
            return ""
        # st.markdown(..., unsafe_allow_html=True)의 <p> 태그 안에서 줄바꿈이 보이도록
        # 개행 문자 대신 <br> 사용 (일반 \n은 HTML에서 공백으로 무시됨)
        return "<br>".join(f"• {line}" for line in lines)
    except Exception:
        return ""


# 💡 종목 헤더용 실시간 현재가 (기업정보 캐시(1시간)와 분리해 60초 캐시로 관리)
@st.cache_data(ttl=60, show_spinner=False)
def fetch_current_price_info(code):
    code = normalize_kr_code(code)
    info = {"price": None, "diff": None, "diff_pct": None, "status": "neutral"}
    try:
        url = f"https://m.stock.naver.com/api/stock/{code}/basic"
        res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=5)
        d = res.json()
        price = float(str(d.get("closePrice", "0")).replace(",", ""))
        diff = float(str(d.get("compareToPreviousClosePrice", "0")).replace(",", ""))
        diff_pct = float(str(d.get("fluctuationsRatio", "0")).replace(",", ""))
        if price > 0:
            info["price"] = price
            info["diff"] = diff
            info["diff_pct"] = diff_pct
            info["status"] = "up" if diff > 0 else ("down" if diff < 0 else "neutral")
    except Exception:
        pass
    return info


# 💡 모바일 페이지 기반 fetch_company_info_fnguide
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_company_info_fnguide(code):
    code = normalize_kr_code(code)
    data = {"name": "알 수 없음", "summary": "제공된 기업개요가 없습니다.", "opinion": "📭 분석의견 없음", "target": "데이터 없음", "opinion_score": "", "analyst_count": "", "consensus_note": ""}

    name_debug = {
        "code": code, "status": None, "resp_len": None,
        "title_raw": None, "title_match": False, "exception": None,
    }

    naver_headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://finance.naver.com/',
    }

    # ① 종목명 / 기업개요(요약): FnGuide 모바일 기업개요 페이지는 여전히 차단(삼성전자 고정)되므로
    #    네이버금융을 1차 소스로 사용한다. (짧은 1~2문장짜리 요약)
    try:
        nv_url = f"https://finance.naver.com/item/main.naver?code={code}"
        res = requests.get(nv_url, headers=naver_headers, timeout=6)
        name_debug["status"] = res.status_code
        # 네이버금융 응답 인코딩이 euc-kr 고정이 아닌 경우가 생겨 자동감지로 변경
        # (apparent_encoding이 실패하면 euc-kr을 최후 fallback으로 사용)
        res.encoding = res.apparent_encoding or 'euc-kr'
        html = res.text
        name_debug["resp_len"] = len(html)

        # 타이틀 포맷이 "종목명 : 네이버페이 증권" / "종목명 - 네이버 증권" 등으로 바뀔 수 있어
        # 뒤쪽 사이트명을 유연하게 잘라내는 방식으로 변경
        title_match = re.search(r'<title>(.*?)</title>', html, re.DOTALL)
        if title_match:
            name_debug["title_match"] = True
            name_debug["title_raw"] = title_match.group(1)[:100]
            raw_title = html_lib.unescape(title_match.group(1)).strip()
            # "종목명 : 네이버/Npay 증권" 등 사이트명이 바뀌어도 안전하도록,
            # 첫 구분자(:  |  -) 뒤는 사이트명으로 간주하고 통째로 제거
            raw_title = re.split(r'\s*[:：|\-–]\s*', raw_title)[0].strip()
            if raw_title:
                data["name"] = raw_title

        summary_match = re.search(r'class="summary_info"[^>]*>(.*?)</p>', html, re.DOTALL)
        if summary_match:
            text = re.sub(r'<[^>]+>', '', summary_match.group(1))
            text = html_lib.unescape(text)
            text = re.sub(r'\s+', ' ', text).strip()
            if text:
                data["summary"] = text
    except Exception as e:
        name_debug["exception"] = f"{type(e).__name__}: {e}"

    _DEBUG_STORE[f"_fnname_debug_{code}"] = name_debug

    # ①-2 기업개요(상세): navercomp.wisereport.co.kr의 <div class="cmp_comment"> 블록에서
    #    실제 FnGuide 기업개요 불릿 설명을 가져와 네이버 요약(짧은 1~2문장)보다
    #    더 길고 구체적인 내용으로 덮어쓴다. 실패 시 위 네이버 요약을 그대로 유지(안전한 폴백).
    try:
        wr_url = f"https://navercomp.wisereport.co.kr/v2/company/c1010001.aspx?cmp_cd={code}"
        res_wr = requests.get(wr_url, headers=_WISEREPORT_HEADERS, timeout=8)
        res_wr.encoding = res_wr.apparent_encoding or 'utf-8'
        # data-cd 속성에 요청 코드가 실제로 있는지 확인 (다른 종목 고정 스냅샷 방지)
        if f'data-cd="{code}"' in res_wr.text:
            detailed_summary = _parse_wisereport_overview(res_wr.text)
            if detailed_summary and len(detailed_summary) > 20:
                data["summary"] = detailed_summary
    except Exception:
        pass

    # ② 투자의견 / 목표주가 컨센서스: FnGuide 모바일 컨센서스 페이지 사용
    consensus = {"opinion": "", "opinion_score": "", "target": "", "analyst_count": ""}
    fetch_failed = False
    try:
        fn_mobile_url = f"https://m.comp.fnguide.com/m2/company_03.asp?pGB=1&gicode=A{code}&MenuYn=Y"
        res3 = requests.get(fn_mobile_url, headers=_FN_MOBILE_HEADERS, timeout=8)
        res3.encoding = res3.apparent_encoding or 'utf-8'
        # 차단(삼성전자 고정) 여부 확인: 요청 코드가 응답 안에 실제로 있는지 검증
        if code in res3.text:
            consensus = _parse_mobile_consensus(res3.text)
        else:
            fetch_failed = True
    except Exception:
        fetch_failed = True

    if consensus["opinion"]:
        data["opinion"] = consensus["opinion"]
    if consensus["opinion_score"]:
        data["opinion_score"] = consensus["opinion_score"]
    if consensus["target"]:
        data["target"] = consensus["target"]
    if consensus["analyst_count"]:
        data["analyst_count"] = consensus["analyst_count"]

    if not consensus["opinion"] and not consensus["target"]:
        if fetch_failed:
            data["consensus_note"] = "⚠️ 컨센서스 데이터를 불러오는 중 통신 오류가 발생했습니다. 잠시 후 다시 시도해 주세요."
        else:
            data["consensus_note"] = "이 종목은 현재 분석을 진행하는 증권사가 없어 매수의견·목표주가 컨센서스가 제공되지 않습니다."

    return data


@st.cache_data(ttl=1800, show_spinner=False)
def fetch_investor_trend_by_code(code, days=20):
    """
    네이버금융 frgn.naver 페이지(외국인·기관 순매매 거래량, 정적 HTML 표)에서
    최근 N영업일의 일별 순매매 수량(주)을 가져온다.

    주의: 네이버는 '개인' 순매매를 별도로 제공하지 않으므로,
    개인 순매매는 (외국인 + 기관)의 반대 부호로 추정한 값이다(코스피/코스닥 시장
    전체 수급이 대략 상쇄된다는 근사 가정). UI에는 반드시 '추정'임을 표기할 것.
    """
    code = normalize_kr_code(code)
    result_df = pd.DataFrame()

    # 디버그 정보 (실패 원인 진단용). 성공/실패와 무관하게 마지막 시도 결과를 세션에 남긴다.
    debug = {
        "code": code, "days": days, "pages_tried": 0,
        "last_status": None, "last_url": None, "exception": None,
        "resp_len": None, "resp_snippet": None, "found_table": False,
        "num_tables": None, "tables_columns": None,
    }

    naver_headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://finance.naver.com/',
    }

    def _find_col(columns, keyword):
        for c in columns:
            if keyword in str(c):
                return c
        return None

    def _find_investor_col(columns, top_kw, sub_kw):
        """네이버 표 헤더가 ('기관', '순매매량') 처럼 2단(멀티인덱스)으로 바뀐 경우와
        예전처럼 '기관순매매' 단일 문자열인 경우를 모두 지원.
        top_kw(예: '기관','외국인')와 sub_kw(예: '순매매')가 각각 상/하위 레벨에
        모두 있는 컬럼만 매칭해서 '보유주수'/'보유율' 같은 다른 외국인 컬럼과 헷갈리지 않게 한다."""
        for c in columns:
            if isinstance(c, tuple):
                top = str(c[0])
                sub = str(c[-1])
            else:
                top = sub = str(c)
            if top_kw in top and sub_kw in sub:
                return c
        return None

    try:
        collected = []
        pages_needed = max(1, (days // 10) + 2)  # 페이지당 10행 기준, 여유분 확보
        for page in range(1, pages_needed + 1):
            url = f"https://finance.naver.com/item/frgn.naver?code={code}&page={page}"
            debug["pages_tried"] = page
            debug["last_url"] = url
            res = requests.get(url, headers=naver_headers, timeout=8)
            debug["last_status"] = res.status_code
            res.encoding = res.apparent_encoding or 'euc-kr'
            debug["resp_len"] = len(res.text)
            debug["resp_snippet"] = res.text[:300]

            if res.status_code != 200:
                break

            dfs = pd.read_html(io.StringIO(res.text))
            debug["num_tables"] = len(dfs)
            debug["tables_columns"] = [
                [str(c) for c in d.columns][:12] for d in dfs
            ][:10]
            target_df = next(
                (d for d in dfs if _find_investor_col(d.columns, '기관', '순매매') is not None),
                None,
            )
            if target_df is None:
                break
            debug["found_table"] = True

            col_date = _find_col(target_df.columns, '날짜')
            if col_date is None:
                break
            target_df = target_df.dropna(subset=[col_date])
            if target_df.empty:
                break

            collected.append(target_df)
            if sum(len(d) for d in collected) >= days:
                break

        if not collected:
            _DEBUG_STORE[f"_trend_debug_{code}"] = debug
            return result_df

        merged = pd.concat(collected, ignore_index=True)
        col_date = _find_col(merged.columns, '날짜')
        col_inst = _find_investor_col(merged.columns, '기관', '순매매')
        col_frgn = _find_investor_col(merged.columns, '외국인', '순매매')
        if not (col_date is not None and col_inst is not None and col_frgn is not None):
            _DEBUG_STORE[f"_trend_debug_{code}"] = debug
            return result_df

        merged = merged.drop_duplicates(subset=[col_date]).head(days)

        out = pd.DataFrame()
        out['날짜'] = merged[col_date].astype(str)
        out['외국인순매매'] = pd.to_numeric(merged[col_frgn].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
        out['기관순매매'] = pd.to_numeric(merged[col_inst].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
        out['개인순매매(추정)'] = -(out['기관순매매'] + out['외국인순매매'])
        result_df = out.reset_index(drop=True)
    except Exception as e:
        debug["exception"] = f"{type(e).__name__}: {e}"
        _DEBUG_STORE[f"_trend_debug_{code}"] = debug

    return result_df


def _fn_clean_numeric(series):
    """콤마/배 등을 제거하고 숫자로 변환."""
    return pd.to_numeric(
        pd.Series(series).astype(str).str.replace(',', '').str.replace('배', '').str.strip(),
        errors='coerce'
    )


def _fn_lookup(df, row_label, col_label):
    """df.loc[row_label, col_label] 값을 안전하게 숫자로 반환. 실패 시 NaN."""
    try:
        if df is None or row_label not in df.index or col_label not in df.columns:
            return float('nan')
        val = df.loc[row_label, col_label]
        if isinstance(val, pd.Series):  # 중복 인덱스 방어
            val = val.iloc[0]
        return _fn_clean_numeric([val]).iloc[0]
    except Exception:
        return float('nan')


def _fn_lookup_row_multi(df, keyword_candidates, col_label):
    """df.index 중 keyword_candidates(부분 일치) 어느 하나라도 포함된 행을 찾아 값을 반환.
    FnGuide가 '주당배당금(원)' / 'DPS(원)' 등 표기를 바꿔도 최대한 안전하게 매칭하기 위한 헬퍼."""
    try:
        if df is None or df.empty or col_label not in df.columns:
            return float('nan')
        idx_str_list = [str(x) for x in df.index.tolist()]
        for kw in keyword_candidates:
            for idx_str, idx_orig in zip(idx_str_list, df.index.tolist()):
                if kw in idx_str:
                    val = df.loc[idx_orig, col_label]
                    if isinstance(val, pd.Series):
                        val = val.iloc[0]
                    result = _fn_clean_numeric([val]).iloc[0]
                    if pd.notna(result):
                        return result
        return float('nan')
    except Exception:
        return float('nan')


def _fn_build_period_table(income_df, balance_df, valuation_df, is_quarter):
    """손익/재무상태/투자지표 테이블을 기간(연도·분기) 기준 한 표로 병합."""
    core_items = ['매출액', '영업이익', '당기순이익', '영업이익률', '순이익률', 'ROE', 'PER', 'PBR', '부채비율']

    period_cols = [c for c in income_df.columns if re.match(r'^\d{4}/\d{2}(\(E\))?$', str(c))]
    rows = []
    for c in period_cols:
        rev = _fn_lookup(income_df, '매출액', c)
        op = _fn_lookup(income_df, '영업이익', c)
        ni = _fn_lookup(income_df, '당기순이익', c)

        op_margin = (op / rev * 100) if pd.notna(rev) and pd.notna(op) and rev != 0 else float('nan')
        ni_margin = (ni / rev * 100) if pd.notna(rev) and pd.notna(ni) and rev != 0 else float('nan')

        debt = _fn_lookup(balance_df, '부채', c)
        equity = _fn_lookup(balance_df, '자본', c)
        debt_ratio = (debt / equity * 100) if pd.notna(debt) and pd.notna(equity) and equity != 0 else float('nan')

        roe = _fn_lookup(valuation_df, 'ROE', c)
        per = _fn_lookup(valuation_df, 'PER(배)', c)
        pbr = _fn_lookup(valuation_df, 'PBR(배)', c)

        rows.append({
            '연도/분기': c.replace('(E)', ''),
            '매출액': rev, '영업이익': op, '당기순이익': ni,
            '영업이익률': op_margin, '순이익률': ni_margin,
            'ROE': roe, 'PER': per, 'PBR': pbr, '부채비율': debt_ratio,
        })

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    label = '성장률(QoQ)' if is_quarter else '성장률(YoY)'
    for col in ['매출액', '영업이익']:
        out[f'{col} {label}'] = out[col].pct_change() * 100

    final_cols = ['연도/분기']
    for item in core_items:
        final_cols.append(item)
        if f'{item} {label}' in out.columns:
            final_cols.append(f'{item} {label}')
    return out[final_cols]


def _fn_build_dividend_table(income_df, valuation_df):
    """이미 안정적으로 잘 나오는 값들(주당배당금, EPS, PER, 당기순이익)만으로
    배당총액·배당수익률·배당성향까지 계산한다.
    (FnGuide 표에는 '배당수익률'이나 '상장주식수' 행이 아예 없는 경우가 많아
     그 값들을 직접 lookup하면 계속 NaN이 나오므로, 아래처럼 이미 존재가 확인된
     항목들의 조합으로 우회 계산한다)

        배당성향(%)   = 주당배당금(DPS) / EPS × 100
        배당총액(억원) = 당기순이익(억원) × 배당성향 / 100
        배당수익률(%)  = 주당배당금(DPS) / (EPS × PER) × 100   (EPS×PER ≒ 그 해 주가 추정)
    """
    if valuation_df is None or valuation_df.empty:
        return pd.DataFrame()

    period_cols = [c for c in valuation_df.columns if re.match(r'^\d{4}/\d{2}(\(E\))?$', str(c))]
    dps_keywords = ['주당배당금', 'DPS']
    eps_keywords = ['EPS']
    per_keywords = ['PER(배)', 'PER']

    rows = []
    for c in period_cols:
        dps = _fn_lookup_row_multi(valuation_df, dps_keywords, c)
        eps = _fn_lookup_row_multi(valuation_df, eps_keywords, c)
        per = _fn_lookup_row_multi(valuation_df, per_keywords, c)
        net_income = _fn_lookup(income_df, '당기순이익', c) if income_df is not None else float('nan')

        payout_ratio = float('nan')
        if pd.notna(dps) and pd.notna(eps) and eps != 0:
            payout_ratio = (dps / eps) * 100

        total_div = float('nan')
        if pd.notna(payout_ratio) and pd.notna(net_income):
            total_div = net_income * payout_ratio / 100  # 억원 단위 (당기순이익 단위 그대로 사용)

        div_yield = float('nan')
        if pd.notna(dps) and pd.notna(eps) and pd.notna(per) and eps != 0 and per != 0:
            est_price = eps * per
            if est_price != 0:
                div_yield = (dps / est_price) * 100

        rows.append({
            '연도': c.replace('(E)', ''),
            '주당배당금': dps,
            '배당총액': total_div,
            '배당수익률': div_yield,
            '배당성향': payout_ratio,
        })

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    # 주당배당금이 없는(=그 해 배당 자체가 없거나 데이터가 없는) 연도는 제외
    out = out[out['주당배당금'].notna()].reset_index(drop=True)
    return out


# 💡 모바일 페이지(company_02: 재무정보) 기반 fetch_fnguide_data
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_fnguide_data(code):
    code = normalize_kr_code(code)
    df_annual, df_quarter, df_dividend = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    debug = {
        "code": code, "status": None, "resp_len": None,
        "code_in_html": None, "num_tables": None,
        "table_shapes": None, "exception": None,
        "income_a_index": None, "balance_a_index": None, "valuation_a_index": None,
        "income_a_columns": None, "period_cols_detected": None,
        "all_values_nan": None,
    }

    try:
        url = f"https://m.comp.fnguide.com/m2/company_02.asp?pGB=1&gicode=A{code}&MenuYn=Y"
        res = requests.get(url, headers=_FN_MOBILE_HEADERS, timeout=10)
        debug["status"] = res.status_code
        res.encoding = res.apparent_encoding or 'utf-8'
        html = res.text
        debug["resp_len"] = len(html)

        # 차단(삼성전자 고정) 여부 확인
        debug["code_in_html"] = (code in html)
        if code not in html:
            _DEBUG_STORE[f"_fnguide_debug_{code}"] = debug
            return df_annual, df_quarter, df_dividend

        dfs = pd.read_html(io.StringIO(html))
        debug["num_tables"] = len(dfs)
        debug["table_shapes"] = [list(d.shape) for d in dfs][:40]

        # ── 표 탐색: 고정 인덱스(dfs[1], dfs[9], dfs[25]...) 대신
        #    표 안의 실제 항목명(행 이름)으로 찾는다. FnGuide가 배너/위젯 표를
        #    추가/삭제해서 표 순서(인덱스)가 밀려도 안 깨지도록 하기 위함.
        #    (과거: 순서 고정 가정이 깨지며 전종목 실적 조회가 실패하는 버그 발생)
        def _indexed(d):
            try:
                return d.set_index(d.columns[0])
            except Exception:
                return None

        def _has_rows(idf, keywords):
            if idf is None or idf.empty:
                return False
            idx_str = [str(x) for x in idf.index.tolist()]
            return all(any(kw in s for s in idx_str) for kw in keywords)

        indexed_dfs = [_indexed(d) for d in dfs]

        income_candidates = [d for d in indexed_dfs if _has_rows(d, ['매출액', '영업이익'])]
        balance_candidates = [d for d in indexed_dfs if _has_rows(d, ['부채', '자본'])]
        valuation_candidates = [d for d in indexed_dfs if _has_rows(d, ['ROE', 'PER(배)'])]

        debug["income_candidates_found"] = len(income_candidates)
        debug["balance_candidates_found"] = len(balance_candidates)
        debug["valuation_candidates_found"] = len(valuation_candidates)

        # 문서상 등장 순서 기준: 첫 번째 = 연간(연결), 두 번째 = 분기(연결)
        if len(income_candidates) < 2 or len(balance_candidates) < 2 or not valuation_candidates:
            debug["exception"] = "필요한 재무 표를 찾지 못함 (항목명 매칭 실패 - FnGuide 페이지 구조 변경 가능성)"
            _DEBUG_STORE[f"_fnguide_debug_{code}"] = debug
            return df_annual, df_quarter, df_dividend

        income_a = income_candidates[0]
        income_q = income_candidates[1]
        balance_a = balance_candidates[0]
        balance_q = balance_candidates[1]
        valuation_a = valuation_candidates[0]
        # ✅ 밸류에이션(ROE/PER/PBR) 표도 손익·재무상태표와 마찬가지로
        #    연간용/분기용이 순서대로 2개 존재. 기존에는 두 번째(분기용)를
        #    찾지 않고 None을 넘겨서 분기 실적의 ROE/PER/PBR이 항상 비어있었음.
        valuation_q = valuation_candidates[1] if len(valuation_candidates) > 1 else None

        debug["income_a_index"] = [str(x) for x in income_a.index.tolist()][:30]
        debug["balance_a_index"] = [str(x) for x in balance_a.index.tolist()][:30]
        debug["valuation_a_index"] = [str(x) for x in valuation_a.index.tolist()][:30]
        debug["valuation_candidates_count"] = len(valuation_candidates)
        debug["income_a_columns"] = [str(x) for x in income_a.columns.tolist()][:20]
        debug["period_cols_detected"] = [
            str(c) for c in income_a.columns if re.match(r'^\d{4}/\d{2}(\(E\))?$', str(c))
        ]

        df_annual = _fn_build_period_table(income_a, balance_a, valuation_a, is_quarter=False)
        df_quarter = _fn_build_period_table(income_q, balance_q, valuation_q, is_quarter=True)

        # ── 배당 히스토리 (주당배당금 / 배당총액(추정) / 배당수익률(추정) / 배당성향) ──────
        try:
            df_dividend = _fn_build_dividend_table(income_a, valuation_a)
            debug["dividend_rows_found"] = len(df_dividend)
        except Exception as e:
            debug["dividend_exception"] = f"{type(e).__name__}: {e}"
        _DEBUG_STORE[f"_fnguide_debug_{code}"] = debug

        value_cols = [c for c in df_annual.columns if c != '연도/분기']
        all_nan = bool(df_annual.empty) or bool(df_annual[value_cols].isna().all().all())
        debug["all_values_nan"] = all_nan

        # ── 값 추출 실패 원인 정밀 진단 ──────────────────────────────
        try:
            debug["income_a_index_repr"] = [repr(x) for x in income_a.index.tolist()][:10]
            debug["income_a_columns_repr"] = [repr(c) for c in income_a.columns.tolist()][:10]
            debug["income_a_columns_dtype"] = [str(type(c)) for c in income_a.columns.tolist()][:5]
            debug["income_a_index_dtype"] = [str(type(x)) for x in income_a.index.tolist()][:5]
            _period_cols_for_sample = [c for c in income_a.columns if re.match(r'^\d{4}/\d{2}(\(E\))?$', str(c))]
            if '매출액' in income_a.index and _period_cols_for_sample:
                sample_col = _period_cols_for_sample[0]
                raw_val = income_a.loc['매출액', sample_col]
                debug["sample_lookup_col_used"] = repr(sample_col)
                debug["sample_lookup_raw_value"] = repr(raw_val)
                debug["sample_lookup_raw_type"] = str(type(raw_val))
                debug["sample_lookup_fn_result"] = str(_fn_lookup(income_a, '매출액', sample_col))
            else:
                debug["sample_lookup_note"] = "매출액 index 또는 매칭되는 기간 컬럼을 df 자체에서 못 찾음 (in 연산자 레벨에서 실패)"
        except Exception as e:
            debug["sample_lookup_exception"] = f"{type(e).__name__}: {e}"

        if (df_annual.empty and df_quarter.empty) or all_nan:
            _DEBUG_STORE[f"_fnguide_debug_{code}"] = debug

    except Exception as e:
        debug["exception"] = f"{type(e).__name__}: {e}"
        _DEBUG_STORE[f"_fnguide_debug_{code}"] = debug

    return df_annual, df_quarter, df_dividend


# =========================
# 📢 DART 전자공시 연동 모듈
# =========================
import zipfile
import xml.etree.ElementTree as ET
from io import BytesIO


def get_dart_api_key():
    """secrets.toml 또는 환경변수에서 DART API 키를 가져온다."""
    try:
        return st.secrets["DART_API_KEY"]
    except Exception:
        return os.environ.get("DART_API_KEY", "")


@st.cache_data(ttl=86400 * 7, show_spinner=False)  # 고유번호 목록은 자주 안 바뀌므로 7일 캐싱
def fetch_dart_corp_code_map():
    """
    DART 전체 기업 고유번호(corp_code) 목록을 받아
    {6자리 종목코드: {"corp_code": ..., "corp_name": ...}} 형태로 반환.
    """
    api_key = get_dart_api_key()
    if not api_key:
        return {}

    try:
        url = "https://opendart.fss.or.kr/api/corpCode.xml"
        res = requests.get(url, params={"crtfc_key": api_key}, timeout=10)
        res.raise_for_status()

        with zipfile.ZipFile(BytesIO(res.content)) as zf:
            xml_bytes = zf.read(zf.namelist()[0])

        root = ET.fromstring(xml_bytes)
        result = {}
        for item in root.findall("list"):
            stock_code = (item.findtext("stock_code") or "").strip()
            corp_code = (item.findtext("corp_code") or "").strip()
            corp_name = (item.findtext("corp_name") or "").strip()
            if stock_code:  # 상장사만 (비상장은 stock_code가 빈 문자열)
                result[normalize_kr_code(stock_code)] = {
                    "corp_code": corp_code,
                    "corp_name": corp_name,
                }
        return result
    except Exception:
        return {}


@st.cache_data(ttl=600, show_spinner=False)  # 10분 캐싱
def fetch_disclosure_list(code, days=90, page_count=30):
    """
    특정 종목코드의 최근 공시 목록을 반환.
    반환 형식: list of dict [{date, title, report_no, url, flag}, ...]
    """
    api_key = get_dart_api_key()
    if not api_key:
        return []

    code = normalize_kr_code(code)
    corp_map = fetch_dart_corp_code_map()
    corp_info = corp_map.get(code)
    if not corp_info:
        return []

    end_dt = datetime.datetime.now()
    start_dt = end_dt - datetime.timedelta(days=days)

    try:
        url = "https://opendart.fss.or.kr/api/list.json"
        params = {
            "crtfc_key": api_key,
            "corp_code": corp_info["corp_code"],
            "bgn_de": start_dt.strftime("%Y%m%d"),
            "end_de": end_dt.strftime("%Y%m%d"),
            "page_no": 1,
            "page_count": page_count,
            "sort": "date",
            "sort_mth": "desc",
        }
        res = requests.get(url, params=params, timeout=8)
        data = res.json()

        if data.get("status") != "000":
            return []

        rows = []
        for item in data.get("list", []):
            title = item.get("report_nm", "")
            rows.append({
                "date": item.get("rcept_dt", ""),
                "title": title,
                "report_no": item.get("rcept_no", ""),
                "url": f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={item.get('rcept_no', '')}",
                "flag": _classify_disclosure(title),
            })
        return rows
    except Exception:
        return []


def _classify_disclosure(title):
    """공시 제목을 보고 중요도/성격을 간단히 분류 (호재/주의/일반)."""
    caution_keywords = ["유상증자", "무상감자", "관리종목", "상장폐지", "횡령", "배임", "소송", "감사의견"]
    positive_keywords = ["자사주", "무상증자", "실적", "특허", "수주", "배당"]

    for kw in caution_keywords:
        if kw in title:
            return "caution"
    for kw in positive_keywords:
        if kw in title:
            return "positive"
    return "neutral"


def render_disclosure_tab(code):
    """관심종목 상세 화면 또는 기업 재무 분석 화면의 '공시' 탭에서 호출."""
    api_key = get_dart_api_key()
    if not api_key:
        st.info("DART API 키가 설정되지 않았습니다. `.streamlit/secrets.toml`에 `DART_API_KEY`를 추가해주세요.")
        return

    col_range, col_refresh = st.columns([5, 1])
    with col_range:
        period_choice = st.radio(
            "조회 기간",
            ["최근 30일", "최근 90일", "최근 180일", "최근 1년"],
            index=1,
            horizontal=True,
            key=f"dart_period_{code}",
            label_visibility="collapsed",
        )
        days = {"최근 30일": 30, "최근 90일": 90, "최근 180일": 180, "최근 1년": 365}[period_choice]
    with col_refresh:
        if st.button("새로고침", key=f"dart_refresh_{code}", use_container_width=True):
            fetch_disclosure_list.clear()

    rows = run_with_progress("공시 데이터 조회 중...", fetch_disclosure_list, code, days)

    if not rows:
        st.caption("최근 공시 내역이 없거나 DART에 등록된 종목코드를 찾을 수 없습니다.")
        return

    flag_style = {
        "positive": ("#16A34A", "#F0FDF4", "🟢"),
        "caution":  ("#DC2626", "#FFF7F7", "🔴"),
        "neutral":  ("#475569", "#F8FAFC", "⚪"),
    }

    for row in rows:
        color, bg, dot = flag_style[row["flag"]]
        date_fmt = f"{row['date'][:4]}.{row['date'][4:6]}.{row['date'][6:8]}" if len(row["date"]) == 8 else row["date"]
        st.markdown(
            f'''<a href="{row['url']}" target="_blank" style="text-decoration:none;">
                <div style="display:flex; align-items:center; gap:10px; background:{bg};
                            border:1px solid #E2E8F0; border-radius:8px; padding:10px 14px; margin-bottom:6px;">
                    <span style="font-size:12px;">{dot}</span>
                    <span style="font-size:11px; color:#94A3B8; min-width:70px;">{date_fmt}</span>
                    <span style="font-size:13px; color:{color}; font-weight:600;">{html_lib.escape(row['title'])}</span>
                </div>
            </a>''',
            unsafe_allow_html=True
        )


def fetch_page_data(sosok, page, headers, cookies):
    time.sleep(random.uniform(0.1, 0.3))
    url = f"https://finance.naver.com/sise/sise_market_sum.naver?sosok={sosok}&page={page}"
    try:
        res = requests.get(url, headers=headers, cookies=cookies, timeout=10)
        res.encoding = res.apparent_encoding or 'euc-kr'
        code_matches = re.findall(r'href="/item/main\.naver\?code=(\d+)" class="tltle">(.*?)</a>', res.text)
        name_to_code = {name: code for code, name in code_matches}
        if not name_to_code: return None
        dfs = pd.read_html(io.StringIO(res.text))
        main_df = next((df for df in dfs if '종목명' in df.columns), None)
        if main_df is None or main_df.empty: return None
        main_df = main_df.dropna(subset=['종목명'])
        main_df['종목코드'] = main_df['종목명'].map(name_to_code)
        main_df['시장'] = "코스피" if sosok == 0 else "코스닥"
        return main_df
    except Exception:
        return None

def fetch_screener_data_generator():
    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://finance.naver.com/sise/sise_market_sum.naver",
    }
    yield "보안 세션 접속 및 쿠키 발급 중...", 5
    
    try:
        session.get("https://finance.naver.com/sise/sise_market_sum.naver", headers=headers, timeout=10)
    except Exception:
        pass  # 세션 워밍업 실패해도 쿠키 없이 계속 진행 (뒤에서 페이지별로 재시도됨)
    time.sleep(0.5)

    field_url = "https://finance.naver.com/sise/field_submit.naver?menu=market_sum&returnUrl=https%3A%2F%2Ffinance.naver.com%2Fsise%2Fsise_market_sum.naver&fieldIds=per&fieldIds=pbr&fieldIds=roe&fieldIds=dividend&fieldIds=property_total&fieldIds=debt_total&fieldIds=high52"
    try:
        session.get(field_url, headers=headers, timeout=10)
    except Exception:
        pass
    cookies = session.cookies.get_dict()
    
    all_data = []
    urls = [(sosok, page) for sosok in [0, 1] for page in range(1, 45)]
    total_pages = len(urls)
    completed = 0
    failed_pages = []

    _executor = get_shared_executor()
    processed = set()
    future_to_url = {_executor.submit(fetch_page_data, s, p, headers, cookies): (s, p) for s, p in urls}
    try:
        for future in concurrent.futures.as_completed(future_to_url, timeout=35):
            completed += 1
            progress_pct = 10 + int((completed / total_pages) * 70)
            yield f"⚡ 스텔스 모드 스캔 중... ({completed}/{total_pages} 페이지)", progress_pct
            s, p = future_to_url[future]
            processed.add((s, p))
            try:
                df = future.result(timeout=10)
            except Exception:
                df = None
            if df is not None and not df.empty:
                all_data.append(df)
            else:
                failed_pages.append((s, p))
    except concurrent.futures.TimeoutError:
        # 전체 상한(35초) 초과 → 아직 결과가 안 온 나머지 페이지는 실패로 간주하고 재시도 라운드로 넘김
        for s, p in urls:
            if (s, p) not in processed:
                failed_pages.append((s, p))
    finally:
        for f in future_to_url:
            f.cancel()

    # ── 실패한 페이지 재시도 (네이버 측 레이트리밋으로 뒷부분 페이지들이 몰려서 실패하는 경우 대응) ──
    # 동시 요청 수를 점점 줄이고, 대기 시간을 늘려가며 최대 3라운드까지 재시도한다.
    retry_round = 0
    backoff_seconds = [2, 5, 10]
    retry_workers = [2, 1, 1]
    while failed_pages and retry_round < 3:
        yield f"⚠️ {len(failed_pages)}개 페이지 재시도 중... ({retry_round + 1}/3 라운드, {backoff_seconds[retry_round]}초 대기)", 82 + retry_round * 3
        time.sleep(backoff_seconds[retry_round])
        still_failed = []
        _retry_processed = set()
        _retry_executor = get_shared_executor()
        future_to_url = {_retry_executor.submit(fetch_page_data, s, p, headers, cookies): (s, p) for s, p in failed_pages}
        try:
            for future in concurrent.futures.as_completed(future_to_url, timeout=18):
                s, p = future_to_url[future]
                _retry_processed.add((s, p))
                try:
                    df = future.result(timeout=8)
                except Exception:
                    df = None
                if df is not None and not df.empty:
                    all_data.append(df)
                else:
                    still_failed.append((s, p))
        except concurrent.futures.TimeoutError:
            for s, p in failed_pages:
                if (s, p) not in _retry_processed:
                    still_failed.append((s, p))
        finally:
            for f in future_to_url:
                f.cancel()
        failed_pages = still_failed
        retry_round += 1

    if failed_pages:
        st.session_state["_screener_missing_pages"] = failed_pages
    else:
        st.session_state["_screener_missing_pages"] = []

    if not all_data: raise Exception("네이버 금융 데이터를 불러오지 못했습니다. (서버 응답 지연)")
    yield "데이터 병합 및 재무 지표 자체 계산 중...", 95
    final_df = pd.concat(all_data, ignore_index=True)
    
    def get_col(df, candidates):
        for c in candidates:
            if c in df.columns: return c
        for c in df.columns:
            for cand in candidates:
                if cand.lower() in c.lower(): return c
        return None
        
    price_c  = get_col(final_df, ['현재가'])
    div_c    = get_col(final_df, ['주당배당금', '배당금'])
    per_c    = get_col(final_df, ['PER', 'PER(배)'])
    pbr_c    = get_col(final_df, ['PBR', 'PBR(배)'])
    roe_c    = get_col(final_df, ['ROE', 'ROE(%)'])
    prop_c   = get_col(final_df, ['자산총계'])
    debt_c   = get_col(final_df, ['부채총계'])
    mkt_c    = get_col(final_df, ['시장'])
    high52_c = get_col(final_df, ['52주최고', '최고가', 'high52', '52주 최고'])
    
    final_df['현재가'] = pd.to_numeric(final_df[price_c].astype(str).str.replace(',', ''), errors='coerce').fillna(0.0) if price_c else 0.0
    final_df['주당배당금'] = pd.to_numeric(final_df[div_c].astype(str).str.replace(',', ''), errors='coerce').fillna(0.0) if div_c else 0.0
    final_df['자산총계'] = pd.to_numeric(final_df[prop_c].astype(str).str.replace(',', ''), errors='coerce').fillna(0.0) if prop_c else 0.0
    final_df['부채비율'] = 0.0
    final_df['부채총계'] = pd.to_numeric(final_df[debt_c].astype(str).str.replace(',', ''), errors='coerce').fillna(0.0) if debt_c else 0.0
    
    final_df['배당수익률'] = 0.0
    mask_div = (final_df['현재가'] > 0) & (final_df['주당배당금'] > 0)
    final_df.loc[mask_div, '배당수익률'] = (final_df.loc[mask_div, '주당배당금'] / final_df.loc[mask_div, '현재가']) * 100
    
    final_df['자본총계'] = final_df['자산총계'] - final_df['부채총계']
    mask_debt = (final_df['자본총계'] > 0) & (final_df['부채총계'] >= 0)
    final_df.loc[mask_debt, '부채비율'] = (final_df.loc[mask_debt, '부채총계'] / final_df.loc[mask_debt, '자본총계']) * 100
    
    final_df['PER'] = pd.to_numeric(final_df[per_c].astype(str).str.replace(',', ''), errors='coerce').fillna(0.0) if per_c else 0.0
    final_df['PBR'] = pd.to_numeric(final_df[pbr_c].astype(str).str.replace(',', ''), errors='coerce').fillna(0.0) if pbr_c else 0.0
    final_df['ROE'] = pd.to_numeric(final_df[roe_c].astype(str).str.replace(',', ''), errors='coerce').fillna(0.0) if roe_c else 0.0
    final_df['시장'] = final_df[mkt_c] if mkt_c else "코스피"

    if high52_c:
        final_df['52주고점'] = pd.to_numeric(final_df[high52_c].astype(str).str.replace(',', ''), errors='coerce').fillna(0.0)
        mask_high = (final_df['현재가'] > 0) & (final_df['52주고점'] > 0)
        final_df['고점대비(%)'] = 0.0
        final_df.loc[mask_high, '고점대비(%)'] = ((final_df.loc[mask_high, '현재가'] - final_df.loc[mask_high, '52주고점']) / final_df.loc[mask_high, '52주고점']) * 100
        final_df = final_df[['종목코드', '종목명', '시장', '현재가', '52주고점', '고점대비(%)', 'PER', 'PBR', '배당수익률', 'ROE', '부채비율']]
    else:
        final_df = final_df[['종목코드', '종목명', '시장', '현재가', 'PER', 'PBR', '배당수익률', 'ROE', '부채비율']]
    yield final_df, 100

@st.cache_data(ttl=3600*12, show_spinner=False)
def fetch_and_cache_screener_data():
    final_df = None
    for item, _ in fetch_screener_data_generator():
        if isinstance(item, pd.DataFrame): final_df = item
    return final_df

@st.cache_data(ttl=60, show_spinner=False)
def fetch_live_price_only(code):
    """관심종목용: 종목코드 하나의 실시간에 가까운 현재가만 빠르게 조회 (60초 캐시)."""
    try:
        url = f"https://m.stock.naver.com/api/stock/{code}/basic"
        res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=3)
        if res.status_code == 200:
            data = res.json()
            price = float(str(data.get('closePrice', '0')).replace(',', ''))
            if price > 0:
                return price
    except Exception:
        pass
    return None


@st.cache_data(ttl=60, show_spinner=False)
def fetch_live_price_change(code):
    """관심종목용: 현재가 + 전일대비 등락률(%) + 등락액을 함께 조회 (60초 캐시). 반환: (현재가, 등락률, 등락액) 또는 (None, None, None)."""
    try:
        url = f"https://m.stock.naver.com/api/stock/{code}/basic"
        res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=3)
        if res.status_code != 200:
            return None, None, None
        data = res.json()
        price = float(str(data.get('closePrice', '0')).replace(',', ''))
        if price <= 0:
            return None, None, None

        diff_raw = str(data.get('compareToPreviousClosePrice', '0')).replace(',', '')
        diff = float(diff_raw) if diff_raw not in ('', 'None') else 0.0

        direction = (data.get('compareToPreviousPrice') or {}).get('code', '')
        if direction in ('4', '5'):      # 하한가/하락
            diff = -abs(diff)
        elif direction in ('1', '2'):    # 상한가/상승
            diff = abs(diff)
        else:                             # 보합 등
            diff = 0.0

        prev_close = price - diff
        pct = (diff / prev_close * 100) if prev_close else 0.0
        return price, pct, diff
    except Exception:
        return None, None, None

HIGH52_PATH = "saved_high52_data.csv"

def merge_high52(df):
    if not os.path.exists(HIGH52_PATH): return df
    try:
        h = pd.read_csv(HIGH52_PATH, dtype={'종목코드': str})
        h['종목코드'] = h['종목코드'].str.replace('.0','', regex=False).str.zfill(6)
        for c in ['52주고점', '고점대비(%)']:
            if c in df.columns: df = df.drop(columns=[c])
        df = df.merge(h[['종목코드', '52주고점', '고점대비(%)']], on='종목코드', how='left')
    except: pass
    return df

def _safe_save_screener_df(new_df, path="saved_screener_data.csv"):
    """스캔 도중 일부 페이지가 실패해 몇몇 종목만 누락되는 경우를 방지.
    페이지 1개 실패 = 전체의 2%밖에 안 돼서 '10% 이상 감소' 기준으로는 안 걸리므로,
    비율과 무관하게 '이번에 못 받아온 종목은 예전 저장분에서 채워 넣는' 방식(업서트)으로 항상 병합한다."""
    try:
        if os.path.exists(path) and len(new_df) > 100:  # 완전 실패(스캔 자체가 거의 빈 결과)일 때는 병합하지 않음
            old_df = pd.read_csv(path, dtype={'종목코드': str})
            if not old_df.empty:
                old_df['종목코드'] = old_df['종목코드'].astype(str).str.replace('.0', '', regex=False).str.zfill(6)
                new_df = new_df.copy()
                new_df['종목코드'] = new_df['종목코드'].astype(str).str.zfill(6)
                new_codes = set(new_df['종목코드'])
                missing = old_df[~old_df['종목코드'].isin(new_codes)]
                if not missing.empty:
                    new_df = pd.concat([new_df, missing], ignore_index=True)
    except Exception:
        pass
    new_df.to_csv(path, index=False, encoding='utf-8-sig')
    return new_df


def load_screener_df():
    save_path = "saved_screener_data.csv"
    try:
        _has_session_df = 'shared_screener_df' in st.session_state and not st.session_state['shared_screener_df'].empty
    except Exception:
        _has_session_df = False  # 백그라운드 스레드 등에서 session_state 접근이 안 되는 경우
    if _has_session_df:
        df = st.session_state['shared_screener_df']
        df = df.dropna(subset=['종목코드'])
        df = df[~df['종목코드'].astype(str).str.lower().str.contains('nan')]
        return df
    if _SCREENER_DF_CACHE["df"] is not None and not _SCREENER_DF_CACHE["df"].empty:
        df = _SCREENER_DF_CACHE["df"]
        df = df.dropna(subset=['종목코드'])
        df = df[~df['종목코드'].astype(str).str.lower().str.contains('nan')]
        return df
    if os.path.exists(save_path):
        try:
            df = pd.read_csv(save_path, dtype={'종목코드': str})
            df = df.dropna(subset=['종목코드'])
            df = df[~df['종목코드'].astype(str).str.lower().str.contains('nan')]
            df['종목코드'] = df['종목코드'].str.replace('.0','', regex=False).str.zfill(6)
            df = merge_high52(df)  
            _set_shared_screener_df(df)
            return df
        except: return pd.DataFrame()
    return pd.DataFrame()

RECO_PATH = "saved_reco_data.csv"

def load_reco_df():
    """추천 종목(2단계 산출물)을 반환. 세션에 있으면 그대로, 없으면 이전 스캔에서 저장해둔
    CSV에서 불러온다 (screener_df와 같은 방식). 앱 재시작·세션 초기화로 메모리가 비워져도
    '대시보드/종목스크리너에서 이미 스캔했는데 추천종목 탭에서 또 스캔해야 하는' 상황을 방지한다."""
    if 'reco_raw_data' in st.session_state and not st.session_state['reco_raw_data'].empty:
        return st.session_state['reco_raw_data']
    if os.path.exists(RECO_PATH):
        try:
            df = pd.read_csv(RECO_PATH, dtype={'종목코드': str})
            if not df.empty:
                df['종목코드'] = df['종목코드'].astype(str).str.zfill(6)
                st.session_state['reco_raw_data'] = df
                return df
        except Exception:
            pass
    return pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def load_high52_map():
    if not os.path.exists(HIGH52_PATH):
        return {}
    try:
        h = pd.read_csv(HIGH52_PATH, dtype={'종목코드': str})
        h['종목코드'] = h['종목코드'].str.replace('.0', '', regex=False).str.zfill(6)
        h['52주고점'] = pd.to_numeric(h['52주고점'], errors='coerce')
        h = h.dropna(subset=['52주고점'])
        return dict(zip(h['종목코드'], h['52주고점']))
    except:
        return {}

def check_naver_52w_robust(row_dict):
    code = str(row_dict['종목코드']).replace('.0','').zfill(6)
    mkt = row_dict.get('시장', '코스피')
    
    price = float(str(row_dict.get('현재가', 0)).replace(',', ''))
    high = 0.0

    if '52주고점' in row_dict and pd.notna(row_dict['52주고점']) and float(row_dict['52주고점']) > 0:
        high = float(row_dict['52주고점'])
    else:
        high52_map = load_high52_map()
        high = high52_map.get(code, 0.0)

    if price > 0 and high > 0:
        drop_pct = ((price - high) / high) * 100
        if drop_pct <= 0.0:
            return {
                "종목명": row_dict['종목명'], "종목코드": code, "시장": mkt,
                "현재가_num": price, "52주최고": high, "고점 / 하락률": drop_pct,
                "PER": row_dict['PER'], "PBR": row_dict['PBR'], "ROE": row_dict['ROE'],
                "부채비율": row_dict['부채비율'], "배당수익률": row_dict.get('배당수익률', 0.0),
                "데이터출처": "📂 스크리너 연동"
            }
        return None  
        
    try:
        time.sleep(random.uniform(0.1, 0.3))
        url = f"https://m.stock.naver.com/api/stock/{code}/basic"
        res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=3)
        if res.status_code == 200:
            data = res.json()
            price_str = data.get('closePrice', '0')
            high_str = data.get('high52WeekPrice') or data.get('high52Week') or '0'
            
            if price <= 0:
                price = float(str(price_str).replace(',', ''))
            if high <= 0:
                high = float(str(high_str).replace(',', ''))
    except: pass

    if price <= 0 or high <= 0:
        try:
            url_pc = f"https://finance.naver.com/item/main.naver?code={code}"
            res_pc = requests.get(url_pc, headers={'User-Agent': 'Mozilla/5.0'}, timeout=3)
            html = res_pc.text
            if price <= 0:
                p_match = re.search(r'<p class="no_today".*?<span class="blind">([\d,]+)</span>', html, re.DOTALL)
                if p_match: price = float(p_match.group(1).replace(',', ''))
            if high <= 0:
                h_match = re.search(r'52주최고.*?<em>([\d,]+)</em>', html, re.DOTALL)
                if h_match: high = float(h_match.group(1).replace(',', ''))
        except: pass

    if price <= 0: price = float(str(row_dict.get('현재가', 0)).replace(',', ''))
    if price <= 0: price = 50000.0
    if high <= 0: high = price

    if price > 0 and high > 0:
        drop_pct = ((price - high) / high) * 100
        if drop_pct <= 0.0:
            return {
                "종목명": row_dict['종목명'], "종목코드": code, "시장": mkt,
                "현재가_num": price, "52주최고": high, "고점 / 하락률": drop_pct,
                "PER": row_dict['PER'], "PBR": row_dict['PBR'], "ROE": row_dict['ROE'],
                "부채비율": row_dict['부채비율'], "배당수익률": row_dict.get('배당수익률', 0.0),
                "데이터출처": "🌐 실시간 보완"
            }
    return None

def run_unified_market_scan():
    """전체 시장 스크리너 스캔 + 52주 고점 매칭(추천 종목 후보 산출)을 한 번에 실행.
    대시보드 / 추천 종목 / 종목 스크리너, 어디서 버튼을 눌러도 이 함수 하나로
    'shared_screener_df'와 'reco_raw_data'가 함께 갱신되어 세 화면 모두 같은 데이터를 공유한다."""
    pb = st.progress(0, text="[1/2] 전체 시장 데이터 스캔 준비 중...")

    # 1단계: 전체 시장 스캔 (종목 스크리너 데이터)
    try:
        fetch_and_cache_screener_data.clear()
        temp_df = pd.DataFrame()
        for status_msg, pct in fetch_screener_data_generator():
            if isinstance(status_msg, str):
                pb.progress(pct, text=f"[1/2] 전체 시장 스캔 중: {status_msg}")
            else:
                temp_df = status_msg

        if temp_df.empty:
            pb.empty()
            st.error("통신 지연으로 시장 스캔에 실패했습니다. 다시 시도해주세요.")
            return False

        temp_df = _safe_save_screener_df(temp_df, "saved_screener_data.csv")
        st.session_state['shared_screener_df'] = temp_df
        screener_df = temp_df

        if st.session_state.get("_screener_missing_pages"):
            st.warning(f"⚠️ 이번 스캔에서 끝내 실패한 페이지 (시장구분, 페이지번호): {st.session_state['_screener_missing_pages']}")
            st.session_state["_screener_missing_pages"] = []
    except Exception as e:
        pb.empty()
        st.error(f"스캔 실패: {e}")
        return False

    # 2단계: 52주 고점 매칭 → 추천 종목 후보 산출
    load_high52_map.clear()
    high52_map = load_high52_map()
    scan_workers = 8 if high52_map else 5

    df = screener_df.copy()
    finance_keywords = '금융|은행|증권|보험|캐피탈|지주|투자|저축'
    cond = (
        (df['PER'] > 0) & (df['PER'] <= 40) &
        (df['PBR'] > 0) & (df['PBR'] <= 4.0) &
        (df['ROE'] >= 0) &
        (df['부채비율'] >= 0) & (df['부채비율'] <= 300) &
        (~df['종목명'].astype(str).str.contains(finance_keywords, regex=True, na=False))
    )
    val_df = df[cond].copy()

    if val_df.empty:
        pb.progress(100, text="✨ 시장 스캔 완료!")
        time.sleep(0.3)
        pb.empty()
        st.warning("현재 시장 데이터 기준, 최소 요건(D급)을 통과한 종목조차 없습니다. 추천 종목 후보 산출은 건너뜁니다.")
        return True

    val_df = val_df.sort_values('ROE', ascending=False).head(150)
    rows = []
    dict_records = val_df.to_dict('records')
    total = len(dict_records)
    progress_text = "⚡ CSV 고점 데이터 매칭 중..." if high52_map else "⚡ 네이버 실시간 API 스캔 중..."
    completed = 0

    _executor = get_shared_executor()
    _futures = {_executor.submit(check_naver_52w_robust, r): r for r in dict_records}
    try:
        for future in concurrent.futures.as_completed(_futures, timeout=25):
            completed += 1
            pb.progress(int((completed / total) * 100), text=f"[2/2] {progress_text} ({completed}/{total})")
            try:
                res = future.result(timeout=8)
            except Exception:
                res = None
            if res: rows.append(res)
    except concurrent.futures.TimeoutError:
        pass  # 전체 상한(25초) 초과 → 지금까지 모인 결과로 계속 진행
    finally:
        for f in _futures:
            f.cancel()

    pb.progress(100, text="✨ 스캔 완료! (스크리너 + 추천 종목 데이터가 함께 갱신되었습니다)")
    time.sleep(0.4)
    pb.empty()

    if rows:
        reco_df = pd.DataFrame(rows)
        st.session_state['reco_raw_data'] = reco_df
        try:
            reco_df.to_csv(RECO_PATH, index=False, encoding='utf-8-sig')
        except Exception:
            pass
    else:
        st.session_state.pop('reco_raw_data', None)
        if os.path.exists(RECO_PATH):
            try:
                os.remove(RECO_PATH)
            except Exception:
                pass
        st.warning("분석 결과 고점 대비 유의미하게 하락한 종목이 없습니다.")

    return True

def estimate_simple_target_price(current_price, per=None, pbr=None):
    """PER 15배 환산 → PBR 1.3배 환산 → 현재가 +25% 순으로 간이 목표가를 추정.
    '전략 계산'에서 이미 쓰는 방식과 동일한 우선순위를 따른다 (일관성 유지 목적)."""
    if not current_price or current_price <= 0:
        return 0, ""
    if per and per > 0:
        return round(current_price * (15.0 / per)), "PER 15× 추정"
    if pbr and pbr > 0:
        return round(current_price * (1.3 / pbr)), "PBR 1.3× 추정"
    return round(current_price * 1.25), "현재가 +25% 추정"

@st.cache_data(ttl=3600 * 6, show_spinner=False)
def estimate_target_hit_probability(stock_code, market_hint, target_price, horizons=(30, 90, 180), n_sims=3000):
    """최근 1년 일별 종가의 변동성을 이용한 몬테카를로 시뮬레이션으로,
    각 기간(일)이 '끝나는 시점(종가 기준)'에 목표가 이상(또는 이하)에 있을 확률을 추정한다.
    (기간 중 잠깐이라도 스치는 '터치 확률'이 아니라, 만기 시점 가격 기준의 더 보수적인 확률이다.)

    ⚠️ 방향성(드리프트)은 과거 수익률로 미래 상승/하락을 예단하지 않기 위해
    보수적으로 0(무추세, 랜덤워크)으로 가정한다. 즉 순수하게 '가격이 그동안
    얼마나 넓게 흔들려왔는가(변동성)'만으로 도달 가능성을 계산한 참고용 통계치이며,
    재무제표·실적·공시 내용을 반영한 예측이 아니다."""
    if target_price is None or target_price <= 0:
        return None
    try:
        import yfinance as yf
        suffix_order = [".KQ", ".KS"] if market_hint == "코스닥" else [".KS", ".KQ"]
        hist = pd.DataFrame()
        for suf in suffix_order:
            hist = call_with_timeout(
                lambda s=suf: yf.Ticker(f"{stock_code}{s}").history(period="1y", interval="1d", timeout=8),
                timeout=10,
            )
            if hist is None:
                hist = pd.DataFrame()
            if not hist.empty:
                break

        if hist.empty or "Close" not in hist.columns:
            return None
        closes = hist["Close"].dropna()
        if len(closes) < 40:
            return None

        current_price = float(closes.iloc[-1])
        if current_price <= 0:
            return None

        log_returns = np.log(closes / closes.shift(1)).dropna().values
        sigma_daily = float(np.std(log_returns))
        if sigma_daily <= 0:
            return None

        max_h = max(horizons)
        rng = np.random.default_rng(7)
        # 드리프트 0 가정(보수적 기본값) : E[가격] = 현재가로 유지되도록 -0.5*sigma^2 보정
        shocks = rng.normal(loc=-0.5 * sigma_daily ** 2, scale=sigma_daily, size=(n_sims, max_h))
        log_paths = np.cumsum(shocks, axis=1)
        price_paths = current_price * np.exp(log_paths)

        probs = {}
        for h in horizons:
            terminal_prices = price_paths[:, h - 1]  # 해당 기간이 끝나는 시점(종가 기준)의 가격
            if target_price >= current_price:
                hit = terminal_prices >= target_price
            else:
                hit = terminal_prices <= target_price
            probs[h] = round(float(np.mean(hit)) * 100, 1)

        return {"current_price": current_price, "sigma_daily_pct": round(sigma_daily * 100, 2), "probs": probs}
    except Exception:
        return None

def _format_hit_probability_badge(result, target_price, target_src="목표가"):
    """estimate_target_hit_probability의 결과를 카드용 인라인 HTML 배지로 포맷팅만 담당.
    (네트워크 호출 없음 — 이미 계산된 result를 넘겨받는다.)"""
    if not result or not target_price or target_price <= 0:
        return ""
    probs = result["probs"]
    p30, p90, p180 = probs.get(30, 0), probs.get(90, 0), probs.get(180, 0)
    return (
        f"<div style='margin-top:8px; padding:9px 12px; background:#F5F3FF; border:1px solid #DDD6FE; border-radius:8px;'>"
        f"<div style='font-size:11px; color:#6D28D9; font-weight:700; margin-bottom:3px;'>🎲 {target_price:,}원({target_src}) 도달 확률 · 종가 기준 통계 추정</div>"
        f"<div style='font-size:13px; color:#4C1D95;'>30일 <b>{p30:.0f}%</b> &nbsp;·&nbsp; 90일 <b>{p90:.0f}%</b> &nbsp;·&nbsp; 180일 <b>{p180:.0f}%</b></div>"
        f"</div>"
    )

def render_hit_probability_badge(stock_code, market_hint, target_price, target_src="목표가"):
    """estimate_target_hit_probability 결과를 카드용 인라인 HTML 배지로 렌더링.
    ⚠️ 이 함수는 그 자리에서 바로 네트워크 호출(call_with_timeout)을 하므로, 여러 종목을
    순차 for문에서 부르면 종목 수만큼 타임아웃이 차례로 쌓인다(예: 10종목 × 최대 10초
    = 최악 100초, 사실상 멈춘 것처럼 보임). 여러 종목을 한 번에 렌더링하는 화면
    (예: 관심종목 목록)에서는 이 함수를 직접 쓰지 말고, 미리 병렬로 한 번에 계산해서
    _format_hit_probability_badge로 포맷팅만 하는 방식을 쓸 것 (render_watchlist 참고).
    이 함수는 화면에 종목이 하나만 뜨는 페이지(재무 분석 상세 등)에서만 안전하다."""
    if not target_price or target_price <= 0:
        return ""
    result = estimate_target_hit_probability(stock_code, market_hint, target_price)
    return _format_hit_probability_badge(result, target_price, target_src)

def find_col(df: pd.DataFrame, candidates: list) -> str | None:
    for c in candidates:
        if c in df.columns: return c
    for c in df.columns:
        for cand in candidates:
            if cand.replace(" ", "").lower() in c.replace(" ", "").lower(): return c
    return None

def get_styled_dataframe(df):
    if "종목코드" in df.columns:
        df = df.rename(columns={"종목코드": "📋종목코드"})
        
    numeric_cols = []
    text_cols = []
    format_dict = {}
    
    for col in df.columns:
        if "기준월" in col:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            format_dict[col] = "{:.2f}"
            numeric_cols.append(col)
            continue

        if any(kw in col for kw in ["현재가", "고점", "최고", "금액", "배당금", "PER", "PBR", "ROE", "수익률", "비율", "하락률", "등락률", "성향", "년전 배당", "표준편차", "Amihud"]):
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(r'[^\d.-]', '', regex=True), errors='coerce')
        
        if col in ["PER", "PBR"]:
            format_dict[col] = "{:,.2f}배"
            numeric_cols.append(col)
        elif any(kw in col for kw in ["ROE", "수익률", "비율", "하락률", "등락률", "성향", "년전 배당", "고점대비"]):
            format_dict[col] = "{:,.1f}%"
            numeric_cols.append(col)
        elif "배당금" in col or any(kw in col for kw in ["현재가", "고점", "최고", "금액"]):
            format_dict[col] = "{:,.0f}" 
            numeric_cols.append(col)
        elif pd.api.types.is_numeric_dtype(df[col]):
            format_dict[col] = "{:,.2f}" if df[col].dtype == float else "{:,.0f}"
            numeric_cols.append(col)
        else:
            text_cols.append(col)

    styled = df.style.format(format_dict, na_rep="-") \
        .set_properties(subset=numeric_cols, **{'text-align': 'right'}) \
        .set_properties(subset=text_cols, **{'text-align': 'left'}) \
        .set_table_styles([
            {'selector': 'th', 'props': [('text-align', 'center !important'), ('background-color', '#F8FAFC'), ('color', '#1E293B'), ('border-bottom', '1px solid #E2E8F0')]},
            {'selector': 'td', 'props': [('vertical-align', 'middle')]}
        ])
    return styled

def _quarter_freshness_badge(df_quarter):
    """분기 실적 데이터의 최신성을 판단해 안내 배지 HTML을 반환.
    한국 상장사 분기보고서 법정 제출기한(분기말 기준 45일, 4분기=사업보고서 90일)을
    기준으로 다음 분기 실적이 '정상적으로 아직 미발표'인지, 기한이 지났는데도
    반영이 안 된 '수집 지연/실패 의심' 상태인지 구분해서 알려준다.
    """
    if df_quarter.empty or '연도/분기' not in df_quarter.columns:
        return ""

    latest_str = str(df_quarter['연도/분기'].iloc[-1])
    m = re.match(r'^(\d{4})/(\d{2})$', latest_str)
    if not m:
        return ""
    year, month = int(m.group(1)), int(m.group(2))
    if month not in (3, 6, 9, 12):
        return ""

    def _quarter_end(y, mo):
        last_day = {3: 31, 6: 30, 9: 30, 12: 31}[mo]
        return datetime.date(y, mo, last_day)

    def _next_quarter(y, mo):
        return (y + 1, 3) if mo == 12 else (y, mo + 3)

    def _deadline(y, mo):
        days = 90 if mo == 12 else 45
        return _quarter_end(y, mo) + datetime.timedelta(days=days)

    next_y, next_m = _next_quarter(year, month)
    next_deadline = _deadline(next_y, next_m)
    today = datetime.date.today()
    grace_days = 14  # 기업별 공시 반영 시차 감안한 유예기간

    if today <= next_deadline:
        d_str = f"{next_deadline.month}월 {next_deadline.day}일"
        return (
            f'<div style="background:#EFF6FF; border:1px solid #BFDBFE; border-radius:6px; '
            f'padding:8px 12px; margin-bottom:10px; font-size:12.5px; color:#1E40AF;">'
            f'ℹ️ 최신 분기는 <b>{year}/{month:02d}</b>입니다. '
            f'<b>{next_y}/{next_m:02d}</b> 실적은 아직 미발표 상태이며, 법정 제출기한(통상 {d_str}경)까지 순차 반영될 예정입니다.'
            f'</div>'
        )
    elif today <= next_deadline + datetime.timedelta(days=grace_days):
        return (
            f'<div style="background:#FFFBEB; border:1px solid #FDE68A; border-radius:6px; '
            f'padding:8px 12px; margin-bottom:10px; font-size:12.5px; color:#92400E;">'
            f'⏳ <b>{next_y}/{next_m:02d}</b> 실적 법정 제출기한이 지났습니다. 기업별 공시 반영 시점 차이로 며칠 내 순차 업데이트될 수 있습니다.'
            f'</div>'
        )
    else:
        return (
            f'<div style="background:#FEF2F2; border:1px solid #FECACA; border-radius:6px; '
            f'padding:8px 12px; margin-bottom:10px; font-size:12.5px; color:#991B1B;">'
            f'⚠️ 최신 분기가 <b>{year}/{month:02d}</b>에 머물러 있습니다. '
            f'<b>{next_y}/{next_m:02d}</b> 법정 제출기한을 넘긴 지 오래됐어요. FnGuide 데이터 수집(스크래핑) 실패 여부를 확인해보세요.'
            f'</div>'
        )


def draw_fnguide_details(code):
    info = fetch_company_info_fnguide(code)
    df_annual, df_quarter, df_dividend = fetch_fnguide_data(code)

    if info['name'] != "알 수 없음" or not df_annual.empty:
        opinion_score_html = f'<span style="color: #94A3B8; font-size: 12px; margin-left: 8px;">({info["opinion_score"]})</span>' if info['opinion_score'] else ''
        analyst_count_html = f'<span style="color: #94A3B8; font-size: 12px; margin-left: 8px;">({info["analyst_count"]})</span>' if info['analyst_count'] else ''
        consensus_note_html = f'<div style="background-color:#FFFBEB; border:1px solid #FDE68A; border-radius:6px; padding:10px 14px; margin-bottom:20px; font-size:12.5px; color:#92400E; line-height:1.6;">ℹ️ {info["consensus_note"]}</div>' if info.get("consensus_note") else ''

        price_info = fetch_current_price_info(code)
        price_html = ""
        if price_info["price"] is not None:
            _p_color = {"up": "#DC2626", "down": "#2563EB", "neutral": "#64748B"}[price_info["status"]]
            _p_arrow = {"up": "▲", "down": "▼", "neutral": "-"}[price_info["status"]]
            _p_sign = "+" if price_info["diff"] >= 0 else ""
            price_html = f'''<span style="font-size: 15px; color: {_p_color}; font-weight: 700; margin-left: 14px;">
                {price_info["price"]:,.0f}원
                <span style="font-size: 12px; font-weight: 600;">{_p_arrow} {_p_sign}{price_info["diff"]:,.0f} ({_p_sign}{price_info["diff_pct"]:.2f}%)</span>
            </span>'''

        st.markdown(f"""
            <div style="background-color: #F8FAFC; border: 1px solid #E2E8F0; border-radius: 8px; padding: 25px; margin-top: 10px; margin-bottom: 20px;">
                <h3 style="margin-top: 0; color: #0F172A; font-size: 20px;">{info['name']} <span style="font-size: 14px; color: #64748B;">({code})</span>{price_html}</h3>
                <div style="display: flex; gap: 20px; margin-bottom: 20px;">
                    <div style="background-color: #FFFFFF; padding: 12px 20px; border-radius: 6px; border: 1px solid #E2E8F0; font-weight: 600;">
                        <span style="color: #64748B; font-size: 12px; display: block; margin-bottom: 4px;">투자의견 (FnGuide)</span>
                        <span style="color: #5A4EE5; font-size: 16px;">{info['opinion']}</span>{opinion_score_html}
                    </div>
                    <div style="background-color: #FFFFFF; padding: 12px 20px; border-radius: 6px; border: 1px solid #E2E8F0; font-weight: 600;">
                        <span style="color: #64748B; font-size: 12px; display: block; margin-bottom: 4px;">목표주가 컨센서스</span>
                        <span style="color: #0F172A; font-size: 16px;">{info['target']}</span>{analyst_count_html}
                    </div>
                </div>{consensus_note_html}
                <p style="color: #334155; font-size: 13px; line-height: 1.7; margin-bottom: 0;">
                    <b>📖 기업개요:</b> {info['summary']}
                </p>
            </div>
        """, unsafe_allow_html=True)

        # ── 최근 수급 동향 (외국인 / 기관 / 개인 추정 순매매) ────────────────────
        st.markdown("<h4 style='font-size:16px; margin:20px 0 4px 0;'>📊 최근 수급 동향</h4>", unsafe_allow_html=True)
        st.markdown(
            "<p style='font-size:12px; color:#64748B; margin-bottom:10px;'>"
            "외국인·기관이 동반 순매수로 돌아서는 구간은 통상 긍정적인 수급 신호로 해석됩니다. "
            "단, <b>개인 순매매는 네이버가 별도 제공하지 않아 (외국인+기관)의 반대부호로 추정한 값</b>입니다."
            "</p>",
            unsafe_allow_html=True,
        )

        period_choice = st.radio(
            "조회 기간",
            ["2주 (14일)", "4주 (1개월)", "24주 (6개월)"],
            index=1,
            horizontal=True,
            key=f"trend_period_{code}",
            label_visibility="collapsed",
        )
        _period_days = {"2주 (14일)": 10, "4주 (1개월)": 20, "24주 (6개월)": 120}[period_choice]
        _period_label = f"{period_choice} · {_period_days}거래일"

        st.caption(f"📅 조회 기간: {_period_label}")

        df_trend = fetch_investor_trend_by_code(code, days=_period_days)

        if not df_trend.empty:
            sum_inst  = int(df_trend['기관순매매'].sum())
            sum_frgn  = int(df_trend['외국인순매매'].sum())
            sum_indiv = int(df_trend['개인순매매(추정)'].sum())

            def _trend_card(title, value, note=""):
                color = "#10B981" if value > 0 else ("#EF4444" if value < 0 else "#64748B")
                bg    = "#F0FDF4" if value > 0 else ("#FEF2F2" if value < 0 else "#F8FAFC")
                sign  = "+" if value > 0 else ""
                note_html = f"<div style='font-size:10px; color:#94A3B8; margin-top:2px;'>{note}</div>" if note else ""
                return (
                    f'<div style="flex:1; background:{bg}; border-radius:8px; padding:12px 16px;">'
                    f'<div style="font-size:11px; color:#64748B; margin-bottom:4px;">{title}</div>'
                    f'<div style="font-size:16px; font-weight:700; color:{color};">{sign}{value:,}주</div>'
                    f'{note_html}'
                    '</div>'
                )

            st.markdown(
                '<div style="display:flex; gap:10px; margin-bottom:12px;">'
                + _trend_card("🌍 외국인 누적 순매매", sum_frgn)
                + _trend_card("🏦 기관 누적 순매매", sum_inst)
                + _trend_card("👤 개인 누적 순매매", sum_indiv, "추정치 (외국인+기관의 반대부호)")
                + '</div>',
                unsafe_allow_html=True,
            )

            if sum_frgn > 0 and sum_inst > 0:
                st.markdown(
                    "<div style='background:#EEF2FF; border:1px solid #C7D2FE; border-radius:6px; "
                    "padding:8px 12px; font-size:12.5px; color:#3730A3; margin-bottom:12px;'>"
                    "🟢 최근 기간 외국인·기관이 동반 순매수 중입니다. 다른 지표와 함께 참고용으로 확인하세요."
                    "</div>", unsafe_allow_html=True,
                )
            elif sum_frgn < 0 and sum_inst < 0:
                st.markdown(
                    "<div style='background:#FEF2F2; border:1px solid #FECACA; border-radius:6px; "
                    "padding:8px 12px; font-size:12.5px; color:#991B1B; margin-bottom:12px;'>"
                    "🔴 최근 기간 외국인·기관이 동반 순매도 중입니다."
                    "</div>", unsafe_allow_html=True,
                )

            def _fmt_shares(v):
                try:
                    v = int(v)
                except Exception:
                    return "-"
                if v > 0:  return f"🔺 +{v:,}"
                if v < 0:  return f"🔻 {v:,}"
                return "0"

            def _style_shares(val):
                s = str(val)
                if '🔺' in s: return 'color: #10B981; font-weight: 600;'
                if '🔻' in s: return 'color: #EF4444; font-weight: 600;'
                return 'color: #111827;'

            display_trend = df_trend.copy()
            _num_cols = ['외국인순매매', '기관순매매', '개인순매매(추정)']
            for col in _num_cols:
                display_trend[col] = display_trend[col].apply(_fmt_shares)

            with st.expander(f"일별 수급 상세 보기 ({_period_days}거래일)"):
                try:
                    styled_trend = display_trend.style.map(_style_shares, subset=_num_cols)
                except AttributeError:
                    styled_trend = display_trend.style.applymap(_style_shares, subset=_num_cols)
                st.dataframe(styled_trend, use_container_width=True, hide_index=True)
        else:
            st.caption("ℹ️ 최근 수급 데이터를 불러오지 못했습니다. (거래정지 종목이거나 일시적 통신 오류일 수 있습니다)")
            _dbg = _DEBUG_STORE.get(f"_trend_debug_{code}")
            if _dbg:
                with st.expander("🔧 디버그 정보 (실패 원인 확인용)"):
                    st.json(_dbg)

        if not df_annual.empty:
            _value_cols = [c for c in df_annual.columns if c != '연도/분기']
            if df_annual[_value_cols].isna().all().all():
                st.warning("⚠️ 기간(연도/분기)은 인식됐지만 실적 수치를 하나도 못 읽어왔어요. FnGuide 표의 항목명(행 이름)이 바뀐 것으로 보입니다.")
                _fdbg3 = _DEBUG_STORE.get(f"_fnguide_debug_{code}")
                if _fdbg3:
                    with st.expander("🔧 디버그 정보 (항목명 매칭 실패 원인 확인용)"):
                        st.json(_fdbg3)

            def custom_formatter(val, col_name):
                try:
                    clean_val = str(val).replace(',', '').strip()
                    f_val = float(clean_val)
                    if pd.isna(f_val) or clean_val == '-' or clean_val == 'nan': return "-"
                    if '성장률' in col_name:
                        if f_val > 0: return f"🔺 +{f_val:.1f}%"
                        elif f_val < 0: return f"🔻 {f_val:.1f}%"
                        else: return "0.0%"
                    if col_name in ['매출액', '영업이익', '당기순이익']:
                        v_int = int(round(f_val))
                        is_minus = v_int < 0
                        abs_v = abs(v_int)
                        cho = abs_v // 10000
                        uk  = abs_v % 10000
                        formatted_num = f"{v_int:,}"
                        if cho > 0: return f"{formatted_num} ({'-' if is_minus else ''}{cho}조 {uk:,}억)" if uk > 0 else f"{formatted_num} ({'-' if is_minus else ''}{cho}조)"
                        return f"{formatted_num} ({'-' if is_minus else ''}{uk:,}억)"
                    elif col_name in ['영업이익률', '순이익률', 'ROE', '부채비율']: return f"{f_val:.1f}%"
                    elif col_name in ['PER', 'PBR']: return f"{f_val:.2f}배"
                    return f"{f_val:,}"
                except: return str(val)

            def format_and_style(input_df):
                display_df = input_df.copy()
                for col in display_df.columns[1:]:
                    display_df[col] = display_df[col].apply(lambda x: custom_formatter(x, col))
                def style_cells(val):
                    if '🔺' in str(val): return 'color: #10B981; font-weight: 600;'
                    if '🔻' in str(val) or ('-' in str(val) and ('조' in str(val) or '억' in str(val))):
                        return 'color: #EF4444; font-weight: 600;'
                    return 'color: #111827;'
                try: return display_df.style.map(style_cells, subset=display_df.columns[1:])
                except AttributeError: return display_df.style.applymap(style_cells, subset=display_df.columns[1:])

            st.markdown("<br>", unsafe_allow_html=True)
            tab1, tab2, tab3, tab4 = st.tabs(["📅 연간 실적 (YoY 흐름)", "📈 분기 실적 (QoQ 흐름)", "💰 배당 히스토리", "📢 공시"])
            current_year = str(datetime.datetime.now().year)

            def mask_incomplete_year_growth(df):
                df = df.copy()
                year_col = df.columns[0]
                growth_cols = [c for c in df.columns if '성장률' in c]
                for i, row_year in enumerate(df[year_col].astype(str)):
                    if current_year in row_year:
                        for gc in growth_cols:
                            df.at[i, gc] = float('nan')
                return df

            with tab1:
                df_annual_masked  = mask_incomplete_year_growth(df_annual)
                df_annual_display = df_annual_masked.iloc[::-1].reset_index(drop=True)
                year_col = df_annual_display.columns[0]
                df_annual_display[year_col] = df_annual_display[year_col].astype(str).apply(
                    lambda y: f"{y} ⚠️잠정" if current_year in y else y
                )
                st.dataframe(format_and_style(df_annual_display), use_container_width=True, hide_index=True)
            with tab2:
                if not df_quarter.empty:
                    _freshness_badge = _quarter_freshness_badge(df_quarter)
                    if _freshness_badge:
                        st.markdown(_freshness_badge, unsafe_allow_html=True)
                    df_quarter_display = df_quarter.iloc[::-1].reset_index(drop=True)
                    st.dataframe(format_and_style(df_quarter_display), use_container_width=True, hide_index=True)
                else:
                    st.info("해당 기업의 분기 실적 데이터가 제공되지 않습니다.")
            with tab3:
                if not df_dividend.empty:
                    df_div_display = df_dividend.iloc[::-1].reset_index(drop=True).head(3)
                    df_div_display = df_div_display[['연도', '주당배당금', '배당총액', '배당수익률', '배당성향']]

                    def _fmt_dividend(val, col_name):
                        try:
                            f_val = float(val)
                            if pd.isna(f_val): return "-"
                            if col_name == '주당배당금': return f"{f_val:,.0f}원"
                            if col_name == '배당총액': return f"{f_val:,.0f}억원"
                            if col_name in ('배당수익률', '배당성향'): return f"{f_val:.1f}%"
                            return f"{f_val:,}"
                        except Exception:
                            return "-"

                    styled_div = df_div_display.copy()
                    for col in ['주당배당금', '배당총액', '배당수익률', '배당성향']:
                        styled_div[col] = styled_div[col].apply(lambda x: _fmt_dividend(x, col))

                    def _style_div_cells(val):
                        if val == "-":
                            return 'color: #94A3B8;'
                        return 'color: #111827; font-weight: 600;'

                    _div_cols = ['주당배당금', '배당총액', '배당수익률', '배당성향']
                    try:
                        styled_div = styled_div.style.map(_style_div_cells, subset=_div_cols)
                    except AttributeError:
                        styled_div = styled_div.style.applymap(_style_div_cells, subset=_div_cols)

                    st.dataframe(styled_div, use_container_width=True, hide_index=True)
                    st.caption(
                        "ℹ️ 배당총액·배당수익률·배당성향은 FnGuide에 별도 항목이 없어 "
                        "당기순이익·EPS·PER·주당배당금을 조합해 추정 계산한 값입니다 "
                        "(배당성향 = 주당배당금÷EPS, 배당총액 = 당기순이익×배당성향, "
                        "배당수익률 = 주당배당금÷추정주가(EPS×PER)). 실제 공시 수치와 소폭 차이가 날 수 있습니다."
                    )
                else:
                    st.info("해당 종목의 배당 데이터가 제공되지 않습니다. (무배당 종목이거나 데이터 수집에 실패했을 수 있습니다)")
                    _fdbg_div = _DEBUG_STORE.get(f"_fnguide_debug_{code}")
                    if _fdbg_div and _fdbg_div.get("valuation_a_index"):
                        with st.expander("🔧 디버그 정보 (배당 데이터 실패 원인 확인용)"):
                            st.json({
                                "valuation_a_index": _fdbg_div.get("valuation_a_index"),
                                "dividend_rows_found": _fdbg_div.get("dividend_rows_found"),
                                "dividend_exception": _fdbg_div.get("dividend_exception"),
                            })
            with tab4:
                render_disclosure_tab(code)
        else:
            st.caption("ℹ️ 연간/분기 실적 데이터를 불러오지 못했습니다.")
            _fdbg = _DEBUG_STORE.get(f"_fnguide_debug_{code}")
            if _fdbg:
                with st.expander("🔧 디버그 정보 (실적 데이터 실패 원인 확인용)"):
                    st.json(_fdbg)

            st.markdown("""
                <div style='background-color: #F9FAFB; padding: 15px; border-radius: 8px; margin-top: 15px; font-size: 13px; color: #6B7280;'>
                    💡 <b>알림:</b> 성장률은 직전 연도/분기 대비 증감률입니다. (🔺초록색: 실적 상승 / 🔻빨간색: 실적 하락 및 적자)<br>
                    ⚠️ <b>잠정 표기된 연도</b>는 결산이 완료되지 않아 일부 기간 데이터만 반영된 수치입니다. 성장률은 신뢰도가 낮아 표시하지 않습니다.
                </div>
            """, unsafe_allow_html=True)
    else:
        st.error("해당 종목의 기업 및 재무 정보를 찾을 수 없습니다.")
        _ndbg = _DEBUG_STORE.get(f"_fnname_debug_{code}")
        _fdbg2 = _DEBUG_STORE.get(f"_fnguide_debug_{code}")
        with st.expander("🔧 디버그 정보 (종목명/실적 조회 실패 원인 확인용)"):
            st.write("종목명 조회(네이버금융 main.naver):")
            st.json(_ndbg or {"info": "호출 안 됨"})
            st.write("실적 조회(FnGuide company_02.asp):")
            st.json(_fdbg2 or {"info": "호출 안 됨"})

# =========================
# 🎨 메인 UI 및 사이드바 설정
# =========================

# =========================
# 🔐 로그인 / 회원가입 (Google Sheets 저장)
# =========================
_USER_COLUMNS = ["아이디", "비밀번호해시", "salt", "이메일", "가입일"]

@st.cache_resource(show_spinner=False)
def _get_gsheet_client():
    """구글 서비스 계정 인증 및 gspread 클라이언트 생성 (앱 실행 중 1회만 수행)."""
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)

def _get_worksheet(sheet_tab_name):
    """지정한 탭(worksheet) 객체를 반환."""
    client = _get_gsheet_client()
    spreadsheet = client.open(st.secrets["gsheet"]["sheet_name"])
    ws = spreadsheet.worksheet(sheet_tab_name)
    return ws

def _hash_password(password, salt=None):
    """비밀번호를 salt와 함께 PBKDF2-SHA256으로 해싱. 평문은 절대 저장하지 않음."""
    if salt is None:
        salt = os.urandom(16).hex()
    pwd_hash = hashlib.pbkdf2_hmac(
        'sha256', password.encode('utf-8'), bytes.fromhex(salt), 100_000
    ).hex()
    return pwd_hash, salt

@st.cache_data(ttl=30, show_spinner=False)
def load_users():
    """users 시트의 모든 사용자 정보를 DataFrame으로 반환 (30초 캐시).

    [탭 멈춤 대응] gspread(requests 기반)는 timeout=을 명시하지 않으면
    응답이 없어도 절대 스스로 끊기지 않는다. socket.setdefaulttimeout()도
    이 경로엔 적용되지 않아서, 메인 스레드에서 직접 부르면 구글 API가
    느려질 때 앱 전체가 무한정 멈출 수 있었다. call_with_timeout으로
    별도 스레드에서 실행하고 메인 스레드 쪽에서 상한을 강제한다.
    """
    def _fetch():
        ws = _get_worksheet("users")
        return ws.get_all_records(numericise_ignore=['all'])

    records = call_with_timeout(_fetch, timeout=10)
    if records is None:
        return pd.DataFrame(columns=_USER_COLUMNS)
    try:
        df = pd.DataFrame(records, dtype=str).fillna("")
        for col in _USER_COLUMNS:
            if col not in df.columns:
                df[col] = ""
        return df[_USER_COLUMNS]
    except Exception:
        return pd.DataFrame(columns=_USER_COLUMNS)

def username_exists(username):
    users = load_users()
    if users.empty:
        return False
    return (users["아이디"].astype(str) == username).any()

def email_exists(email):
    users = load_users()
    if users.empty:
        return False
    return (users["이메일"].astype(str).str.lower() == email.lower()).any()

def save_user(username, password, email):
    """새 사용자를 users 시트에 한 행 추가."""
    pwd_hash, salt = _hash_password(password)
    new_row = [
        username, pwd_hash, salt, email,
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
    ]
    def _write():
        ws = _get_worksheet("users")
        ws.append_row(new_row, value_input_option="RAW")
        return True

    ok = call_with_timeout(_write, timeout=10)
    if ok:
        load_users.clear()  # 캐시 무효화
        return True
    st.error("일시적인 통신 오류로 가입 처리에 실패했습니다. 잠시 후 다시 시도해주세요.")
    return False

def authenticate_user(username, password):
    users = load_users()
    if users.empty:
        return False
    matched = users[users["아이디"].astype(str) == username]
    if matched.empty:
        return False
    row = matched.iloc[0]
    check_hash, _ = _hash_password(password, salt=row["salt"])
    return check_hash == row["비밀번호해시"]

# ── F5 새로고침에도 로그인이 풀리지 않도록: 서명된 세션 토큰을 URL에 저장 ──
# (Streamlit은 브라우저를 새로고침하면 session_state가 초기화되므로,
#  URL의 쿼리파라미터에 위변조 불가능한 토큰을 넣어두고 새로고침 시 이를 검증해 로그인 상태를 복원한다.)
def _get_session_secret():
    try:
        return str(st.secrets["SESSION_SECRET"])
    except Exception:
        return os.environ.get("SESSION_SECRET", "insecure-default-please-set-SESSION_SECRET")

def make_session_token(username, ttl_days=7):
    """로그인 성공 시 호출: username과 만료시각을 HMAC으로 서명한 토큰 문자열을 생성."""
    expire_at = int(time.time()) + ttl_days * 86400
    payload = f"{username}:{expire_at}"
    sig = hmac.new(_get_session_secret().encode(), payload.encode(), hashlib.sha256).hexdigest()
    raw = f"{payload}:{sig}"
    return base64.urlsafe_b64encode(raw.encode()).decode()

def verify_session_token(token):
    """URL에 있는 토큰을 검증해 유효하면 username을, 아니면 None을 반환."""
    try:
        raw = base64.urlsafe_b64decode(token.encode()).decode()
        username, expire_at, sig = raw.rsplit(":", 2)
        if int(expire_at) < time.time():
            return None
        expected_sig = hmac.new(_get_session_secret().encode(), f"{username}:{expire_at}".encode(), hashlib.sha256).hexdigest()
        if hmac.compare_digest(sig, expected_sig):
            return username
    except Exception:
        pass
    return None

def verify_user_email(username, email):
    """비밀번호 재설정 전 본인확인용: 아이디와 이메일이 실제로 일치하는지 확인."""
    users = load_users()
    if users.empty:
        return False
    matched = users[users["아이디"].astype(str) == username]
    if matched.empty:
        return False
    return str(matched.iloc[0]["이메일"]).strip().lower() == email.strip().lower()

def update_user_password(username, new_password):
    """해당 사용자의 비밀번호 해시/salt를 새 값으로 갱신."""
    def _do():
        ws = _get_worksheet("users")
        records = ws.get_all_records(numericise_ignore=['all'])
        row_idx = None
        for i, rec in enumerate(records):
            if str(rec.get("아이디", "")) == username:
                row_idx = i + 2  # 헤더가 1행이므로 데이터는 2행부터 시작
                break
        if row_idx is None:
            return "not_found"
        pwd_hash, salt = _hash_password(new_password)
        ws.update_cell(row_idx, _USER_COLUMNS.index("비밀번호해시") + 1, pwd_hash)
        ws.update_cell(row_idx, _USER_COLUMNS.index("salt") + 1, salt)
        return True

    result = call_with_timeout(_do, timeout=12)
    if result is True:
        load_users.clear()
        return True
    if result == "not_found":
        return False
    st.error("일시적인 통신 오류로 비밀번호 변경에 실패했습니다. 잠시 후 다시 시도해주세요.")
    return False


# =========================
# ⭐ 관심종목 (마이페이지, Google Sheets 저장)
# =========================
_WATCHLIST_COLUMNS = ["아이디", "종목코드", "종목명", "추가일", "매수가", "수량", "1차진입가", "2차진입가", "3차진입가", "고정"]

@st.cache_data(ttl=30, show_spinner=False)
def _load_all_watchlist():
    """watchlist 시트 전체를 DataFrame으로 반환 (30초 캐시, 내부용)."""
    def _fetch():
        ws = _get_worksheet("watchlist")
        return ws.get_all_records(numericise_ignore=['all'])

    records = call_with_timeout(_fetch, timeout=10)
    if records is None:
        return pd.DataFrame(columns=_WATCHLIST_COLUMNS)
    try:
        df = pd.DataFrame(records, dtype=str).fillna("")
        for col in _WATCHLIST_COLUMNS:
            if col not in df.columns:
                df[col] = ""
        return df[_WATCHLIST_COLUMNS]
    except Exception:
        return pd.DataFrame(columns=_WATCHLIST_COLUMNS)

def load_watchlist(username):
    """현재 로그인한 사용자의 관심종목만 반환."""
    df = _load_all_watchlist()
    if df.empty:
        return df
    return df[df["아이디"] == username].reset_index(drop=True)

def is_in_watchlist(username, code):
    wl = load_watchlist(username)
    if wl.empty:
        return False
    return (wl["종목코드"] == normalize_kr_code(code)).any()

def add_to_watchlist(username, code, name):
    code = normalize_kr_code(code)
    if is_in_watchlist(username, code):
        return False  # 이미 있음
    new_row = [
        username, code, name,
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
        "", "", "", "", "", "",
    ]
    def _write():
        ws = _get_worksheet("watchlist")
        ws.append_row(new_row, value_input_option="RAW")
        return True

    ok = call_with_timeout(_write, timeout=10)
    if ok:
        _load_all_watchlist.clear()
        return True
    st.error("일시적인 통신 오류로 관심종목 추가에 실패했습니다. 잠시 후 다시 시도해주세요.")
    return False

def _find_watchlist_row_indices(ws, username, code):
    """해당 사용자+종목코드에 해당하는 시트 상의 실제 행 번호(1-base, 헤더 포함)를 반환."""
    records = ws.get_all_records(numericise_ignore=['all'])
    indices = []
    for i, rec in enumerate(records):
        if str(rec.get("아이디", "")) == username and str(rec.get("종목코드", "")) == code:
            indices.append(i + 2)  # 헤더가 1행이므로 데이터는 2행부터 시작
    return indices

def remove_from_watchlist(username, code):
    code = normalize_kr_code(code)

    def _do():
        ws = _get_worksheet("watchlist")
        row_indices = _find_watchlist_row_indices(ws, username, code)
        if not row_indices:
            return "not_found"
        for row_idx in sorted(row_indices, reverse=True):
            ws.delete_rows(row_idx)
        return True

    result = call_with_timeout(_do, timeout=12)
    if result is True:
        _load_all_watchlist.clear()
    elif result == "not_found":
        st.warning(f"삭제할 항목을 시트에서 찾지 못했습니다. (종목코드 {code})")
    else:
        st.error("일시적인 통신 오류로 관심종목 삭제에 실패했습니다. 잠시 후 다시 시도해주세요.")

def update_watchlist_holding(username, code, buy_price, qty):
    """관심종목 항목에 매수가/수량(보유 정보)을 저장."""
    code = normalize_kr_code(code)

    def _do():
        ws = _get_worksheet("watchlist")
        row_indices = _find_watchlist_row_indices(ws, username, code)
        for row_idx in row_indices:
            ws.update_cell(row_idx, _WATCHLIST_COLUMNS.index("매수가") + 1, str(buy_price) if buy_price else "")
            ws.update_cell(row_idx, _WATCHLIST_COLUMNS.index("수량") + 1, str(qty) if qty else "")
        return True

    ok = call_with_timeout(_do, timeout=12)
    if ok:
        _load_all_watchlist.clear()
    else:
        st.error("일시적인 통신 오류로 보유 정보 저장에 실패했습니다. 잠시 후 다시 시도해주세요.")

def update_watchlist_entries(username, code, entry1, entry2, entry3):
    """관심종목 항목에 1차/2차/3차 매수 진입가를 저장."""
    code = normalize_kr_code(code)

    def _do():
        ws = _get_worksheet("watchlist")
        row_indices = _find_watchlist_row_indices(ws, username, code)
        for row_idx in row_indices:
            ws.update_cell(row_idx, _WATCHLIST_COLUMNS.index("1차진입가") + 1, str(entry1) if entry1 else "")
            ws.update_cell(row_idx, _WATCHLIST_COLUMNS.index("2차진입가") + 1, str(entry2) if entry2 else "")
            ws.update_cell(row_idx, _WATCHLIST_COLUMNS.index("3차진입가") + 1, str(entry3) if entry3 else "")
        return True

    ok = call_with_timeout(_do, timeout=12)
    if not ok:
        st.error("일시적인 통신 오류로 진입가 저장에 실패했습니다. 잠시 후 다시 시도해주세요.")
    else:
        _load_all_watchlist.clear()

def toggle_watchlist_pin(username, code):
    """관심종목 항목의 상단 고정 상태를 토글."""
    code = normalize_kr_code(code)
    is_pinned = False
    wl = load_watchlist(username)
    if not wl.empty:
        match = wl[wl["종목코드"] == code]
        if not match.empty:
            is_pinned = (match.iloc[0]["고정"] == "Y")
    def _do():
        ws = _get_worksheet("watchlist")
        row_indices = _find_watchlist_row_indices(ws, username, code)
        if not row_indices:
            return "not_found"
        for row_idx in row_indices:
            ws.update_cell(row_idx, _WATCHLIST_COLUMNS.index("고정") + 1, "" if is_pinned else "Y")
        return True

    result = call_with_timeout(_do, timeout=12)
    if result is True:
        _load_all_watchlist.clear()
    elif result == "not_found":
        st.warning(f"항목을 시트에서 찾지 못해 고정 상태를 변경하지 못했습니다. (종목코드 {code})")
    else:
        st.error("일시적인 통신 오류로 고정 상태 변경에 실패했습니다. 잠시 후 다시 시도해주세요.")


@st.cache_data(ttl=1800, show_spinner=False)
def fetch_watchlist_sparkline_prices(stock_code, market=None, days=30):
    """관심종목 미니차트용: 최근 N영업일 종가 리스트를 반환 (30분 캐시).
    market('코스피'/'코스닥')을 알고 있으면 해당 심볼로 바로 조회하고,
    데이터가 너무 부실하면(포인트 10개 미만) 반대쪽 시장 심볼도 시도해 더 나은 쪽을 채택한다."""
    try:
        import yfinance as yf

        def _fetch(suffix):
            try:
                df = yf.Ticker(f"{stock_code}{suffix}").history(period="60d", interval="1d", timeout=8)
                return df["Close"].dropna().tolist() if not df.empty else []
            except Exception:
                return []

        primary_suffix = ".KQ" if market == "코스닥" else ".KS"
        other_suffix = ".KS" if primary_suffix == ".KQ" else ".KQ"

        closes = _fetch(primary_suffix)
        if len(closes) < 10:
            alt_closes = _fetch(other_suffix)
            if len(alt_closes) > len(closes):
                closes = alt_closes

        return closes[-days:] if closes else []
    except Exception:
        return []


def render_mini_sparkline_svg(prices, width=76, height=28):
    """가격 리스트를 받아 작은 선 그래프 SVG 문자열을 반환.
    포인트가 너무 적으면(5개 미만) 의미 없는 대각선 대신 '데이터 부족'을 표시한다."""
    if not prices or len(prices) < 5:
        return (
            f"<div style='width:{width}px; height:{height}px; display:flex; align-items:center; "
            f"justify-content:center; color:#CBD5E1; font-size:9.5px; white-space:nowrap;'>데이터 부족</div>"
        )

    lo, hi = min(prices), max(prices)
    rng = (hi - lo) or 1
    pad = 2
    n = len(prices)
    step = (width - pad * 2) / (n - 1)

    points = []
    for i, p in enumerate(prices):
        x = pad + i * step
        y = pad + (1 - (p - lo) / rng) * (height - pad * 2)
        points.append(f"{x:.1f},{y:.1f}")

    up = prices[-1] >= prices[0]
    color = "#DC2626" if up else "#2563EB"  # 국내 관례: 상승 빨강, 하락 파랑
    poly = " ".join(points)
    last_x, last_y = points[-1].split(",")

    return (
        f"<svg width='{width}' height='{height}' viewBox='0 0 {width} {height}' "
        f"xmlns='http://www.w3.org/2000/svg' style='display:block;'>"
        f"<polyline points='{poly}' fill='none' stroke='{color}' stroke-width='1.6' "
        f"stroke-linecap='round' stroke-linejoin='round'/>"
        f"<circle cx='{last_x}' cy='{last_y}' r='2' fill='{color}'/>"
        f"</svg>"
    )


def render_watchlist_portfolio_summary(df):
    """매수가·수량이 입력된 보유 종목들의 총 매입금액/평가금액/손익/수익률을 계산해 요약 카드로 표시."""
    buy_price = pd.to_numeric(df["매수가"], errors="coerce")
    qty = pd.to_numeric(df["수량"], errors="coerce")
    mask = buy_price.notna() & qty.notna() & (qty > 0)
    holdings = df[mask].copy()
    if holdings.empty:
        return

    codes = holdings["종목코드"].tolist()
    current_prices = {}
    _executor = get_shared_executor()
    _futures = {_executor.submit(fetch_live_price_change, code): code for code in codes}
    try:
        for future in concurrent.futures.as_completed(_futures, timeout=12):
            code = _futures[future]
            try:
                price, _pct, _diff = future.result(timeout=6)
            except Exception:
                price = None
            current_prices[code] = price
    except concurrent.futures.TimeoutError:
        pass
    finally:
        for f in _futures:
            f.cancel()

    total_buy = 0.0
    total_eval = 0.0
    for _, row in holdings.iterrows():
        code = row["종목코드"]
        bp = float(row["매수가"])
        q = float(row["수량"])
        cur = current_prices.get(code) or bp  # 현재가 조회 실패 시 매입가로 대체
        total_buy += bp * q
        total_eval += cur * q

    total_pl = total_eval - total_buy
    ret_pct = (total_pl / total_buy * 100) if total_buy else 0.0
    is_profit = total_pl >= 0
    box_bg = "#F0FDF4" if is_profit else "#FFF7F7"
    box_border = "#16A34A" if is_profit else "#DC2626"
    box_text = "#15803D" if is_profit else "#B91C1C"
    sign = "+" if is_profit else ""

    st.markdown(f"""
    <div style="background:#F8FAFC; border:1px solid #E2E8F0; border-radius:12px; padding:20px 22px; margin-bottom:20px;">
      <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:16px;">
        <span style="font-size:15px; font-weight:700; color:#1E293B;">내 포트폴리오</span>
        <span style="font-size:11px; color:#94A3B8;">{datetime.datetime.now().strftime('%Y-%m-%d %H:%M')} 기준</span>
      </div>
      <div style="display:grid; grid-template-columns:repeat(4,1fr); gap:10px;">
        <div style="background:#FFFFFF; border:1px solid #E2E8F0; border-radius:8px; padding:12px;">
          <div style="font-size:11px; color:#64748B; margin-bottom:4px;">총 매입금액</div>
          <div style="font-size:17px; font-weight:800; color:#1E293B;">{total_buy:,.0f}원</div>
        </div>
        <div style="background:#FFFFFF; border:1px solid #E2E8F0; border-radius:8px; padding:12px;">
          <div style="font-size:11px; color:#64748B; margin-bottom:4px;">평가금액</div>
          <div style="font-size:17px; font-weight:800; color:#1E293B;">{total_eval:,.0f}원</div>
        </div>
        <div style="background:{box_bg}; border:1.5px solid {box_border}; border-radius:8px; padding:12px;">
          <div style="font-size:11px; color:{box_border}; font-weight:700; margin-bottom:4px;">총 손익</div>
          <div style="font-size:17px; font-weight:800; color:{box_text};">{sign}{total_pl:,.0f}원</div>
        </div>
        <div style="background:{box_bg}; border:1.5px solid {box_border}; border-radius:8px; padding:12px;">
          <div style="font-size:11px; color:{box_border}; font-weight:700; margin-bottom:4px;">수익률</div>
          <div style="font-size:17px; font-weight:800; color:{box_text};">{sign}{ret_pct:.2f}%</div>
        </div>
      </div>
      <div style="margin-top:12px; padding-top:12px; border-top:1px solid #E2E8F0; font-size:11px; color:#94A3B8;">
        보유 종목 {len(holdings)}개 중 매수가·수량이 입력된 종목만 집계 (미입력 종목 제외)
      </div>
    </div>
    """, unsafe_allow_html=True)


def render_watchlist():
    st.header(
        "관심종목",
        help="""💡 **[관심종목 안내]**\n\n관심 있는 종목을 저장해두고 한눈에 모아볼 수 있는 마이페이지입니다.\n\n종목코드 또는 종목명으로 검색해 추가하면, 계정에 저장되어 다음에 로그인해도 그대로 유지됩니다.\n\n'종목 스크리너'를 한 번 불러온 상태라면 현재가·PER·PBR·배당수익률도 함께 표시됩니다.\n\n각 카드 우측의 ⭐를 누르면 해당 종목이 목록 맨 위에 고정됩니다. 📊는 재무분석 바로가기, 🗑️는 삭제입니다.\n\n각 종목 카드의 '보유 정보 · 매수 타점 입력'에서 1차/2차/3차 진입가를 입력해두면, 현재가가 그 가격 이하로 내려왔을 때 카드와 목록 상단에 자동으로 알림이 표시됩니다."""
    )
    st.markdown("<hr style='margin: 10px 0 25px 0; border-color: #E5E7EB;'>", unsafe_allow_html=True)

    username = st.session_state.get("auth_user")

    st.markdown(
        """
        <style>
            /* 검색창+추가버튼 행: 버튼 쪽 컬럼은 고정폭(176px), 검색창 쪽 컬럼만 남은 폭을 채우도록 */
            div[class*="st-key-watchlist_search_row"] div[data-testid="stColumn"]:nth-of-type(1) {
                flex: 1 1 0% !important;
                min-width: 0 !important;
            }
            div[class*="st-key-watchlist_search_row"] div[data-testid="stColumn"]:nth-of-type(2) {
                flex: 0 0 176px !important;
                width: 176px !important;
                max-width: 176px !important;
            }
            /* '관심종목' 버튼: 둥근사각형 + 160px 고정폭, 검정 텍스트 */
            div[class*="st-key-watchlist_add_btn"] .stButton > button {
                padding: 6px 10px !important;
                font-size: 12px !important;
                font-weight: 600 !important;
                color: #64748B !important;
                background-color: #F1F5F9 !important;
                border: 1.5px solid #94A3B8 !important;
                border-radius: 8px !important;
                width: 160px !important;
            }
            div[class*="st-key-watchlist_add_btn"] .stButton > button p {
                color: #64748B !important;
                font-weight: 600 !important;
            }
            div[class*="st-key-watchlist_add_btn"] .stButton > button:hover {
                color: #5A4EE5 !important;
                border-color: #5A4EE5 !important;
                background-color: #EEF2FF !important;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )
    with st.container(key="watchlist_search_row"):
        col1, col2 = st.columns([3.44, 0.56])
        with col1:
            wl_query = st.text_input(
                "종목코드 또는 종목명 입력",
                placeholder="예: 005930, 삼성전자",
                label_visibility="collapsed",
                key="watchlist_query_input",
            )
        with col2:
            wl_add_btn = st.button("⭐ 관심종목", key="watchlist_add_btn")

    if wl_add_btn and wl_query:
        resolved_code, resolved_name, candidates = resolve_stock_query(wl_query)
        st.session_state.pop('watchlist_not_found', None)
        if candidates:
            st.session_state['watchlist_candidates'] = candidates
        elif resolved_code:
            final_name = resolved_name
            if not final_name:
                _info = fetch_company_info_fnguide(resolved_code)
                final_name = _info['name'] if _info['name'] != "알 수 없음" else resolved_code
            added = add_to_watchlist(username, resolved_code, final_name)
            st.session_state.pop('watchlist_candidates', None)
            if added:
                st.success(f"'{final_name}'을(를) 관심종목에 추가했습니다.")
            else:
                st.info(f"'{final_name}'은(는) 이미 관심종목에 있습니다.")
            st.rerun()
        else:
            st.session_state.pop('watchlist_candidates', None)
            st.session_state['watchlist_not_found'] = wl_query

    if st.session_state.get('watchlist_candidates'):
        candidates = st.session_state['watchlist_candidates']
        options = [f"{c['name']} ({c['code']}) · {c['market']}" for c in candidates]
        col_pick, col_pick_btn, _ = st.columns([2.6, 1, 3.4])
        with col_pick:
            picked = st.selectbox(
                "검색 결과가 여러 건입니다. 종목을 선택해주세요.",
                options,
                key="watchlist_pick_select",
            )
        with col_pick_btn:
            st.markdown("<div style='height:28px;'></div>", unsafe_allow_html=True)
            if st.button("이 종목 추가", use_container_width=True, key="watchlist_pick_confirm"):
                picked_idx = options.index(picked)
                picked_stock = candidates[picked_idx]
                added = add_to_watchlist(username, picked_stock['code'], picked_stock['name'])
                st.session_state.pop('watchlist_candidates', None)
                if added:
                    st.success(f"'{picked_stock['name']}'을(를) 관심종목에 추가했습니다.")
                else:
                    st.info(f"'{picked_stock['name']}'은(는) 이미 관심종목에 있습니다.")
                st.rerun()

    if st.session_state.get('watchlist_not_found'):
        st.warning(f"'{st.session_state['watchlist_not_found']}'에 해당하는 종목을 찾을 수 없습니다. 정확한 종목명 또는 6자리 종목코드로 다시 검색해주세요.")

    st.markdown("<br>", unsafe_allow_html=True)

    watchlist_df = load_watchlist(username)
    if watchlist_df.empty:
        st.info("아직 저장된 관심종목이 없습니다. 위 검색창에서 종목을 추가해보세요.")
        return

    render_watchlist_portfolio_summary(watchlist_df)

    # 고정(즐겨찾기)한 종목이 먼저 오도록 정렬 (안정 정렬이라 같은 그룹 내 원래 순서는 유지)
    watchlist_df = watchlist_df.assign(_pinned=(watchlist_df["고정"] == "Y")).sort_values(
        "_pinned", ascending=False, kind="stable"
    ).drop(columns="_pinned").reset_index(drop=True)

    # 종목 스크리너를 한 번이라도 불러온 상태면 현재가·PER·PBR·배당수익률을 함께 보여줌
    # (아래 병렬 조회에서 시장 구분에도 필요해서 여기로 끌어올림)
    screener_df = load_screener_df()
    has_live_data = screener_df is not None and not screener_df.empty and '종목코드' in screener_df.columns
    if has_live_data:
        screener_df = screener_df.copy()
        screener_df['종목코드'] = screener_df['종목코드'].astype(str).str.zfill(6)
        _wl_market_map = (
            dict(zip(screener_df['종목코드'], screener_df['시장'])) if '시장' in screener_df.columns else {}
        )
    else:
        _wl_market_map = {}

    # ── 진입가(1차/2차/3차) 도달 여부 사전 계산 (카드 렌더링 + 상단 요약에 공용 사용) ──
    def _parse_entry(v):
        try:
            f = float(str(v).replace(",", "").strip())
            return f if f > 0 else None
        except Exception:
            return None

    # ── 관심종목 전체의 현재가/스파크라인/AI점수/도달확률을 병렬로 미리 조회 (캐시 예열) ──
    # 예전엔 종목마다 순차적으로 여러 개의 네트워크 호출(현재가·차트·재무데이터·도달확률)을
    # 했었는데, 관심종목이 많으면 이게 다 더해져서 페이지 전환할 때마다 체감상 무한
    # 로딩처럼 느껴졌음. ThreadPoolExecutor로 종목별 조회를 동시에 실행해서 전체
    # 대기시간을 크게 줄임(종목당 작업 1개로 합쳐서 스레드풀 워커도 절반만 씀).
    wl_price_cache = {}
    ai_score_cache = {}
    sparkline_cache = {}
    hit_prob_cache = {}
    _wl_codes = list(dict.fromkeys(watchlist_df['종목코드'].tolist()))

    # 목표가 도달확률 계산에 필요한 목표가는 네트워크 호출 없이(세션 상태 커스텀 값 또는
    # 스크리너 PER/PBR만으로) 먼저 다 구해둔다. 그래야 아래 프리페치에서 종목당 작업을
    # 딱 하나로 합칠 수 있다(시세+스파크라인+AI점수+도달확률을 한 스레드에서 순서대로).
    _hp_targets = {}  # code -> (market_hint, target_price, target_src)
    for _, _hp_row in watchlist_df.iterrows():
        _hp_code = _hp_row['종목코드']
        if _hp_code in _hp_targets:
            continue
        _hp_live = None
        if has_live_data:
            _hp_match = screener_df[screener_df['종목코드'] == _hp_code]
            if not _hp_match.empty:
                _hp_live = _hp_match.iloc[0]
        _hp_custom_raw = re.sub(r"[^\d]", "", str(st.session_state.get(f"wl_target_{_hp_code}", "")))
        _hp_custom = int(_hp_custom_raw) if _hp_custom_raw else 0
        if _hp_custom > 0:
            _hp_tgt, _hp_src = _hp_custom, "직접 입력"
            _hp_market = _hp_live.get('시장') if _hp_live is not None else None
        elif _hp_live is not None:
            _hp_base = _hp_live.get('현재가')
            _hp_per = _hp_live.get('PER')
            _hp_pbr = _hp_live.get('PBR')
            _hp_base = float(_hp_base) if pd.notna(_hp_base) and _hp_base else 0.0
            _hp_per = float(_hp_per) if pd.notna(_hp_per) and _hp_per else None
            _hp_pbr = float(_hp_pbr) if pd.notna(_hp_pbr) and _hp_pbr else None
            _hp_tgt, _hp_src = estimate_simple_target_price(_hp_base, _hp_per, _hp_pbr)
            _hp_market = _hp_live.get('시장')
        else:
            _hp_tgt, _hp_src, _hp_market = 0, "", None
        if _hp_tgt:
            _hp_targets[_hp_code] = (_hp_market, _hp_tgt, _hp_src)

    def _wl_prefetch_one(code):
        market = _wl_market_map.get(code)
        price_info = fetch_live_price_change(code)
        spark = fetch_watchlist_sparkline_prices(code, market)
        ai_score = get_ai_total_score(code, screener_df=screener_df)
        hit_prob_html = ""
        if code in _hp_targets:
            _mh, _tp, _ts = _hp_targets[code]
            _hp_res = estimate_target_hit_probability(code, _mh, _tp)
            if _hp_res:
                hit_prob_html = _format_hit_probability_badge(_hp_res, _tp, _ts)
        return code, price_info, spark, ai_score, hit_prob_html

    if _wl_codes:
        # ⚠️ Streamlit은 새 탭 클릭(재실행 요청)이 와도, 지금 실행 중인 스크립트가
        # 여기처럼 순수 파이썬 블로킹 호출(as_completed/future.result) 안에 있으면 그
        # 자리에서 끼어들 수 없다. 사용자가 몇 초 안에 여러 번 연달아 탭을 누르면,
        # 이전 실행이 아직 이 대기 구간에 갇혀 있는 채로 새 실행들이 밀리고, 그 새
        # 실행들도 각자 같은 공유 스레드풀에 작업을 또 밀어넣어 워커가 금방 동나버려서
        # 사실상 멈춘 것처럼 보이는 현상으로 이어진다. 그래서 상한을 짧게(15초/8초)
        # 유지한다 — 상한을 넘기면 이번 렌더링에서는 일부 종목 데이터가 비어있는 채로
        # 넘어가고(다음 새로고침 때 캐시가 채워지며 자연히 보임), 그 대신 Streamlit이
        # 최대한 빨리 제어권을 되찾아서 대기 중인 새 클릭을 처리할 수 있게 하는 걸
        # 더 우선한다.
        with st.spinner(f"🔄 관심종목 {len(_wl_codes)}건 시세 조회 중..."):
            _wl_executor = get_shared_executor()
            _wl_futures = {_wl_executor.submit(_wl_prefetch_one, c): c for c in _wl_codes}
            try:
                for _wl_future in concurrent.futures.as_completed(_wl_futures, timeout=8):
                    try:
                        _code, _price_info, _spark, _ai_score, _hp_html = _wl_future.result(timeout=4)
                    except Exception:
                        continue
                    wl_price_cache[_code] = _price_info
                    sparkline_cache[_code] = _spark
                    ai_score_cache[_code] = _ai_score
                    if _hp_html:
                        hit_prob_cache[_code] = _hp_html
            except concurrent.futures.TimeoutError:
                pass  # 전체 상한 초과 → 나머지는 건너뛰고 계속 진행
            finally:
                for f in _wl_futures:
                    f.cancel()


    reached_summary = []  # [(종목명, 종목코드, 도달차수, 진입가, 현재가), ...]
    for _, _row in watchlist_df.iterrows():
        _code = _row['종목코드']
        _live_price, _chg_pct, _chg_amt = wl_price_cache.get(_code, (None, None, None))
        if _live_price is None or _live_price <= 0:
            continue
        entries = [
            (1, _parse_entry(_row.get('1차진입가'))),
            (2, _parse_entry(_row.get('2차진입가'))),
            (3, _parse_entry(_row.get('3차진입가'))),
        ]
        for _n, _ep in entries:
            if _ep is not None and _live_price <= _ep:
                reached_summary.append((_row['종목명'], _code, _n, _ep, _live_price))

    if reached_summary:
        _items_html = "".join(
            f"<div style='padding:6px 0; font-size:13px; color:#334155;'>"
            f"🎯 <b style='color:#0F172A;'>{html_lib.escape(str(_nm))}</b> ({_cd}) "
            f"— <span style='color:#DC2626; font-weight:700;'>{_n}차 타점 도달</span> "
            f"(진입가 {_ep:,.0f}원 / 현재가 {_lp:,.0f}원)</div>"
            for _nm, _cd, _n, _ep, _lp in reached_summary
        )
        st.markdown(
            f"""<div style="background:#FFF7ED; border:1px solid #FDBA74; border-radius:8px;
                    padding:12px 16px; margin-bottom:18px;">
                <div style="font-weight:700; font-size:13.5px; color:#C2410C; margin-bottom:4px;">
                    🔔 매수 타점 도달 알림 ({len(reached_summary)}건)
                </div>
                {_items_html}
            </div>""",
            unsafe_allow_html=True,
        )

    # ── 아직 도달하지 않은 종목: 가장 가까운 타점까지 남은 하락폭(%) 정리 ──
    pending_summary = []  # [(종목명, 종목코드, 차수, 진입가, 현재가, 남은%), ...]
    for _, _row in watchlist_df.iterrows():
        _code = _row['종목코드']
        _live_price, _chg_pct, _chg_amt = wl_price_cache.get(_code, (None, None, None))
        if _live_price is None or _live_price <= 0:
            continue
        entries = [
            (1, _parse_entry(_row.get('1차진입가'))),
            (2, _parse_entry(_row.get('2차진입가'))),
            (3, _parse_entry(_row.get('3차진입가'))),
        ]
        unreached = [(_n, _ep) for _n, _ep in entries if _ep is not None and _live_price > _ep]
        if not unreached:
            continue
        # 현재가와 가장 가까운(=곧 도달할 가능성이 높은) 타점 하나만 선택
        _n, _ep = min(unreached, key=lambda x: _live_price - x[1])
        _remain_pct = (_ep / _live_price - 1) * 100  # 음수 값 = 몇 % 더 떨어져야 하는지
        pending_summary.append((_row['종목명'], _code, _n, _ep, _live_price, _remain_pct))

    # 0%에 가까운(=임박한) 순으로 정렬
    pending_summary.sort(key=lambda x: x[5], reverse=True)

    if pending_summary:
        _pending_html = "".join(
            f"<div style='padding:6px 0; font-size:13px; color:#334155; display:flex; justify-content:space-between; gap:8px;'>"
            f"<span><b style='color:#0F172A;'>{html_lib.escape(str(_nm))}</b> ({_cd}) "
            f"— {_n}차 타점 (진입가 {_ep:,.0f}원 / 현재가 {_lp:,.0f}원)</span>"
            f"<span style='color:#1D4ED8; font-weight:700; white-space:nowrap;'>{_rp:+.1f}%</span></div>"
            for _nm, _cd, _n, _ep, _lp, _rp in pending_summary[:10]
        )
        st.markdown(
            f"""<div style="background:#EFF6FF; border:1px solid #93C5FD; border-radius:8px;
                    padding:12px 16px; margin-bottom:18px;">
                <div style="font-weight:700; font-size:13.5px; color:#1D4ED8; margin-bottom:4px;">
                    다음 매수 타점까지 (가까운 순, 최대 10건)
                </div>
                {_pending_html}
            </div>""",
            unsafe_allow_html=True,
        )

    if not has_live_data:
        st.caption("ℹ️ PER·배당수익률은 '종목 스크리너' 탭에서 전체 데이터를 한 번 불러오면 함께 표시됩니다. (현재가는 실시간으로 조회됩니다)")

    st.markdown(
        """
        <style>
            /* 상단 행: 창 폭이 좁아져도 줄바꿈되지 않고 한 줄을 유지하도록 고정 */
            div[class*="st-key-wl_top_row_"] div[data-testid="stHorizontalBlock"] {
                flex-wrap: nowrap !important;
            }
            /* 이름/현재가/차트/지표 컬럼: 폭이 줄어들 때 다음 줄로 밀리지 않고 자연스럽게 축소되도록 */
            div[class*="st-key-wl_top_row_"] div[data-testid="stColumn"]:nth-of-type(1),
            div[class*="st-key-wl_top_row_"] div[data-testid="stColumn"]:nth-of-type(2),
            div[class*="st-key-wl_top_row_"] div[data-testid="stColumn"]:nth-of-type(3),
            div[class*="st-key-wl_top_row_"] div[data-testid="stColumn"]:nth-of-type(4) {
                min-width: 0 !important;
            }
            /* 아이콘 3개(⭐ 고정 / 📊 재무분석 / 🗑️ 삭제)가 속한 컬럼: 고정폭 + 우측 정렬 */
            div[class*="st-key-wl_top_row_"] div[data-testid="stColumn"]:nth-of-type(5) {
                flex: 0 0 148px !important;
                width: 148px !important;
                max-width: 148px !important;
            }
            div[class*="st-key-wl_icons_"] div[data-testid="stHorizontalBlock"] {
                gap: 6px !important;
                justify-content: flex-end !important;
            }
            /* 아이콘 버튼 공통 스타일: 작고 균일한 정사각형, 은은한 회색 */
            div[class*="st-key-wl_pin_"] .stButton > button,
            div[class*="st-key-wl_view_"] .stButton > button,
            div[class*="st-key-wl_delbtn_"] .stButton > button {
                padding: 7px 0 !important;
                font-size: 13px !important;
                width: 100% !important;
                background-color: #FFFFFF !important;
                border: 1px solid #E2E8F0 !important;
                color: #94A3B8 !important;
                border-radius: 6px !important;
            }
            /* ⭐ 고정: 비활성 시 은은한 회색, hover는 골드 톤 */
            div[class*="st-key-wl_pin_off_"] .stButton > button:hover {
                background-color: #FFFBEB !important;
                border-color: #FCD34D !important;
                color: #D97706 !important;
            }
            /* ⭐ 고정: 활성 상태는 골드 톤으로 항상 강조 */
            div[class*="st-key-wl_pin_on_"] .stButton > button {
                background-color: #FFFBEB !important;
                border-color: #FBBF24 !important;
                color: #D97706 !important;
            }
            /* 📊 재무분석: hover는 브랜드 컬러(보라) 톤 */
            div[class*="st-key-wl_view_"] .stButton > button:hover {
                background-color: #EEF2FF !important;
                border-color: #5A4EE5 !important;
                color: #5A4EE5 !important;
            }
            /* 🗑️ 삭제: 작고 은은하게, 호버 시 빨간색으로 경고 표시 */
            div[class*="st-key-wl_delbtn_"] .stButton > button:hover {
                background-color: #FEF2F2 !important;
                border-color: #FCA5A5 !important;
                color: #DC2626 !important;
            }
            /* 보유정보 expander 헤더: 펼쳤을 때 배경이 너무 진하지 않도록 */
            div[class*="st-key-wl_card_"] details summary {
                background-color: #F8FAFC !important;
            }
            div[class*="st-key-wl_card_"] details summary:hover {
                background-color: #F1F5F9 !important;
            }
            div[class*="st-key-wl_card_"] details[open] summary {
                background-color: #EEF2FF !important;
            }
            /* 보유 정보 초기화 버튼: '7일' 필처럼 연한 비활성 캡슐 스타일 */
            div[class*="st-key-wl_reset_"] .stButton > button {
                padding: 6px 10px !important;
                font-size: 12px !important;
                font-weight: 600 !important;
                color: #64748B !important;
                opacity: 1 !important;
                background-color: #F1F5F9 !important;
                border: 1.5px solid #94A3B8 !important;
                border-radius: 999px !important;
                width: 100% !important;
                margin: 0 !important;
                display: block !important;
            }
            div[class*="st-key-wl_reset_"] .stButton > button p {
                color: #64748B !important;
                font-weight: 600 !important;
            }
            div[class*="st-key-wl_reset_"] .stButton > button:hover {
                color: #DC2626 !important;
                border-color: #FCA5A5 !important;
                background-color: #FEF2F2 !important;
            }
            div[class*="st-key-wl_reset_"] .stButton > button:hover p {
                color: #DC2626 !important;
            }
            /* 보유 정보 저장 버튼: '30일' 필처럼 진한 활성 캡슐 스타일 */
            div[class*="st-key-wl_save_"] .stButton > button {
                padding: 6px 10px !important;
                font-size: 12px !important;
                font-weight: 700 !important;
                color: #FFFFFF !important;
                background-color: #5A4EE5 !important;
                border: 1px solid #5A4EE5 !important;
                border-radius: 999px !important;
                width: 100% !important;
                margin: 0 !important;
                display: block !important;
            }
            div[class*="st-key-wl_save_"] .stButton > button p {
                color: #FFFFFF !important;
                font-weight: 700 !important;
            }
            div[class*="st-key-wl_save_"] .stButton > button:hover {
                background-color: #4A3ED0 !important;
                border-color: #4A3ED0 !important;
            }
            /* 매수가/수량/버튼 행: 매수가·수량은 고정폭, 버튼 쪽은 남는 공간을 채우고 우측 정렬 */
            div[class*="st-key-wl_inputs_row_"] div[data-testid="stHorizontalBlock"] {
                gap: 12px !important;
            }
            div[class*="st-key-wl_inputs_row_"] div[data-testid="stColumn"]:nth-of-type(1) {
                flex: 0 0 100px !important;
                width: 100px !important;
                max-width: 100px !important;
                min-width: 100px !important;
            }
            div[class*="st-key-wl_inputs_row_"] div[data-testid="stColumn"]:nth-of-type(2) {
                flex: 0 0 100px !important;
                width: 100px !important;
                max-width: 100px !important;
                min-width: 100px !important;
            }
            div[class*="st-key-wl_inputs_row_"] div[data-testid="stColumn"]:nth-of-type(3) {
                flex: 1 1 0% !important;
                min-width: 0 !important;
            }
            /* 저장/초기화 버튼 묶음: 컬럼을 100px 고정폭으로, 두 버튼 사이 간격 축소, 우측 정렬 */
            div[class*="st-key-wl_btns_"] div[data-testid="stHorizontalBlock"] {
                gap: 8px !important;
                justify-content: flex-end !important;
            }
            div[class*="st-key-wl_btns_"] div[data-testid="stColumn"][data-testid="stColumn"] {
                flex: 0 0 100px !important;
                width: 100px !important;
                max-width: 100px !important;
                min-width: 100px !important;
            }
            /* 매수 타점(1차/2차/3차 진입가) 입력 행 */
            div[class*="st-key-wl_entry_row_"] div[data-testid="stHorizontalBlock"] {
                gap: 12px !important;
            }
            /* 매수 타점 저장/초기화 버튼 묶음 */
            div[class*="st-key-wl_entry_btns_"] div[data-testid="stHorizontalBlock"] {
                gap: 8px !important;
                justify-content: flex-end !important;
            }
            div[class*="st-key-wl_entry_btns_"] div[data-testid="stColumn"][data-testid="stColumn"] {
                flex: 0 0 100px !important;
                width: 100px !important;
                max-width: 100px !important;
                min-width: 100px !important;
            }
            div[class*="st-key-wl_entry_save_"] .stButton > button {
                padding: 6px 10px !important;
                font-size: 12px !important;
                font-weight: 700 !important;
                color: #FFFFFF !important;
                background-color: #EA580C !important;
                border: 1px solid #EA580C !important;
                border-radius: 999px !important;
                width: 100% !important;
                margin: 0 !important;
                display: block !important;
            }
            div[class*="st-key-wl_entry_save_"] .stButton > button p {
                color: #FFFFFF !important;
                font-weight: 700 !important;
            }
            div[class*="st-key-wl_entry_save_"] .stButton > button:hover {
                background-color: #C2410C !important;
                border-color: #C2410C !important;
            }
            div[class*="st-key-wl_entry_reset_"] .stButton > button {
                padding: 6px 10px !important;
                font-size: 12px !important;
                font-weight: 600 !important;
                color: #64748B !important;
                background-color: #F1F5F9 !important;
                border: 1.5px solid #94A3B8 !important;
                border-radius: 999px !important;
                width: 100% !important;
                margin: 0 !important;
                display: block !important;
            }
            div[class*="st-key-wl_entry_reset_"] .stButton > button p {
                color: #64748B !important;
                font-weight: 600 !important;
            }
            div[class*="st-key-wl_entry_reset_"] .stButton > button:hover {
                color: #DC2626 !important;
                border-color: #FCA5A5 !important;
                background-color: #FEF2F2 !important;
            }
            div[class*="st-key-wl_entry_reset_"] .stButton > button:hover p {
                color: #DC2626 !important;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )

    for _, row in watchlist_df.iterrows():
        code, name, added_at = row['종목코드'], row['종목명'], row['추가일']

        live = None
        if has_live_data:
            match = screener_df[screener_df['종목코드'] == code]
            if not match.empty:
                live = match.iloc[0]

        ai_total = ai_score_cache.get(code)

        with st.container(border=True, key=f"wl_card_{code}"):
            with st.container(key=f"wl_top_row_{code}"):
                c_name, c_price, c_ai, c_chart, c_icons = st.columns([2.6, 1.9, 1.4, 1.1, 1.3])
                with c_name:
                    per_v = live.get('PER') if live is not None else None
                    div_v = live.get('배당수익률') if live is not None else None
                    per_txt = f"{per_v:.1f}배" if pd.notna(per_v) else "-"
                    div_txt = f"{div_v:.1f}%" if pd.notna(div_v) else "-"
                    st.markdown(
                        f"<div style='font-size:12px; color:#475569;'>{code} "
                        f"<span style='color:#CBD5E1;'>|</span> "
                        f"<span style='color:#475569;'>PER {per_txt} · 배당수익률 {div_txt}</span></div>"
                        f"<div style='font-weight:700; font-size:15px; color:#0F172A; margin-top:1px;'>{html_lib.escape(str(name))}</div>",
                        unsafe_allow_html=True,
                    )
                with c_price:
                    live_price, chg_pct, chg_amt = wl_price_cache.get(code, (None, None, None))
                    if live_price is None and live is not None and pd.notna(live.get('현재가')):
                        live_price = live['현재가']
                        chg_pct = None
                        chg_amt = None
                    if live_price is not None and live_price > 0:
                        if chg_pct is not None:
                            if chg_pct > 0:
                                chg_color, chg_arrow = "#DC2626", "▲"
                            elif chg_pct < 0:
                                chg_color, chg_arrow = "#2563EB", "▼"
                            else:
                                chg_color, chg_arrow = "#94A3B8", "-"
                            amt_txt = f" {abs(chg_amt):,.0f}원" if chg_amt is not None else ""
                            chg_html = f"<span style='color:{chg_color}; font-size:12px; font-weight:600; margin-left:6px;'>{chg_arrow}{amt_txt} ({abs(chg_pct):.2f}%)</span>"
                        else:
                            chg_html = ""
                        _reached_ns = [n for _nm, _cd, n, _ep, _lp in reached_summary if _cd == code]
                        badge_html = (
                            f"<span style='display:inline-block; margin-left:6px; font-size:10.5px; font-weight:700; "
                            f"color:#C2410C; background:#FFF7ED; border:1px solid #FDBA74; border-radius:6px; padding:1px 6px; "
                            f"vertical-align:middle;'>🎯 {min(_reached_ns)}차 타점 도달</span>"
                        ) if _reached_ns else ""
                        st.markdown(
                            f"<div style='font-size:12px; color:#64748B;'>현재가{badge_html}</div>"
                            f"<div style='font-weight:700; font-size:15px; color:#0F172A;'>{live_price:,.0f}원{chg_html}</div>",
                            unsafe_allow_html=True,
                        )
                    else:
                        st.markdown("<div style='color:#CBD5E1; font-size:13px;'>현재가 -</div>", unsafe_allow_html=True)
                with c_ai:
                    if ai_total is not None:
                        _c = _ai_score_color(ai_total)
                        st.markdown(
                            f"<div style='font-size:12px; color:#64748B;'>AI 종합점수</div>"
                            f"<div style='font-weight:700; font-size:15px; color:{_c};'>{ai_total}</div>",
                            unsafe_allow_html=True,
                        )
                    else:
                        st.markdown("<div style='color:#CBD5E1; font-size:13px;'>AI 종합점수 -</div>", unsafe_allow_html=True)
                with c_chart:
                    sp_prices = sparkline_cache.get(code, [])
                    st.markdown(
                        f"<div style='display:flex; justify-content:flex-start; padding-top:4px;'>{render_mini_sparkline_svg(sp_prices)}</div>",
                        unsafe_allow_html=True,
                    )
                with c_icons:
                    is_pinned = str(row.get('고정', '')).strip().upper() == 'Y'
                    with st.container(key=f"wl_icons_{code}"):
                        icol1, icol2, icol3 = st.columns(3)
                        with icol1:
                            pin_key = f"wl_pin_on_{code}" if is_pinned else f"wl_pin_off_{code}"
                            pin_help = "상단 고정 해제" if is_pinned else "상단에 고정"
                            if st.button("⭐" if is_pinned else "☆", key=pin_key, use_container_width=True, help=pin_help):
                                toggle_watchlist_pin(username, code)
                                st.rerun()
                        with icol2:
                            if st.button("📊", key=f"wl_view_{code}", use_container_width=True, help="재무분석 보기"):
                                st.session_state['fnguide_code'] = code
                                st.session_state.pop('fnguide_candidates', None)
                                st.session_state.current_page = "기업 재무 분석"
                                st.rerun()
                        with icol3:
                            if st.button("🗑️", key=f"wl_delbtn_{code}", use_container_width=True, help="관심종목에서 삭제"):
                                remove_from_watchlist(username, code)
                                st.rerun()

            # 도달 확률은 위에서 이미 전 종목 병렬 프리페치를 끝냈으므로, 여기서는
            # 네트워크 호출 없이 캐시에서 꺼내 포맷팅만 한다(종목별 blocking 호출 금지).
            _prob_html = hit_prob_cache.get(code, "")
            if _prob_html:
                st.markdown(_prob_html, unsafe_allow_html=True)

            wl_buy_raw, wl_qty_raw = row.get('매수가', ''), row.get('수량', '')
            try:
                wl_buy_val = float(wl_buy_raw) if str(wl_buy_raw).strip() not in ("", "nan") else 0.0
            except Exception:
                wl_buy_val = 0.0
            try:
                wl_qty_val = float(wl_qty_raw) if str(wl_qty_raw).strip() not in ("", "nan") else 0.0
            except Exception:
                wl_qty_val = 0.0
            is_holding = wl_buy_val > 0 and wl_qty_val > 0

            pnl_header_txt = ""
            eval_amount_txt = ""
            if is_holding and live_price is not None and live_price > 0:
                eval_amount = live_price * wl_qty_val
                eval_amount_txt = f"  ·  평가금액 {eval_amount:,.0f}원"

                pnl_amt = (live_price - wl_buy_val) * wl_qty_val
                pnl_pct2 = (live_price - wl_buy_val) / wl_buy_val * 100
                if pnl_amt > 0:
                    pnl_arrow, pnl_sign, pnl_color = "▲", "+", "red"
                elif pnl_amt < 0:
                    pnl_arrow, pnl_sign, pnl_color = "▼", "", "blue"
                else:
                    pnl_arrow, pnl_sign, pnl_color = "-", "", "gray"
                pnl_header_txt = (
                    f"  ┃  평가손익 :{pnl_color}[{pnl_arrow} {pnl_sign}{pnl_amt:,.0f}원 "
                    f"({pnl_sign}{pnl_pct2:.2f}%)]"
                )

            e1_raw, e2_raw, e3_raw = row.get('1차진입가', ''), row.get('2차진입가', ''), row.get('3차진입가', '')
            e1_val = _parse_entry(e1_raw)
            e2_val = _parse_entry(e2_raw)
            e3_val = _parse_entry(e3_raw)
            has_entries = any(v is not None for v in (e1_val, e2_val, e3_val))
            _entry_parts = []
            if e1_val: _entry_parts.append(f"1차 {e1_val:,.0f}원")
            if e2_val: _entry_parts.append(f"2차 {e2_val:,.0f}원")
            if e3_val: _entry_parts.append(f"3차 {e3_val:,.0f}원")

            _label_parts = []
            if is_holding:
                _label_parts.append(f"💰 보유중  ·  {wl_qty_val:,.0f}주  ·  매수가 {wl_buy_val:,.0f}원{eval_amount_txt}{pnl_header_txt}")
            if has_entries:
                _label_parts.append("🎯 매수 타점  ·  " + " / ".join(_entry_parts))
            exp_label = "   ┃   ".join(_label_parts) if _label_parts else "📌 보유 정보 · 매수 타점 입력 (선택)"

            with st.expander(exp_label):
                with st.container(key=f"wl_inputs_row_{code}"):
                    ec1, ec2, ec_btns = st.columns([1.3, 1, 1.3])
                    with ec1:
                        buy_key = f"wl_buy_{code}"
                        if buy_key not in st.session_state:
                            st.session_state[buy_key] = f"{int(wl_buy_val):,}" if wl_buy_val else ""

                        def _format_wl_buy(k=buy_key):
                            digits = re.sub(r"[^\d]", "", str(st.session_state.get(k, "")))
                            st.session_state[k] = f"{int(digits):,}" if digits else ""

                        st.text_input("매수가(원)", key=buy_key, on_change=_format_wl_buy)
                        new_buy = int(re.sub(r"[^\d]", "", str(st.session_state.get(buy_key, ""))) or 0)
                    with ec2:
                        new_qty = st.number_input("수량(주)", min_value=0, value=int(wl_qty_val), step=1, key=f"wl_qty_{code}")
                    with ec_btns:
                        st.markdown("<div style='height:28px;'></div>", unsafe_allow_html=True)
                        with st.container(key=f"wl_btns_{code}"):
                            bcol1, bcol2 = st.columns(2)
                            with bcol1:
                                if st.button("저장", key=f"wl_save_{code}", use_container_width=True):
                                    update_watchlist_holding(username, code, new_buy, new_qty)
                                    st.rerun()
                            with bcol2:
                                if st.button("↺", key=f"wl_reset_btn_{code}", use_container_width=True, help="매수가·수량 초기화"):
                                    update_watchlist_holding(username, code, 0, 0)
                                    st.rerun()

                st.markdown("<hr style='margin: 10px 0; border-color: #EEF0F3;'>", unsafe_allow_html=True)

                with st.container(key=f"wl_entry_row_{code}"):
                    en1, en2, en3, en_btns = st.columns([1, 1, 1, 1.3])

                    def _make_entry_input(col, label, key, val):
                        with col:
                            if key not in st.session_state:
                                st.session_state[key] = f"{int(val):,}" if val else ""

                            def _fmt(k=key):
                                digits = re.sub(r"[^\d]", "", str(st.session_state.get(k, "")))
                                st.session_state[k] = f"{int(digits):,}" if digits else ""

                            st.text_input(label, key=key, on_change=_fmt)
                            return int(re.sub(r"[^\d]", "", str(st.session_state.get(key, ""))) or 0)

                    new_e1 = _make_entry_input(en1, "1차 진입가", f"wl_entry1_{code}", e1_val)
                    new_e2 = _make_entry_input(en2, "2차 진입가", f"wl_entry2_{code}", e2_val)
                    new_e3 = _make_entry_input(en3, "3차 진입가", f"wl_entry3_{code}", e3_val)
                    with en_btns:
                        st.markdown("<div style='height:28px;'></div>", unsafe_allow_html=True)
                        with st.container(key=f"wl_entry_btns_{code}"):
                            ebcol1, ebcol2 = st.columns(2)
                            with ebcol1:
                                if st.button("저장", key=f"wl_entry_save_{code}", use_container_width=True):
                                    update_watchlist_entries(username, code, new_e1, new_e2, new_e3)
                                    st.rerun()
                            with ebcol2:
                                if st.button("↺", key=f"wl_entry_reset_{code}", use_container_width=True, help="진입가 초기화"):
                                    update_watchlist_entries(username, code, 0, 0, 0)
                                    st.rerun()

                st.markdown("<hr style='margin: 10px 0; border-color: #EEF0F3;'>", unsafe_allow_html=True)

                with st.container(key=f"wl_target_row_{code}"):
                    tg1, tg2 = st.columns([1, 2.3])
                    with tg1:
                        _tgt_key = f"wl_target_{code}"
                        if _tgt_key not in st.session_state:
                            st.session_state[_tgt_key] = ""

                        def _fmt_target(k=_tgt_key):
                            digits = re.sub(r"[^\d]", "", str(st.session_state.get(k, "")))
                            st.session_state[k] = f"{int(digits):,}" if digits else ""

                        st.text_input("목표가 직접 입력 (원)", key=_tgt_key, on_change=_fmt_target, placeholder="예: 150,000")
                    with tg2:
                        st.markdown(
                            "<div style='padding-top:28px; font-size:11px; color:#94A3B8;'>"
                            "입력하면 위 도달 확률 배지가 이 금액 기준으로 즉시 갱신됩니다. "
                            "단, 이 값은 <b>현재 세션에서만</b> 유지되며 새로고침·재로그인 시 초기화됩니다."
                            "</div>",
                            unsafe_allow_html=True,
                        )

def render_login_page():
    st.markdown("""
        <style>
            .stApp { background-color: #F8FAFC !important; }

            .auth-title {
                font-size: 20px; font-weight: 800; color: #111827;
                text-align: center; margin: -20px 0 8px 0;
                line-height: 1;
                display: flex;
                align-items: center;
                justify-content: center;
                min-height: 20px;
            }

            /* 로그인 박스 전체(입력창 + 버튼)를 감싸는 카드 - 창 크기와 무관하게 고정 폭, 단일 박스 */
            section[data-testid="stMain"] .st-key-auth_box {
                background: #FFFFFF !important;
                border: 1px solid #E5E7EB !important;
                border-radius: 16px !important;
                padding: 56px 30px 22px 30px !important;
                box-shadow: 0 8px 24px rgba(15,23,42,0.06) !important;
                width: 420px !important;
                max-width: 420px !important;
                min-width: 420px !important;
                margin: 0 auto !important;
                box-sizing: border-box !important;
            }
            /* 안쪽 래퍼 div들은 배경/테두리 없이 투명하게 - 이중 박스 방지 */
            section[data-testid="stMain"] .st-key-auth_box > div {
                background: transparent !important;
                border: none !important;
                box-shadow: none !important;
                padding: 0 !important;
                width: 100% !important;
                max-width: 100% !important;
                min-width: 0 !important;
                margin: 0 !important;
            }

            /* 입력창: 연한 인디고 배경 + 테두리 없음 + 넉넉한 여백 */
            section[data-testid="stMain"] .stTextInput input,
            section[data-testid="stMain"] div[data-baseweb="input"],
            section[data-testid="stMain"] div[data-baseweb="base-input"] {
                background-color: #EEF2FF !important;
                border: 1px solid transparent !important;
                border-radius: 10px !important;
            }
            section[data-testid="stMain"] .stTextInput input {
                color: #111827 !important;
                padding: 10px 14px !important;
            }
            section[data-testid="stMain"] .stTextInput input::placeholder {
                color: #9CA3AF !important;
            }
            /* 비밀번호 표시/숨기기(눈 모양) 아이콘 버튼 완전히 숨김 */
            section[data-testid="stMain"] .stTextInput button[aria-label="Show password"],
            section[data-testid="stMain"] .stTextInput button[aria-label="Hide password"] {
                display: none !important;
            }

            /* 로그인/회원가입 제출 버튼: 크고 둥글게 (st.button, st.form_submit_button 둘 다 적용) */
            section[data-testid="stMain"] .stButton > button,
            section[data-testid="stMain"] .stFormSubmitButton > button {
                background-color: #5A4EE5 !important;
                border: none !important;
                border-radius: 10px !important;
                padding: 12px 0 !important;
                font-weight: 700 !important;
            }
            section[data-testid="stMain"] .stButton > button p,
            section[data-testid="stMain"] .stFormSubmitButton > button p {
                color: #FFFFFF !important; font-weight: 700 !important;
            }
            section[data-testid="stMain"] .stButton > button:hover,
            section[data-testid="stMain"] .stFormSubmitButton > button:hover {
                background-color: #4C41C3 !important;
            }
            /* st.form의 기본 테두리 제거 - 카드 안에 이중 박스 안생기게 */
            section[data-testid="stMain"] div[data-testid="stForm"] {
                border: none !important;
                padding: 0 !important;
            }

            section[data-testid="stMain"] .stTabs [data-baseweb="tab"] p {
                color: #374151 !important; font-weight: 600 !important;
            }
        </style>
    """, unsafe_allow_html=True)

    _, mid, _ = st.columns([1, 1.5, 1])
    with mid:
        with st.container(border=False, key="auth_box"):
            st.markdown("<div class='auth-title'>Inventory Manager</div>", unsafe_allow_html=True)

            # 회원가입 성공 직후에는 탭 위젯이 만들어지기 전에 먼저 값을 세팅해야
            # "위젯 생성 후에는 session_state를 못 바꾼다"는 예외를 피할 수 있음
            if st.session_state.pop("_force_login_tab", False):
                st.session_state["auth_tabs"] = "로그인"

            tab_login, tab_signup = st.tabs(
                ["로그인", "회원가입"],
                key="auth_tabs",
                on_change="rerun",
            )

            with tab_login:
                if st.session_state.get("signup_success_msg"):
                    st.success(st.session_state.pop("signup_success_msg"))
                with st.form("login_form", clear_on_submit=False):
                    login_id = st.text_input("아이디", key="login_id", placeholder="아이디")
                    login_pw = st.text_input("비밀번호", type="password", key="login_pw", placeholder="비밀번호")
                    login_submitted = st.form_submit_button("로그인", use_container_width=True)
                if login_submitted:
                    if not login_id or not login_pw:
                        st.warning("아이디와 비밀번호를 모두 입력해주세요.")
                    elif authenticate_user(login_id.strip(), login_pw):
                        st.session_state.auth_user = login_id.strip()
                        st.query_params["session_token"] = make_session_token(login_id.strip())
                        st.rerun()
                    else:
                        st.error("아이디 또는 비밀번호가 올바르지 않습니다.")

                with st.expander("비밀번호를 잊으셨나요?"):
                    if st.session_state.get("reset_success_msg"):
                        st.success(st.session_state.pop("reset_success_msg"))
                    with st.form("reset_pw_form", clear_on_submit=False):
                        rs_id = st.text_input("아이디", key="reset_id", placeholder="아이디")
                        rs_email = st.text_input("가입 시 등록한 이메일", key="reset_email", placeholder="example@email.com")
                        rs_pw = st.text_input("새 비밀번호", type="password", key="reset_pw", placeholder="4자 이상")
                        rs_pw2 = st.text_input("새 비밀번호 확인", type="password", key="reset_pw2", placeholder="비밀번호 다시 입력")
                        reset_submitted = st.form_submit_button("비밀번호 재설정", use_container_width=True)

                    if reset_submitted:
                        rs_id_clean = (rs_id or "").strip()
                        rs_email_clean = (rs_email or "").strip()
                        reset_errors = []
                        if not rs_id_clean or not rs_email_clean:
                            reset_errors.append("아이디와 이메일을 모두 입력해주세요.")
                        elif not verify_user_email(rs_id_clean, rs_email_clean):
                            reset_errors.append("아이디와 이메일이 일치하는 계정을 찾을 수 없습니다.")
                        if not rs_pw or len(rs_pw) < 4:
                            reset_errors.append("새 비밀번호는 4자 이상 입력해주세요.")
                        elif rs_pw != rs_pw2:
                            reset_errors.append("새 비밀번호가 서로 일치하지 않습니다.")

                        if reset_errors:
                            for e in reset_errors:
                                st.error(e)
                        else:
                            update_user_password(rs_id_clean, rs_pw)
                            st.session_state["reset_success_msg"] = "비밀번호가 재설정되었습니다. 새 비밀번호로 로그인해주세요."
                            st.rerun()

            with tab_signup:
                with st.form("signup_form", clear_on_submit=False):
                    su_id = st.text_input("아이디", key="signup_id", placeholder="3자 이상")
                    su_email = st.text_input("이메일", key="signup_email", placeholder="example@email.com")
                    su_pw = st.text_input("비밀번호", type="password", key="signup_pw", placeholder="4자 이상")
                    su_pw2 = st.text_input("비밀번호 확인", type="password", key="signup_pw2", placeholder="비밀번호 다시 입력")
                    submitted = st.form_submit_button("회원가입", use_container_width=True)

                if submitted:
                    su_id_clean = (su_id or "").strip()
                    su_email_clean = (su_email or "").strip()
                    errors = []
                    if len(su_id_clean) < 3:
                        errors.append("아이디는 3자 이상 입력해주세요.")
                    elif username_exists(su_id_clean):
                        errors.append("이미 사용 중인 아이디입니다.")

                    if not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", su_email_clean):
                        errors.append("올바른 이메일 형식이 아닙니다.")
                    elif email_exists(su_email_clean):
                        errors.append("이미 가입된 이메일입니다.")

                    if not su_pw or len(su_pw) < 4:
                        errors.append("비밀번호는 4자 이상 입력해주세요.")
                    elif su_pw != su_pw2:
                        errors.append("비밀번호가 서로 일치하지 않습니다.")

                    if errors:
                        for e in errors:
                            st.error(e)
                    else:
                        save_user(su_id_clean, su_pw, su_email_clean)
                        st.session_state["_force_login_tab"] = True
                        st.session_state["signup_success_msg"] = f"회원가입이 완료되었습니다! 아이디 '{su_id_clean}'로 로그인해주세요."
                        st.rerun()

def render_change_password():
    st.header(
        "비밀번호 변경",
        help="현재 비밀번호를 확인한 뒤 새 비밀번호로 변경합니다."
    )
    st.markdown("<hr style='margin: 10px 0 25px 0; border-color: #E5E7EB;'>", unsafe_allow_html=True)

    _, mid, _ = st.columns([1, 1.5, 1])
    with mid:
        with st.form("change_pw_form", clear_on_submit=True):
            cur_pw = st.text_input("현재 비밀번호", type="password", key="change_pw_cur", placeholder="현재 비밀번호")
            new_pw = st.text_input("새 비밀번호", type="password", key="change_pw_new", placeholder="4자 이상")
            new_pw2 = st.text_input("새 비밀번호 확인", type="password", key="change_pw_new2", placeholder="새 비밀번호 다시 입력")
            submitted = st.form_submit_button("비밀번호 변경", use_container_width=True)

        if submitted:
            username = st.session_state.auth_user
            errors = []
            if not cur_pw:
                errors.append("현재 비밀번호를 입력해주세요.")
            elif not authenticate_user(username, cur_pw):
                errors.append("현재 비밀번호가 올바르지 않습니다.")

            if not new_pw or len(new_pw) < 4:
                errors.append("새 비밀번호는 4자 이상 입력해주세요.")
            elif new_pw != new_pw2:
                errors.append("새 비밀번호가 서로 일치하지 않습니다.")
            elif cur_pw and new_pw == cur_pw:
                errors.append("현재 비밀번호와 다른 비밀번호를 입력해주세요.")

            if errors:
                for e in errors:
                    st.error(e)
            else:
                update_user_password(username, new_pw)
                st.success("비밀번호가 변경되었습니다.")

def _show_debug_memory():
    """사이드바 하단에 현재 프로세스의 실시간 메모리 사용량을 표시한다.

    탭 이동을 반복할 때 이 숫자가 계속 우상향하기만 하고 안 떨어진다면
    메모리 누수(스레드/캐시 누적 등)를 의심할 수 있다.
    """
    try:
        process = psutil.Process(os.getpid())
        mem_mb = process.memory_info().rss / 1024 / 1024

        # psutil.virtual_memory()는 컨테이너 환경에서 호스트 전체 메모리를
        # 반환해버려서(예: 128GB) 실제 컨테이너 한도와 무관하게 부정확하다.
        # Streamlit Community Cloud의 실제 메모리 한도(약 2.7GB)를 기준으로 삼는다.
        total_mb = 2700

        if mem_mb >= total_mb * 0.85:
            color = "#DC2626"   # 위험
        elif mem_mb >= total_mb * 0.6:
            color = "#D97706"   # 주의
        else:
            color = "#64748B"   # 정상

        # ── [원인 진단용] 스레드/큐 상태 ─────────────────────────────────────
        # Streamlit Community Cloud는 앱 전체가 프로세스 하나를 여러 사용자가
        # 공유한다. get_shared_executor / get_orchestration_executor /
        # get_yf_safety_executor 도 @st.cache_resource라서 전 사용자가 같은
        # 스레드풀 하나씩을 공유한다. 즉 "나 혼자 여러 탭을 왔다갔다" 뿐 아니라
        # "다른 접속자가 동시에 쓰는 상황"도 이 숫자에 같이 반영된다.
        # active_threads가 시간이 지나도 안 줄고 계속 우상향하거나, 아래 큐
        # 대기(pending) 숫자가 0이 아닌 채로 한동안 유지된다면 → 어딘가에서
        # 스레드가 끝나지 않고 계속 쌓이는 중이라는 뜻이고, 그 풀에 새로 던져진
        # 작업은 일꾼이 없어 대기하다가(=timeout 설정이 없는 코드 경로라면
        # 영원히) "실행 중" 스피너만 도는 멈춤으로 보이게 된다.
        active_threads = threading.active_count()
        pool_info = []
        for _name, _getter in (
            ("공유", get_shared_executor),
            ("오케스트레이션", get_orchestration_executor),
            ("yf안전", get_yf_safety_executor),
        ):
            try:
                _ex = _getter()
                _queued = _ex._work_queue.qsize()
                _spawned = len(_ex._threads)
                pool_info.append(f"{_name} {_spawned}/{_ex._max_workers}(대기{_queued})")
            except Exception:
                pass

        thread_color = "#DC2626" if active_threads >= 150 else ("#D97706" if active_threads >= 80 else "#64748B")
        st.markdown(
            f'<div style="padding: 14px 14px 10px 14px; margin-top: 10px; '
            f'border-top: 1px solid rgba(255,255,255,0.08);">'
            f'<span style="font-size: 11px; color: {color}; font-weight: 600;">'
            f'🧠 메모리 {mem_mb:,.0f} MB / {total_mb:,.0f} MB'
            f'</span><br>'
            f'<span style="font-size: 11px; color: {thread_color}; font-weight: 600;">'
            f'🧵 전체 스레드 {active_threads}개'
            f'</span><br>'
            f'<span style="font-size: 10px; color: #94A3B8;">{" · ".join(pool_info)}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )
    except Exception:
        # 모니터링 실패는 앱 동작에 영향을 주면 안 되므로 조용히 무시한다.
        pass


def main():
    if 'auth_user' not in st.session_state:
        st.session_state.auth_user = None

    # 🔧 [디버깅 모드] 탭 이동 멈춤 현상이 로그인/세션 복원 쪽과 관련있는지
    # 확인하기 위해 로그인을 완전히 우회함(환경변수 설정 없이 강제 적용).
    # 원상복구하려면 아래 줄을 지우고 밑의 두 블록 주석을 해제하면 됨.
    st.session_state.auth_user = "debug_user"

    # # 🔁 F5 새로고침 대응: session_state는 새로고침 시 초기화되지만
    # # URL 쿼리파라미터는 유지되므로, 그 안의 서명 토큰으로 로그인 상태를 복원한다.
    # if not st.session_state.auth_user:
    #     _token = st.query_params.get("session_token")
    #     if _token:
    #         _restored_user = verify_session_token(_token)
    #         if _restored_user:
    #             st.session_state.auth_user = _restored_user

    # 🛠️ 개발용 로그인 우회: 터미널에서 DEV_SKIP_LOGIN=admin 으로 실행할 때만 적용됨.
    # (환경변수를 설정하지 않고 배포하면 다른 사용자는 평소처럼 로그인해야 함)
    dev_skip_user = os.environ.get("DEV_SKIP_LOGIN")
    if dev_skip_user and not st.session_state.auth_user:
        st.session_state.auth_user = dev_skip_user

    if not st.session_state.auth_user:
        render_login_page()
        return

    st.markdown("""
        <style>
            .stApp { background-color: #F8FAFC !important; }
            section[data-testid="stMain"] > div.block-container { 
                background-color: #FFFFFF !important; border-radius: 12px; 
                box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05); padding: 40px !important; 
                margin-top: 20px; margin-bottom: 20px; margin-left: auto !important; margin-right: auto !important;
                width: calc(100% - 48px) !important; max-width: 1600px !important; border: 1px solid #E5E7EB; 
            }
            
            [data-testid="stSidebar"] { background-color: #0F141F !important; border-right: none !important; }
            [data-testid="stSidebar"] [data-testid="stSidebarUserContent"] {
                padding-left: 0 !important; padding-right: 0 !important; padding-top: 30px !important; padding-bottom: 0 !important;
            }
            [data-testid="stSidebar"] * { color: #CBD5E1 !important; }
            .sidebar-logo-text { color: #FFFFFF !important; font-size: 22px !important; font-weight: 900 !important; padding: 0px 25px 30px 25px !important; letter-spacing: -0.5px !important; display: block;}
            
            section[data-testid="stMain"] h1, section[data-testid="stMain"] h2, section[data-testid="stMain"] p { color: #111827 !important; }
            .stTextInput input, .stNumberInput input, .stSelectbox > div > div { background-color: #FFFFFF !important; color: #111827 !important; border: 1px solid #D1D5DB !important; border-radius: 6px !important; }
            .stTextInput button[aria-label="Show password"],
            .stTextInput button[aria-label="Hide password"] { display: none !important; }
            
            button[data-testid="stNumberInputStepUp"], button[data-testid="stNumberInputStepDown"] { color: #5A4EE5 !important; background-color: #F8FAFC !important; }
            button[data-testid="stNumberInputStepUp"] svg, button[data-testid="stNumberInputStepDown"] svg { fill: #5A4EE5 !important; }
            button[data-testid="stNumberInputStepUp"]:hover, button[data-testid="stNumberInputStepDown"]:hover { background-color: #EEF2FF !important; }

            section[data-testid="stMain"] .stButton > button { background-color: #5A4EE5 !important; border: 1px solid #5A4EE5 !important; border-radius: 6px !important; padding: 8px 24px !important; }
            section[data-testid="stMain"] .stButton > button p, section[data-testid="stMain"] .stButton > button span, section[data-testid="stMain"] .stButton > button div { color: #FFFFFF !important; font-weight: 600 !important; }
            section[data-testid="stMain"] .stButton > button:hover { background-color: #4C41C3 !important; border-color: #4C41C3 !important; }
            
            .info-box-modern { background-color: #F0F9FF !important; border: 1px solid #E0F2FE !important; border-radius: 8px !important; padding: 20px 24px !important; margin-bottom: 25px !important; color: #374151 !important; font-size: 14px; line-height: 1.6; }
            
            .cond-chips { display: flex; flex-wrap: wrap; gap: 6px; margin: 8px 0 14px 0; }
            .cond-chip { display: inline-block; padding: 3px 10px; border-radius: 20px; font-size: 12px; font-weight: 600; color: #5A4EE5; background: #EEF2FF; border: 1px solid #C7D2FE; white-space: nowrap; }
            div[data-testid="stFileUploader"] section { background-color: #F9FAFB !important; border: 1px dashed #D1D5DB !important; color: #111827 !important;}
            div[data-testid="stFileUploader"] * { color: #111827 !important; }
            
            .header-container { display: flex; justify-content: flex-end; gap: 10px; margin-bottom: 30px; }
            .btn-template-white { padding: 8px 16px; border-radius: 6px; font-size: 13px; font-weight: 600; cursor: pointer; border: 1px solid #D1D5DB !important; background-color: #FFFFFF !important; color: #111827 !important; }
            .btn-template-blue { padding: 8px 16px; border-radius: 6px; font-size: 13px; font-weight: 600; cursor: pointer; border: none !important; background-color: #5A4EE5 !important; color: #FFFFFF !important; }
            
            div[data-testid="stExpander"] { border: 1px solid #E5E7EB !important; border-radius: 8px !important; background-color: #F9FAFB !important; }
            div[data-testid="stExpander"] summary p { color: #374151 !important; font-weight: 600 !important; }
            
            div[data-testid="stCheckbox"] label { color: #374151 !important; font-weight: 500 !important; }

            div[data-testid="stRadio"],
            div[data-testid="stRadio"] > div,
            .element-container:has(div[data-testid="stRadio"]),
            div[data-testid="stElementContainer"]:has(div[data-testid="stRadio"]) {
                width: 100% !important; max-width: none !important; margin: 0 !important; padding: 0 !important; left: 0 !important; position: relative !important;
            }
            div[data-testid="stRadio"] > label[data-testid="stWidgetLabel"] {
                display: none !important;
            }

            div[data-testid="stRadio"] > div[role="radiogroup"] {
                display: flex !important; flex-direction: row !important; flex-wrap: nowrap !important; gap: 10px !important; width: 100% !important; margin: 0 !important; margin-left: 0 !important; padding-left: 0 !important; background-color: transparent !important; border: none !important; padding: 0 !important; align-items: stretch !important; justify-content: flex-start !important;
            }

            div[data-testid="stRadio"] > div[role="radiogroup"] > * {
                margin: 0 !important; min-width: 0 !important; flex: 1 1 0% !important; 
            }
            div[data-testid="stRadio"] > div[role="radiogroup"] > *:first-child {
                margin-left: 0 !important;
            }

            div[data-testid="stRadio"] label[data-baseweb="radio"] {
                background-color: rgba(248, 250, 252, 0.7) !important; backdrop-filter: blur(4px) !important; border: none !important; outline: 2px solid #CBD5E1 !important; outline-offset: -2px !important; border-radius: 8px !important; padding: 10px 5px !important; margin: 0 !important; cursor: pointer !important; transition: background-color 0.2s ease-in-out, outline-color 0.2s ease-in-out, box-shadow 0.2s ease-in-out !important; display: flex !important; flex-direction: row !important; justify-content: center !important; align-items: center !important; width: 100% !important; min-height: 44px !important; box-sizing: border-box !important; box-shadow: none !important;
            }

            div[data-testid="stRadio"] label[data-baseweb="radio"]:hover {
                background-color: rgba(226, 232, 240, 0.8) !important;
            }

            div[data-testid="stRadio"] label[data-baseweb="radio"]:has(input[type="radio"]:checked) {
                background-color: #EEF2FF !important; outline-color: #6366F1 !important; box-shadow: 0 4px 10px rgba(99, 102, 241, 0.15) !important;
            }

            div[data-testid="stRadio"] label[data-baseweb="radio"] > div:first-child {
                display: none !important; width: 0 !important; margin: 0 !important;
            }

            div[data-testid="stRadio"] label[data-baseweb="radio"] > div:last-child {
                flex: 1 1 auto !important; width: 100% !important; text-align: center !important; display: flex !important; justify-content: center !important; align-items: center !important;
            }

            div[data-testid="stRadio"] label[data-baseweb="radio"] p {
                color: #475569 !important; font-size: 14.5px !important; font-weight: 600 !important; margin: 0 !important; line-height: 1.3 !important; white-space: nowrap !important; overflow: hidden !important; text-overflow: ellipsis !important; text-align: center !important;
            }

            div[data-testid="stRadio"] label[data-baseweb="radio"]:has(input[type="radio"]:checked) p {
                color: #4338CA !important; font-weight: 800 !important;
            }

            .st-key-market_filter_box div[data-testid="stRadio"] > div[role="radiogroup"] {
                justify-content: flex-start !important; max-width: 320px !important; 
            }
            .st-key-market_filter_box div[data-testid="stRadio"] > div[role="radiogroup"] > * {
                flex: 1 1 0% !important; min-width: 0 !important; max-width: none !important;
            }
            .st-key-market_filter_box div[data-testid="stRadio"] label[data-baseweb="radio"] {
                min-height: 40px !important; padding: 8px 10px !important;
            }

            .dash-section-title { font-size: 16px; font-weight: 700; color: #0F172A; margin: 18px 0 14px 0; letter-spacing: -0.3px; }
            .section-divider { border: none; border-top: 1px solid #E5E7EB; margin: 28px 0; }

            .index-card {
                background: #FFFFFF; border: 1px solid #E2E8F0; border-radius: 12px; padding: 20px 22px 20px 22px; min-height: 110px; box-shadow: 0 1px 4px rgba(0,0,0,0.04); transition: box-shadow 0.22s ease, border-color 0.22s ease, padding-bottom 0.25s ease; position: relative; overflow: visible; cursor: default;
            }
            .index-card:hover { box-shadow: 0 6px 20px rgba(0,0,0,0.10); border-color: #C7D2FE; }
            .index-card-title { font-size: 13px; font-weight: 700; color: #1E293B; margin-bottom: 6px; letter-spacing: -0.2px; }
            .index-card-value { font-size: 26px; font-weight: 800; color: #0F172A; margin-bottom: 6px; letter-spacing: -0.5px; }
            .index-card-up   { font-size: 13px; font-weight: 600; color: #DC2626; margin-bottom: 6px; }
            .index-card-down { font-size: 13px; font-weight: 600; color: #2563EB; margin-bottom: 6px; }
            .index-card-neutral { font-size: 13px; font-weight: 600; color: #64748B; margin-bottom: 6px; }
            .index-card-sub  { font-size: 12px; color: #94A3B8; }
            .index-card-chart {
                max-height: 0; overflow: hidden; opacity: 0; transition: max-height 0.32s cubic-bezier(0.4,0,0.2,1), opacity 0.25s ease, margin-top 0.25s ease; margin-top: 0;
            }
            .index-card:hover .index-card-chart {
                max-height: 80px; opacity: 1; margin-top: 12px;
            }
            .index-card-chart-label { font-size: 10px; color: #94A3B8; font-weight: 500; margin-bottom: 3px; letter-spacing: 0.2px; }

            /* Streamlit 컬럼 컨테이너의 overflow 클리핑 해제 (스파크라인 잘림 방지) */
            [data-testid="column"] { overflow: visible !important; }
            [data-testid="stVerticalBlock"] { overflow: visible !important; }
            [data-testid="stHorizontalBlock"] { overflow: visible !important; }

            /* 로그아웃 버튼 영역: 샘플(이미지2)처럼 작은 아웃라인 스타일 알약형, 한 줄 배치 */
            .st-key-user_header_row { display: flex !important; flex-direction: row !important; align-items: center !important; justify-content: flex-end !important; gap: 8px !important; flex-wrap: nowrap !important; }
            .st-key-user_header_row > div { flex: 0 0 auto !important; width: auto !important; min-width: 0 !important; }
            .st-key-user_header_row .stMarkdown { width: auto !important; }
            .user-name-pill {
                display: inline-block; font-size: 12.5px; color: #374151; white-space: nowrap;
                padding: 5px 2px; font-weight: 500; line-height: 1;
                transform: translateY(-6px);
            }
            .st-key-logout_btn { width: auto !important; margin-top: 0 !important; }
            .st-key-logout_btn .stButton { display: inline-block !important; width: auto !important; }
            section[data-testid="stMain"] .st-key-logout_btn .stButton > button {
                width: auto !important; min-width: unset !important; padding: 5px 14px !important;
                white-space: nowrap !important; flex-shrink: 0 !important;
                background-color: #FFFFFF !important; border: 1px solid #E2E8F0 !important;
                border-radius: 6px !important; font-size: 12.5px !important;
            }
            section[data-testid="stMain"] .st-key-logout_btn .stButton > button p {
                white-space: nowrap !important; color: #374151 !important; font-weight: 500 !important;
            }
            section[data-testid="stMain"] .st-key-logout_btn .stButton > button:hover {
                background-color: #F8FAFC !important; border-color: #CBD5E1 !important;
            }
        </style>
    """, unsafe_allow_html=True)

    col_sp, col_user = st.columns([7.5, 2.5])
    with col_user:
        with st.container(key="user_header_row"):
            st.markdown(
                f"<div class='user-name-pill'>{html_lib.escape(st.session_state.auth_user)}님</div>",
                unsafe_allow_html=True
            )
            if st.button("로그아웃", key="logout_btn"):
                st.session_state.auth_user = None
                if "session_token" in st.query_params:
                    del st.query_params["session_token"]
                st.rerun()

    # 🔧 [디버깅 모드] 탭 이동 시 멈춤 현상의 원인을 좁혀보기 위해
    # 관심종목 탭만 제거하고 나머지는 그대로 둠(대시보드 포함).
    # 원상복구하려면 아래 주석을 해제하면 됨.
    SIDEBAR_GROUPS = [
        ("OVERVIEW", [
            ("대시보드 홈", ":material/space_dashboard:"),
        ]),
        ("STOCK DISCOVERY", [
            ("추천 종목", ":material/target:"),
            ("종목 스크리너", ":material/tune:"),
        ]),
        ("DEEP ANALYSIS", [
            ("기업 재무 분석", ":material/bar_chart:"),
            ("실시간 배당 순위", ":material/payments:"),
        ]),
        ("MY PAGE", [
            # ("관심종목", ":material/bookmark:"),
            ("비밀번호 변경", ":material/lock:"),
        ]),
    ]

    if "current_page" not in st.session_state:
        st.session_state.current_page = "대시보드 홈"

    with st.sidebar:
        st.markdown('<span class="sidebar-logo-text">Inventory Manager</span>', unsafe_allow_html=True)

        st.markdown("""
            <style>
            section[data-testid="stSidebar"] div[data-testid="stVerticalBlock"],
            section[data-testid="stSidebar"] div[data-testid="stVerticalBlockBorderWrapper"] {
                gap: 0 !important;
            }
            section[data-testid="stSidebar"] div[data-testid="stElementContainer"] {
                margin: 0 !important;
                padding: 0 !important;
            }
            section[data-testid="stSidebar"] div[data-testid="stMarkdownContainer"] p {
                margin: 0 !important;
            }
            section[data-testid="stSidebar"] div[data-testid="stButton"] {
                padding: 0 !important; margin: 0 0 6px 0 !important;
            }
            section[data-testid="stSidebar"] div[data-testid="stButton"] button {
                width: 100%; justify-content: flex-start;
                text-align: left;
                border: none;
                background-color: transparent;
                color: #CBD5E1;
                font-weight: 400;
                font-size: 15px;
                padding: 12px 14px 12px 28px;
                border-radius: 0; box-shadow: none;
                transition: background-color 0.15s ease, box-shadow 0.15s ease;
            }
            section[data-testid="stSidebar"] div[data-testid="stButton"] button > div {
                justify-content: flex-start !important; }
            section[data-testid="stSidebar"] div[data-testid="stButton"] button p,
            section[data-testid="stSidebar"] div[data-testid="stButton"] button span {
                color: inherit !important; font-weight: inherit !important;
            }
            section[data-testid="stSidebar"] div[data-testid="stButton"] button:hover {
                background-color: rgba(255, 255, 255, 0.06); color: #FFFFFF;
                border: none;
            }
            section[data-testid="stSidebar"] div[data-testid="stButton"] button:focus,
            section[data-testid="stSidebar"] div[data-testid="stButton"] button:focus:not(:hover),
            section[data-testid="stSidebar"] div[data-testid="stButton"] button:active {
                background-color: transparent !important; box-shadow: none !important;
                outline: none !important;
            }
            section[data-testid="stSidebar"] div[data-testid="stButton"] button:hover:focus {
                background-color: rgba(255, 255, 255, 0.06) !important; }
            section[data-testid="stSidebar"] div[data-testid="stButton"] button[kind="primary"] {
                background-color: #4F46E5; color: #FFFFFF !important;
                font-weight: 600;
                border-radius: 0; box-shadow: none;
                padding: 12px 14px 12px 28px; }
            section[data-testid="stSidebar"] div[data-testid="stButton"] button[kind="primary"] p,
            section[data-testid="stSidebar"] div[data-testid="stButton"] button[kind="primary"] span {
                color: #FFFFFF !important; font-weight: 600 !important;
            }
            section[data-testid="stSidebar"] div[data-testid="stButton"] button[kind="primary"]:hover,
            section[data-testid="stSidebar"] div[data-testid="stButton"] button[kind="primary"]:focus,
            section[data-testid="stSidebar"] div[data-testid="stButton"] button[kind="primary"]:active {
                background-color: #4338CA !important; color: #FFFFFF !important;
                box-shadow: none !important; }
            </style>
        """, unsafe_allow_html=True)

        for gi, (group_title, group_items) in enumerate(SIDEBAR_GROUPS):
            top_pad = "10px" if gi == 0 else "26px"
            st.markdown(
                f'<div style="color: #64748B; font-size: 11px; font-weight: 700; '
                f'letter-spacing: 0.5px; padding: {top_pad} 14px 18px 14px;">{group_title}</div>',
                unsafe_allow_html=True
            )
            for label, icon in group_items:
                is_selected = st.session_state.current_page == label
                if st.button(
                    label,
                    icon=icon,
                    key=f"nav_{label}",
                    type="primary" if is_selected else "secondary",
                    use_container_width=True,
                ):
                    if st.session_state.current_page != label:
                        st.session_state.current_page = label
                        st.rerun()

        # 🔧 [디버깅 모드] 데이터 조회가 전혀 없는 탭끼리 왔다갔다 해도 멈추는 현상이
        # 확인되어, 모든 페이지에서 공통으로 도는 코드부터 하나씩 제거하며 테스트 중.
        # _show_debug_memory()

    selected = st.session_state.current_page

    # ── [원인 진단용 임시 로그] ────────────────────────────────────────
    # 멈춤 현상이 완전히 해결됐다고 확신이 들 때까지만 남겨둔다. 로그의 마지막
    # "진입"만 있고 "완료"가 없는 페이지가 바로 멈춘 지점이다.
    print(f"[DEBUG {datetime.datetime.now().strftime('%H:%M:%S')}] 페이지 진입: {selected}", file=sys.stderr, flush=True)

    if   selected == "대시보드 홈":      render_dashboard()
    elif selected == "추천 종목":        render_recommendations()
    elif selected == "종목 스크리너":    render_screener()
    elif selected == "기업 재무 분석":   render_fnguide()
    elif selected == "실시간 배당 순위": render_dividend()
    # elif selected == "관심종목":         render_watchlist()
    elif selected == "비밀번호 변경":     render_change_password()

    print(f"[DEBUG {datetime.datetime.now().strftime('%H:%M:%S')}] 페이지 렌더링 완료: {selected}", file=sys.stderr, flush=True)


def render_rate_strip():
    """기준금리 현황을 한 줄짜리 컴팩트 형태로 우측 정렬 표시."""
    fed = fetch_fed_rate_data()
    bok = fetch_bok_rate_data()

    def status_word(history):
        if not history:
            return "-"
        return history[0]["action"].split(" ")[0]

    def tooltip_text(history, value_key):
        if not history:
            return "최근 10건 변동 이력을 가져오지 못했습니다."
        lines = [f"{h['date']}  {h[value_key]}  ({h['action']})" for h in history[:10]]
        return "최근 10건 변동 이력\n" + "\n".join(lines)

    fed_val = fed["current"]["range"] if fed else "-"
    fed_status = status_word(fed["history"]) if fed else "-"
    fed_tip = html_lib.escape(tooltip_text(fed["history"], "range") if fed else "데이터를 불러오지 못했습니다.")
    fed_next = next_meeting_label(FOMC_MEETING_DATES)
    fed_meta = f"{fed_status}, 다음 회의 {fed_next}" if fed_next else fed_status

    bok_val = bok["current"]["rate"] if bok else "-"
    bok_status = status_word(bok["history"]) if bok else "-"
    bok_tip = html_lib.escape(tooltip_text(bok["history"], "range") if bok else "데이터를 불러오지 못했습니다.")
    bok_next = next_meeting_label(BOK_MEETING_DATES)
    bok_meta = f"{bok_status}, 다음 회의 {bok_next}" if bok_next else bok_status

    strip_html = (
        '<div style="display:flex; justify-content:flex-end; align-items:center; gap:20px; height:38px;">'
        f'<span title="{fed_tip}" style="font-size:13px; color:#374151; cursor:help; white-space:nowrap; border-bottom:1px dashed #CBD5E1;">'
        f'🇺🇸 미국(FOMC) <b style="color:#0F172A;">{fed_val}</b> <span style="color:#64748B;">({fed_meta})</span></span>'
        f'<span title="{bok_tip}" style="font-size:13px; color:#374151; cursor:help; white-space:nowrap; border-bottom:1px dashed #CBD5E1;">'
        f'🇰🇷 한국(한국은행) <b style="color:#0F172A;">{bok_val}</b> <span style="color:#64748B;">({bok_meta})</span></span>'
        '</div>'
    )
    st.markdown(strip_html, unsafe_allow_html=True)

def render_dashboard():
    now_str = datetime.datetime.now().strftime("%Y.%m.%d %H:%M")

    col_title, col_time = st.columns([3, 1])
    with col_title:
        st.header("대시보드 홈")
    with col_time:
        st.markdown(
            f"<div style='text-align:right; color:#64748B; font-size:13px; padding-top:18px;'>"
            f"🟢 실시간 &nbsp;·&nbsp; {now_str}</div>",
            unsafe_allow_html=True
        )
    st.markdown("<hr style='margin: 10px 0 25px 0; border-color: #E5E7EB;'>", unsafe_allow_html=True)

    col_refresh, col_scan, col_rate_strip = st.columns([1.5, 1.5, 4.0])
    with col_refresh:
        if st.button("데이터 새로고침", use_container_width=True):
            fetch_market_index_table.clear()
            fetch_investor_trend.clear()
            fetch_investor_trend_monthly.clear()
            fetch_sparkline_data.clear()
            fetch_sector_ranking.clear()
            fetch_fed_rate_data.clear()
            fetch_bok_rate_data.clear()
            st.rerun()
    with col_scan:
        if st.button("종목 스캔 (스크리너+추천)", use_container_width=True, key="dash_unified_scan_btn"):
            run_unified_market_scan()
            st.rerun()
    with col_rate_strip:
        render_rate_strip()

    if load_screener_df().empty:
        st.markdown(
            "<div style='font-size:12.5px; color:#B45309; margin: -6px 0 10px 0;'>"
            "⚠️ 아직 스캔된 시장 데이터가 없습니다. 위 [종목 스캔] 버튼을 눌러 스크리너·추천 종목 데이터를 한 번에 받아오세요. (약 15~20초 소요)"
            "</div>",
            unsafe_allow_html=True,
        )

    st.markdown("<div class='dash-section-title'>📈 시장 지수</div>", unsafe_allow_html=True)

    # ── 대시보드에 필요한 4가지 데이터(시장지수/스파크라인/수급동향/섹터순위)를 하나씩
    # 순서대로 부르는 대신 한꺼번에 병렬로 미리 가져온다.
    # [탭 이동 멈춤 대응] 예전에는 as_completed(timeout=15)로 메인 스크립트가 최대
    # 15초를 한 번에 몰아서 기다렸다 — 그동안은 사이드바 탭 클릭 같은 새 상호작용을
    # Streamlit이 전혀 받아줄 수 없어서 멈춘 것처럼 보였다. 지금은 render_async_multi로
    # 0.4초 간격 폴링만 하고 그 사이사이에 새 클릭을 정상적으로 받아준다.
    def _submit_dash_jobs():
        _dash_executor = get_orchestration_executor()
        return {
            "indices":    _dash_executor.submit(fetch_market_index_table),
            "sparklines": _dash_executor.submit(fetch_sparkline_data),
            "trend":      _dash_executor.submit(fetch_investor_trend),
            "df_sector":  _dash_executor.submit(fetch_sector_ranking),
        }

    def _collect_dash_results(futures):
        out = {"indices": {}, "sparklines": {}, "trend": {}, "df_sector": pd.DataFrame()}
        for key, f in futures.items():
            if f.done():
                try:
                    val = f.result(timeout=0.1)
                    if val is not None:
                        out[key] = val
                except Exception:
                    pass
        return out

    _dash_results, _dash_ready = render_async_multi(
        job_key="dashboard_main_data",
        submit_fn=_submit_dash_jobs,
        collect_fn=_collect_dash_results,
        default_result={"indices": {}, "sparklines": {}, "trend": {}, "df_sector": pd.DataFrame()},
        spinner_text="대시보드 데이터를 불러오는 중...",
        overall_timeout=15,
    )
    if not _dash_ready:
        return  # 아직 로딩 중 — 이후 렌더링은 건너뛰고, 폴링 프래그먼트가 알아서 이어간다

    indices = _dash_results["indices"] or {}
    sparklines = _dash_results["sparklines"] or {}
    trend = _dash_results["trend"] or {}
    df_sector = _dash_results["df_sector"] if _dash_results["df_sector"] is not None else pd.DataFrame()

    def index_color_class(status):
        if status == "up":   return "index-card-up"
        if status == "down": return "index-card-down"
        return "index-card-neutral"
    def index_arrow(status):
        if status == "up":   return "▲"
        if status == "down": return "▼"
        return "–"

    def render_index_card(col, key, idx, show_volume=True):
        label     = idx.get("name", "-")
        subtitle  = idx.get("subtitle", "")
        status    = idx.get("status", "neutral")
        arrow     = index_arrow(status)
        chg       = idx.get("change", "-")
        chgpct    = idx.get("change_pct", "")
        vol       = idx.get("volume", "-")
        closes    = sparklines.get(key, [])
        svg       = make_sparkline_svg(closes, status, width=240, height=56)

        chg_color = "#DC2626" if status == "up" else ("#2563EB" if status == "down" else "#64748B")
        chg_bg    = "#FEF2F2" if status == "up" else ("#EFF6FF" if status == "down" else "#F8FAFC")

        vol_html = (
            f'<div style="font-size:11px;color:#94A3B8;margin-top:3px;">거래량 {vol}</div>'
            if show_volume else ""
        )

        chart_section = (
            f'<div style="margin-top:10px;border-top:1px solid #F1F5F9;padding-top:6px;">'
            f'<div style="text-align:right;font-size:10px;color:#CBD5E1;font-weight:500;margin-bottom:3px;letter-spacing:0.2px;">180일 추이</div>'
            f'{svg}'
            f'</div>'
        ) if svg else ""

        html = f"""
        <style>
        .icard-{key} {{
            background:#FFFFFF; border:1px solid #E2E8F0; border-radius:12px;
            padding:16px 18px 14px 18px; box-shadow:0 1px 4px rgba(0,0,0,0.04);
            transition:box-shadow .22s ease, border-color .22s ease;
            font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
        }}
        .icard-{key}:hover {{ box-shadow:0 6px 20px rgba(0,0,0,0.10); border-color:#C7D2FE; }}
        </style>
        <div class="icard-{key}">
            <div style="display:flex;align-items:baseline;gap:5px;margin-bottom:8px;">
                <span style="font-size:13px;font-weight:700;color:#1E293B;">{label}</span>
                <span style="font-size:11px;color:#94A3B8;">{subtitle}</span>
            </div>
            <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:5px;">
                <span style="font-size:22px;font-weight:800;color:#0F172A;letter-spacing:-0.5px;">{idx.get('value', '-')}</span>
                <span style="font-size:12px;font-weight:600;color:{chg_color};">{chg}</span>
                <span style="display:inline-flex;align-items:center;gap:3px;
                             background:{chg_bg};border-radius:6px;
                             padding:3px 7px;font-size:12px;font-weight:700;color:{chg_color};">
                    {arrow} {chgpct}
                </span>
            </div>
            {vol_html}
            {chart_section}
        </div>
        """
        with col:
            import streamlit.components.v1 as components
            components.html(html, height=230 if show_volume else 210, scrolling=False)

    c1, c2, c3 = st.columns(3)
    for col, key in [(c1, "kospi"), (c2, "kosdaq"), (c3, "nasdaq")]:
        render_index_card(col, key, indices.get(key, {}), show_volume=True)

    st.markdown("<br>", unsafe_allow_html=True)

    c4, c5, c6 = st.columns(3)
    for col, key in [(c4, "usdkrw"), (c5, "gold"), (c6, "wti")]:
        render_index_card(col, key, indices.get(key, {}), show_volume=False)

    st.markdown("<hr class='section-divider'>", unsafe_allow_html=True)
    st.markdown(
        "<div class='dash-section-title'>💰 투자자별 수급 동향 "
        "<span style='font-size:11px; color:#94A3B8; font-weight:500;'>(최근 거래일 순매수, 억원)</span></div>",
        unsafe_allow_html=True
    )
    # trend는 위에서 이미 병렬로 미리 가져왔음 (여기서 다시 부르지 않음)

    def investor_value_html(val):
        if val is None:
            return "#94A3B8", "데이터 없음"
        status = "up" if val > 0 else ("down" if val < 0 else "neutral")
        color = "#DC2626" if status == "up" else ("#2563EB" if status == "down" else "#64748B")
        arrow = "▲" if status == "up" else ("▼" if status == "down" else "–")
        sign = "+" if val > 0 else ""
        return color, f"{arrow} {sign}{val:,.0f}억"

    if "investor_open" not in st.session_state:
        st.session_state["investor_open"] = {"kospi": False, "kosdaq": False}

    def render_investor_row(market_label, market_key, sosok, data):
        items = [("foreign", "외국인"), ("institution", "기관"), ("individual", "개인")]
        is_open = st.session_state["investor_open"].get(market_key, False)
        arrow_icon = "▲" if is_open else "▼"

        cells_html = ""
        for i, (key, name) in enumerate(items):
            color, text = investor_value_html((data or {}).get(key))
            border = "border-left:1px solid #E5E7EB;" if i > 0 else ""
            cells_html += (
                f'<div style="flex:1;text-align:center;padding:0 6px;{border}">'
                f'<span style="font-size:12.5px;color:#64748B;font-weight:600;">{name}</span>'
                f'<span style="font-size:14px;font-weight:800;color:{color};margin-left:6px;">{text}</span>'
                f'</div>'
            )

        border_bottom = "border-radius:8px 8px 0 0;" if is_open else "border-radius:8px;"
        row_html = (
            f'<div class="inv-row-{market_key}" style="display:flex;align-items:center;background:#FFFFFF;border:1px solid #E2E8F0;'
            f'{border_bottom}padding:10px 16px;margin-bottom:0;min-height:44px;box-sizing:border-box; transition:all 0.2s ease;">'
            f'<div style="font-size:13px;font-weight:800;color:#0F172A;min-width:50px;">{market_label}</div>'
            f'<div style="display:flex;flex:1;align-items:center;">{cells_html}</div>'
            f'<div style="font-size:11px;color:#94A3B8;margin-left:8px;">{arrow_icon}</div>'
            f'</div>'
        )

        st.markdown(row_html, unsafe_allow_html=True)
        
        if st.button(" ", key=f"inv_btn_{market_key}", use_container_width=True, help=f"{market_label} 수급 추이 토글하기"):
            st.session_state["investor_open"][market_key] = not is_open
            st.rerun()

        br_css = "8px 8px 0 0" if is_open else "8px"
        
        st.markdown(f"""
        <style>
        div.st-key-inv_btn_{market_key} {{
            margin-top: -44px !important;
            position: relative;
            z-index: 10;
        }}
        div.st-key-inv_btn_{market_key} button {{
            height: 44px !important; width: 100% !important; background: transparent !important; border: none !important; box-shadow: none !important;
            border-radius: {br_css} !important; cursor: pointer !important; padding: 0 !important; transition: none !important;
        }}
        div.st-key-inv_btn_{market_key} button:hover,
        div.st-key-inv_btn_{market_key} button:active,
        div.st-key-inv_btn_{market_key} button:focus {{
            background: transparent !important; box-shadow: none !important; outline: none !important; border: none !important;
        }}
        div.st-key-inv_btn_{market_key} button * {{
            display: none !important; color: transparent !important;
        }}
        </style>
        """, unsafe_allow_html=True)

        if is_open:
            monthly = run_with_progress(f"{market_label} 월별 수급 불러오는 중...", fetch_investor_trend_monthly, sosok)
            if not monthly:
                st.markdown(
                    '<div style="border:1px solid #E2E8F0;border-top:none;border-radius:0 0 8px 8px;'
                    'padding:12px 16px;font-size:12px;color:#94A3B8;">데이터를 불러올 수 없습니다.</div>',
                    unsafe_allow_html=True
                )
            else:
                monthly_desc = list(reversed(monthly))
                investor_colors = {"외국인": "#DC2626", "기관": "#2563EB", "개인": "#16A34A"}
                max_abs = max(abs(r[k]) for r in monthly_desc for k in ["외국인", "기관", "개인"]) or 1

                header_html = (
                    '<div style="border:1px solid #E2E8F0;border-top:none;padding:10px 12px 6px 12px;">'
                    '<div style="display:grid;grid-template-columns:48px repeat(3,1fr);gap:4px;'
                    'font-size:11px;font-weight:700;padding-bottom:6px;border-bottom:1px solid #F1F5F9;">'
                    '<div style="color:#94A3B8;">날짜</div>'
                    '<div style="text-align:center;color:#DC2626;">외국인</div>'
                    '<div style="text-align:center;color:#2563EB;">기관</div>'
                    '<div style="text-align:center;color:#16A34A;">개인</div>'
                    '</div></div>'
                )
                st.markdown(header_html, unsafe_allow_html=True)

                for row_d in monthly_desc:
                    date_lbl = row_d["날짜"]
                    cols_m = st.columns([1, 3, 3, 3])
                    with cols_m[0]:
                        st.markdown(
                            f'<div style="font-size:11px;font-weight:600;color:#475569;padding-top:5px;">{date_lbl}</div>',
                            unsafe_allow_html=True
                        )
                    for ci, inv_key in enumerate(["외국인", "기관", "개인"]):
                        val = row_d[inv_key]
                        color = investor_colors[inv_key]
                        bar_w = int(abs(val) / max_abs * 100)
                        sign = "+" if val > 0 else ""
                        val_str = f"{sign}{val:,.0f}억"
                        bar_color = color if val >= 0 else color + "88"
                        with cols_m[ci + 1]:
                            st.markdown(f"""
                            <div style="padding:3px 0;">
                                <div style="font-size:11px;font-weight:700;color:{color};text-align:center;margin-bottom:2px;">{val_str}</div>
                                <div style="background:#F1F5F9;border-radius:3px;height:6px;">
                                    <div style="width:{bar_w}%;background:{bar_color};border-radius:3px;height:6px;"></div>
                                </div>
                            </div>
                            """, unsafe_allow_html=True)

                st.markdown(
                    '<div style="border:1px solid #E2E8F0;border-top:none;border-radius:0 0 8px 8px;height:8px;"></div>',
                    unsafe_allow_html=True
                )

        st.markdown("<div style='margin-bottom:8px;'></div>", unsafe_allow_html=True)

    render_investor_row("코스피", "kospi", "01", trend.get("kospi"))
    render_investor_row("코스닥", "kosdaq", "02", trend.get("kosdaq"))

    if all(v is None for v in (trend.get("kospi") or {}).values()) and all(v is None for v in (trend.get("kosdaq") or {}).values()):
        st.caption("⚠️ 네이버 금융 수급 데이터를 일시적으로 불러오지 못했습니다. 새로고침을 눌러 다시 시도해보세요.")

    st.markdown("<hr class='section-divider'>", unsafe_allow_html=True)
    st.markdown("<div class='dash-section-title'>🔥 오늘의 핫 섹터 TOP 10</div>", unsafe_allow_html=True)
    # df_sector도 위에서 이미 병렬로 미리 가져왔음 (여기서 다시 부르지 않음)

    if not df_sector.empty:
        max_abs = df_sector["등락률_num"].abs().max() or 1
        col_s1, col_s2 = st.columns(2, gap="large")
        for i, row in df_sector.iterrows():
            target_col = col_s1 if i < 5 else col_s2
            pct   = row["등락률_num"]
            name  = row["업종명"]
            bar_w = int(abs(pct) / max_abs * 100)
            bar_color = "#DC2626" if pct >= 0 else "#16A34A"
            sign  = "+" if pct >= 0 else ""
            pct_disp = f"{sign}{pct:.1f}%" 
            with target_col:
                st.markdown(f"""
                <div style="display:flex; align-items:center; gap:10px; margin-bottom:12px; padding:10px; background:#FFFFFF; border:1px solid #E2E8F0; border-radius:8px;">
                    <span style="font-size:14px; color:#1E293B; min-width:120px; font-weight:600;">{name}</span>
                    <div style="flex:1; background:#F1F5F9; border-radius:4px; height:10px;">
                        <div style="width:{bar_w}%; background:{bar_color}; border-radius:4px; height:10px;"></div>
                    </div>
                    <span style="font-size:14px; font-weight:700; color:{bar_color}; min-width:60px; text-align:right;">{pct_disp}</span>
                </div>
                """, unsafe_allow_html=True)
    else:
        st.info("업종 데이터를 불러올 수 없습니다.")

    st.markdown("""
        <div style='background:#F0F9FF; border:1px solid #BAE6FD; border-radius:8px; padding:14px 20px; font-size:13px; color:#374151; line-height:1.7; margin-top:20px;'>
            💡 <b>데이터 안내</b> &nbsp;|&nbsp;
            시장 지수, 수급 동향 및 업종 테마는 <b>네이버 금융</b> 실시간 데이터를 기반으로 합니다. 시장 개장 시간(09:00~15:30) 외에는 전일 종가/마감 기준으로 표시될 수 있습니다.
        </div>
    """, unsafe_allow_html=True)

# =========================
# 🤖 AI 종목 진단 엔진
# =========================
def calc_ai_scores(per, pbr, roe, debt, drop_pct, div):
    if debt < 0:           health = 50   
    elif debt == 0:        health = 100  
    elif debt <= 30:       health = 100
    elif debt <= 60:       health = 85
    elif debt <= 100:      health = 68
    elif debt <= 150:      health = 50
    elif debt <= 200:      health = 33
    elif debt <= 300:      health = 18
    else:                  health = 5

    if pbr > 0:  
        if pbr <= 0.4:   health = min(100, health + 8)
        elif pbr <= 0.8: health = min(100, health + 4)
        elif pbr >= 2.5: health = max(0,   health - 8)
        elif pbr >= 1.5: health = max(0,   health - 4)

    if roe == -999:  growth = 16   
    elif roe >= 25:  growth = 100
    elif roe >= 20:  growth = 85
    elif roe >= 15:  growth = 65
    elif roe >= 10:  growth = 42
    elif roe >= 5:   growth = 22
    elif roe >= 0:   growth = 10
    else:            growth = 3   

    if drop_pct == 0.0:        pass           
    elif drop_pct <= -40:      growth = min(100, growth + 18)
    elif drop_pct <= -30:      growth = min(100, growth + 12)
    elif drop_pct <= -20:      growth = min(100, growth + 6)
    elif drop_pct <= -10:      growth = min(100, growth + 2)
    elif drop_pct < 0:         growth = min(100, growth + 1)  
    else:                      growth = max(0,   growth - 8)  

    if per == 0.0:    profit = 30   
    elif per < 0:     profit = 10   
    elif per <= 4:    profit = 100
    elif per <= 6:    profit = 88
    elif per <= 8:    profit = 73
    elif per <= 10:   profit = 58
    elif per <= 13:   profit = 43
    elif per <= 18:   profit = 28
    elif per <= 25:   profit = 15
    else:             profit = 5

    if roe != -999:
        if roe >= 20:   profit = min(100, profit + 10)
        elif roe >= 15: profit = min(100, profit + 6)
        elif roe >= 10: profit = min(100, profit + 2)
        elif roe < 5:   profit = max(0,   profit - 10)

    if div >= 8.0:        dividend = 100  
    elif div >= 5.0:      dividend = 70   
    elif div >= 3.5:      dividend = 50   
    elif div >= 2.5:      dividend = 35   
    elif div >= 1.5:      dividend = 18   
    elif div >= 0.5:      dividend = 7    
    else:                 dividend = 0

    if div == 0.0:
        total = int(health * 0.37 + profit * 0.37 + growth * 0.26)
    else:
        total = int(health * 0.30 + profit * 0.30 + growth * 0.25 + dividend * 0.15)

    total = max(0, min(100, total))

    return {
        "total":    total,
        "health":   int(health),
        "growth":   int(growth),
        "profit":   int(profit),
        "dividend": int(dividend),
    }

def get_ai_total_score(code, screener_df=None):
    """관심종목 카드 등에서 재사용: 종목코드만으로 AI 종합점수(0~100)를 계산. 실패 시 None.
    screener_df를 미리 전달하면(예: 관심종목 병렬조회) 백그라운드 스레드에서 다시
    load_screener_df()를 호출하지 않아도 되어 session_state 접근을 피할 수 있다."""
    try:
        df_annual_ai, _, _ = fetch_fnguide_data(code)
        per_ai, pbr_ai, roe_ai, debt_ai, drop_pct_ai, div_ai = get_ai_diagnosis_inputs(code, df_annual_ai, screener_df=screener_df)
        return calc_ai_scores(per_ai, pbr_ai, roe_ai, debt_ai, drop_pct_ai, div_ai)["total"]
    except Exception:
        return None

def _ai_score_color(total):
    if total >= 85:   return "#7C3AED"
    elif total >= 70: return "#2563EB"
    elif total >= 55: return "#16A34A"
    elif total >= 40: return "#D97706"
    else:             return "#DC2626"

def _fmt_shares(v):
    v = abs(v)
    if v >= 10000:
        return f"약 {v/10000:.0f}만 주"
    return f"약 {v:,.0f}주"

def _get_recent_flow_signal(code, days=10):
    """최근 N영업일 기관·외국인 순매매 부호/규모를 보고 수급 흐름을 한 줄로 요약.
    반환값: {"text": 요약 문장, "inst_stance": ..., "frgn_stance": ...} 또는 None"""
    try:
        df = fetch_investor_trend_by_code(code, days=days)
        if df.empty:
            return None

        inst = df['기관순매매']
        frgn = df['외국인순매매']
        n = len(df)

        inst_buy_days = int((inst > 0).sum())
        frgn_buy_days = int((frgn > 0).sum())
        inst_sum = inst.sum()
        frgn_sum = frgn.sum()

        def _stance(buy_days, total_sum, n):
            if buy_days >= max(1, round(n * 0.7)) and total_sum > 0:
                return "steady_buy"
            if buy_days <= max(0, round(n * 0.3)) and total_sum < 0:
                return "steady_sell"
            return "mixed"

        inst_stance = _stance(inst_buy_days, inst_sum, n)
        frgn_stance = _stance(frgn_buy_days, frgn_sum, n)
        period_label = f"최근 {n}거래일"

        if inst_stance == "steady_buy" and frgn_stance == "steady_buy":
            text = (f"{period_label} 동안 기관이 {_fmt_shares(inst_sum)}, 외국인이 {_fmt_shares(frgn_sum)} "
                    f"동시에 순매수하며 매수세가 겹치고 있어 수급 측면에서 긍정적인 흐름입니다.")
        elif inst_stance == "steady_buy":
            text = f"{period_label} 동안 기관이 {_fmt_shares(inst_sum)} 순매수하며 꾸준히 사들이고 있는 점은 참고해볼 만합니다."
        elif frgn_stance == "steady_buy":
            text = f"{period_label} 동안 외국인이 {_fmt_shares(frgn_sum)} 순매수를 이어가고 있어 수급상 긍정적인 신호로 볼 수 있습니다."
        elif inst_stance == "steady_sell" and frgn_stance == "steady_sell":
            text = (f"다만 {period_label} 동안 기관·외국인 모두 매도 우위(기관 {_fmt_shares(inst_sum)}, "
                     f"외국인 {_fmt_shares(frgn_sum)} 순매도)를 보이고 있어 수급 측면은 다소 부담스러운 구간입니다.")
        elif inst_stance == "steady_sell":
            text = f"다만 {period_label} 동안 기관이 {_fmt_shares(inst_sum)} 순매도하며 매도세가 이어지고 있어 단기 수급은 조심스러운 편입니다."
        elif frgn_stance == "steady_sell":
            text = f"다만 {period_label} 동안 외국인이 {_fmt_shares(frgn_sum)} 순매도하며 매도세가 이어지고 있어 단기 수급은 조심스러운 편입니다."
        else:
            text = f"{period_label} 수급은 기관·외국인 모두 뚜렷한 방향 없이 혼조세를 보이고 있습니다."

        return {"text": text, "inst_stance": inst_stance, "frgn_stance": frgn_stance}
    except Exception:
        return None


def _build_ai_comment(name, code, per, pbr, roe, debt, drop_pct, div, scores, total_label):
    """AI 종합점수 + 재무지표 + 수급 흐름 + 최근 공시를 조합해 항목별(아이콘+라벨) 코멘트를 생성.
    반환값: {"icon": str, "label": str, "text": str} 딕셔너리 리스트.
    각 text 안의 숫자·핵심어는 '【…】'로 감싸 렌더링 시 강조 처리됨(공시 제목은 기존대로 「」『』 사용).
    마지막 항목은 전체를 한 줄로 요약하는 '종합 정리' 문단.

    표현 다양화: 조건별로 여러 개의 문장 후보(variant pool)를 두고, 종목코드로 시드를
    고정한 난수(rng)로 하나를 선택한다. 그 결과 '같은 종목은 새로고침해도 항상 같은
    문구', '다른 종목은 서로 다른 문구'가 나와 전체적으로 코멘트가 획일적으로 반복되는
    현상을 줄인다."""
    sections = []
    total = scores["total"]

    # 종목코드 기반 고정 시드 난수 — 매번 바뀌지 않고 종목별로만 다른 문구가 나오게 함
    seed = int(hashlib.md5(f"{code}-{name}".encode("utf-8")).hexdigest(), 16)
    rng = random.Random(seed)
    def pick(options):
        return options[rng.randrange(len(options))]

    sub = {"재무 건전성": scores["health"], "성장성": scores["growth"],
           "수익성": scores["profit"], "배당 매력": scores["dividend"]}
    strengths  = [k for k, v in sub.items() if v >= 65]
    weaknesses = [k for k, v in sub.items() if v < 40]

    # 이후 문단·요약에서 재사용할 판단 플래그
    is_cheap      = per > 0 and per <= 6
    is_expensive  = per > 25
    is_profitable = roe != -999 and roe >= 15
    deep_drop     = drop_pct <= -25
    mild_drop     = -25 < drop_pct <= -10
    div_good      = div >= 3.5
    debt_risk     = debt > 150

    # ── 총평 : 네 항목 종합 밸런스 ────────────────────────────────────
    p1 = []
    opening_map = {
        "최우량": [
            f"{name}은(는) 재무·수익성·성장성 전반이 고르게 우수해 지금 봐도 꽤 매력 있는 종목입니다.",
            f"{name}은(는) 여러 지표가 고르게 강한 편이라, 지금 시점에도 눈여겨볼 만합니다.",
            f"{name}은(는) 재무부터 성장성까지 흠잡을 데가 적어 우량주로 분류할 만한 모습이에요.",
        ],
        "우량": [
            f"{name}은(는) 전반적으로 안정적인 지표를 갖추고 있어 볼 만한 종목입니다.",
            f"{name}은(는) 큰 약점 없이 균형 잡힌 지표를 보여주고 있습니다.",
            f"{name}은(는) 대체로 탄탄한 흐름을 유지하고 있어 관심 대상으로 삼을 만해요.",
        ],
        "양호": [
            f"{name}은(는) 눈에 띄는 결점 없이 무난한 흐름을 이어가고 있습니다.",
            f"{name}은(는) 특별히 튀지는 않지만 안정적인 편에 속합니다.",
            f"{name}은(는) 큰 리스크 없이 평이한 흐름을 보이고 있어요.",
        ],
        "보통": [
            f"{name}은(는) 딱히 튀는 매력은 없는 평범한 종목이라, 굳이 서두를 필요는 없어 보입니다.",
            f"{name}은(는) 장단점이 뚜렷하지 않은 평범한 지표를 보이고 있어요.",
            f"{name}은(는) 지금 당장 움직이기보다는 좀 더 관찰이 필요해 보입니다.",
        ],
        "주의": [
            f"{name}은(는) 지금 지표만 보면 매력이 크지 않아 보수적으로 접근하시는 게 좋아 보입니다.",
            f"{name}은(는) 여러 지표가 아쉬운 편이라 신중한 접근이 필요합니다.",
            f"{name}은(는) 현재로선 리스크 요인이 두드러져 보수적으로 볼 필요가 있어요.",
        ],
    }
    p1.append(pick(opening_map.get(total_label, [f"{name}의 AI 종합점수는 【{total}점】입니다."])))

    if strengths and weaknesses:
        p1.append(pick([
            f"네 가지 항목 중 【{'·'.join(strengths)}】은(는) 강점으로 볼 수 있지만, "
            f"【{'·'.join(weaknesses)}】은(는) 상대적으로 약한 편이라 시간을 두고 지켜볼 필요가 있어요.",
            f"강점은 【{'·'.join(strengths)}】 쪽이고, 반대로 【{'·'.join(weaknesses)}】 부분은 약점으로 꼽혀 균형을 맞춰볼 필요가 있습니다.",
            f"【{'·'.join(strengths)}】에서는 강점이 뚜렷하지만, 【{'·'.join(weaknesses)}】은(는) 개선 여지가 있는 항목이에요.",
        ]))
    elif strengths and not weaknesses:
        p1.append(pick([
            f"【{'·'.join(strengths)}】을(를) 포함해 특별한 약점 없이 전반적으로 고르게 안정적인 모습이에요.",
            f"뚜렷한 약점 없이 【{'·'.join(strengths)}】이(가) 특히 돋보이는 구성입니다.",
            f"네 항목 중 약점은 보이지 않고, 【{'·'.join(strengths)}】 쪽에서 확실한 강점이 나타납니다.",
        ]))
    elif weaknesses and not strengths:
        p1.append(pick([
            f"뚜렷한 강점은 없고 【{'·'.join(weaknesses)}】이(가) 약점으로 나타나 다소 아쉬운 흐름이에요.",
            f"강점이라 할 만한 항목은 없고, 【{'·'.join(weaknesses)}】에서 약점이 두드러집니다.",
            f"【{'·'.join(weaknesses)}】 항목이 특히 부진해 전반적으로 아쉬운 지표예요.",
        ]))
    else:
        p1.append(pick([
            "네 항목 모두 중간 수준으로, 어느 한쪽에 크게 치우치지 않은 무난한 편이에요.",
            "네 항목이 대체로 비슷한 수준을 유지하고 있어 특별히 튀는 부분은 없습니다.",
            "강점도 약점도 뚜렷하지 않은, 고르게 평이한 지표 구성이에요.",
        ]))
    sections.append({"icon": "🏁", "label": "총평", "text": " ".join(p1)})

    # ── 밸류에이션 × 수익성 조합 인사이트 ─────────────────────────────
    p2 = []
    if is_cheap and is_profitable:
        p2.append(pick([
            f"PER 【{per:.1f}배】로 낮은데 ROE도 【{roe:.1f}%】로 준수해서, 저평가되어 있으면서 수익성도 갖춘 흔치 않은 조합으로 보여요.",
            f"PER 【{per:.1f}배】의 낮은 밸류에이션에 ROE 【{roe:.1f}%】의 수익성까지 더해져, 저평가·고수익 조합이 눈에 띕니다.",
            f"이익 대비 주가(PER 【{per:.1f}배】)도 싸고 ROE 【{roe:.1f}%】도 준수해서, 밸류에이션과 수익성 두 마리 토끼를 잡은 모습이에요.",
        ]))
    elif is_cheap:
        p2.append(pick([
            f"PER이 【{per:.1f}배】로 낮아 이익 대비 저평가 구간으로 볼 여지가 있습니다.",
            f"PER 【{per:.1f}배】 수준으로, 이익 규모에 비해 주가는 낮게 형성되어 있는 편이에요.",
            f"밸류에이션 부담이 크지 않은 PER 【{per:.1f}배】대라 저평가 매력을 고려해볼 만합니다.",
        ]))
    elif is_expensive and roe != -999 and roe < 10:
        p2.append(pick([
            f"PER은 【{per:.1f}배】로 높은데 ROE는 【{roe:.1f}%】에 그쳐, 이익 대비 주가가 비싸게 매겨진 편이라 향후 실적이 뒷받침되는지 지켜볼 필요가 있어요.",
            f"PER 【{per:.1f}배】는 부담스러운 수준인데 ROE 【{roe:.1f}%】는 낮아, 지금 주가가 실적을 앞서가는 모습으로 보입니다.",
            f"수익성(ROE 【{roe:.1f}%】)이 뒷받침되지 않는 상태에서 PER 【{per:.1f}배】까지 높아 밸류에이션 매력은 떨어지는 편이에요.",
        ]))
    elif is_expensive:
        p2.append(pick([
            f"다만 PER이 【{per:.1f}배】로 다소 높아 이익 대비 주가는 비싼 편입니다.",
            f"PER 【{per:.1f}배】는 시장 평균 대비 높은 편이라, 밸류에이션 부담이 있는 구간이에요.",
            f"이익 대비 주가 수준(PER 【{per:.1f}배】)이 높아 추가 상승을 위해선 실적 개선이 필요해 보여요.",
        ]))
    if p2:
        sections.append({"icon": "💰", "label": "밸류에이션", "text": " ".join(p2)})

    # ── 수급 흐름 × 진입 타이밍 조합 ──────────────────────────────────
    p3 = []
    flow = _get_recent_flow_signal(code, days=10)
    flow_buy = bool(flow) and ("steady_buy" in (flow["inst_stance"], flow["frgn_stance"]))

    if deep_drop and flow_buy:
        p3.append(pick([
            f"52주 고점 대비 【{drop_pct:.1f}%】 빠진 상태에서 수급까지 순매수로 돌아서고 있어, 저점 매수 심리가 반영되는 구간으로 볼 수 있어요. 1차 진입을 슬슬 고려해볼 만합니다.",
            f"고점 대비 【{drop_pct:.1f}%】나 밀린 가운데 수급도 매수 우위로 바뀌고 있어, 바닥을 다지는 신호로 해석할 여지가 있습니다. 1차 진입을 검토해볼 만해요.",
            f"낙폭이 【{drop_pct:.1f}%】에 달한 상황에서 매수세가 유입되고 있어, 저가 매수세가 형성되는 구간으로 보입니다.",
        ]))
    elif deep_drop:
        p3.append(pick([
            f"52주 고점 대비 【{drop_pct:.1f}%】 빠진 상태라, 저가 매수 관점에서는 1차 진입을 슬슬 고려해볼 만한 구간이에요.",
            f"고점 대비 【{drop_pct:.1f}%】나 하락해 있어, 분할 매수 관점에서 첫 진입을 검토해볼 만한 구간입니다.",
            f"낙폭이 【{drop_pct:.1f}%】로 큰 편이라, 저점 매수 기회로 볼 수 있는 구간이에요.",
        ]))
    elif mild_drop:
        p3.append(pick([
            f"52주 고점 대비 【{drop_pct:.1f}%】 하락해 있어, 소량으로 먼저 들어가보고 더 빠지면 2·3차로 나눠 담는 것도 방법입니다.",
            f"고점 대비 【{drop_pct:.1f}%】 조정을 받은 상태로, 소액으로 분할 진입을 시작해볼 수 있는 구간입니다.",
            f"【{drop_pct:.1f}%】 정도 눌린 상태라, 무리하지 않는 선에서 소량 진입 후 흐름을 지켜보는 것도 방법이에요.",
        ]))
    elif drop_pct < 0:
        p3.append(pick([
            "52주 고점 대비 낙폭이 크지 않은 편이라, 굳이 서둘러 들어가기보다는 조금 더 지켜봐도 될 것 같아요.",
            "고점 대비 하락폭이 제한적이라, 지금 당장 진입을 서두를 이유는 크지 않아 보입니다.",
            "낙폭이 크지 않아 추가 조정 여부를 좀 더 지켜본 뒤 판단해도 늦지 않을 듯합니다.",
        ]))

    if flow:
        connector = pick(["이런 가운데, ", "동시에, ", "여기에 더해, "]) if p3 else ""
        p3.append(connector + flow["text"])
    if p3:
        sections.append({"icon": "📊", "label": "수급 · 진입 타이밍", "text": " ".join(p3)})

    # ── 배당 · 부채비율 ───────────────────────────────────────────────
    p4 = []
    if div_good:
        p4.append(pick([
            f"배당수익률도 【{div:.1f}%】로 준수한 편이라, 보유하면서 배당까지 함께 챙기기 좋습니다.",
            f"배당수익률 【{div:.1f}%】 수준이라, 시세차익 외에 배당 매력도 함께 누릴 수 있는 편이에요.",
            f"【{div:.1f}%】대의 배당수익률을 갖추고 있어 장기 보유 시 배당 측면에서도 나쁘지 않습니다.",
        ]))
    if debt_risk:
        prefix = "다만 " if p4 else ""
        p4.append(pick([
            f"{prefix}부채비율이 【{debt:.0f}%】로 높은 편이라 이 부분은 한 번 더 확인해보시는 게 좋습니다.",
            f"{prefix}부채비율 【{debt:.0f}%】는 다소 부담스러운 수준이라 재무 안정성 측면은 유의할 필요가 있어요.",
            f"{prefix}【{debt:.0f}%】에 달하는 부채비율은 리스크 요인으로 함께 봐두시는 게 좋습니다.",
        ]))
    if p4:
        sections.append({"icon": "🎯", "label": "배당 · 재무 안정성", "text": " ".join(p4)})

    # ── 최근 공시 (주의 + 긍정 신호) ─────────────────────────────────
    try:
        recent = fetch_disclosure_list(code, days=60)
        caution_titles = [d["title"] for d in recent if d["flag"] == "caution"][:2]
        positive_titles = [d["title"] for d in recent if d["flag"] == "positive"][:1]

        if caution_titles:
            joined = ", ".join(f"「{t}」" for t in caution_titles)
            sections.append({"icon": "📰", "label": "최근 공시",
                              "text": pick([
                                  f"최근 60일 내 {joined} 같은 공시가 있었으니, 투자 전에 원문은 꼭 한 번 확인해보세요.",
                                  f"최근 60일 사이 {joined} 공시가 나온 만큼, 세부 내용은 원문으로 직접 확인해보시길 권합니다.",
                              ])})
        elif positive_titles:
            sections.append({"icon": "📰", "label": "최근 공시",
                              "text": pick([
                                  f"최근에는 『{positive_titles[0]}』 공시도 있었는데, 참고해볼 만한 소식입니다.",
                                  f"『{positive_titles[0]}』 공시가 최근 있었던 만큼, 눈여겨볼 만한 흐름이에요.",
                              ])})
    except Exception:
        pass

    # ── 종합 정리 : 위 항목들을 한 문장으로 압축 ──────────────────────
    summary_bits = []
    if is_cheap and is_profitable:
        summary_bits.append(pick(["저평가 매력과 수익성을 동시에 갖췄고", "밸류에이션과 수익성 모두 매력적이고"]))
    elif is_cheap:
        summary_bits.append(pick(["밸류에이션 매력이 있고", "가격 부담은 크지 않고"]))
    elif is_expensive:
        summary_bits.append(pick(["밸류에이션 부담은 있지만", "가격은 다소 부담스럽지만"]))

    if deep_drop and flow_buy:
        summary_bits.append(pick(["낙폭 과대 구간에서 수급까지 개선되고 있어", "저점 구간에서 매수세까지 유입되고 있어"]))
    elif deep_drop:
        summary_bits.append(pick(["낙폭이 커 저가 매수 관점에서 접근해볼 만하며", "하락폭이 큰 편이라 분할 매수를 고려해볼 만하며"]))
    elif mild_drop:
        summary_bits.append(pick(["고점 대비 조정을 받은 상태이며", "소폭 눌림목 구간에 있으며"]))

    if div_good:
        summary_bits.append(pick(["배당 매력도 갖추고 있어", "배당 수익까지 기대할 수 있어"]))
    if debt_risk:
        summary_bits.append(pick(["다만 부채비율은 유의해야 하는", "다만 재무 리스크는 체크가 필요한"]))

    verdict_map = {
        "최우량": ["적극적으로 관심 가져볼 만한 종목입니다.", "우선순위를 두고 살펴볼 만한 종목입니다."],
        "우량":   ["긍정적으로 검토해볼 만한 종목입니다.", "관심 종목에 담아둘 만합니다."],
        "양호":   ["부담 없이 지켜볼 만한 종목입니다.", "여유 있게 관찰해볼 만한 종목입니다."],
        "보통":   ["서두르기보다는 추가 확인 후 판단해도 될 종목입니다.", "당장보다는 좀 더 지켜본 뒤 결정해도 될 종목입니다."],
        "주의":   ["보수적인 접근이 필요한 종목입니다.", "신중한 판단이 요구되는 종목입니다."],
    }
    verdict = pick(verdict_map.get(total_label, ["참고 후 신중히 판단해볼 종목입니다."]))

    if summary_bits:
        summary_text = f"종합적으로 {', '.join(summary_bits)} {verdict}"
    else:
        summary_text = f"종합적으로 【{total_label}】 등급에 해당하며, {verdict}"
    sections.append({"icon": "✅", "label": "종합 정리", "text": summary_text})

    return sections

def render_ai_diagnosis(name, code, per, pbr, roe, debt, drop_pct, div, grade_label):
    scores = calc_ai_scores(per, pbr, roe, debt, drop_pct, div)
    total  = scores["total"]

    if total >= 85:   total_color = "#7C3AED"; total_label = "최우량"
    elif total >= 70: total_color = "#2563EB"; total_label = "우량"
    elif total >= 55: total_color = "#16A34A"; total_label = "양호"
    elif total >= 40: total_color = "#D97706"; total_label = "보통"
    else:             total_color = "#DC2626"; total_label = "주의"

    grade_badge = (
        f'<span style="font-size:11px; background:#EEF2FF; color:#4F46E5; '
        f'border-radius:4px; padding:2px 7px; margin-left:8px; font-weight:600;">'
        f'{grade_label}</span>'
    ) if grade_label else ""

    def score_bar(score):
        if score >= 80:   bar_color = "#7C3AED"
        elif score >= 65: bar_color = "#2563EB"
        elif score >= 50: bar_color = "#16A34A"
        elif score >= 35: bar_color = "#D97706"
        else:             bar_color = "#DC2626"
        return (
            '<div style="display:flex; align-items:center; gap:10px; margin-bottom:8px;">'
            '<div style="width:80px; font-size:12px; color:#64748B; text-align:right;">' + str(score) + '점</div>'
            '<div style="flex:1; background:#F1F5F9; border-radius:4px; height:8px;">'
            '<div style="width:' + str(score) + '%; background:' + bar_color + '; border-radius:4px; height:8px;"></div>'
            '</div></div>'
        )

    bar_health   = score_bar(scores['health'])
    bar_growth   = score_bar(scores['growth'])
    bar_profit   = score_bar(scores['profit'])
    bar_dividend = score_bar(scores['dividend'])

    html = (
        '<div style="background:#FAFBFF; border:1px solid #C7D2FE; border-radius:10px; padding:18px 20px; margin-top:12px;">'
        '<div style="display:flex; align-items:center; gap:12px; margin-bottom:14px;">'
        '<div style="text-align:center;">'
        '<div style="font-size:32px; font-weight:900; color:' + total_color + '; line-height:1;">' + str(total) + '</div>'
        '<div style="font-size:11px; color:#94A3B8;">/ 100점</div>'
        '</div>'
        '<div>'
        '<div style="font-size:14px; font-weight:700; color:#0F172A;">AI 종합 점수' + grade_badge + '</div>'
        '<div style="font-size:12px; color:' + total_color + '; font-weight:600;">● ' + total_label + '</div>'
        '</div></div>'
        '<div style="display:grid; grid-template-columns:1fr 1fr; gap:10px;">'
        '<div style="background:#FFFFFF; border:1px solid #E2E8F0; border-radius:8px; padding:10px 12px;">'
        '<div style="font-size:11px; color:#94A3B8; margin-bottom:4px;">🏦 재무 건전성</div>'
        + bar_health +
        '</div>'
        '<div style="background:#FFFFFF; border:1px solid #E2E8F0; border-radius:8px; padding:10px 12px;">'
        '<div style="font-size:11px; color:#94A3B8; margin-bottom:4px;">📈 성장성</div>'
        + bar_growth +
        '</div>'
        '<div style="background:#FFFFFF; border:1px solid #E2E8F0; border-radius:8px; padding:10px 12px;">'
        '<div style="font-size:11px; color:#94A3B8; margin-bottom:4px;">💰 수익성</div>'
        + bar_profit +
        '</div>'
        '<div style="background:#FFFFFF; border:1px solid #E2E8F0; border-radius:8px; padding:10px 12px;">'
        '<div style="font-size:11px; color:#94A3B8; margin-bottom:4px;">🎯 배당 매력</div>'
        + bar_dividend +
        '</div>'
        '</div></div>'
    )
    st.markdown(html, unsafe_allow_html=True)

    def _escape_and_mark(text):
        """이스케이프 후 강조 마커(「」『』【】)를 실제 태그로 치환."""
        return (
            html_lib.escape(text)
            .replace("「", '<b style="color:#DC2626;">').replace("」", "</b>")
            .replace("『", '<b style="color:#16A34A;">').replace("』", "</b>")
            .replace("【", f'<b style="color:{total_color};">').replace("】", "</b>")
        )

    comment_sections = _build_ai_comment(name, code, per, pbr, roe, debt, drop_pct, div, scores, total_label)

    body_blocks = []
    for i, sec in enumerate(comment_sections):
        text_html = _escape_and_mark(sec["text"])
        if sec["label"] == "종합 정리":
            # 종합 정리는 별도 강조 박스로 분리
            continue
        margin_bottom = "14px" if i < len(comment_sections) - 1 else "0px"
        body_blocks.append(
            f'<div style="margin-bottom:{margin_bottom};">'
            f'<div style="font-size:11.5px; font-weight:700; color:{total_color}; margin-bottom:4px;">'
            f'{sec["icon"]} {sec["label"]}</div>'
            f'<div>{text_html}</div>'
            f'</div>'
        )
    comment_html = "".join(body_blocks)

    summary_sec = next((s for s in comment_sections if s["label"] == "종합 정리"), None)
    summary_html = ""
    if summary_sec:
        summary_html = (
            f'<div style="margin-top:14px; background:#F8FAFF; border:1px solid {total_color}33; '
            f'border-radius:6px; padding:10px 12px; font-size:12.5px; color:#1E293B; line-height:1.7;">'
            f'<b style="color:{total_color};">{summary_sec["icon"]} {summary_sec["label"]}</b><br>'
            f'{_escape_and_mark(summary_sec["text"])}'
            f'</div>'
        )

    st.markdown(
        '<div style="background:#FFFFFF; border:1px solid #E2E8F0; border-left:4px solid ' + total_color + '; '
        'border-radius:8px; padding:14px 16px; margin-top:10px; font-size:13px; color:#334155; line-height:1.75;">'
        '<b style="color:#0F172A;">💬 AI 코멘트</b><br><br>' + comment_html + summary_html +
        '<div style="margin-top:12px; font-size:11px; color:#94A3B8;">⚠️ 본 코멘트는 규칙 기반으로 자동 생성된 참고용 정보이며, 매수·매도 추천이 아닙니다.</div>'
        '</div>',
        unsafe_allow_html=True
    )

def calc_entry_points(entry1, pbr, drop_pct, cur_price):
    """1차 진입가(entry1)를 기준으로 2차/3차 진입가를 추정.
    '기업 재무 분석' 탭의 진입가 계산기와 같은 방식(고정비율 + 52주 고점 하락률 반영 +
    PBR 기반 장부가 추정치의 가중평균)을 사용해 탭 간 숫자가 일치하도록 통일함.
    (MA20/MA60은 종목별 네트워크 조회가 필요해 목록 화면에서는 제외)"""
    e2_fixed = round(entry1 * 0.92)
    e3_fixed = round(entry1 * 0.83)

    e2_drop, e3_drop = 0, 0
    if drop_pct != 0.0:
        e2_drop = round(entry1 * 0.90)
        e3_drop = round(entry1 * 0.80)

    e2_fund, e3_fund = 0, 0
    if pbr > 0 and cur_price > 0:
        bps_est = cur_price / pbr
        e2_fund = round(bps_est * 1.0)
        e3_fund = round(bps_est * 0.8)

    def _wavg(*candidates, weights):
        vals = [(v, w) for v, w in zip(candidates, weights) if v > 0]
        if not vals:
            return 0
        return round(sum(v * w for v, w in vals) / sum(w for _, w in vals))

    entry2 = _wavg(e2_fixed, e2_drop, e2_fund, weights=[1.5, 1.0, 2.0])
    if entry2 == 0 or entry2 >= entry1:
        entry2 = e2_fixed
    entry3 = _wavg(e3_fixed, e3_drop, e3_fund, weights=[1.0, 1.5, 2.5])
    if entry3 == 0 or entry3 >= entry2:
        entry3 = round(entry2 * 0.92)

    return entry2, entry3

def render_recommendations():
    st.header(
        "추천 종목",
        help="""💡 **[추천 종목 엔진 목표]**\n\n단순히 재무제표만 좋은 기업을 찾는 것이 아닙니다.\n안정적인 실적과 고배당 매력을 갖춘 '우량주' 중에서도,\n최근 52주 고점 대비 유의미하게 하락하여 **'안전 마진'이 확보된 진입하기 좋은 저평가 종목**만을 엄선합니다."""
    )
    st.markdown("<hr style='margin: 10px 0 15px 0; border-color: #E5E7EB;'>", unsafe_allow_html=True)

    # 버튼/안내 문구에 필요한 데이터를 먼저 계산 (헤더에 버튼을 바로 배치하기 위해)
    screener_df = load_screener_df()
    high52_map = load_high52_map()

    if high52_map:
        info_kind, info_text = "success", f"스크리너 CSV 고점 데이터 사용 중 — {len(high52_map):,}종목 로드됨 (네이버 개별 호출 최소화)"
        scan_workers = 8
    else:
        info_kind, info_text = "info", "스크리너 탭 → [52주 고점 데이터 업데이트]에서 KRX CSV를 업로드하면 더 빠르고 정확해집니다. (현재: 네이버 실시간 API 사용)"
        scan_workers = 5

    warn_text = None
    if screener_df.empty:
        warn_text = "저장된 전체 시장 데이터가 없습니다. 스캔 버튼 클릭 시 '전체 시장 스캔'이 1단계로 자동 진행됩니다. (약 15초 추가 소요)"
    elif not load_reco_df().empty:
        warn_text = "✅ 이전 스캔 결과를 그대로 불러왔습니다. 최신 시세로 다시 확인하려면 [스캔 실행]을 눌러주세요."

    with st.container(key="quant_card_box"):
        st.markdown("""
            <style>
            .st-key-quant_card_box { background:#F8FAFC; border:1px solid #E2E8F0; border-radius:8px; padding:20px; margin-bottom:20px; }
            .st-key-quant_card_box .stButton > button { padding: 4px 14px !important; font-size: 12.5px !important; }
            </style>
        """, unsafe_allow_html=True)

        col_title, col_btn = st.columns([5, 1])
        with col_title:
            st.markdown("<h4 style='margin-top:0; margin-bottom:0; font-size:16px; color:#0F172A;'>🎯 퀀트 스코어링 추천 엔진 (사용자 맞춤 등급제)</h4>", unsafe_allow_html=True)
        with col_btn:
            btn_scan = st.button("스캔 실행", use_container_width=True, key="quant_scan_btn")

        st.markdown("""
            <p style='font-size:13px; color:#475569; line-height:1.7; margin-bottom:12px;'>
                펀더멘털(PER, PBR, ROE, 부채비율)과 타이밍(52주 고점 하락률) 및 배당수익률 안전마진을 결합하여 종목을 세분화합니다.<br>
                <b>⚠️ 본 데이터는 투자 판단의 참고 자료이며, 매수 추천이 아닙니다.</b>
            </p>
            <table style='width:100%; border-collapse: collapse; font-size:12px; text-align:center;'>
                <tr style='background-color:#F1F5F9; border-bottom:2px solid #CBD5E1;'>
                    <th style='padding:6px;'>등급</th><th style='padding:6px;'>PER</th><th style='padding:6px;'>PBR</th><th style='padding:6px;'>ROE</th><th style='padding:6px;'>배당수익률</th><th style='padding:6px;'>부채비율(엄격)</th><th style='padding:6px;'>고점 하락률</th>
                </tr>
                <tr style='border-bottom:1px solid #E2E8F0;'>
                    <td style='padding:6px; font-weight:600; color:#F59E0B;'>🥉 C급 성장 기대주</td><td>25 이하</td><td>2.5 이하</td><td>5% 이상</td><td>-</td><td>200% 이하</td><td>-5% 이하</td>
                </tr>
                <tr style='border-bottom:1px solid #E2E8F0;'>
                    <td style='padding:6px; font-weight:600; color:#10B981;'>🥈 B급 적정 가치주</td><td>15 이하</td><td>1.5 이하</td><td>8% 이상</td><td>-</td><td>150% 이하</td><td>-10% 이하</td>
                </tr>
                <tr style='border-bottom:1px solid #E2E8F0;'>
                    <td style='padding:6px; font-weight:600; color:#3B82F6;'>🥇 A급 우량 가치주</td><td>12 이하</td><td>1.2 이하</td><td>10% 이상</td><td>1.5% 이상</td><td>120% 이하</td><td>-15% 이하</td>
                </tr>
                <tr style='background-color:#EEF2FF;'>
                    <td style='padding:6px; font-weight:600; color:#7C3AED;'>💎 S급 초저평가 고배당</td><td style='color:#7C3AED;'>8 이하</td><td style='color:#7C3AED;'>0.8 이하</td><td style='color:#7C3AED;'>12% 이상</td><td style='color:#7C3AED;'>3.0% 이상</td><td style='color:#7C3AED;'>100% 이하</td><td style='color:#7C3AED;'>-20% 이하</td>
                </tr>
            </table>
        """, unsafe_allow_html=True)

    with st.container(key="quant_info_box"):
        st.markdown("""
            <style>
            .st-key-quant_info_box { margin-bottom: 8px !important; margin-top: -6px !important; }
            </style>
        """, unsafe_allow_html=True)
        _text_color = {"success": "#15803D", "info": "#64748B"}[info_kind]
        st.markdown(f"<div style='font-size:12.5px; color:{_text_color}; line-height:1.5;'>{info_text}</div>", unsafe_allow_html=True)
        if warn_text:
            st.markdown(f"<div style='font-size:12.5px; color:#B45309; line-height:1.5; margin-top:2px;'>{warn_text}</div>", unsafe_allow_html=True)

    if btn_scan:
        run_unified_market_scan()

    _reco_df = load_reco_df()
    if not _reco_df.empty:
        st.markdown("<hr style='margin: 25px 0 20px 0; border-color: #E5E7EB;'>", unsafe_allow_html=True)
        st.markdown("<h4 style='font-size: 16px; margin-bottom:15px;'>🎛️ 추천 종목 제어판 (실시간 필터링)</h4>", unsafe_allow_html=True)
        
        col1, col2 = st.columns(2)
        with col1:
            market_filter = st.selectbox("시장 분류", ["전체", "코스피", "코스닥"], key="reco_mkt_filter")
        with col2:
            st.markdown("<div style='padding-top:28px;'></div>", unsafe_allow_html=True)
            strict_debt = st.toggle("부채비율 '엄격 기준' 적용 (권장)", value=True, help="해제 시 모든 등급의 부채비율 허들을 300%로 완화하여 더 많은 종목을 탐색합니다.")

        st.markdown("<br>", unsafe_allow_html=True)
        selected_grade = st.radio(
            "등급 필터", 
            ["전체보기", "💎 S급", "🥇 A급", "🥈 B급", "🥉 C급", "👀 D급"], 
            horizontal=True, 
            label_visibility="collapsed"
        )

        def assign_grade(row, is_strict):
            per, pbr, roe, debt, drop, div = row['PER'], row['PBR'], row['ROE'], row['부채비율'], row['고점 / 하락률'], row['배당수익률']
            
            s_debt = 100 if is_strict else 300
            a_debt = 120 if is_strict else 300
            b_debt = 150 if is_strict else 300
            c_debt = 200 if is_strict else 300
            d_debt = 300
            
            if per <= 8 and pbr <= 0.8 and roe >= 12 and debt <= s_debt and drop <= -20.0 and div >= 3.0:
                return "💎 S급 초저평가 고배당"
            elif per <= 12 and pbr <= 1.2 and roe >= 10 and debt <= a_debt and drop <= -15.0 and div >= 1.5:
                return "🥇 A급 우량 가치주"
            elif per <= 15 and pbr <= 1.5 and roe >= 8 and debt <= b_debt and drop <= -10.0:
                return "🥈 B급 적정 가치주"
            elif per <= 25 and pbr <= 2.5 and roe >= 5 and debt <= c_debt and drop <= -5.0:
                return "🥉 C급 성장 기대주"
            elif per <= 40 and pbr <= 4.0 and roe >= 0 and debt <= d_debt and drop <= 0.0:
                return "👀 D급 관심 종목"
            return None

        display_df = _reco_df.copy()
        display_df['등급'] = display_df.apply(lambda row: assign_grade(row, strict_debt), axis=1)
        display_df = display_df.dropna(subset=['등급']) 

        if market_filter != "전체":
            display_df = display_df[display_df['시장'].str.contains(market_filter)]

        if selected_grade != "전체보기":
            grade_key = selected_grade.split()[1] 
            display_df = display_df[display_df['등급'].str.contains(grade_key)]

        display_df = display_df.sort_values('고점 / 하락률', ascending=True).reset_index(drop=True)

        username = st.session_state.get("auth_user")

        if display_df.empty:
            st.info(f"현재 설정된 필터({market_filter}, {selected_grade})에 부합하는 종목이 없습니다. 조건을 완화해보세요.")
        else:
            for _, row in display_df.iterrows():
                name  = row['종목명']
                code  = str(row['종목코드']).zfill(6)
                market_str = row.get('시장', '')
                price = row['현재가_num']
                drop_pct = row['고점 / 하락률']
                per, pbr, roe, debt = row['PER'], row['PBR'], row['ROE'], row['부채비율']
                div = row.get('배당수익률', 0.0)
                grade_label = row['등급']
                source_badge = row.get('데이터출처', '🌐 실시간')

                entry_2nd, entry_3rd = calc_entry_points(price, pbr, drop_pct, price)
                entry_2nd_pct = round((entry_2nd / price - 1) * 100, 1) if price else 0.0
                entry_3rd_pct = round((entry_3rd / price - 1) * 100, 1) if price else 0.0

                if "S급" in grade_label: bg_color = "#EEF2FF"
                elif "A급" in grade_label: bg_color = "#F0FDF4"
                elif "B급" in grade_label: bg_color = "#FEFCE8"
                elif "C급" in grade_label: bg_color = "#FFF5F5"
                else: bg_color = "#F8FAFC"

                is_saved = is_in_watchlist(username, code)

                with st.container(key=f"reco_card_{code}"):
                    st.markdown(f"""
                        <style>
                        .st-key-reco_card_{code} {{
                            background:{bg_color}; border:1px solid #E2E8F0; border-radius:8px;
                            padding:16px 20px 4px 20px; margin-bottom:5px; margin-top:12px;
                        }}
                        .st-key-reco_card_{code} div[data-testid="stButton"] {{
                            margin-top:6px; display:flex; justify-content:flex-end;
                        }}
                        .st-key-reco_card_{code} div[data-testid="stButton"] button {{
                            background-color:transparent !important; border:none !important;
                            outline:none !important; box-shadow:none !important;
                            padding:0 !important; margin:0 !important; min-height:auto !important;
                            line-height:1 !important; font-size:19px !important; color:#F59E0B !important;
                        }}
                        .st-key-reco_card_{code} div[data-testid="stButton"] button:hover,
                        .st-key-reco_card_{code} div[data-testid="stButton"] button:focus,
                        .st-key-reco_card_{code} div[data-testid="stButton"] button:focus:not(:active),
                        .st-key-reco_card_{code} div[data-testid="stButton"] button:active {{
                            background-color:transparent !important; border:none !important;
                            outline:none !important; box-shadow:none !important; color:#D97706 !important;
                        }}
                        </style>
                    """, unsafe_allow_html=True)

                    col_content, col_star = st.columns([25, 1])
                    with col_content:
                        st.markdown(f"""
                            <div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:8px; padding-top:6px;">
                                <div>
                                    <span style="font-size:15px; font-weight:700; color:#0F172A;">{name}</span>
                                    <span style="font-size:11px; color:#94A3B8; margin-left:6px;">{code} | {market_str}</span>
                                    <span style="font-size:11px; font-weight:700; color:#111827; background:#FFFFFF; border: 1px solid #D1D5DB; padding:2px 10px; border-radius:10px; margin-left:8px;">{grade_label}</span>
                                    <span style="font-size:10px; color:#94A3B8; background:#F1F5F9; border: 1px solid #E2E8F0; padding:2px 7px; border-radius:8px; margin-left:6px;">{source_badge}</span>
                                </div>
                                <div style="text-align:right;">
                                    <span style="font-size:15px; font-weight:700; color:#0F172A;">{int(price):,}</span>
                                    <span style="font-size:12px; font-weight:700; color:#16A34A; background:#FFFFFF; border: 1px solid #D1D5DB; padding:2px 8px; border-radius:12px; margin-left:8px;">52주최고 대비 {drop_pct:.1f}%</span>
                                </div>
                            </div>
                        """, unsafe_allow_html=True)
                    with col_star:
                        if st.button("⭐" if is_saved else "☆", key=f"reco_star_{code}", help="관심종목에서 제거" if is_saved else "관심종목에 추가"):
                            if is_saved:
                                remove_from_watchlist(username, code)
                                st.toast(f"'{name}'을(를) 관심종목에서 제거했습니다.")
                            else:
                                add_to_watchlist(username, code, name)
                                st.toast(f"'{name}'을(를) 관심종목에 추가했습니다.")
                            st.rerun()

                    st.markdown(f"""
                        <div style="display:flex; gap:18px; font-size:12px; color:#64748B; margin:8px 0 12px 0; flex-wrap:wrap;">
                            <span>PER <b style="color:#1E293B;">{per:.2f}배</b></span>
                            <span>PBR <b style="color:#1E293B;">{pbr:.2f}배</b></span>
                            <span>ROE <b style="color:#1E293B;">{roe:.1f}%</b></span>
                            <span>부채비율 <b style="color:#1E293B;">{debt:.1f}%</b></span>
                            <span>배당수익률 <b style="color:#DC2626;">{div:.1f}%</b></span>
                        </div>
                        <div style="display:flex; gap:10px; padding-bottom:16px;">
                            <div style="flex:1; background:#FFFFFF; border:1px solid #E2E8F0; border-radius:6px; padding:8px 4px; text-align:center;">
                                <div style="font-size:11px; color:#94A3B8; margin-bottom:2px;">1차 진입 (비중 25%)</div>
                                <div style="font-size:13px; font-weight:700; color:#5A4EE5;">{int(price):,}</div>
                            </div>
                            <div style="flex:1; background:#FFFFFF; border:1px solid #E2E8F0; border-radius:6px; padding:8px 4px; text-align:center;">
                                <div style="font-size:11px; color:#94A3B8; margin-bottom:2px;">2차 진입 ({entry_2nd_pct:+.1f}% / 35%)</div>
                                <div style="font-size:13px; font-weight:700; color:#1E293B;">{int(entry_2nd):,}</div>
                            </div>
                            <div style="flex:1; background:#FFFFFF; border:1px solid #E2E8F0; border-radius:6px; padding:8px 4px; text-align:center;">
                                <div style="font-size:11px; color:#94A3B8; margin-bottom:2px;">3차 진입 ({entry_3rd_pct:+.1f}% / 40%)</div>
                                <div style="font-size:13px; font-weight:700; color:#1E293B;">{int(entry_3rd):,}</div>
                            </div>
                        </div>
                    """, unsafe_allow_html=True)
                
                with st.expander(f"{name} · AI 진단 · 재무분석"):
                    render_ai_diagnosis(name, code, per, pbr, roe, debt, drop_pct, div, grade_label)
                    st.markdown("<hr style='margin:16px 0 12px 0; border-color:#E5E7EB;'>", unsafe_allow_html=True)

                    btn_key = f"reco_fn_{code}"
                    data_key = f"reco_fn_data_{code}"
                    if st.button(f"📊 실시간 재무 데이터 불러오기 (FnGuide)", key=btn_key):
                        with st.spinner(f"'{name}'의 최신 기업 개요와 재무제표를 가져오는 중입니다..."):
                            st.session_state[data_key] = True
                    if st.session_state.get(data_key):
                        draw_fnguide_details(code)

SCREENER_PRESETS = {
    "1단계 · 배당형 저평가": {
        "desc": "PER ≤ 10 · PBR ≤ 1.0 · 배당 ≥ 2% · ROE ≥ 10% · 부채비율 ≤ 100% — 싸고 배당주면서 실제로 돈도 버는 기업",
        "per": 10.0, "pbr": 1.0, "div": 2.0, "roe": 10.0, "debt": 100.0, "use_div": True,
    },
    "2단계 · 가치주 (밸런스)": {
        "desc": "PER ≤ 15 · PBR ≤ 1.0 · ROE ≥ 10% · 부채비율 ≤ 150% — 싸면서 돈 잘 버는 기업, 업종 다양",
        "per": 15.0, "pbr": 1.0, "div": 0.0, "roe": 10.0, "debt": 150.0, "use_div": False,
    },
    "3단계 · 성장형 저평가": {
        "desc": "PER ≤ 20 · PBR ≤ 1.5 · ROE ≥ 15% · 부채비율 ≤ 200% — 성장성 있으면서 아직 저평가인 기업",
        "per": 20.0, "pbr": 1.5, "div": 0.0, "roe": 15.0, "debt": 200.0, "use_div": False,
    },
    "직접 설정": None,
}

def render_screener():
    st.header(
        "종목 스크리너",
        help="""💡 **[종목 스크리너 안내]**\n\n네이버 금융의 전체 시장 데이터를 실시간으로 스캔하여 원하는 조건(PER, PBR, ROE, 배당수익률 등)에 맞는 종목을 빠르게 필터링합니다.\n\n나만의 맞춤형 가치주를 직접 발굴해 보세요."""
    )
    st.markdown("<hr style='margin: 10px 0 15px 0; border-color: #E5E7EB;'>", unsafe_allow_html=True)

    preset_names = list(SCREENER_PRESETS.keys())
    selected_preset = st.radio("필터 단계", preset_names, horizontal=True, key="screener_preset", label_visibility="collapsed")
    preset = SCREENER_PRESETS[selected_preset]

    if preset:
        st.markdown(f"<div style='font-size:13px; color:#5A4EE5; margin: 6px 0 14px 0;'>💡 {preset['desc']}</div>", unsafe_allow_html=True)
        max_per = preset['per']; max_pbr = preset['pbr']
        min_div = preset['div']; min_roe = preset['roe']
        max_debt = preset['debt']; use_div = preset['use_div']
    else:
        st.markdown("<div style='margin-top: 15px; margin-bottom: 5px;'><span style='font-weight: 700; color: #1E293B; font-size: 14px;'>⚙️ 나만의 상세 지표 커스텀 설정</span></div>", unsafe_allow_html=True)
        c1, c2, c3, c4, c5 = st.columns(5)
        with c1: max_per = st.number_input("PER 이하 (배)", value=15.0, step=0.5, format="%.1f")
        with c2: max_pbr = st.number_input("PBR 이하 (배)", value=1.0, step=0.1, format="%.1f")
        with c3: min_div = st.number_input("배당수익률 이상 (%)", value=0.0, step=0.1, format="%.1f")
        with c4: min_roe = st.number_input("ROE 이상 (%)", value=10.0, step=0.5, format="%.1f")
        with c5: max_debt = st.number_input("부채비율 이하 (%)", value=150.0, step=10.0, format="%.0f")
        use_div = min_div > 0
        st.markdown("<div style='height: 10px;'></div>", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    save_path = "saved_screener_data.csv"

    st.markdown("""
        <div class="info-box-modern">
            • 네이버 금융 사이트를 스캔하여 시장 전 종목의 최신 지표를 가져옵니다.<br>
            • <b>4코어 안전 멀티스레딩</b> 기술이 적용되어 빠르면서도 네이버 서버 차단을 완벽히 회피합니다.
        </div>
    """, unsafe_allow_html=True)

    if st.button("실시간 데이터 ⚡초고속 스캔 실행"):
        run_unified_market_scan()

    col_h52_title, col_h52_help = st.columns([9, 1])
    with col_h52_title:
        st.markdown(
            "<p style='font-size:15px; font-weight:600; color:#111827; margin:4px 0 4px 0;'>📈 52주 고점 데이터 업데이트</p>",
            unsafe_allow_html=True,
        )
    with col_h52_help:
        with st.popover("❓", use_container_width=True):
            st.markdown(
                """💡 **[52주 고점 데이터 안내]**

**어떤 데이터가 필요한가요?**
KRX 정보데이터시스템의 **[12004] 종목 시세 추이(월/연도)** 화면 데이터입니다.
(⚠️ [12002] 전종목 등락률 화면은 최고가 컬럼이 없어서 사용할 수 없습니다.)

**받는 순서**
1. 아래 '🔗 KRX 데이터 다운로드' 버튼 클릭
2. 시장구분 **KOSPI** 선택 → 조회기간 최근 1년 → **조회**
3. 결과표 우측 상단 다운로드 아이콘 클릭 → **CSV 저장**
4. 시장구분을 **KOSDAQ**으로 바꿔 동일하게 한 번 더 다운로드

받은 CSV 2개(코스피용 / 코스닥용)를 아래에 각각 업로드하면
**52주고점**과 **고점대비(%)** 컬럼이 자동으로 계산되어
스크리너 · 추천종목 탭에 반영됩니다."""
            )

    with st.expander("업로드하기", expanded=False):
        st.markdown("""
            <p style="font-size: 13px; color: #5D6475; margin-bottom: 6px;">
                KRX 종목시세추이 CSV(약 1년치)를 <b>코스피 / 코스닥 각각 업로드</b>하면 종목코드 매칭 후
                <b>52주고점</b>과 <b>고점대비(%)</b> 컬럼이 자동으로 추가됩니다.
            </p>
        """, unsafe_allow_html=True)
        st.link_button("🔗 KRX [52주 최고/최저] 데이터 다운로드", "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020104", use_container_width=True)

        def process_high52_upload(uploaded_file, market_label):
            try:
                try: h_df = pd.read_csv(uploaded_file, encoding='cp949')
                except:
                    uploaded_file.seek(0)
                    h_df = pd.read_csv(uploaded_file, encoding='utf-8')
                h_df.columns = h_df.columns.str.strip()
                h_code_col = find_col(h_df, ['종목코드', '단축코드'])
                h_high_col = find_col(h_df, ['최고가(종가)', '최고가', '52주최고'])
                
                if not h_code_col or not h_high_col:
                    st.error(f"[{market_label}] 종목코드 또는 최고가 컬럼을 찾을 수 초과니다. (감지된 컬럼: {list(h_df.columns)})")
                    return None
                h_df['종목코드'] = h_df[h_code_col].astype(str).str.zfill(6)
                h_df[h_high_col] = pd.to_numeric(h_df[h_high_col].astype(str).str.replace(',', ''), errors='coerce')
                return h_df.groupby('종목코드')[h_high_col].max()
            except Exception as e:
                st.error(f"[{market_label}] 파일 처리 오류: {e}")
                return None

        st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)
        col_kp, col_kq = st.columns(2)

        with col_kp:
            st.markdown("<b style='font-size:13px;'>🔵 KOSPI (코스피) CSV</b>", unsafe_allow_html=True)
            uploaded_kospi = st.file_uploader("코스피 파일 업로드", type=['csv'], key='high52_kospi')

        with col_kq:
            st.markdown("<b style='font-size:13px;'>🟢 KOSDAQ (코스닥) CSV</b>", unsafe_allow_html=True)
            uploaded_kosdaq = st.file_uploader("코스닥 파일 업로드", type=['csv'], key='high52_kosdaq')

        if uploaded_kospi or uploaded_kosdaq:
            maps = {}
            if uploaded_kospi:
                m = process_high52_upload(uploaded_kospi, "코스피")
                if m is not None: maps['코스피'] = m
            if uploaded_kosdaq:
                m = process_high52_upload(uploaded_kosdaq, "코스닥")
                if m is not None: maps['코스닥'] = m

            if maps:
                combined_map = pd.concat(maps.values()).groupby(level=0).max()
                base_df = load_screener_df()
                if base_df.empty:
                    st.error("먼저 실시간 스캔 데이터가 필요합니다. 위 [실시간 데이터 ⚡초고속 스캔 실행] 버튼을 눌러주세요.")
                else:
                    base_df['52주고점'] = base_df['종목코드'].map(combined_map)
                    mask_h = (base_df['현재가'] > 0) & (base_df['52주고점'] > 0)
                    base_df['고점대비(%)'] = None
                    base_df.loc[mask_h, '고점대비(%)'] = (
                        (base_df.loc[mask_h, '현재가'] - base_df.loc[mask_h, '52주고점'])
                        / base_df.loc[mask_h, '52주고점']
                    ) * 100
                    base_cols = [c for c in base_df.columns if c not in ['52주고점', '고점대비(%)']]
                    base_df[base_cols].to_csv(save_path, index=False, encoding='utf-8-sig')
                    high52_save = base_df[['종목코드', '52주고점', '고점대비(%)']].dropna(subset=['52주고점'])
                    high52_save.to_csv(HIGH52_PATH, index=False, encoding='utf-8-sig')
                    st.session_state['shared_screener_df'] = base_df
                    load_high52_map.clear()
                    if 'reco_raw_data' in st.session_state:
                        del st.session_state['reco_raw_data']
                    if os.path.exists(RECO_PATH):
                        try:
                            os.remove(RECO_PATH)
                        except Exception:
                            pass
                
                    matched = int(mask_h.sum())
                    markets_done = " + ".join(maps.keys())
                    st.success(f"✅ 52주 고점 매칭 완료! ({markets_done}) {matched}종목 업데이트되었습니다. 추천 종목 탭에서 재스캔 시 새 데이터가 바로 적용됩니다.")

    df = load_screener_df()

    if not df.empty:
        st.markdown("<hr style='margin: 30px 0 20px 0; border-color: #E5E7EB;'>", unsafe_allow_html=True)

        ETF_KEYWORDS = 'TIGER|KODEX|ARIRANG|KBSTAR|HANARO|KOSEF|TREX|ACE|SOL|RISE|ETF|인버스|레버리지|선물|리츠|REIT|인덱스|TR$'
        df = df[~df['종목명'].str.contains(ETF_KEYWORDS, regex=True, case=False, na=False)]

        col_tools1, col_tools2 = st.columns([3, 2])
        with col_tools1:
            with st.container(key="market_filter_box"):
                market_filter = st.radio("시장", ["전체", "코스피", "코스닥"], horizontal=True, key="screener_market", label_visibility="collapsed")
            
            t1, t2 = st.columns(2)
            with t1:
                if 'screener_show_all' not in st.session_state:
                    st.session_state['screener_show_all'] = False
                show_all = st.toggle("📋 전체 종목 보기 (필터 해제)", value=st.session_state['screener_show_all'], key="screener_toggle")
                st.session_state['screener_show_all'] = show_all
            with t2:
                exclude_finance = st.toggle("🚫 금융/지주 업종 제외", value=False, key="screener_excl_finance")
                
        with col_tools2:
            st.markdown("<div style='height: 8px;'></div>", unsafe_allow_html=True)
            search_text = st.text_input("검색", placeholder="🔍 결과 내 종목명 또는 코드 검색 (예: 삼성전자)", label_visibility="collapsed")

        if market_filter != "전체":
            df = df[df['시장'].str.contains(market_filter, na=False)]

        finance_keywords = '금융|은행|증권|보험|캐피탈|지주|투자|저축'

        if show_all:
            result_df = df.copy()
            if exclude_finance:
                result_df = result_df[~result_df['종목명'].str.contains(finance_keywords, regex=True, na=False)]
            if search_text:
                result_df = result_df[result_df['종목명'].str.contains(search_text, case=False, na=False) | result_df['종목코드'].astype(str).str.contains(search_text, case=False, na=False)]
            label = f"전체 종목{' · ' + market_filter if market_filter != '전체' else ''} ({len(result_df)}건) · 지표 필터 미적용"
        else:
            cond = (df['PER'] <= max_per) & (df['PER'] > 0) & (df['PBR'] <= max_pbr) & (df['PBR'] > 0) & (df['ROE'] >= min_roe) & (df['부채비율'] <= max_debt) & (df['부채비율'] >= 0)
            if use_div:
                cond = cond & (df['배당수익률'] >= min_div)
            if exclude_finance:
                cond = cond & (~df['종목명'].str.contains(finance_keywords, regex=True, na=False))
            result_df = df[cond].sort_values('ROE', ascending=False).reset_index(drop=True)
            if search_text:
                result_df = result_df[result_df['종목명'].str.contains(search_text, case=False, na=False) | result_df['종목코드'].astype(str).str.contains(search_text, case=False, na=False)]
            label = f"{selected_preset} 결과 ({len(result_df)}건) · ROE 높은 순"

        st.markdown(f"<div style='margin-bottom: 10px; font-weight: 600; color: #374151;'>{label}</div>", unsafe_allow_html=True)
        st.dataframe(get_styled_dataframe(result_df), use_container_width=True, hide_index=True)

def _to_float_safe(val):
    try:
        v = float(str(val).replace(',', '').strip())
        return v if pd.notna(v) else 0.0
    except Exception:
        return 0.0

def get_ai_diagnosis_inputs(code, df_annual, screener_df=None):
    code = normalize_kr_code(code)
    per, pbr, roe, debt, div, drop_pct = 0.0, 0.0, -999.0, -1.0, 0.0, 0.0

    if df_annual is not None and not df_annual.empty:
        latest = df_annual.iloc[-1]
        if 'PER' in df_annual.columns:
            v = _to_float_safe(latest['PER'])
            per = v  
        if 'PBR' in df_annual.columns:
            pbr = _to_float_safe(latest['PBR'])
        if 'ROE' in df_annual.columns:
            raw = latest['ROE']
            if pd.notna(raw) and str(raw).strip() not in ('', '-', 'nan'):
                roe = _to_float_safe(raw)   
        if '부채비율' in df_annual.columns:
            raw = latest['부채비율']
            if pd.notna(raw) and str(raw).strip() not in ('', '-', 'nan'):
                debt = _to_float_safe(raw)  

    try:
        if screener_df is None:
            screener_df = load_screener_df()
        if screener_df is not None and not screener_df.empty:
            matched = screener_df[screener_df['종목코드'].astype(str).str.zfill(6) == code]
            if not matched.empty:
                row = matched.iloc[0]
                if '배당수익률' in row.index and pd.notna(row['배당수익률']):
                    div = _to_float_safe(row['배당수익률'])
                if '고점대비(%)' in row.index and pd.notna(row['고점대비(%)']):
                    drop_pct = -abs(_to_float_safe(row['고점대비(%)']))
                if per == 0.0 and 'PER' in row.index and pd.notna(row['PER']):
                    per = _to_float_safe(row['PER'])
                if pbr == 0.0 and 'PBR' in row.index and pd.notna(row['PBR']):
                    pbr = _to_float_safe(row['PBR'])
                if roe == -999.0 and 'ROE' in row.index and pd.notna(row['ROE']):
                    raw_roe = row['ROE']
                    if str(raw_roe).strip() not in ('', '-', 'nan'):
                        roe = _to_float_safe(raw_roe)
                if debt < 0 and '부채비율' in row.index and pd.notna(row['부채비율']):
                    raw_debt = row['부채비율']
                    if str(raw_debt).strip() not in ('', '-', 'nan'):
                        debt = _to_float_safe(raw_debt)
    except Exception:
        pass

    return per, pbr, roe, debt, drop_pct, div

@st.cache_data(ttl=1800, show_spinner=False)
def search_naver_stock_by_name(query):
    """
    스크리너 캐시(load_screener_df)에 없는 종목명도 찾을 수 있도록
    네이버 금융 검색 결과에서 종목코드를 실시간으로 찾아온다.
    반환: [{"code": "005930", "name": "삼성전자", "market": "-"}, ...]
    """
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        url = f"https://finance.naver.com/search/searchList.naver?query={requests.utils.quote(query)}"
        res = requests.get(url, headers=headers, timeout=6)
        res.encoding = res.apparent_encoding or 'euc-kr'
        matches = re.findall(r'href="/item/main\.naver\?code=(\d+)"[^>]*>\s*([^<]+?)\s*</a>', res.text)
        seen = {}
        for code, name in matches:
            name = html_lib.unescape(name).strip()
            if code not in seen and name:
                seen[code] = name
        return [{"code": normalize_kr_code(c), "name": n, "market": "-"} for c, n in seen.items()]
    except Exception:
        return []

# 영문 표기와 실제 종목명이 크게 다른 경우를 위한 별칭 사전.
# key는 정규화(공백/하이픈/대소문자 제거) 상태로 비교되므로 그대로 적당히 적으면 됨.
# 필요한 종목이 있으면 이 딕셔너리에 계속 추가하면 됨.
_STOCK_NAME_ALIASES = {
    "LGCNS": "LG씨엔에스",
    "LGCNSC": "LG씨엔에스",
    "SKT": "SK텔레콤",
    "SKHYNIX": "SK하이닉스",
    "SKINNOVATION": "SK이노베이션",
    "SKSQUARE": "SK스퀘어",
    "LGUPLUS": "LG유플러스",
    "LGU": "LG유플러스",
    "LGCHEM": "LG화학",
    "LGELECTRONICS": "LG전자",
    "LGENERGYSOLUTION": "LG에너지솔루션",
    "LGENSOL": "LG에너지솔루션",
    "SAMSUNGELECTRONICS": "삼성전자",
    "SAMSUNGSDI": "삼성SDI",
    "SAMSUNGBIOLOGICS": "삼성바이오로직스",
    "HYUNDAIMOTOR": "현대차",
    "HYUNDAIMOBIS": "현대모비스",
    "KIA": "기아",
    "POSCOHOLDINGS": "POSCO홀딩스",
    "NAVER": "NAVER",
    "KAKAO": "카카오",
    "KT&G": "KT&G",
    "KTNG": "KT&G",
    "HANWHAAEROSPACE": "한화에어로스페이스",
    "CELLTRION": "셀트리온",
}

def _normalize_for_match(s):
    """비교용 정규화: 공백/하이픈/언더바/마침표 제거 + 대문자 통일."""
    return re.sub(r"[\s\-_.·]", "", str(s)).upper()

def resolve_stock_query(query):
    """
    입력값이 6자리 종목코드인지, '삼성전자' / '엑셈' 같은 종목명인지 자동 판별.
    - 코드로 특정되면 (code, name, [])
    - 이름 후보가 여럿이면 (None, None, candidates)
    - 아무것도 못 찾으면 (None, None, [])

    영문 약칭(LGCNS 등)이나 오타가 섞여도 최대한 후보를 찾아주도록
    별칭 사전 → 정규화 매칭 → 유사도 기반 추천 순으로 단계적으로 시도한다.
    """
    query = str(query).strip()
    if not query:
        return None, None, []

    digits_only = re.sub(r"\D", "", query)
    # 숫자 위주 입력(예: 005930, 5930)은 종목코드로 간주
    if digits_only and len(digits_only) >= max(len(query) - 1, 1):
        return normalize_kr_code(query), None, []

    norm_query = _normalize_for_match(query)
    # 별칭 사전에 있으면 실제 종목명으로 치환해서 이후 검색에 사용
    alias_target = _STOCK_NAME_ALIASES.get(norm_query)

    screener_df = load_screener_df()
    if screener_df is not None and not screener_df.empty and '종목명' in screener_df.columns:
        names = screener_df['종목명'].astype(str).str.strip()

        def _build_candidates(mask):
            candidates = []
            for _, row in screener_df[mask].head(10).iterrows():
                candidates.append({
                    "code": normalize_kr_code(row['종목코드']),
                    "name": row['종목명'],
                    "market": row['시장'] if '시장' in row.index and pd.notna(row['시장']) else "-",
                })
            return candidates

        # 1) 별칭 사전 매칭 우선
        if alias_target:
            exact_alias = screener_df[names == alias_target]
            if not exact_alias.empty:
                row = exact_alias.iloc[0]
                return normalize_kr_code(row['종목코드']), row['종목명'], []

        # 2) 정확히 일치
        exact = screener_df[names == query]
        if not exact.empty:
            row = exact.iloc[0]
            return normalize_kr_code(row['종목코드']), row['종목명'], []

        # 3) 일반 부분 문자열 포함 (기존 로직)
        partial = screener_df[names.str.contains(query, case=False, na=False, regex=False)]

        # 4) 공백/하이픈/대소문자를 무시한 정규화 부분 문자열 포함 (예: "LG 씨엔에스" ↔ "LGCNS" 계열 표기 차이 보완)
        if partial.empty:
            norm_names = names.map(_normalize_for_match)
            partial = screener_df[norm_names.str.contains(norm_query, na=False, regex=False)]

        if len(partial) == 1:
            row = partial.iloc[0]
            return normalize_kr_code(row['종목코드']), row['종목명'], []
        if len(partial) > 1:
            return None, None, _build_candidates(screener_df.index.isin(partial.index))

        # 5) 그래도 못 찾으면 철자가 비슷한 종목명을 유사도 기반으로 추천
        close = difflib.get_close_matches(query, names.tolist(), n=8, cutoff=0.55)
        if close:
            return None, None, _build_candidates(names.isin(close))

    # 스크리너 캐시에서 못 찾았으면 네이버 실시간 검색으로 대체 (별칭이 있으면 실제 이름으로 검색)
    live_candidates = search_naver_stock_by_name(alias_target or query)
    if len(live_candidates) == 1:
        return live_candidates[0]["code"], live_candidates[0]["name"], []
    if len(live_candidates) > 1:
        return None, None, live_candidates

    return None, None, []

def render_fnguide():
    st.header(
        "기업 재무 분석",
        help="""💡 **[기업 재무 분석 안내]**\n\n특정 종목의 상세한 재무 상태를 분석합니다.\n\nFnGuide 기반의 최신 연간/분기 실적 흐름, 매출 및 이익 성장률(YoY, QoQ), 증권사 목표주가 컨센서스와 통합 AI 종합 진단 결과를 한눈에 확인할 수 있습니다."""
    )
    st.markdown("<hr style='margin: 10px 0 25px 0; border-color: #E5E7EB;'>", unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1.6, 1, 3.4])
    with col1:
        query = st.text_input(
            "종목코드 또는 종목명 입력",
            placeholder="예: 005930, 삼성전자",
            label_visibility="collapsed",
            key="fnguide_query_input"
        )
    with col2:
        search_btn = st.button("🔍 조회", use_container_width=True)

    if search_btn and query:
        resolved_code, resolved_name, candidates = resolve_stock_query(query)
        st.session_state.pop('fnguide_not_found', None)
        if candidates:
            st.session_state['fnguide_candidates'] = candidates
        elif resolved_code:
            st.session_state['fnguide_code'] = resolved_code
            st.session_state.pop('fnguide_candidates', None)
        else:
            st.session_state.pop('fnguide_candidates', None)
            st.session_state['fnguide_not_found'] = query

    if st.session_state.get('fnguide_candidates'):
        candidates = st.session_state['fnguide_candidates']
        options = [f"{c['name']} ({c['code']}) · {c['market']}" for c in candidates]
        col_pick, col_pick_btn, _ = st.columns([2.6, 1, 3.4])
        with col_pick:
            picked = st.selectbox(
                "검색 결과가 여러 건입니다. 종목을 선택해주세요.",
                options,
                label_visibility="visible",
                key="fnguide_pick_select"
            )
        with col_pick_btn:
            st.markdown("<div style='height:28px;'></div>", unsafe_allow_html=True)
            if st.button("이 종목 조회", use_container_width=True, key="fnguide_pick_confirm"):
                picked_idx = options.index(picked)
                st.session_state['fnguide_code'] = candidates[picked_idx]['code']
                st.session_state.pop('fnguide_candidates', None)
                st.rerun()

    if st.session_state.get('fnguide_not_found'):
        st.warning(f"'{st.session_state['fnguide_not_found']}'에 해당하는 종목을 찾을 수 없습니다. 정확한 종목명 또는 6자리 종목코드로 다시 검색해주세요.")

    active_code = st.session_state.get('fnguide_code', '')

    if active_code:
        code = active_code
        cache_key = f'fnguide_result_{code}'
        
        if search_btn or cache_key not in st.session_state:
            with st.spinner("에프앤가이드(FnGuide) 서버에서 데이터를 분석 중입니다..."):
                _info = fetch_company_info_fnguide(code)
                _df_annual, _, _ = fetch_fnguide_data(code)
                _per_ai, _pbr_ai, _roe_ai, _debt_ai, _drop_pct_ai, _div_ai = get_ai_diagnosis_inputs(code, _df_annual)
                st.session_state[cache_key] = {
                    'info': _info,
                    'per_ai': _per_ai, 'pbr_ai': _pbr_ai, 'roe_ai': _roe_ai,
                    'debt_ai': _debt_ai, 'drop_pct_ai': _drop_pct_ai, 'div_ai': _div_ai,
                }

        cached = st.session_state[cache_key]
        info        = cached['info']
        per_ai      = cached['per_ai']
        pbr_ai      = cached['pbr_ai']
        roe_ai      = cached['roe_ai']
        debt_ai     = cached['debt_ai']
        drop_pct_ai = cached['drop_pct_ai']
        div_ai      = cached['div_ai']

        draw_fnguide_details(code)

        if info['name'] != "알 수 없음":
            st.markdown("<hr style='margin:20px 0 16px 0; border-color:#E5E7EB;'>", unsafe_allow_html=True)
            st.markdown("<h4 style='font-size:16px; margin-bottom:0;'>🤖 AI 종목 진단</h4>", unsafe_allow_html=True)
            render_ai_diagnosis(info['name'], code, per_ai, pbr_ai, roe_ai, debt_ai, drop_pct_ai, div_ai, "")
            if drop_pct_ai == 0.0 and div_ai == 0.0:
                st.caption("ℹ️ 배당수익률·52주 하락률은 '종목 스크리너' 탭에서 전체 데이터를 한 번 불러온 종목에 한해 정확히 반영됩니다. (해당 데이터가 없으면 0으로 처리되어 점수가 보수적으로 나올 수 있습니다)")

        # ── 분할매수 전략 계산기 ─────────────────────────────────────────────
        st.markdown("<hr style='margin:24px 0 16px 0; border-color:#E5E7EB;'>", unsafe_allow_html=True)
        st.markdown("<h4 style='font-size:16px; margin-bottom:4px;'>📐 분할매수 전략 계산기</h4>", unsafe_allow_html=True)
        st.markdown(
            "<p style='font-size:12px; color:#64748B; margin-bottom:16px;'>"
            "1차 진입가(또는 현재가)를 입력하면 펀더멘털·낙폭·이동평균 기반으로 "
            "2차·3차 진입가와 비중·평균단가·목표가·손절가를 자동 산출합니다.</p>",
            unsafe_allow_html=True,
        )

        # ── 비중 프리셋 선택 ──────────────────────────────────────────────────
        _PRESETS = {
            "🎯 공격형 (20:30:50)": (20, 30, 50),
            "⚖️ 표준형 (25:35:40)": (25, 35, 40),
            "🛡️ 보수형 (20:40:40)": (20, 40, 40),
            "✏️ 직접 입력":          None,
        }
        preset_choice = st.radio(
            "분할 비중 프리셋",
            list(_PRESETS.keys()),
            index=0,
            horizontal=True,
            key=f"preset_{code}",
            help="3차까지 하락이 왔을 때 실탄이 가장 많은 공격형(20:30:50)이 평균단가 절감 효과가 가장 큽니다."
        )

        if _PRESETS[preset_choice] is not None:
            _pw1, _pw2, _pw3 = _PRESETS[preset_choice]
        else:
            _pc1, _pc2, _pc3 = st.columns(3)
            with _pc1:
                _pw1 = st.number_input("1차 비중 (%)", min_value=5, max_value=60, value=20, step=5, key=f"w1_{code}")
            with _pc2:
                _pw2 = st.number_input("2차 비중 (%)", min_value=5, max_value=60, value=30, step=5, key=f"w2_{code}")
            with _pc3:
                _pw3 = st.number_input("3차 비중 (%)", min_value=5, max_value=60, value=50, step=5, key=f"w3_{code}")
        
            _total = _pw1 + _pw2 + _pw3
            if _total != 100:
                st.warning(f"⚠️ 비중 합계가 {_total}%입니다. 합계가 100%가 되도록 조정해주세요.")

        # ── 실시간 현재가 자동 세팅 ─────────────────────────────────────────
        @st.cache_data(ttl=60, show_spinner=False)
        def _fetch_cur_price_for_fill(stock_code):
            try:
                url = f"https://m.stock.naver.com/api/stock/{stock_code}/basic"
                r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=5)
                d = r.json()
                return int(str(d.get("closePrice", "0")).replace(",", ""))
            except Exception:
                return 0

        _auto_price = _fetch_cur_price_for_fill(code)
        _entry1_key = f"entry1_{code}"
        if search_btn or _entry1_key not in st.session_state:
            if _auto_price > 0:
                st.session_state[_entry1_key] = _auto_price

        # ── 입력 영역 (진입가 / 주 개수 / 총 투자금액 / 계산 버튼) ─────────
        _c1, _c2, _c3, _c4 = st.columns([1.2, 1, 1, 1.6])
        with _c1:
            entry1_input = st.number_input(
                "1차 진입가 (원)",
                min_value=0, max_value=10_000_000,
                value=st.session_state.get(_entry1_key, 0),
                step=100,
                key=_entry1_key,
                help="실시간 현재가가 자동으로 입력됩니다. 직접 수정도 가능합니다.",
                format="%d",
            )
        with _c2:
            _shares_key = f"shares_text_{code}"
            _shares_raw = st.text_input(
                "주 개수 (주)",
                value="",
                placeholder="예: 100",
                key=_shares_key,
                help="1차 진입 시 매수할 주 수를 입력하세요.",
            )
            try:
                shares_input = max(0, int(_shares_raw.replace(",", "").strip()))
            except Exception:
                shares_input = 0
        with _c3:
            _total_invest = entry1_input * shares_input
            st.markdown(
                f"<div style='padding-top:4px;'>"
                f"<div style='font-size:12px; color:#64748B; margin-bottom:4px;'>총 투자금액</div>"
                f"<div style='font-size:20px; font-weight:800; color:#1E293B; letter-spacing:-0.5px;'>"
                f"{_total_invest:,}"
                f"<span style='font-size:13px; font-weight:600; color:#64748B;'>원</span>"
                f"</div>"
                f"<div style='font-size:10px; color:#94A3B8; margin-top:2px;'>"
                f"{entry1_input:,}원 × {shares_input:,}주"
                f"</div></div>",
                unsafe_allow_html=True,
            )
        with _c4:
            st.markdown("<div style='padding-top:22px;'>", unsafe_allow_html=True)
            calc_btn = st.button("🧮 전략 계산", key=f"calc_btn_{code}", use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)

        budget_won_from_shares = _total_invest

        if calc_btn and entry1_input > 0:
            _per_ai      = cached['per_ai']
            _pbr_ai      = cached['pbr_ai']
            _roe_ai      = cached['roe_ai']
            _debt_ai     = cached['debt_ai']
            _drop_pct_ai = cached['drop_pct_ai']
            _div_ai      = cached['div_ai']

            _target_raw = info.get("target", "")
            _target_price = 0
            try:
                _target_price = int(re.sub(r"[^\d]", "", str(_target_raw)))
            except Exception:
                _target_price = 0

            _cur_price = _fetch_cur_price_for_fill(code)
            e1 = entry1_input

            e2_fixed = round(e1 * 0.92)
            e3_fixed = round(e1 * 0.83)

            e2_drop, e3_drop = 0, 0
            if _drop_pct_ai != 0.0:
                e2_drop = round(e1 * 0.90)
                e3_drop = round(e1 * 0.80)

            e2_fund, e3_fund = 0, 0
            if _pbr_ai > 0 and _cur_price > 0:
                _bps_est = _cur_price / _pbr_ai
                e2_fund = round(_bps_est * 1.0)
                e3_fund = round(_bps_est * 0.8)

            @st.cache_data(ttl=300, show_spinner=False)
            def _fetch_ma(stock_code):
                try:
                    import yfinance as yf
                    df_ma = call_with_timeout(
                        lambda: yf.Ticker(f"{stock_code}.KS").history(period="90d", interval="1d", timeout=8),
                        timeout=10,
                    )
                    if df_ma is None or df_ma.empty:
                        df_ma = call_with_timeout(
                            lambda: yf.Ticker(f"{stock_code}.KQ").history(period="90d", interval="1d", timeout=8),
                            timeout=10,
                        )
                    if df_ma is None or df_ma.empty:
                        return 0, 0
                    closes = df_ma["Close"].dropna()
                    ma20 = round(closes.tail(20).mean()) if len(closes) >= 20 else 0
                    ma60 = round(closes.tail(60).mean()) if len(closes) >= 60 else 0
                    return ma20, ma60
                except Exception:
                    return 0, 0

            _ma20, _ma60 = _fetch_ma(code)

            def _wavg(*candidates, weights=None):
                vals = [(v, w) for v, w in zip(candidates, weights or [1]*len(candidates)) if v > 0]
                if not vals:
                    return 0
                return round(sum(v * w for v, w in vals) / sum(w for _, w in vals))

            entry2 = _wavg(e2_fixed, e2_drop, e2_fund, _ma20, weights=[1.5, 1.0, 2.0, 1.5])
            if entry2 == 0: entry2 = e2_fixed
            entry3 = _wavg(e3_fixed, e3_drop, e3_fund, _ma60, weights=[1.0, 1.5, 2.5, 1.5])
            if entry3 == 0: entry3 = e3_fixed

            if entry2 >= e1:
                entry2 = e2_fixed
            if entry3 >= entry2:
                entry3 = round(entry2 * 0.92)

            W1, W2, W3 = _pw1, _pw2, _pw3
            _w_total = W1 + W2 + W3
            if _w_total <= 0:
                _w_total = 100
            avg_price = round((e1 * W1 + entry2 * W2 + entry3 * W3) / _w_total)

            if _target_price > 0:
                target_price = _target_price
                expected_ret = round((_target_price / avg_price - 1) * 100, 1)
                target_src   = "증권사 컨센서스"
            else:
                if _per_ai > 0:
                    target_price = round(e1 * (15.0 / _per_ai))
                elif _pbr_ai > 0 and _cur_price > 0:
                    target_price = round((_cur_price / _pbr_ai) * 1.3)
                else:
                    target_price = round(avg_price * 1.25)
                expected_ret = round((target_price / avg_price - 1) * 100, 1)
                target_src   = "PER 15× 추정" if _per_ai > 0 else ("PBR 1.3× 추정" if _pbr_ai > 0 else "평균단가 +25%")

            stop_loss_basic = round(entry3 * 0.90)
            stop_loss_pbr   = round(_cur_price / _pbr_ai * 0.7) if (_pbr_ai > 0 and _cur_price > 0) else 0
            
            stop_loss = max(stop_loss_basic, stop_loss_pbr) if stop_loss_pbr > 0 else stop_loss_basic
            if stop_loss >= entry3:
                stop_loss = stop_loss_basic

            _shares_1st = shares_input
            _w_total_s  = W1 + W2 + W3 if (W1 + W2 + W3) > 0 else 100
            _shares_2nd = round(_shares_1st * W2 / W1) if W1 > 0 else 0
            _shares_3rd = round(_shares_1st * W3 / W1) if W1 > 0 else 0

            def _fmt_shares_amt(price, sh):
                if sh <= 0 or price <= 0: return "-", "-"
                return f"{sh:,}주", f"{sh * price:,}원"

            budget_won = budget_won_from_shares
            sh1, am1 = _fmt_shares_amt(e1,     _shares_1st)
            sh2, am2 = _fmt_shares_amt(entry2, _shares_2nd)
            sh3, am3 = _fmt_shares_amt(entry3, _shares_3rd)

            def _basis_tags(fixed, drop, fund, ma):
                tags = []
                if fixed > 0: tags.append(f"고정%({fixed:,})")
                if drop  > 0: tags.append(f"낙폭({drop:,})")
                if fund  > 0: tags.append(f"PBR({fund:,})")
                if ma    > 0: tags.append(f"MA({ma:,})")
                return " · ".join(tags)

            basis2    = _basis_tags(e2_fixed, e2_drop, e2_fund, _ma20)
            basis3    = _basis_tags(e3_fixed, e3_drop, e3_fund, _ma60)
            ret_color = "#16A34A" if expected_ret >= 0 else "#DC2626"
            ret_sign  = "+" if expected_ret >= 0 else ""

            if shares_input > 0:
                _sh1_html = f'<div style="font-size:11px; color:#6366F1; margin-top:4px;">{sh1} · {am1}</div>'
                _sh2_html = f'<div style="font-size:11px; color:#475569; margin-top:4px;">{sh2} · {am2}</div>'
                _sh3_html = f'<div style="font-size:11px; color:#475569; margin-top:4px;">{sh3} · {am3}</div>'
            else:
                _sh1_html = '<div style="font-size:10px; color:#94A3B8; margin-top:6px;">📌 주 개수를 입력하면<br>수량·금액이 표시됩니다</div>'
                _sh2_html = ""
                _sh3_html = ""

            _e2_pct = f"{round((entry2/e1-1)*100,1):+.1f}%"
            _e3_pct = f"{round((entry3/e1-1)*100,1):+.1f}%"
            _card_html = (
                f'''<div style="background:#F8FAFC; border:1px solid #E2E8F0; border-radius:10px; padding:20px; margin-top:8px;">
              <div style="display:flex; gap:10px; margin-bottom:16px;">
                <div style="flex:1; background:#EEF2FF; border:1.5px solid #5A4EE5; border-radius:8px; padding:12px; text-align:center;">
                  <div style="font-size:11px; color:#5A4EE5; font-weight:700; margin-bottom:4px;">1차 진입 · {W1}%</div>
                  <div style="font-size:18px; font-weight:800; color:#3730A3;">{e1:,}원</div>'''
                + _sh1_html +
                f'''</div>
                <div style="flex:1; background:#FFFFFF; border:1px solid #CBD5E1; border-radius:8px; padding:12px; text-align:center;">
                  <div style="font-size:11px; color:#475569; font-weight:700; margin-bottom:4px;">2차 진입 · {W2}%</div>
                  <div style="font-size:18px; font-weight:800; color:#0F172A;">{entry2:,}원</div>
                  <div style="font-size:10px; color:#94A3B8; margin-top:3px;">({_e2_pct})</div>'''
                + _sh2_html +
                f'''</div>
                <div style="flex:1; background:#FFFFFF; border:1px solid #CBD5E1; border-radius:8px; padding:12px; text-align:center;">
                  <div style="font-size:11px; color:#475569; font-weight:700; margin-bottom:4px;">3차 진입 · {W3}%</div>
                  <div style="font-size:18px; font-weight:800; color:#0F172A;">{entry3:,}원</div>
                  <div style="font-size:10px; color:#94A3B8; margin-top:3px;">({_e3_pct})</div>'''
                + _sh3_html +
                '''</div>
              </div>'''
            )
            st.markdown(_card_html + f"""
              <div style="display:flex; gap:10px; margin-bottom:16px;">
                <div style="flex:1; background:#F1F5F9; border-radius:8px; padding:10px 12px;">
                  <div style="font-size:11px; color:#64748B; margin-bottom:3px;">📊 예상 평균단가</div>
                  <div style="font-size:16px; font-weight:700; color:#1E293B;">{avg_price:,}원</div>
                  <div style="font-size:10px; color:#94A3B8;">3차 완료 기준 ({W1}:{W2}:{W3} 비중)</div>
                </div>
                <div style="flex:1; background:#F0FDF4; border-radius:8px; padding:10px 12px;">
                  <div style="font-size:11px; color:#16A34A; margin-bottom:3px;">🎯 목표가 ({target_src})</div>
                  <div style="font-size:16px; font-weight:700; color:#15803D;">{target_price:,}원</div>
                  <div style="font-size:10px; color:{ret_color}; font-weight:600;">기대수익률 {ret_sign}{expected_ret}%</div>
                </div>
                <div style="flex:1; background:#FFF7F7; border-radius:8px; padding:10px 12px;">
                  <div style="font-size:11px; color:#DC2626; margin-bottom:3px;">🛑 손절가 제안</div>
                  <div style="font-size:16px; font-weight:700; color:#B91C1C;">{stop_loss:,}원</div>
                  <div style="font-size:10px; color:#94A3B8;">3차 진입가 대비 {round((stop_loss/entry3-1)*100,1):+.1f}%</div>
                </div>
              </div>
              <div style="background:#FFFFFF; border:1px solid #E2E8F0; border-radius:6px; padding:10px 14px; font-size:11px; color:#475569; line-height:1.9;">
                <b style="color:#1E293B;">📌 산출 근거</b><br>
                <b>2차 진입가</b> {entry2:,}원 — {basis2}<br>
                <b>3차 진입가</b> {entry3:,}원 — {basis3}<br>
                {'<b>이동평균</b> MA20 ' + f'{_ma20:,}원 / MA60 {_ma60:,}원<br>' if _ma20 > 0 else ''}
                {'<b>PBR 기반 장부가 추정</b> ' + f'BPS ≈ {round(_cur_price/_pbr_ai):,}원 (현재가 {_cur_price:,}원 ÷ PBR {_pbr_ai:.2f}×)<br>' if _pbr_ai > 0 and _cur_price > 0 else ''}
                <span style="color:#94A3B8; font-size:10px;">⚠️ 본 수치는 투자 참고용이며 매수·매도 추천이 아닙니다.</span>
              </div>
            </div>
            """, unsafe_allow_html=True)

            _prob_html2 = render_hit_probability_badge(code, None, target_price, target_src)
            if _prob_html2:
                st.markdown(_prob_html2, unsafe_allow_html=True)


def render_dividend():
    st.header(
        "실시간 배당 순위",
        help="""💡 **[실시간 배당 순위 안내]**\n\n현재 주가 기준으로 가장 배당 매력이 높은 종목들을 순위별로 보여줍니다.\n\n'정상만 보기' 필터를 켜면 리츠(REITs)나 배당성향이 비정상적으로 높은(100% 초과) 위험 종목을 자동으로 제외하여 건강한 고배당주를 찾기 쉽습니다."""
    )
    st.markdown("<hr style='margin: 10px 0 25px 0; border-color: #E5E7EB;'>", unsafe_allow_html=True)

    col_search, col_btn = st.columns([8, 1])
    with col_search:
        search_text = st.text_input(
            "검색",
            placeholder="종목명 또는 종목코드 검색",
            label_visibility="collapsed",
            key="dividend_search"
        )
    with col_btn:
        st.button("조회", key="dividend_search_btn", use_container_width=True)

    col_refresh, col_caption2, col_toggle = st.columns([1.5, 5, 1.5])
    with col_refresh:
        if st.button("데이터 새로고침"):
            fetch_dividend_ranking.clear()
            st.session_state["dividend_scanned"] = True
    with col_toggle:
        st.markdown("<div style='display:flex; justify-content:flex-end; align-items:center; padding-top:4px; width:100%;'>", unsafe_allow_html=True)
        st.toggle(
            "정상만 보기",
            value=True,
            key="dividend_clean_filter",
            help="리츠(REITs) / 배당성향 100% 초과 / 배당수익률 30% 초과 종목을 제외합니다."
        )
        st.markdown("</div>", unsafe_allow_html=True)

    # 🔧 [탭 이동 멈춤 대응] 예전에는 이 탭에 들어오기만 해도 fetch_dividend_ranking()이
    # 자동으로 실행되면서(네이버 배당 페이지 여러 장 병렬 스크래핑) 스크립트가 그 자리에서
    # 블로킹됐다. 그래서 다른 탭으로 빠르게 넘어가려 해도 이 스캔이 끝날 때까지 클릭이
    # 씹혔다. 이제는 사용자가 명시적으로 버튼을 눌러야만 스캔을 시작하도록 변경.
    if not st.session_state.get("dividend_scanned"):
        st.info("💡 아직 배당 데이터를 조회하지 않았습니다. 아래 버튼을 눌러 조회해주세요. (약 5~15초 소요)")
        if st.button("🔍 배당 데이터 조회", key="dividend_manual_scan_btn", type="primary"):
            st.session_state["dividend_scanned"] = True
            st.rerun()
        return

    df = run_with_progress("마켓 데이터 수집 중...", fetch_dividend_ranking)
    
    if not df.empty: 
        if isinstance(df.columns, pd.MultiIndex):
            new_cols = []
            for col in df.columns:
                valid_parts = [str(c).strip() for c in col if str(c).strip() and "Unnamed" not in str(c)]
                unique_parts = list(dict.fromkeys(valid_parts))
                new_cols.append(" ".join(unique_parts))
            df.columns = new_cols

        price_col = find_col(df, ["현재가"])
        if price_col:
            df[price_col] = pd.to_numeric(df[price_col].astype(str).str.replace(",", ""), errors="coerce")

        past_years = ["1년전", "2년전", "3년전"]
        for yr in past_years:
            col_name = find_col(df, [yr])
            if col_name and price_col:
                past_div_amount = pd.to_numeric(df[col_name].astype(str).str.replace(",", ""), errors="coerce").fillna(0)
                yield_col_name = f"{yr} 배당" 
                df[yield_col_name] = (past_div_amount / df[price_col]) * 100
                df = df.drop(columns=[col_name])

        drop_cols = [c for c in df.columns if re.search(r"과거.*배당금", str(c)) and not any(yr in str(c) for yr in past_years)]
        if drop_cols:
            df = df.drop(columns=drop_cols)

        df = df.loc[:, ~df.columns.duplicated()]

        # ✅ 1. 시장 정보 매핑 (스크리너 데이터 활용)
        screener_df = load_screener_df()
        if not screener_df.empty and '종목코드' in df.columns:
            market_map = dict(zip(screener_df['종목코드'], screener_df['시장']))
            df['시장'] = df['종목코드'].map(market_map).fillna("-")
        else:
            df['시장'] = "-"

        # ✅ 2. 컬럼 순서 재배치 (종목코드, 종목명, 시장 순서)
        cols = list(df.columns)
        name_col = find_col(df, ["종목명"])
        
        front_cols = []
        if '종목코드' in cols:
            front_cols.append('종목코드')
            cols.remove('종목코드')
        if name_col in cols:
            front_cols.append(name_col)
            cols.remove(name_col)
        if '시장' in cols:
            front_cols.append('시장')
            cols.remove('시장')
            
        df = df[front_cols + cols]

        clean_filter_val = st.session_state.get("dividend_clean_filter", True)
        if clean_filter_val:
            name_col = find_col(df, ["종목명"])
            REIT_KEYWORDS = r'리츠|REIT|reit|부동산투자|리얼티'
            if name_col:
                df = df[~df[name_col].astype(str).str.contains(REIT_KEYWORDS, regex=True, case=False, na=False)]
            payout_col = find_col(df, ["배당성향", "성향"])
            if payout_col:
                payout_num = pd.to_numeric(df[payout_col].astype(str).str.replace(r'[^\d.-]', '', regex=True), errors='coerce')
                df = df[payout_num.isna() | (payout_num <= 100)]
            yield_col = find_col(df, ["배당수익률", "수익률"])
            if yield_col:
                yield_num = pd.to_numeric(df[yield_col].astype(str).str.replace(r'[^\d.-]', '', regex=True), errors='coerce')
                df = df[yield_num.isna() | (yield_num <= 30)]

        if search_text:
            name_col = find_col(df, ["종목명"])
            code_col = find_col(df, ["종목코드", "코드"])
            mask = pd.Series([False] * len(df), index=df.index)
            if name_col:
                mask = mask | df[name_col].astype(str).str.contains(search_text, case=False, na=False)
            if code_col:
                mask = mask | df[code_col].astype(str).str.contains(search_text, case=False, na=False)
            df = df[mask]

        with col_caption2:
            st.markdown("<div style='padding-top:8px;'>", unsafe_allow_html=True)
            if search_text:
                st.caption(f"🔍 '{search_text}' 검색 결과 {len(df)}건")
            elif clean_filter_val:
                st.caption(f"ℹ️ 리츠·배당성향 100% 초과·수익률 30% 초과 종목 제외 후 {len(df)}건 표시 중")
            st.markdown("</div>", unsafe_allow_html=True)

        st.dataframe(get_styled_dataframe(df), use_container_width=True, hide_index=True)
    else: 
        st.error("데이터를 불러올 수 없습니다. 네이버 금융 서버 통신이 지연되고 있습니다.")

if __name__ == '__main__':
    main()