import os
import sys
import json
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
import secrets
import difflib
import numpy as np
import faulthandler
from itertools import zip_longest

# ── 전역 소켓 기본 타임아웃 ──────────────────────────────────────────────
# gspread(Google Sheets API)처럼 자체적으로 timeout 파라미터를 노출하지 않는
# 라이브러리가 있어서, requests/yfinance 호출마다 timeout=을 넣는 것만으로는
# 모든 네트워크 호출을 다 방어할 수 없었다. Python 소켓 레벨에서 기본 타임아웃을
# 걸어두면, 코드에서 개별적으로 timeout을 안 준 소켓 통신도(= 위 라이브러리들 포함)
# 이 값을 넘기면 예외를 던지고 빠져나온다. 각 함수는 이미 try/except로
# 감싸져 있어서, 이 타임아웃이 걸려도 앱이 멈추는 대신 "빈 결과"로 정상 진행된다.
socket.setdefaulttimeout(20)
# ────────────────────────────────────────────────────────────────────────

# ── [워치독 근본 수정] dump_traceback_later는 프로세스 전역(세션 공유) 알람이다 ──
# faulthandler.dump_traceback_later(N) / cancel_dump_traceback_later()는 파이썬
# 공식 문서상 스레드별/세션별이 아니라 "프로세스 전체에서 딱 하나만 존재하는"
# 전역 알람이다 — 두 번째 호출은 첫 번째 예약을 그냥 덮어쓰고 취소한다.
# Streamlit은 한 프로세스 안에서 여러 세션(여러 탭·사용자)의 스크립트를 동시에
# 실행하고, 폴링 fragment는 0.4초마다 자기 몫의 dump_traceback_later(8)→cancel을
# 반복한다. 그래서 어떤 세션이 실제로 멈춰서 45초/60초짜리 워치독을 걸어놔도,
# 곧바로 다른 세션(또는 다른 fragment 틱)이 그 전역 알람을 덮어쓰고 즉시
# 취소해버려서, 진짜로 멈춰도 트레이스백이 "절대" 안 찍히는 상태였다.
# (지금까지 "워치독에도 안 찍힌다"던 미스터리의 정체 — 실측 로그로 확인됨:
# "페이지 진입" 로그만 있고 "페이지 렌더링 완료"가 끝내 안 찍혔는데도 dump가 없었음)
#
# 해결: 여기저기서 개별적으로 걸었다 취소했다 하지 않는다. 프로세스가 시작될 때
# 딱 한 번, repeat=True로 영구 등록한다. 이러면 몇 초마다 무조건(멈췄든 안
# 멈췄든) 그 순간 살아있는 모든 스레드의 파이썬 콜스택을 stderr(Streamlit Cloud
# 로그)에 찍는다. 평소엔 로그가 조금 시끄러워지지만, 실제로 멈추는 순간의
# 스택트레이스를 이번엔 확실히 잡아낼 수 있다. 원인을 특정한 뒤에는 interval을
# 늘리거나(예: 120초) 제거해도 된다.
faulthandler.dump_traceback_later(30, repeat=True, file=sys.stderr)
# ────────────────────────────────────────────────────────────────────────

# ── 백그라운드 스레드(ThreadPoolExecutor)에서도 안전하게 쓸 수 있는 디버그 정보 저장소 ──
# st.session_state는 스크립트를 실행하는 메인 스레드 밖(예: 관심종목 페이지의 병렬 조회
# 워커 스레드)에서 접근하면 Streamlit 공식 문서상 지원되지 않으며, 실제로 이로 인해
# 스크립트 실행이 멈춰버리는(무한 로딩) 문제가 발생했다. 단순 dict 대입/조회는 CPython의
# GIL 덕분에 스레드에서 안전하므로, 디버그용 정보는 여기로 옮겨서 저장한다.
#
# ── [버그 수정: 매 rerun마다 내용이 사라지던 문제] ──────────────────────────
# Streamlit은 상호작용(버튼 클릭, st.rerun() 등)이 있을 때마다 스크립트 파일 전체를
# 처음부터 다시 실행한다. 그런데 이 저장소가 그냥 `_DEBUG_STORE = {}`처럼 모듈
# 최상단에 평범한 전역 변수로 선언되어 있으면, 이 줄 자체도 매 rerun마다 다시
# 실행되어 매번 새로운 빈 딕셔너리로 초기화된다. 반면 백그라운드 스레드는 자신이
# "시작될 때(=이전 rerun)의 객체"를 계속 붙잡고 쓰기 때문에, 다음 rerun에서 메인
# 스크립트가 읽는 객체와 실제로 값이 쓰이는 객체가 서로 다른 개체가 되어버려
# 값이 항상 비어 보이는 문제가 있었다. 아래 스레드풀들(get_shared_executor 등)과
# 동일하게 @st.cache_resource로 감싸서, 이 줄이 매 rerun마다 다시 실행되더라도
# 항상 "동일한" 딕셔너리 객체를 돌려받도록 고친다.
# (⚠️ @st.cache_resource는 streamlit을 import한 뒤에만 쓸 수 있어서, 실제 정의는
# 파일 아래쪽 "import streamlit as st" 직후로 옮겨뒀다. 여기서는 아직 st가 없으므로
# 나중에 정의될 함수 이름만 미리 적어둔다.)
_DEBUG_STORE = None       # ← import streamlit as st 직후 블록에서 실제 값 채움
_SCREENER_DF_CACHE = None  # ← import streamlit as st 직후 블록에서 실제 값 채움

def _set_shared_screener_df(df):
    """스크리너 결과 df를 session_state(메인 스레드용)와 모듈 캐시(백그라운드 스레드용)에 동시 반영."""
    _SCREENER_DF_CACHE["df"] = df
    try:
        st.session_state['shared_screener_df'] = df
    except Exception:
        pass  # 백그라운드 스레드 등 session_state 접근이 불가능한 상황이면 모듈 캐시만 사용

# ==== 🚀 [테마 강제 고정 로직] ====
# ── [세션/프로세스 전체 프리징의 진짜 근본 원인 수정] ─────────────────────────
# 문제: Streamlit은 상호작용(버튼 클릭, 탭 이동, rerun 등) 때마다 이 .py 파일
# 전체를 처음부터 다시 실행한다. 그런데 이 블록이 무조건 매번
# .streamlit/config.toml을 새로 "쓰기"(open(..., "w"))만 하고 있어서, 내용이
# 완전히 똑같더라도 파일의 mtime이 매 렌더링마다 갱신됐다. Streamlit 자체의
# 파일 감시 스레드(polling_path_watcher)가 이걸 "설정이 바뀌었다"고 매번 감지해서
# config를 다시 읽어들이는데(get_config_options() 내부에서 락을 잡음), 하필 메인
# 스크립트 실행 스레드도 스크립트를 컴파일하는 단계(get_bytecode → magic 처리 →
# config.get_option)에서 같은 config 락이 필요하다. 두 스레드가 이 락을 두고
# 계속 경합하다 보니(rerun이 잦을수록, 특히 0.4초 폴링 fragment까지 겹치면 경합
# 빈도가 급증), 결국 메인 스크립트 스레드가 이 지점에서 영원히 멈추는 상태가
# 됐다 — 워치독 스택 덤프에서 정확히 이 지점(streamlit/config.py:222 get_option)
# 이 잡혔다. config 락은 세션별이 아니라 프로세스 전역이라, 한번 물리면 그 뒤로는
# 어떤 세션의 어떤 rerun도 이 지점을 통과하지 못해 "리붓 아니면 답이 없는" 상태가
# 된 것 — 지금까지 겪은 프리징 중 가장 근본적인 원인이었을 가능성이 높다.
# 해결: 내용이 실제로 다를 때만 쓴다. 최초 1회(또는 실제로 테마를 바꿀 때)만
# 디스크에 쓰기가 발생하고, 그 이후의 모든 rerun에서는 파일을 건드리지 않으므로
# Streamlit 파일 감시자가 "변경 없음"으로 보고 config 재로딩 자체가 트리거되지
# 않는다.
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
    _existing_theme_config = None
    if os.path.exists(config_path):
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                _existing_theme_config = f.read()
        except Exception:
            _existing_theme_config = None

    if _existing_theme_config != theme_config:
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
from streamlit.runtime.scriptrunner import add_script_run_ctx, get_script_run_ctx

# ── _DEBUG_STORE / _SCREENER_DF_CACHE 실제 정의 (st 임포트 직후) ──────────────
# 위쪽(파일 상단)에서 이 두 이름을 None으로 미리 선언해둔 이유는 @st.cache_resource가
# streamlit이 import된 뒤에만 쓸 수 있기 때문이다. 여기서 진짜 값을 채운다.
@st.cache_resource(show_spinner=False)
def _get_debug_store():
    return {}

_DEBUG_STORE = _get_debug_store()

@st.cache_resource(show_spinner=False)
def _get_screener_df_cache():
    return {"df": None}

_SCREENER_DF_CACHE = _get_screener_df_cache()

# =========================
# ⚙️ 페이지 설정
# =========================
st.set_page_config(page_title="Inventory Manager", page_icon="📦", layout="wide")

# ── [임시 디버그 스위치] 대시보드 "종목 스캔" 기능 끄기 ──────────────────────
# 코스피/코스닥 수급 토글에서 멈추는 문제의 원인을 좁히기 위한 임시 조치.
# True로 두면 대시보드 스캔 버튼이 비활성화되고 실제 스캔 잡(오케스트레이션
# 스레드+공유 스레드풀 사용)이 전혀 시작되지 않는다. 원인 파악 후 다시 False로
# 되돌리면 된다. (추천종목/스크리너 페이지의 스캔은 영향 없음 — 필요하면 같이 끌 수 있음)
DEBUG_DISABLE_DASHBOARD_SCAN = False

# =========================
# 🕸️ 데이터 처리 엔진
# =========================
def normalize_kr_code(code):
    return re.sub(r"\D", "", str(code)).zfill(6)[:6]

# ── [버그 수정: 빈/NaN 종목코드가 "000000"으로 변환되며 발생하는 불필요한 API 호출] ──
# 종목코드가 비어있거나 NaN인 행이 normalize_kr_code()를 거치면 숫자가 하나도
# 없어 zfill(6)로 "000000"이 된다. 이 값은 실제로 존재하지 않는 종목코드라서
# yfinance/네이버/FnGuide에 매번 요청을 보내봤자 항상 404/빈 응답으로 실패한다
# (로그에 반복적으로 찍히던 "$000000.KS: No data found" / "HTTP Error 404"가
# 이 케이스다). 아래 헬퍼로 각 fetch 함수 진입 시점에 조기 차단해 헛된 네트워크
# 왕복을 없앤다.
def _is_invalid_kr_code(code):
    return code == "000000"

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
def _get_shared_executor_raw():
    return concurrent.futures.ThreadPoolExecutor(max_workers=32)

# ── [자가치유] 스레드풀이 "좀비 스레드"로 완전히 막힌 채 오래 지속되면 자동 교체 ──
# 문제: future.result(timeout=X)는 메인 스크립트가 기다리는 시간만 제한할 뿐,
# 실제로 멈춘 워커 스레드 자체를 강제로 죽이지는 못한다(파이썬은 실행 중인 스레드를
# 강제 종료할 방법이 없다). curl_cffi/requests 소켓이 완전히 멎어버리는 극단적인
# 경우, 그 스레드는 풀 안에 죽은 채로 영원히 자리만 차지하게 된다. 이게 쌓여 풀의
# max_workers를 전부 채우면, 그 순간부터 이 풀에 던져지는 모든 새 작업은 (다른
# 세션이 새로고침을 해도) 영원히 대기하게 된다 — 이게 "리붓 아니면 답이 없는" 멈춤의
# 진짜 정체다. 해결: 풀의 모든 워커가 꽉 차 있고(active >= max_workers) 큐에도
# 대기 작업이 쌓여있는(queued > 0) 상태가 STUCK_THRESHOLD초 이상 지속되면, 그 풀을
# 통째로 새 걸로 교체한다. 죽은 스레드들은 백그라운드에 버려두고 새 작업은 새 풀로
# 보내지므로, 수동 리붓 없이도 자동으로 회복된다.
@st.cache_resource(show_spinner=False)
def _get_pool_stuck_tracker():
    return {}

def _get_or_heal_executor(raw_getter, name, max_workers, stuck_threshold=25):
    ex = raw_getter()
    try:
        active = len(ex._threads)
        queued = ex._work_queue.qsize()
    except Exception:
        return ex

    tracker = _get_pool_stuck_tracker()
    now = time.time()

    # ── [자가치유 무한 리셋 버그 수정] ──────────────────────────────────
    # 기존에는 "queued > 0"이 아니면 곧바로 tracker.pop()으로 감시 타이머를
    # 지워버렸다. 그런데 풀이 좀비 스레드로 완전히 막힌 뒤 사용자가 클릭을
    # 멈추면(=새로 쌓이는 대기 작업이 없어짐) queued는 0으로 떨어지지만,
    # active는 여전히 max_workers에 머물러 있다(죽은 스레드가 슬롯을 영원히
    # 점유). 이 순간 매번 타이머가 리셋되어 stuck_threshold에 절대 도달하지
    # 못했고, 그 결과 자가치유가 사실상 한 번도 발동하지 않았다(=수동 리붓
    # 이외에는 회복 방법이 없는 상태로 이어진 진짜 원인).
    # 해결: "막힘 의심" 타이머는 active가 max_workers 밑으로 내려왔을 때만
    # (=실제로 워커가 하나라도 정상적으로 반환됐을 때만) 리셋한다. queued가
    # 일시적으로 0이 되는 것만으로는 리셋하지 않는다. 단, 타이머 시작 자체는
    # 여전히 queued > 0인 순간(=진짜로 밀린 대기열이 있었던 순간)에만 건다 —
    # 그래야 "일시적으로 바쁘기만 한" 정상 상태를 오탐하지 않는다.
    if active >= max_workers:
        since = tracker.get(name)
        if since is None:
            if queued > 0:
                tracker[name] = now
        elif now - since >= stuck_threshold:
            print(f"[POOL HEAL {datetime.datetime.now().strftime('%H:%M:%S')}] "
                  f"'{name}' 풀이 {stuck_threshold}초 이상 완전히 막혀있어 새 풀로 교체함 "
                  f"(active={active}, queued={queued})", file=sys.stderr, flush=True)
            raw_getter.clear()
            tracker.pop(name, None)
            ex = raw_getter()
    else:
        tracker.pop(name, None)

    return ex

def get_shared_executor():
    return _get_or_heal_executor(_get_shared_executor_raw, "공유", 32)

# ── [진짜 원인] ScriptRunContext 없이 st.cache_data 함수를 스레드에서 호출하면
# 세션이 통째로 영원히 멈춘다 ─────────────────────────────────────────────────
# 문제: fetch_market_index_table/fetch_investor_trend/fetch_sparkline_data/
# fetch_sector_ranking/fetch_investor_trend_monthly는 전부 @st.cache_data가
# 붙어있는데, 이걸 순수 ThreadPoolExecutor 워커 스레드(=Streamlit의
# ScriptRunContext가 없는 스레드) 안에서 직접 호출하고 있었다. st.cache_data는
# 동일 캐시 키로 동시에 여러 번 불리면 "먼저 온 호출이 계산하는 동안 나머지는
# 내부 락으로 대기"하는 구조인데, 이 락 대기에는 우리가 건 어떤
# overall_timeout/per_result_timeout도 적용되지 않는다. ScriptRunContext가 없는
# 스레드에서 이 내부 락/캐시 로직이 꼬이면 해당 세션의 스크립트 실행 스레드가
# 말 그대로 영원히 멈춘다 — 서버 프로세스(healthz)는 멀쩡하니 다른 세션에는
# 영향이 없고, 딱 그 세션만 "리붓 아니면 답이 없는" 상태가 된다(로그로 실측 확인:
# healthz는 200을 계속 내려주는데 그 세션의 "페이지 렌더링 완료" 로그만 다시는
# 안 찍힘).
# 해결: 메인 스레드(=정상적인 ScriptRunContext를 가진 스레드)에서 submit하는
# 시점에 현재 컨텍스트를 캡처해서, 실제로 워커 스레드에서 실행될 때 그 스레드에
# 컨텍스트를 심어준다. Streamlit 공식 문서가 권장하는 "백그라운드 스레드에서
# Streamlit API(캐시 포함)를 쓰려면 add_script_run_ctx로 컨텍스트를 넘겨야 한다"
# 패턴을 그대로 적용한 것.
def _run_with_ctx(_ctx, _fn, *_args, **_kwargs):
    try:
        add_script_run_ctx(threading.current_thread(), _ctx)
    except Exception:
        pass  # 컨텍스트를 못 붙이더라도 최소한 함수 자체는 시도한다
    try:
        return _fn(*_args, **_kwargs)
    finally:
        # ── [워커 스레드 재사용 시 컨텍스트 오염 방지] ──────────────────────
        # 공유/오케스트레이션 풀은 워커 스레드를 계속 재사용한다. 작업이 끝난
        # 뒤 이 스레드에 심어둔 ScriptRunContext를 지우지 않으면, 다음번에 이
        # 물리 스레드가 (완전히 다른 세션의) 다른 작업을 처리할 때 방금 세션의
        # 컨텍스트가 그대로 남아있게 되어 세션 간 상태가 뒤섞일 수 있다.
        try:
            add_script_run_ctx(threading.current_thread(), None)
        except Exception:
            pass

def submit_with_ctx(executor, fn, *args, **kwargs):
    """executor.submit(fn, *args, **kwargs)와 동일하지만, 호출 시점(메인 스레드)의
    ScriptRunContext를 워커 스레드에 심어준 채로 실행한다. st.cache_data가 붙은
    함수를 스레드풀에 던질 때는 반드시 이 함수를 통해서만 던져야 한다."""
    ctx = get_script_run_ctx()
    return executor.submit(_run_with_ctx, ctx, fn, *args, **kwargs)

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
def _get_yf_safety_executor_raw():
    return concurrent.futures.ThreadPoolExecutor(max_workers=16)

def get_yf_safety_executor():
    return _get_or_heal_executor(_get_yf_safety_executor_raw, "yf안전", 16)

def call_with_timeout(fn, timeout=10):
    future = get_yf_safety_executor().submit(fn)
    try:
        return future.result(timeout=timeout)
    except Exception:
        future.cancel()
        return None
# ────────────────────────────────────────────────────────────────────────

# ── [AI 종합점수 내부 호출 병렬화 전용 풀 — 2026-08-18] ─────────────────────
# 문제: calc_ai_scores_detailed()는 종목 1개당 가격이력·코스피 스파크라인·재무·
# 수급, 이렇게 4번의 네트워크 호출이 필요하다. 이 함수 자체가 이미
# get_shared_executor()의 워커 스레드 안(=AI 점수 배치, _score_one_for_ai_batch)
# 에서 돌고 있으므로, 예전에 이 4개를 병렬화하려고 다시 get_shared_executor()에
# 던졌더니 워커가 서로를 기다리는 순환대기(교착)가 발생해 순차 호출로 되돌렸었다
# (아래 calc_ai_scores_detailed 주석 참고). 그 결과 종목 1개 계산 시간이 4개
# 호출의 "합"이 되어(각각 몇 초씩만 걸려도 금방 10초를 넘김), AI 점수 일괄
# 계산/점수 구간 필터가 눈에 띄게 느려지는 근본 원인이 됐다(실측: 배치당
# overall_timeout 10초 안에 거의 매번 0~소수건만 완료되고 나머지는 버려진 뒤
# 다음 배치에서 처음부터 다시 시도되는 낭비가 반복됨).
# 해결: get_yf_safety_executor()와 똑같은 이유로 공유 풀과 완전히 무관한
# 별도 풀을 새로 둔다. 배치당 최대 30종목 × 4호출 = 최대 120개까지 동시에
# 밀려들 수 있어서, yf_safety(16개, 다른 용도로도 쓰임)를 같이 쓰면 그쪽이
# 병목이 될 수 있어 이 용도 전용으로 더 넉넉하게(40개) 분리했다.
@st.cache_resource(show_spinner=False)
def _get_ai_score_inner_executor_raw():
    return concurrent.futures.ThreadPoolExecutor(max_workers=40)

def get_ai_score_inner_executor():
    return _get_or_heal_executor(_get_ai_score_inner_executor_raw, "AI점수내부", 40)
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
def _get_orchestration_executor_raw():
    return concurrent.futures.ThreadPoolExecutor(max_workers=8)

def get_orchestration_executor():
    return _get_or_heal_executor(_get_orchestration_executor_raw, "오케스트레이션", 8)

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
                        poll_interval=0.4, overall_timeout=20, result_ttl=45):
    """
    job_key     : 이 로딩 작업을 구분하는 고유 문자열 키 (페이지마다 겹치지 않게 지정)
    submit_fn() : 인자 없이 호출하면 {"이름": future, ...} 형태의 dict를 반환해야 함
                  (예: lambda: {"indices": executor.submit(fetch_market_index_table), ...})
    collect_fn(futures_dict) : 완료된(또는 일부만 완료된) futures_dict를 받아
                  실제 결과 dict로 변환하는 함수. future.done()이 False인 항목은
                  건드리지 말고 default 값으로 채워서 반환할 것.
    default_result : 아직 하나도 준비 안 됐을 때 렌더링에 쓸 기본값
    result_ttl  : 마지막으로 성공한 결과를 몇 초까지 그대로 재사용할지 (초)
    반환값: (result, ready)
      - ready=False  → 아직 로딩 중. 호출부는 이 시점에 바로 return 해서
                        이후의 무거운 렌더링을 건너뛰어야 한다.
      - ready=True   → 완료(또는 상한시간 초과). result를 바로 사용하면 된다.
    """
    jobs = st.session_state.setdefault("_bg_jobs", {})
    job = jobs.get(job_key)

    # ── [수급 추이 토글 등에서 매번 "불러오는 중..."으로 되돌아가는 문제 수정] ──
    # 이전에는 작업이 한 번 끝나면 바로 jobs.pop()으로 지워버렸다. 그런데 이 페이지
    # 안의 아주 사소한 상호작용(예: 투자자별 수급 추이 펼치기/접기 버튼)도 Streamlit
    # 입장에서는 "스크립트 전체를 처음부터 다시 실행"이라서, job이 없어진 상태로 매번
    # 여기 다시 도달해 4개 API를 새 비동기 작업으로 또 던지고, 최소 한 번의 폴링
    # 주기 동안 화면 전체가 "불러오는 중..." 문구로 바뀌어버렸다(실제 데이터는
    # st.cache_data로 이미 캐싱돼 있어 다시 받아올 필요가 없었는데도).
    # 해결: 마지막으로 성공한 결과를 session_state에 타임스탬프와 함께 남겨두고,
    # result_ttl 이내에 다시 여기 도달하면(=job이 없어도) 그 결과를 바로 재사용해서
    # 불필요한 재조회/로딩 화면 깜빡임 자체를 건너뛴다.
    results_cache = st.session_state.setdefault("_bg_job_results", {})
    if job is None:
        cached = results_cache.get(job_key)
        if cached is not None and (time.time() - cached["ts"]) < result_ttl:
            return cached["result"], True
        job = {"futures": submit_fn(), "started_at": time.time(), "overall_timeout": overall_timeout}
        jobs[job_key] = job

    futures = job["futures"]
    all_done = all(f.done() for f in futures.values())
    timed_out = (time.time() - job["started_at"]) > job.get("overall_timeout", overall_timeout)

    if not all_done and not timed_out:
        # ── [fragment #10719 회피] 여기서 직접 st.fragment(run_every=...)를 만들지
        # 않는다. 대신 job을 _bg_jobs에 남겨둔 채로 그냥 "아직" 상태를 반환하면,
        # _main_impl()이 페이지 렌더링 뒤 호출하는 maybe_run_global_poller() →
        # 세션 전체에 단 하나뿐인 _global_poll_fragment()가 이 job을 포함해 모든
        # 대기 중인 작업을 함께 감시한다. spinner_text는 더 이상 여기서 개별
        # 표시하지 않고, 전역 fragment가 진행 중인 작업 개수로 통합해서 보여준다.
        return default_result, False

    # 다 끝났거나 상한 시간을 넘김 → 끝난 것만 회수, 안 끝난 항목은 collect_fn이
    # 알아서 기본값으로 채우도록 한다.
    result = collect_fn(futures)
    for f in futures.values():
        if not f.done():
            f.cancel()  # 이미 실행 중이면 취소는 안 되지만, 큐 대기중이었다면 자리를 비워준다
    jobs.pop(job_key, None)
    results_cache[job_key] = {"result": result, "ts": time.time()}  # 짧은 재사용 캐시 갱신
    return result, True

# ── [근본 수정] 세션당 폴링 fragment는 딱 1개만 존재하도록 통합 ──────────────
# Streamlit 확인된 버그(#10719, github.com/streamlit/streamlit/issues/10719):
# run_every로 자동 재실행되는 fragment가 세션 안에 2개 이상 동시에 존재하면,
# 어느 한쪽의 재실행 타이밍이 겹치는 순간 "The fragment with id ... does not
# exist anymore" 예외와 함께 그 세션이 죽는다. 이건 파이썬 스레드가 블로킹되는
# 게 아니라 Streamlit 프레임워크 내부에서 예외로 죽는 것이라, faulthandler
# 워치독(멈춘 스레드의 콜스택을 찍는 도구)에는 아무것도 안 잡힌다 — 애초에
# "멈춰있는" 스레드가 없기 때문이다.
#
# 기존에는 render_async_multi/run_unified_market_scan_async를 호출하는 곳마다
# 각자 자기 fragment를 만들었다(대시보드 카드, 수급 추이 토글, 관심종목 프리페치,
# 전체 스캔 등). 이 중 두 곳이 동시에 "아직 완료 안 됨" 상태가 되면(예: 스캔이
# 도는 중에 관심종목 탭으로 이동) 정확히 이 버그 조건이 만들어졌다. 일부 호출부는
# "스캔 중이면 건너뛰기" 식으로 개별 가드를 달아뒀지만, 새 호출부가 생길 때마다
# 매번 수동으로 챙겨야 해서 재발 여지가 있었다(실제로 대시보드 자체 데이터 로딩과
# 관심종목 프리페치 두 곳은 이 가드가 빠져 있었다).
#
# 해결: fragment 정의/호출 지점을 앱 전체에서 이 함수 하나로 통합한다. 개별
# render_async_multi/run_unified_market_scan_async 호출은 더 이상 자기 fragment를
# 만들지 않고, 그저 "완료 안 됨" 상태만 반환한다. 실제 폴링은 _main_impl()이
# 페이지 렌더링을 마친 뒤 딱 한 번 호출하는 이 전역 fragment가 _bg_jobs와
# _scan_jobs를 전부 훑어서 담당한다 — 물리적으로 fragment 호출이 세션당 항상
# 최대 1개이므로, 정의상 #10719가 재현될 수 없다.
def _all_jobs_settled():
    """_bg_jobs / _scan_jobs에 아직 완료되지도, 시간 초과되지도 않은 작업이
    하나라도 남아있으면 False."""
    now = time.time()
    for job in list(st.session_state.get("_bg_jobs", {}).values()):
        futures = job.get("futures", {})
        done = all(f.done() for f in futures.values())
        timed_out = (now - job["started_at"]) > job.get("overall_timeout", 20)
        if not done and not timed_out:
            return False
    for job in list(st.session_state.get("_scan_jobs", {}).values()):
        future = job.get("future")
        if future is None:
            continue
        timed_out = (now - job["started_at"]) > job.get("overall_timeout", 150)
        stalled = (now - job.get("_last_pct_change_at", job["started_at"])) > 50
        if not future.done() and not timed_out and not stalled:
            return False
    return True


@st.fragment(run_every=0.4)
def _global_poll_fragment():
    """세션 전체를 통틀어 유일하게 존재하는 주기적(run_every) fragment.
    _bg_jobs/_scan_jobs에 대기 중인 작업이 하나라도 있을 때만 main()에서 호출된다.
    ⚠️ 여기서 dump_traceback_later를 걸었다 취소하면 안 된다 — 이 함수는 0.4초마다
    실행되므로, 그러면 파일 상단에서 걸어둔 영구(repeat=True) 워치독을 계속
    덮어쓰고 취소해버려서 다른 세션이 실제로 멈췄을 때 트레이스백이 안 찍히게 된다
    (실제로 이게 원인이었던 사례가 있었다)."""
    if _all_jobs_settled():
        st.rerun()  # 전부 끝남 → 전체 rerun으로 실제 데이터를 반영
    else:
        n_bg = len(st.session_state.get("_bg_jobs", {}))
        scan_jobs = st.session_state.get("_scan_jobs", {})
        # 스캔이 진행 중이면(가장 오래 걸리는 작업이므로) 그 실시간 %를 우선 보여준다.
        # 이게 없으면 사용자 입장에서 게이지가 그대로 멈춘 것처럼 보인다.
        scan_pct_text = ""
        for job in scan_jobs.values():
            _job_id = job.get("job_id")
            _st = _SCAN_JOB_STATE.get(_job_id, {})
            if _st:
                scan_pct_text = f" — {_st.get('text', '스캔 중...')} ({min(_st.get('pct', 0), 100)}%)"
            break
        n_total = n_bg + len(scan_jobs)
        st.info(f"🔄 데이터를 불러오는 중... (백그라운드 작업 {n_total}건 진행 중){scan_pct_text}")


# ── [탭 이동 중 "끊겼다 됐다" 반복 현상 수정] 다른 페이지 소유의 완료된 job 정리 ──
# 문제: render_async_multi()가 만드는 _bg_jobs 항목(dashboard_main_data,
# investor_monthly_kospi/kosdaq, watchlist_prefetch_new)은 전부 "그 job을 만든
# 페이지 코드가 다시 실행돼야만" collect_fn이 호출되어 jobs.pop()으로 치워진다.
# 그런데 사용자가 그 job이 끝나기도 전에 다른 탭으로 이동해버리면, 백그라운드
# 스레드는 계속 돌다가 실제로 끝나긴 하지만(future.done()=True), 그걸 수거해줄
# 페이지 코드는 더 이상 실행되지 않는다(사용자가 그 페이지를 안 보고 있으므로).
# 그 결과 _bg_jobs에는 "이미 끝났는데 아무도 안 치운" job이 계속 남아있게 되고,
# 전역 폴링 fragment(_global_poll_fragment)는 이걸 볼 때마다 "설정=끝남"으로
# 판단해서 현재 보고 있는 페이지가 무엇이든 상관없이 즉시 st.rerun()을 반복
# 호출한다 — 이게 로그에서 보이는 "다른 탭으로 갔는데 계속 순간순간 다시
# 그려지는" 현상의 정체다 (실제로 렌더링은 매번 정상 완료되지만, 사용자가
# 아무 것도 안 눌렀는데 초당 몇 번씩 전체 스크립트가 다시 실행됨).
# 해결: 각 job_key가 "원래 어느 페이지 소유인지" 매핑해두고, 현재 페이지가
# 그 소유 페이지가 "아니면서" 이미 다 끝난 job을 발견하면, 여기서 대신 조용히
# 치워버린다(collect_fn 없이 그냥 버림 → 해당 페이지를 나중에 다시 열면 캐시가
# 없으니 한 번 더 새로 조회하게 됨 — 약간의 재조회 비용은 있지만 무한 반복보다
# 훨씬 낫다). 현재 페이지가 실제 소유 페이지인 job은 절대 건드리지 않는다 —
# 그건 이 함수가 호출되기 "전에" 이미 그 페이지 자신의 render_async_multi() 호출이
# 정상적으로 수거해갔어야 하는 게 원칙이고, 안 끝났다면 여전히 폴링이 필요하다.
_BG_JOB_OWNER_PAGES = {
    "dashboard_main_data": {"대시보드 홈"},
    "investor_monthly_kospi": {"대시보드 홈"},
    "investor_monthly_kosdaq": {"대시보드 홈"},
    "watchlist_prefetch_new": {"관심종목"},
}
# ⚠️ [버그 수정] 추천 종목 탭의 AI 점수/유동성 배치는 job_key가 매 배치마다
# 종목코드 조합으로 달라지는 동적 키(reco_ai_batch_005930-000660-..., 등)라서
# 위의 고정 키 딕셔너리로는 등록할 수 없었다. 그래서 이 job들이 소유 페이지
# 매핑에서 계속 빠진 채로 남아있었고, 사용자가 '추천 종목'을 벗어나도 정리가
# 안 돼서 전역 폴러가 다른 페이지에 있을 때까지 계속 재실행을 유발했다(실측:
# 대시보드/기업 재무 분석 등 무관한 페이지에서까지 재실행 스팸 + 스레드풀
# 반복 고갈 + 화면 전환 시 이전 페이지 DOM이 덜 지워지는 것으로 추정되는
# 렌더링 겹침 현상). 접두사 기반으로도 소유 페이지를 판별하도록 확장했다.
_BG_JOB_OWNER_PREFIXES = {
    "reco_ai_batch_": {"추천 종목"},
}
_SCAN_JOB_OWNER_PAGES = {"대시보드 홈", "종목 스크리너"}


def _bg_job_owners(key):
    """job_key의 소유 페이지 집합을 반환한다. 고정 키 딕셔너리에 없으면 접두사
    매칭도 시도한다. 둘 다 없으면 None(소유 페이지를 모름 → 안전하게 보존)."""
    owners = _BG_JOB_OWNER_PAGES.get(key)
    if owners is not None:
        return owners
    for prefix, prefix_owners in _BG_JOB_OWNER_PREFIXES.items():
        if key.startswith(prefix):
            return prefix_owners
    return None


def _bg_job_key_is_dynamic(key):
    """job_key가 _BG_JOB_OWNER_PREFIXES(예: reco_ai_batch_)로 매칭되는
    '동적 키'인지 여부. 동적 키는 매 배치(종목 조합)마다 값이 달라지므로,
    한 번 제출된 job은 그 정확한 조합이 다시 필요해지지 않는 한 같은 페이지
    안에서도 두 번 다시 같은 key로 조회되지 않을 수 있다(예: AI 등급 필터를
    "전체보기"↔"700점대"로 바꾸면 매번 다른 종목 조합=다른 key가 만들어짐).
    반면 _BG_JOB_OWNER_PAGES의 고정 키(dashboard_main_data 등)는 그 페이지가
    열려 있는 한 매 rerun마다 항상 '같은' key로 다시 조회되도록 설계돼 있다."""
    return any(key.startswith(prefix) for prefix in _BG_JOB_OWNER_PREFIXES)


def _purge_orphaned_jobs():
    current = st.session_state.get("current_page")

    bg_jobs = st.session_state.get("_bg_jobs")
    if bg_jobs:
        for key in list(bg_jobs.keys()):
            owners = _bg_job_owners(key)
            if owners is None:
                continue  # 소유 페이지를 모름 → 안전하게 보존
            # ⚠️ [버그 수정 2026-08-18] 동적 키(reco_ai_batch_* 등)는 소유 페이지가
            # 지금 열려 있어도 정리 대상이다. render_async_multi()는 매 rerun마다
            # "지금 필요한 종목 조합"으로 job_key를 새로 계산하기 때문에, 필터가
            # 바뀌는 순간 이전 배치의 key는 그 페이지 안에서도 두 번 다시 조회되지
            # 않는다. 그 결과 이미 완료된(all futures done) 이전 배치 job이
            # _bg_jobs에 영원히 남아, _all_jobs_settled()가 계속 "settled=True"로
            # 오판 → _global_poll_fragment가 0.4초마다 무한 st.rerun()을 던져
            # 화면이 계속 깜빡였다(로그: done=300/300 still_loading=False인데도
            # rerun이 초당 반복). 이 시점에 남아있는 동적 키 job은, 같은 회차의
            # render_async_multi 호출이 이미 자기 몫은 알아서 수거(pop)한 뒤이므로,
            # 남아있다는 것 자체가 곧 아무도 다시 찾지 않는 orphan이라는 뜻이라
            # 소유 페이지 여부와 무관하게 지워도 안전하다.
            # 고정 키(대시보드/관심종목 등) job은 기존대로 소유 페이지가 지금
            # 열려있으면 건드리지 않는다 — 그 페이지 코드가 다음 rerun에 같은
            # key로 다시 찾아와 정상 수거할 것이기 때문이다.
            if not _bg_job_key_is_dynamic(key) and current in owners:
                continue
            futures = bg_jobs[key].get("futures", {})
            if futures and all(f.done() for f in futures.values()):
                bg_jobs.pop(key, None)

    scan_jobs = st.session_state.get("_scan_jobs")
    if scan_jobs and current not in _SCAN_JOB_OWNER_PAGES:
        for key in list(scan_jobs.keys()):
            job = scan_jobs[key]
            future = job.get("future")
            if future is not None and future.done():
                scan_jobs.pop(key, None)
                _SCAN_JOB_STATE.pop(job.get("job_id"), None)
# ────────────────────────────────────────────────────────────────────────


def maybe_run_global_poller():
    """_main_impl()의 페이지 디스패치 직후 딱 한 번 호출. 대기 중인 백그라운드
    작업이 있을 때만 전역 폴링 fragment를 띄운다(없으면 아무것도 안 함)."""
    _purge_orphaned_jobs()
    has_pending = bool(st.session_state.get("_bg_jobs")) or bool(st.session_state.get("_scan_jobs"))
    if has_pending:
        _global_poll_fragment()
# ────────────────────────────────────────────────────────────────────────

# ── [스캔 완료 후 AI 점수 자동 일괄 계산 — 페이지 이동과 무관하게 이어짐] ──────
# 문제: AI 점수 배치 계산 코드(_render_ai_grade_filter_and_score)는 원래
# render_recommendations() 안에서만 호출된다. 그런데 스캔 직후 사용자는 보통
# 대시보드에 그대로 머물러 있지, 곧바로 추천 종목 탭을 열지는 않는다. 그
# 상태로는 배치를 "제출"하는 코드 자체가 한 번도 실행되지 않아서, 스캔이
# 끝나도 AI 점수 계산은 사용자가 실제로 추천 종목 탭을 열기 전까지 그냥
# 멈춰있는 것처럼 보인다.
# 해결: 페이지 디스패치 직후(어느 페이지에 있든) 이 함수를 호출해서, 진행
# 플래그(_reco_ai_bulk_scan)만 보고 배치 제출/수거를 계속 이어가게 한다.
# 추천 종목 탭이 열려 있을 때는 render_recommendations() 자신이 이미 이
# 경로를 처리하므로, 같은 스크립트 실행 안에서 여기서 또 부르면 캐시가
# 갱신된 직후의 서로 다른 배치가 중복 제출될 수 있어(1회 실행당 최대 2배
# 네트워크 요청) 호출부에서 "추천 종목 탭이 아닐 때만" 이 함수를 부르도록
# 가드한다 (_main_impl 참고).
def maybe_kickoff_ai_bulk_scan():
    if not st.session_state.get('_reco_ai_bulk_scan'):
        return
    _reco_df = load_reco_df()
    if _reco_df.empty:
        # 후보 자체가 없어졌으면(스캔 결과 없음 등) 더 계산할 게 없으니 끈다.
        st.session_state['_reco_ai_bulk_scan'] = False
        return
    _, _still_loading, _, _, _ = _render_ai_grade_filter_and_score(_reco_df, _reco_df)
    if not _still_loading:
        st.session_state['_reco_ai_bulk_scan'] = False
# ────────────────────────────────────────────────────────────────────────

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
            # 가격/등락률은 원래부터 안정적으로 동작하던 /basic 엔드포인트를 그대로 사용.
            url = f"https://m.stock.naver.com/api/index/{meta['symbol']}/basic"
            res = requests.get(url, headers=headers, timeout=6)
            data = res.json()
            price = float(str(data.get("closePrice", "0")).replace(",", ""))
            diff = float(str(data.get("compareToPreviousClosePrice", "0")).replace(",", ""))
            diff_pct = float(str(data.get("fluctuationsRatio", "0")).replace(",", ""))
            sign = "+" if diff >= 0 else ""

            # ── [거래량 수정] ──────────────────────────────────────────────────
            # /basic 엔드포인트는 애초에 accumulatedTradingVolume 필드를 안 내려줘서
            # 거래량이 항상 N/A였다. 거래량은 realtime 엔드포인트에서 "별도로" 시도하되,
            # 이 호출이 실패하더라도(예: Referer 체크, 응답 형식 차이 등) 위에서 이미
            # 받아온 가격/등락률까지 통째로 날아가지 않도록 완전히 분리된 try/except로
            # 감싼다. 실패하면 그냥 거래량만 N/A로 남고 카드의 나머지는 정상 표시된다.
            vol = "N/A"
            try:
                vol_headers = {**headers, "Referer": "https://m.stock.naver.com/"}
                vol_url = f"https://polling.finance.naver.com/api/realtime/domestic/index/{meta['symbol']}"
                vres = requests.get(vol_url, headers=vol_headers, timeout=6)
                vpayload = vres.json()
                vdatas = vpayload.get("datas") or []
                vdata = vdatas[0] if vdatas else {}
                # accumulatedTradingVolume 필드는 "301,494천주"처럼 "천주" 단위 접미사가
                # 붙은 표시용 문자열이라, 접미사 없는 순수 숫자 문자열인
                # accumulatedTradingVolumeRaw 필드를 우선 사용한다.
                vol_raw = vdata.get("accumulatedTradingVolumeRaw", None)
                if vol_raw in (None, ""):
                    vol_raw = re.sub(r"[^\d]", "", str(vdata.get("accumulatedTradingVolume", ""))) or None
                if vol_raw:
                    vol = f"{int(str(vol_raw).replace(',', '')):,}"
            except Exception:
                pass  # 실패하면 거래량만 N/A로 남고 카드의 나머지(가격/등락률)는 정상 표시

            return key, {
                "name": meta["name"], "subtitle": meta["subtitle"],
                "value": f"{price:,.2f}",
                "change": f"{sign}{diff:,.2f}",
                "change_pct": f"{sign}{diff_pct:.2f}%",
                "status": "up" if diff > 0 else ("down" if diff < 0 else "neutral"),
                "volume": vol,
            }
        except Exception as e:
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

# ── [로그인 직후 스파크라인 차트가 가끔 통째로 빈 상태로 굳어버리는 문제 수정] ────
# 문제: fetch_sparkline_data()는 st.cache_data(ttl=86400)로 하루 종일 캐싱되는데,
# 야후 파이낸스(yfinance) 쪽이 그 순간 일시적으로 느리거나(클라우드 IP 레이트리밋 등)
# df.empty로 응답하면 그 종목은 빈 리스트([])로 채워지고, 그 "빈 결과"가 그대로
# 하루 종일 캐싱되어버린다. 그래서 로그인 시점에 우연히 한 번 실패한 지수(코스피/
# 코스닥 등)는 캐시가 갱신되는 다음 날까지 계속 차트 없는 카드로 보였다.
# 해결: (1) 첫 시도가 실패하면 짧게 대기 후 한 번 더 재시도해서 순간적인 실패 자체를
# 줄이고, (2) 그래도 실패하면 완전히 빈 값 대신 "마지막으로 성공했던 값"을 대신
# 돌려준다. _SPARKLINE_LAST_GOOD은 모듈 전역(프로세스 생존 기간 동안 유지)이라,
# 한 번이라도 성공한 적이 있는 종목은 이후 일시적 실패가 있어도 차트가 비어 보이지
# 않는다(값이 하루 정도 오래된 것일 수는 있지만, 완전히 비는 것보다 훨씬 낫다).
#
# ── [버그 수정] ── 이것도 평범한 전역 변수였다면 위 _DEBUG_STORE와 똑같은 이유로
# 매 rerun마다 초기화되어 "마지막으로 성공했던 값"을 절대 기억하지 못했을 것이다.
# @st.cache_resource로 감싸 프로세스 생존 기간 동안 동일 객체가 유지되게 한다.
@st.cache_resource(show_spinner=False)
def _get_sparkline_last_good_store():
    return {}

_SPARKLINE_LAST_GOOD = _get_sparkline_last_good_store()

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
        for attempt in range(2):  # 일시적 실패 대비 1회 재시도
            try:
                import yfinance as yf
                df = yf.Ticker(symbol).history(period="180d", interval="1d", timeout=8)
                if not df.empty and "Close" in df.columns:
                    closes = df["Close"].dropna().tolist()
                    if closes:
                        _SPARKLINE_LAST_GOOD[key] = closes  # 성공한 값만 "마지막 성공값"으로 갱신
                        return key, closes
            except Exception:
                pass
            if attempt == 0:
                time.sleep(0.5)
        # 이번 조회가 끝내 실패했으면, 진짜 빈 리스트 대신 마지막 성공값으로 대체한다.
        # (한 번도 성공한 적이 없으면 어쩔 수 없이 빈 리스트 — 카드 자체는 정상 렌더링됨)
        return key, _SPARKLINE_LAST_GOOD.get(key, [])

    result = run_parallel_safe(
        lambda kv: get_history(kv[0], kv[1]), list(targets.items()),
        max_workers=6, overall_timeout=12, per_result_timeout=6,
    )
    # 실패/타임아웃난 종목은 마지막 성공값(없으면 빈 리스트)으로 채워서 카드가 항상 렌더링되게 함
    for k in targets.keys():
        result.setdefault(k, _SPARKLINE_LAST_GOOD.get(k, []))
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
            res.encoding = 'euc-kr'  # 네이버금융(finance.naver.com)은 euc-kr 고정 — apparent_encoding 추측에 의존하면 특정 종목명 바이트 패턴에서 오탐(예: 키릴 계열로 오판)해 파싱이 깨진다
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
            res.encoding = 'euc-kr'  # 네이버금융(finance.naver.com)은 euc-kr 고정 — apparent_encoding 추측에 의존하면 특정 종목명 바이트 패턴에서 오탐(예: 키릴 계열로 오판)해 파싱이 깨진다
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
    {"date": "2026-07-16", "rate": 2.75, "action": "인상 (+0.25%p)"},
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

    # ── [자동 라벨 판정] BOK_RATE_HISTORY 갱신을 깜빡해도 숫자만은 최신으로 보이게 ──
    # 실시간 조회는 "현재 금리 숫자"만 줄 뿐 "이번에 인상/인하/동결됐는지"는 알려주지
    # 않는다. 예전엔 그 라벨을 항상 하드코딩된 BOK_RATE_HISTORY에서만 가져왔는데,
    # 회의가 끝나고 이 리스트를 사람이 업데이트하는 걸 깜빡하면(2026-07-16 인상 건이
    # 실제로 그랬다) 화면에는 옛날 숫자+옛날 라벨이 함께 나왔다.
    # 지금은 실시간 숫자가 하드코딩된 최신 이력과 다르면, 그 차이를 보고 인상/인하
    # 라벨을 자동 계산해서 이력 맨 앞에 끼워넣는다. 다만 회의 날짜·정확한 %p폭 같은
    # 세부 정보는 모르므로 "(자동감지)"를 붙여 사람이 넣은 값과 구분한다.
    history = [
        {"date": h["date"], "range": f"{h['rate']:.2f}%", "action": h["action"]}
        for h in BOK_RATE_HISTORY[:10]
    ]
    latest_hardcoded = BOK_RATE_HISTORY[0]

    try:
        url = "https://m.stock.naver.com/api/index/IRR_BOK/basic"
        res = requests.get(url, headers=headers, timeout=8)
        data = res.json()
        rate_val = float(str(data.get("closePrice", "0")).replace(",", ""))
        date_str = str(data.get("localTradedAt", ""))[:10]
        dt = pd.to_datetime(date_str, errors="coerce")
        date_display = dt.strftime("%Y-%m-%d") if pd.notna(dt) else "최신"

        if rate_val > 0:
            diff = round(rate_val - latest_hardcoded["rate"], 2)
            if abs(diff) >= 0.01:
                auto_action = f"{'인상' if diff > 0 else '인하'} ({diff:+.2f}%p, 자동감지)"
                history = [{"date": date_display, "range": f"{rate_val:.2f}%", "action": auto_action}] + history
            return {"current": {"rate": f"{rate_val:.2f}%", "date": date_display}, "history": history}
    except Exception:
        pass

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
            res.encoding = 'euc-kr'  # 네이버금융(finance.naver.com)은 euc-kr 고정 — apparent_encoding 추측에 의존하면 특정 종목명 바이트 패턴에서 오탐(예: 키릴 계열로 오판)해 파싱이 깨진다
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
        res0.encoding = 'euc-kr'  # 네이버금융 euc-kr 고정
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

# 🔧 [FnGuide 신버전 대응] 모바일 페이지(m.comp.fnguide.com/m2/...)가 폐지되어
# PC용 comp.fnguide.com/SVO2/ 페이지로 전환하면서 쓰는 전용 헤더.
_FN_DESKTOP_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Referer': 'https://comp.fnguide.com/',
    'Accept-Language': 'ko-KR,ko;q=0.9',
}


def _parse_desktop_consensus(html_text):
    """wcomp.fnguide.com/CompanyInfo/Consensus 페이지 파싱.
    페이지 구조가 자주 바뀌므로, 특정 태그/클래스에 의존하지 않고
    '목표주가'/'투자의견' 같은 키워드 주변의 숫자·텍스트를 느슨하게 추출한다."""
    result = {"opinion": "", "opinion_score": "", "target": "", "analyst_count": ""}

    # 목표주가: '목표주가' 글자 뒤 400자 이내에 나오는 첫 콤마 포함 숫자(예: 95,000)
    target_search = re.search(r'목표주가.{0,400}?(\d{2,3}(?:,\d{3})+)', html_text, re.DOTALL)
    if target_search:
        try:
            tg = int(target_search.group(1).replace(',', ''))
            if tg > 0:
                result["target"] = f"{tg:,} 원"
        except ValueError:
            pass

    # 투자의견 점수: '투자의견' 근처의 'X.X' 형태 점수(5점 만점 기준)
    opinion_score_search = re.search(r'투자의견.{0,300}?([0-5]\.\d{1,2})', html_text, re.DOTALL)
    if opinion_score_search:
        try:
            op_val = float(opinion_score_search.group(1))
            if op_val > 0:
                result["opinion_score"] = f"{op_val:.1f} / 5.0"
                if op_val >= 4.5:   result["opinion"] = "🔥 강력매수"
                elif op_val >= 3.5: result["opinion"] = "👍 매수"
                elif op_val >= 2.5: result["opinion"] = "✋ 중립"
                elif op_val >= 1.5: result["opinion"] = "👎 매도"
                else:               result["opinion"] = "💀 강력매도"
        except ValueError:
            pass

    # 추정 증권사(기관) 수
    count_search = re.search(r'(\d+)\s*개\s*(?:증권사|기관)', html_text)
    if count_search:
        result["analyst_count"] = f"추정기관 {count_search.group(1)}곳"

    return result


def _parse_mobile_consensus(html_text):
    """m.comp.fnguide.com company_03.asp(컨센서스) 페이지 파싱. (구버전 폴백용으로만 유지)"""
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


# ── [종목명 폰트 깨짐 버그 수정 — 2026-08-06] ────────────────────────────────
# 문제: finance.naver.com 응답 인코딩을 이 파일 전체에서 'euc-kr'로 고정해왔다
# (apparent_encoding 자동감지가 특정 종목명 바이트 패턴을 키릴 계열 등으로
# 오판해서 파싱이 깨졌던 전례 때문). 그런데 제이브이엠(054950)처럼 페이지가
# 실제로는 utf-8로 내려오는 종목이 있으면, 그 utf-8 바이트를 euc-kr로 강제
# 디코딩하게 되어 일부 바이트 시퀀스만 우연히 유효한 euc-kr 글자로 맞아떨어지고
# (예: '대', '댁') 나머지는 디코딩 실패로 대체 문자(물음표)로 바뀐다 —
# "????대????댁??"처럼 일부만 정상, 일부는 깨지는 정확히 그 증상이다.
#
# 해결: 확률적 추측(chardet/apparent_encoding)에 기대는 대신, HTML 문서 자체가
# <meta charset="..."> 로 "명시적으로 선언한" 인코딩을 읽어서 그것만 신뢰한다.
# charset 선언 자체는 항상 ASCII 문자이므로, 원본 바이트를 latin-1로 읽어도
# (byte ↔ codepoint 1:1 매핑이라 절대 깨지지 않음) 그 선언값 자체는 안전하게
# 찾아낼 수 있다. 선언을 못 찾은 페이지(예전 방식의 낡은 페이지)만 기존과 동일하게
# euc-kr로 폴백한다 — 그래서 이전에 euc-kr 고정으로 고쳤던 종목들은 그대로
# euc-kr을 계속 쓰고, 실제로 utf-8인 페이지만 올바르게 utf-8로 디코딩된다.
def _decode_naver_html(res, fallback_encoding='euc-kr'):
    """requests 응답 바이트를, 통계적 추측이 아니라 문서가 선언한 charset을
    직접 읽어서 안전하게 str로 디코딩한다. 선언이 없으면 fallback_encoding 사용."""
    raw = res.content
    try:
        probe = raw[:2048].decode('latin-1', errors='ignore')
        m = re.search(r'charset\s*=\s*["\']?\s*([\w\-]+)', probe, re.IGNORECASE)
        declared = m.group(1).lower().replace('_', '-') if m else None
    except Exception:
        declared = None

    if declared in ('utf-8', 'utf8'):
        chosen = 'utf-8'
    elif declared in ('euc-kr', 'euckr', 'ks-c-5601-1987', 'cp949', 'ms949'):
        chosen = 'euc-kr'
    else:
        chosen = fallback_encoding  # 선언을 못 찾았거나 못 알아보는 값 → 안전한 기존 기본값 유지

    try:
        return raw.decode(chosen, errors='replace')
    except Exception:
        return raw.decode(fallback_encoding, errors='replace')


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
        # [종목명 폰트 깨짐 버그 수정] chardet 추측(apparent_encoding)도, 무조건
        # euc-kr 고정도 아니라 — 문서가 실제로 선언한 charset을 직접 읽는다.
        # 자세한 이유는 위 _decode_naver_html() 정의부 주석 참고.
        html = _decode_naver_html(res, fallback_encoding='euc-kr')
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

    # ② 투자의견 / 목표주가 컨센서스
    # 🔧 [FnGuide 신버전 대응] 기존에 쓰던 m.comp.fnguide.com/m2/company_03.asp는
    #    이미 폐지된 모바일 페이지였음(재무제표 소스를 wcomp.fnguide.com으로 옮길 때
    #    이 함수만 함께 옮기지 못해 계속 통신 오류가 나고 있었음, 2026-08 확인).
    #    재무제표와 동일 계열의 신버전 desktop URL(wcomp.fnguide.com/CompanyInfo/Consensus)로 전환.
    consensus = {"opinion": "", "opinion_score": "", "target": "", "analyst_count": ""}
    fetch_failed = False
    consensus_debug = {"code": code, "status": None, "resp_len": None, "code_in_html": None, "exception": None}
    try:
        fn_desktop_url = f"https://wcomp.fnguide.com/CompanyInfo/Consensus?cmp_cd={code}"
        res3 = requests.get(fn_desktop_url, headers=_FN_DESKTOP_HEADERS, timeout=8)
        res3.encoding = res3.apparent_encoding or 'utf-8'
        consensus_debug["status"] = res3.status_code
        consensus_debug["resp_len"] = len(res3.text)
        consensus_debug["code_in_html"] = (code in res3.text)
        # 차단(다른 종목 고정) 여부 확인: 요청 코드가 응답 안에 실제로 있는지 검증
        if code in res3.text:
            consensus = _parse_desktop_consensus(res3.text)
            consensus_debug["parsed"] = consensus
        else:
            fetch_failed = True
    except Exception as e:
        fetch_failed = True
        consensus_debug["exception"] = f"{type(e).__name__}: {e}"
    _DEBUG_STORE[f"_consensus_debug_{code}"] = consensus_debug

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
            res.encoding = 'euc-kr'  # 네이버금융(finance.naver.com)은 euc-kr 고정 — apparent_encoding 추측에 의존하면 특정 종목명 바이트 패턴에서 오탐(예: 키릴 계열로 오판)해 파싱이 깨진다
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


def _apply_forecast_valuation_fallback(rows, cols=('ROE', 'PER', 'PBR', '부채비율')):
    """마지막 기간이 아직 마감되지 않은 컨센서스 추정치('...(E)')인데 ROE/PER/PBR/
    부채비율이 원본 표 자체에 없어 NaN인 경우(분기별 컨센서스는 이 지표들을 아예
    제공하지 않는 경우가 흔함), 직전 실제(마감된) 기간의 값을 근사치로 이어서
    채운다. 어떤 항목이 이렇게 대체됐는지는 '_est_cols'(콤마 구분 문자열)에 남겨서
    화면에서 '≈'로 구분 표시할 수 있게 한다. 실제 값이 있으면 아무것도 건드리지
    않는다."""
    if not rows:
        return rows
    last = rows[-1]
    if '(E)' not in str(last.get('연도/분기', '')):
        for r in rows:
            r.setdefault('_est_cols', '')
        return rows
    filled = []
    for col in cols:
        if pd.isna(last.get(col)):
            for prev in reversed(rows[:-1]):
                if pd.notna(prev.get(col)):
                    last[col] = prev[col]
                    filled.append(col)
                    break
    for r in rows:
        r.setdefault('_est_cols', '')
    last['_est_cols'] = ','.join(filled)
    return rows


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

        # 연도/분기 라벨의 '(E)' 표기는 마지막에 붙여서 다시 표시하되, 마감 전
        # 기간 판별(_apply_forecast_valuation_fallback)은 원본 컬럼명 c로 판단해야
        # 하므로 라벨에도 그대로 남겨둔다(기존에는 여기서 지워서 아래 fallback이
        # 마감 전 기간을 인식하지 못했다).
        rows.append({
            '연도/분기': c,
            '매출액': rev, '영업이익': op, '당기순이익': ni,
            '영업이익률': op_margin, '순이익률': ni_margin,
            'ROE': roe, 'PER': per, 'PBR': pbr, '부채비율': debt_ratio,
        })

    # 🔧 [2026-08-06] 분기별 컨센서스에는 ROE/PER/PBR/부채비율 자체가 없는 경우가
    # 흔해서, 마감 전 마지막 기간은 직전 실제 값을 근사치로 이어서 채운다.
    rows = _apply_forecast_valuation_fallback(rows)
    for r in rows:
        r['연도/분기'] = r['연도/분기'].replace('(E)', '')

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
    final_cols.append('_est_cols')
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
    if _is_invalid_kr_code(code):
        return df_annual, df_quarter, df_dividend

    debug = {
        "code": code, "status": None, "resp_len": None,
        "code_in_html": None, "num_tables": None,
        "table_shapes": None, "exception": None,
        "income_a_index": None, "balance_a_index": None, "valuation_a_index": None,
        "income_a_columns": None, "period_cols_detected": None,
        "all_values_nan": None,
    }

    try:
        # 🔧 [FnGuide 신버전 대응 v2] SVD_Finance.asp(구 PC 버전)도 이미 폐지되어
        # 있었음("페이지가 없습니다"). 실제 신버전은 wcomp.fnguide.com 이고, 파라미터
        # 형식도 gicode=A{code} → cmp_cd={code}(A 접두어 없음)로 바뀌었다(2026-07-31 확인).
        # ⚠️ 이 URL이 SPA(자바스크립트 렌더링) 구조라면 requests.get()으로는 빈 뼈대만
        # 받아올 수 있다 — 이 경우 아래 code_in_html 체크가 다시 실패할 것이므로,
        # 디버그 정보로 확인 후 필요하면 실제 데이터 API 엔드포인트를 찾아야 한다.
        url = f"https://wcomp.fnguide.com/CompanyInfo/Finance?cmp_cd={code}"
        res = requests.get(url, headers=_FN_DESKTOP_HEADERS, timeout=10)
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
            # 🔧 [FnGuide 구조 변경 대응] 예전엔 항상 첫 번째 열(d.columns[0])이
            #    항목명(매출액/영업이익 등) 열이었지만, 2026-08 확인 결과 첫 번째 열이
            #    통째로 빈 칸(아이콘/토글용으로 추정)으로 바뀌어 전부 NaN이 되는 케이스가
            #    발생함 → 이 경우 실제 항목명은 두 번째 열 이후에 있음.
            #    그래서 첫 3개 열 중 "빈 값이 가장 적고 텍스트가 많은" 열을 자동으로 찾아
            #    그 열을 인덱스로 사용한다 (숫자만 있는 열은 항목명 열이 아니므로 제외 유도).
            try:
                ncols_to_check = min(3, len(d.columns))
                best_col, best_score = None, -1
                for c in d.columns[:ncols_to_check]:
                    col = d[c].dropna()
                    if col.empty:
                        score = 0
                    else:
                        # 숫자/퍼센트/콤마 형식이 아닌(=글자로 된) 값의 개수를 우선시
                        non_numeric = col.apply(
                            lambda v: not re.match(r'^-?[\d,\.]+%?$', str(v).strip())
                        ).sum()
                        score = non_numeric * 100 + len(col)
                    if score > best_score:
                        best_score, best_col = score, c
                if best_col is None or best_score <= 0:
                    return None
                return d.set_index(best_col)
            except Exception:
                return None

        def _has_rows(idf, keywords):
            if idf is None or idf.empty:
                return False
            idx_str = [str(x) for x in idf.index.tolist()]
            return all(any(kw in s for s in idx_str) for kw in keywords)

        indexed_dfs = [_indexed(d) for d in dfs]

        # 🔧 [진단 강화] 실패 시 원인을 바로 알 수 있도록, 매칭 성공 여부와 무관하게
        #    찾아낸 모든 표의 실제 행 이름(index)·컬럼명을 debug에 항상 남긴다.
        #    (이전에는 매칭에 실패하면 표 안에 실제로 뭐가 들어있었는지 전혀 알 수 없어
        #     원인 파악이 불가능했음 — 이 필드로 FnGuide 쪽 표기 변경 여부를 바로 확인)
        # 🔧 [진단 강화 v2 — 종목별 구조 차이 원인 규명] 기존에는 인덱싱 실패 시
        # "인덱싱 실패 또는 빈 표"라고만 남겨서, 왜 실패했는지(항목명 열이 다른
        # 위치에 있는 건지 / 값 형식이 다른 건지 / 진짜로 빈 표인지)를 알 방법이
        # 없었다. 특히 대부분 종목은 정상 조회되는데 특정 종목(예: 086280)만
        # 실패하는 경우, 문제는 사이트 전체 구조 변경이 아니라 그 종목 페이지만의
        # 표 레이아웃 차이일 가능성이 높은데도 그걸 확인할 로그가 없었다.
        # 지금은 _indexed()가 실패한 표에 한해 원본(raw, 인덱싱 전) dfs[_ti]의
        # 컬럼명과 앞부분 몇 행을 그대로 첨부한다 — 이러면 다음 실패 시 "표 자체가
        # 정말 비어있었는지" 아니면 "항목명 열이 예상 밖 위치/형식이었는지"를
        # 바로 구분할 수 있다.
        debug["all_tables_preview"] = []
        for _ti, _d in enumerate(indexed_dfs):
            if _d is None or _d.empty:
                _raw = dfs[_ti]
                try:
                    _raw_cols = [str(c) for c in _raw.columns.tolist()][:10]
                    _raw_head = _raw.head(6).astype(str).values.tolist()
                except Exception:
                    _raw_cols, _raw_head = None, None
                debug["all_tables_preview"].append({
                    "table_idx": _ti,
                    "note": "인덱싱 실패 또는 빈 표 (원본 raw 표 미리보기 첨부)",
                    "raw_shape": list(_raw.shape),
                    "raw_columns": _raw_cols,
                    "raw_head": _raw_head,
                })
                continue
            debug["all_tables_preview"].append({
                "table_idx": _ti,
                "shape": list(_d.shape),
                "columns": [str(c) for c in _d.columns.tolist()][:10],
                "index_preview": [str(x) for x in _d.index.tolist()][:20],
            })

        income_candidates = [d for d in indexed_dfs if _has_rows(d, ['매출액', '영업이익'])]
        balance_candidates = [d for d in indexed_dfs if _has_rows(d, ['부채', '자본'])]
        valuation_candidates = [d for d in indexed_dfs if _has_rows(d, ['ROE', 'PER(배)'])]

        # 🔧 [완화 매칭 폴백] 엄격한 AND 매칭(예: '매출액'과 '영업이익' 둘 다 필요)이
        #    실패하면, 키워드 하나만 있어도 되는 느슨한 조건으로 한 번 더 시도한다.
        #    FnGuide가 세부 표기(예: 'PER(배)' → 'PER')를 살짝 바꿔도 버티기 위함.
        #    엄격 매칭이 이미 성공한 경우에는 건드리지 않는다(기존 동작 유지).
        if len(income_candidates) < 2:
            loose = [d for d in indexed_dfs if _has_rows(d, ['매출액']) or _has_rows(d, ['영업이익'])]
            if len(loose) >= 2:
                income_candidates = loose
                debug["income_matched_via"] = "loose_fallback"
        if len(balance_candidates) < 2:
            loose = [d for d in indexed_dfs if _has_rows(d, ['자산총계']) or (_has_rows(d, ['부채']) and _has_rows(d, ['자본']))]
            if len(loose) >= 2:
                balance_candidates = loose
                debug["balance_matched_via"] = "loose_fallback"
        if not valuation_candidates:
            loose = [d for d in indexed_dfs if _has_rows(d, ['ROE']) or _has_rows(d, ['PER'])]
            if loose:
                valuation_candidates = loose
                debug["valuation_matched_via"] = "loose_fallback"

        debug["income_candidates_found"] = len(income_candidates)
        debug["balance_candidates_found"] = len(balance_candidates)
        debug["valuation_candidates_found"] = len(valuation_candidates)

        # 문서상 등장 순서 기준: 첫 번째 = 연간(연결), 두 번째 = 분기(연결)
        if len(income_candidates) < 2 or len(balance_candidates) < 2 or not valuation_candidates:
            debug["exception"] = "필요한 재무 표를 찾지 못함 (항목명 매칭 실패 - FnGuide 페이지 구조 변경 가능성). all_tables_preview를 확인하세요."
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
# 🟢 네이버 금융(WiseReport) 기반 재무 데이터 - FnGuide 폴백 소스
#
# 배경: FnGuide(wcomp.fnguide.com)가 SPA 렌더링 구조로 바뀌거나 페이지 구조가
# 바뀌면 fetch_fnguide_data()가 빈 결과를 반환한다. 네이버 금융이 종목 상세
# 페이지의 "기업실적분석" 표를 그릴 때 실제로 쓰는 소스는 FnGuide가 아니라
# NICE평가정보(WiseReport, navercomp.wisereport.co.kr)이며, 이쪽은 완전히
# 다른 시스템이라 FnGuide 장애와 무관하게 독립적으로 살아있을 가능성이 높다.
#
# 동작 방식: 이 엔드포인트는 페이지 로딩마다 새로 발급되는 encparam/id 토큰이
# 필요한 인증형 AJAX API다. 1) 먼저 c1010001.aspx 페이지를 GET해서 HTML 안에
# 박혀있는 토큰을 정규식으로 추출하고, 2) 그 토큰을 붙여 cF1001.aspx를 호출하면
# 연간 4개 기간 + 분기 4개 기간 데이터가 담긴 HTML 표 조각을 돌려준다.
#
# ⚠️ 응답 안에는 표가 2개 들어있는데, 첫 번째 표는 전부 동일한 가짜 숫자(예:
# "4,485")로 채워진 스크래핑 방지용 더미 표다. 반드시 항목명(매출액 등)이
# 실제로 들어있는 표를 찾아서 써야 한다 (실제 응답으로 확인 완료, 2026-07-31).
# =========================

_NAVER_WISE_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest',
    'Accept-Language': 'ko-KR,ko;q=0.9',
}

_NAVER_WISE_CORE_ITEMS = ['매출액', '영업이익', '당기순이익', '영업이익률', '순이익률',
                          'ROE', 'PER', 'PBR', '부채비율']


def _naver_wise_lookup(df, row_label, col):
    """df.loc[row_label, col]을 안전하게 숫자로 반환. 실패 시 NaN."""
    try:
        if row_label not in df.index or col not in df.columns:
            return float('nan')
        val = df.loc[row_label, col]
        if isinstance(val, pd.Series):  # 중복 인덱스 방어
            val = val.iloc[0]
        return pd.to_numeric(str(val).replace(',', '').strip(), errors='coerce')
    except Exception:
        return float('nan')


def _naver_wise_build_period_table(real_df, col_period_pairs, is_quarter):
    """네이버 WiseReport 표에서 연간/분기 한쪽 기간 그룹만 골라
    fetch_fnguide_data()와 동일한 컬럼 구조('연도/분기', 매출액, 성장률 등)로 조립.
    → 이 구조를 맞춰야 render_company_analysis 등 기존 렌더링 코드를 그대로 재사용 가능."""
    rows = []
    for col, period in col_period_pairs:
        rev = _naver_wise_lookup(real_df, '매출액', col)
        op = _naver_wise_lookup(real_df, '영업이익', col)
        op_margin = _naver_wise_lookup(real_df, '영업이익률', col)
        # 🔧 [2026-08-06] 마감 전 분기/연도(E)는 WiseReport 원본에 영업이익률만
        # 먼저 채워지고 영업이익(금액) 행은 비어있는 경우가 흔하다. 매출액과
        # 영업이익률이 둘 다 있으면 금액을 역산해서 채운다(매출액 × 영업이익률).
        if pd.isna(op) and pd.notna(rev) and pd.notna(op_margin):
            op = rev * op_margin / 100.0
        rows.append({
            '연도/분기': period,
            '매출액': rev,
            '영업이익': op,
            '당기순이익': _naver_wise_lookup(real_df, '당기순이익', col),
            '영업이익률': op_margin,
            '순이익률': _naver_wise_lookup(real_df, '순이익률', col),
            'ROE': _naver_wise_lookup(real_df, 'ROE(%)', col),
            'PER': _naver_wise_lookup(real_df, 'PER(배)', col),
            'PBR': _naver_wise_lookup(real_df, 'PBR(배)', col),
            '부채비율': _naver_wise_lookup(real_df, '부채비율', col),
        })

    # 🔧 [2026-08-06] 마감 전 마지막 기간(E)은 ROE/PER/PBR/부채비율 항목 자체가
    # WiseReport 컨센서스 원본에 없는 경우가 흔하다(연간 컨센서스에는 있어도
    # 분기 컨센서스에는 이 지표들을 아예 제공하지 않는 경우가 많음). 값이 급격히
    # 바뀌는 지표가 아니므로, 직전 실제(마감된) 기간의 값을 근사치로 이어서
    # 보여주고 어떤 항목이 대체됐는지 '_est_cols'에 기록해 화면에서 '≈'로 구분
    # 표시할 수 있게 한다.
    rows = _apply_forecast_valuation_fallback(rows)

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    label = '성장률(QoQ)' if is_quarter else '성장률(YoY)'
    for c in ['매출액', '영업이익']:
        out[f'{c} {label}'] = out[c].pct_change() * 100

    final_cols = ['연도/분기']
    for item in _NAVER_WISE_CORE_ITEMS:
        final_cols.append(item)
        if f'{item} {label}' in out.columns:
            final_cols.append(f'{item} {label}')
    final_cols.append('_est_cols')
    return out[final_cols]


def _naver_wise_build_dividend_table(real_df, annual_col_period_pairs):
    """네이버 WiseReport 표의 '현금DPS(원)'/'현금배당수익률'/'현금배당성향(%)'는
    FnGuide처럼 EPS×PER로 역산 추정한 값이 아니라 실측 배당 데이터라 더 정확하다.
    반환 형식은 _fn_build_dividend_table()과 동일: ['연도','주당배당금','배당총액','배당수익률','배당성향']
    (연간 데이터만 대상으로 함 - 배당은 보통 연 단위로 발표되므로 분기 컬럼은 사용 안 함)
    """
    rows = []
    for col, period in annual_col_period_pairs:
        dps = _naver_wise_lookup(real_df, '현금DPS(원)', col)
        div_yield = _naver_wise_lookup(real_df, '현금배당수익률', col)
        payout = _naver_wise_lookup(real_df, '현금배당성향(%)', col)
        net_income = _naver_wise_lookup(real_df, '당기순이익', col)
        shares = _naver_wise_lookup(real_df, '발행주식수(보통주)', col)

        # 배당총액(억원): DPS(원) × 발행주식수(주) ÷ 1억이 가장 직접적이고 정확함.
        # 발행주식수를 못 구한 경우에만 당기순이익×배당성향 방식으로 대체(FnGuide와 동일 로직).
        total_div = float('nan')
        if pd.notna(dps) and pd.notna(shares) and shares != 0:
            total_div = dps * shares / 1e8
        elif pd.notna(payout) and pd.notna(net_income):
            total_div = net_income * payout / 100

        rows.append({
            '연도': period,
            '주당배당금': dps,
            '배당총액': total_div,
            '배당수익률': div_yield,
            '배당성향': payout,
        })

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    # 주당배당금이 없는(=그 해 배당 자체가 없거나 데이터가 없는) 연도는 제외
    out = out[out['주당배당금'].notna()].reset_index(drop=True)
    return out


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_naver_wisereport_data(code):
    """네이버 금융(NICE평가정보 WiseReport)에서 연간/분기 재무 요약 데이터를 가져온다.
    반환: (df_annual, df_quarter, df_dividend) - fetch_fnguide_data()와 동일한 컬럼 구조.

    💰 [배당 데이터 추가] 이 표에는 '현금DPS(원)'/'현금배당수익률'/'현금배당성향(%)'이
    실측치로 그대로 들어있다. FnGuide 쪽처럼 EPS×PER로 주가를 추정해 배당수익률을
    역산할 필요가 없어서, 오히려 이쪽이 더 정확한 배당 데이터 소스다.
    """
    code = normalize_kr_code(code)
    df_annual, df_quarter, df_dividend = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    if _is_invalid_kr_code(code):
        return df_annual, df_quarter, df_dividend
    debug = {"code": code, "step": None}

    try:
        page_url = f"https://navercomp.wisereport.co.kr/v2/company/c1010001.aspx?cmp_cd={code}"
        headers = dict(_NAVER_WISE_HEADERS, Referer=page_url)

        # 1단계: 페이지에서 encparam/id 토큰 추출
        res0 = requests.get(page_url, headers=headers, timeout=10)
        res0.encoding = 'utf-8'
        debug["page_status"] = res0.status_code

        m_enc = re.search(r"encparam\s*:\s*'([^']+)'", res0.text)
        m_id = re.search(r"[^_a-zA-Z]id\s*:\s*'([^']+)'", res0.text)
        if not m_enc or not m_id:
            debug["step"] = "토큰(encparam/id) 추출 실패 - 페이지 구조가 바뀌었을 수 있음"
            _DEBUG_STORE[f"_naver_wise_debug_{code}"] = debug
            return df_annual, df_quarter, df_dividend
        encparam, id_ = m_enc.group(1), m_id.group(1)

        # 2단계: 실제 데이터 요청 (fin_typ=0, freq_typ=A 조합이 연간+분기를 함께 반환)
        ajax_url = "https://navercomp.wisereport.co.kr/v2/company/ajax/cF1001.aspx"
        params = {"cmp_cd": code, "fin_typ": 0, "freq_typ": "A",
                  "encparam": encparam, "id": id_}
        res = requests.get(ajax_url, headers=headers, params=params, timeout=10)
        res.encoding = 'utf-8'
        debug["ajax_status"] = res.status_code
        debug["resp_len"] = len(res.text)

        dfs = pd.read_html(io.StringIO(res.text), header=[0, 1])
        debug["num_tables"] = len(dfs)

        # 더미 표(전부 같은 값)를 걸러내고 실제 항목명이 있는 표만 채택
        real_df = None
        for d in dfs:
            first_col_vals = [str(x) for x in d.iloc[:, 0]]
            if any('매출액' in s for s in first_col_vals):
                real_df = d
                break
        if real_df is None:
            debug["step"] = "실제 데이터 표를 못 찾음 (더미 표만 있거나 구조 변경)"
            _DEBUG_STORE[f"_naver_wise_debug_{code}"] = debug
            return df_annual, df_quarter, df_dividend

        item_col = real_df.columns[0]
        real_df = real_df.set_index(item_col)
        real_df.index = [str(x).strip() for x in real_df.index]

        annual_cols, quarter_cols = [], []
        for c in real_df.columns:
            top, sub = str(c[0]), str(c[1])
            m = re.match(r'^(\d{4}/\d{2}(?:\(E\))?)', sub.strip())
            if not m:
                continue
            period = m.group(1)
            if '분기' in top:
                quarter_cols.append((c, period))
            elif '연간' in top:
                annual_cols.append((c, period))

        debug["annual_periods_found"] = [p for _, p in annual_cols]
        debug["quarter_periods_found"] = [p for _, p in quarter_cols]
        debug["dividend_rows_present"] = [
            kw for kw in ['현금DPS(원)', '현금배당수익률', '현금배당성향(%)', '발행주식수(보통주)']
            if kw in real_df.index
        ]

        df_annual = _naver_wise_build_period_table(real_df, annual_cols, is_quarter=False)
        df_quarter = _naver_wise_build_period_table(real_df, quarter_cols, is_quarter=True)
        df_dividend = _naver_wise_build_dividend_table(real_df, annual_cols)
        debug["dividend_rows_found"] = len(df_dividend)

        if df_annual.empty and df_quarter.empty and df_dividend.empty:
            _DEBUG_STORE[f"_naver_wise_debug_{code}"] = debug

    except Exception as e:
        debug["exception"] = f"{type(e).__name__}: {e}"
        _DEBUG_STORE[f"_naver_wise_debug_{code}"] = debug

    return df_annual, df_quarter, df_dividend


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_financial_data(code):
    """연간/분기/배당 재무 데이터 통합 진입점.
    1순위: FnGuide(fetch_fnguide_data)
    2순위(폴백): 네이버 WiseReport(fetch_naver_wisereport_data)

    ⚠️ [배당 폴백 버그 수정] 예전에는 연간/분기가 FnGuide에서 성공하면(=네이버로
    안 넘어가면) 배당은 무조건 FnGuide 값만 썼는데, FnGuide가 전체적으로 막혀있는
    상황에서는 연간/분기·배당 모두 실패한다. 그래서 배당은 연간/분기와 별개로
    "FnGuide 배당이 비어있으면 네이버로 보강"하도록 독립적으로 폴백을 건다
    (연간/분기가 이미 네이버로 넘어간 경우든, FnGuide로 성공한 경우든 상관없이).
    """
    df_annual, df_quarter, df_dividend = fetch_fnguide_data(code)
    financial_source = "fnguide" if not (df_annual.empty and df_quarter.empty) else None

    naver_annual = naver_quarter = naver_dividend = None
    need_naver = (df_annual.empty and df_quarter.empty) or df_dividend.empty
    if need_naver:
        naver_annual, naver_quarter, naver_dividend = fetch_naver_wisereport_data(code)

    if df_annual.empty and df_quarter.empty and naver_annual is not None:
        if not naver_annual.empty or not naver_quarter.empty:
            df_annual, df_quarter = naver_annual, naver_quarter
            financial_source = "naver_wisereport_fallback"

    if df_dividend.empty and naver_dividend is not None and not naver_dividend.empty:
        df_dividend = naver_dividend

    _DEBUG_STORE[f"_financial_source_{normalize_kr_code(code)}"] = financial_source or "all_failed"

    return df_annual, df_quarter, df_dividend


# =========================
# 📢 DART 전자공시 연동 모듈
# =========================
import zipfile
import xml.etree.ElementTree as ET
import urllib.parse
from io import BytesIO


def get_dart_api_key():
    """secrets.toml 또는 환경변수에서 DART API 키를 가져온다."""
    try:
        return st.secrets["DART_API_KEY"]
    except Exception:
        return os.environ.get("DART_API_KEY", "")


# ── ⚠️ [임시 우회] Streamlit Cloud → opendart.fss.or.kr 직접 연결 차단 우회용 프록시 ──
# 문제: Streamlit Community Cloud(해외 서버)에서 opendart.fss.or.kr로 직접 요청하면
# 매번 ConnectTimeout이 발생한다(로컬 PC에서는 정상 동작 확인됨). DART가 국내 IP가
# 아닌 요청을 막고 있는 것으로 추정됨.
# 임시 조치: 무료 공개 CORS 프록시(allorigins)를 경유해서 우회한다.
# ⚠️ 이 경로를 쓰면 DART API 키가 제3자 서버(allorigins.win)에 노출된다.
#    임시 검증/우회용으로만 사용할 것 — 장기적으로는 자체 국내 리전 프록시 서버로
#    교체 필요. 자체 프록시로 바꿀 때는 _DART_USE_PROXY = False로 내리고
#    _dart_request() 내부에서 자체 프록시 URL을 호출하도록 바꾸면 된다.
_DART_USE_PROXY = True
# ── [폴백 체인] 무료 공개 프록시는 개별적으로(때로는 동시에) 다운되는 경우가 흔해서
# (allorigins 내부 오류, codetabs 521 다운 모두 실제로 관측됨), 하나가 실패하면
# 다음 프록시로 자동 전환하도록 여러 개를 순서대로 등록해둔다. 앞쪽이 우선순위가 높다.
# mode="query": 대상 URL을 인코딩해서 쿼리 파라미터로 붙이는 방식 (allorigins, codetabs, corsproxy.io)
# mode="path" : 대상 URL을 인코딩하지 않고 경로 뒤에 그대로 이어붙이는 방식 (thingproxy)
_DART_PROXY_BASES = [
    # ⭐ 1순위: 자체 Cloudflare Worker 프록시 (배포 완료, 가장 안정적).
    {"base": "https://restless-fog-8937.daimon8835.workers.dev/?url=", "mode": "query"},
    {"base": "https://api.allorigins.win/raw?url=", "mode": "query"},
    {"base": "https://api.codetabs.com/v1/proxy?quest=", "mode": "query"},
    # ❌ corsproxy.io는 제외함: 무료 플랜이 서버→서버 요청 자체를 막아놔서
    # ("Server-side requests are not allowed on your plan") 여기서는 구조적으로
    # 항상 실패한다. 일시적 장애가 아니라 이 조합에서는 영구적으로 못 쓰는 서비스.
    {"base": "https://thingproxy.freeboard.io/fetch/", "mode": "path"},
]


def _is_proxy_error_response(res):
    """DART가 아니라 프록시 서비스 자체가 낸 오류인지 판별.

    - HTTP 4xx/5xx (예: codetabs가 죽었을 때 뜨는 521 "Web server is down" 에러 페이지)
    - allorigins처럼 {"error": "...", "stack": "..."} 형태의 JSON 내부 오류
    이런 응답을 DART 응답으로 착각해 그대로 파싱하면 status=None/json_decode_error로
    흘러가 버리므로, 여기서 미리 걸러내고 다음 프록시로 넘어간다.
    """
    if res.status_code >= 400:
        return True
    body_preview = res.text[:200].lstrip()
    if body_preview.startswith("{"):
        try:
            body = res.json()
        except Exception:
            return False
        if isinstance(body, dict) and "error" in body and "stack" in body:
            return True
    return False


def _dart_request(url, params, timeout):
    """DART API에 요청을 보낸다.

    _DART_USE_PROXY가 True면 등록된 CORS 프록시들을 순서대로 시도한다.
    한 프록시가 네트워크 예외를 던지거나 프록시 자체 내부 오류를 반환하면,
    바로 실패 처리하지 않고 다음 프록시로 자동 폴백한다. 프록시 하나가 응답이
    느리거나 멈춰있을 때 전체 요청이 timeout(기본 30초) x 프록시 개수만큼 오래
    걸리지 않도록, 프록시별 시도는 더 짧은 attempt_timeout으로 빠르게 실패시키고
    넘어간다. 모든 프록시가 실패하면 마지막으로 받은 응답(또는 마지막 예외)을
    그대로 반환/발생시켜 기존 호출부의 에러 처리 로직(디버그 로깅 등)이 그대로
    동작하게 한다.
    """
    if not _DART_USE_PROXY:
        return requests.get(url, params=params, timeout=timeout)

    full_target_url = f"{url}?{urllib.parse.urlencode(params)}"
    attempt_timeout = min(timeout, 12)
    last_res = None
    last_exc = None
    for proxy in _DART_PROXY_BASES:
        if proxy["mode"] == "query":
            proxied_url = proxy["base"] + urllib.parse.quote(full_target_url, safe="")
        else:  # "path" 모드
            proxied_url = proxy["base"] + full_target_url
        try:
            res = requests.get(proxied_url, timeout=attempt_timeout)
        except Exception as e:
            last_exc = e
            continue
        last_res = res
        if not _is_proxy_error_response(res):
            return res
        # 이 프록시가 내부 오류를 낸 경우 → 다음 프록시로 계속 시도
            return res
        # 이 프록시가 내부 오류를 낸 경우 → 다음 프록시로 계속 시도

    if last_res is not None:
        return last_res
    raise last_exc


@st.cache_data(ttl=86400 * 7, show_spinner=False)  # 고유번호 목록은 자주 안 바뀌므로 7일 캐싱
def fetch_dart_corp_code_map():
    """
    DART 전체 기업 고유번호(corp_code) 목록을 받아
    {6자리 종목코드: {"corp_code": ..., "corp_name": ...}} 형태로 반환.

    ⚠️ [정적 파일 우선] Streamlit Cloud에서 opendart.fss.or.kr의 corpCode.xml
    (3.5MB zip)을 실시간으로 받아오는 게 네트워크 제약(느림/차단/프록시 불안정)으로
    어려워서, generate_dart_corp_map.py로 로컬에서 미리 생성해둔
    dart_corp_code_map.json을 같은 폴더에서 우선 읽는다. 이 매핑은 자주 안 바뀌므로
    실시간 API 호출이 굳이 필요 없다. 파일이 없거나 읽기 실패하면 기존처럼
    API에서 직접(또는 프록시로) 받아오는 것으로 폴백한다.

    🔧 [디버그] 실패 원인을 _DEBUG_STORE에 남겨서 render_disclosure_tab에서
    expander로 확인할 수 있게 한다.
    """
    debug_info = {"step": "start", "api_key_set": bool(get_dart_api_key())}

    # ── 1순위: 로컬 정적 파일 ─────────────────────────────────────────
    static_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dart_corp_code_map.json")
    try:
        if os.path.exists(static_path):
            with open(static_path, "r", encoding="utf-8") as f:
                result = json.load(f)
            debug_info["step"] = "success_from_static_file"
            debug_info["source"] = static_path
            debug_info["total_listed_companies"] = len(result)
            debug_info["samsung_005930_found"] = "005930" in result
            _DEBUG_STORE["_dart_corpmap_debug"] = debug_info
            return result
    except Exception as e:
        debug_info["static_file_error"] = f"{type(e).__name__}: {e}"
        # 정적 파일 읽기가 실패하면 아래 API 폴백으로 계속 진행

    # ── 2순위: API에서 직접(또는 프록시로) 받아오기 (기존 로직 폴백) ──────
    api_key = get_dart_api_key()
    if not api_key:
        debug_info["step"] = "no_api_key"
        _DEBUG_STORE["_dart_corpmap_debug"] = debug_info
        return {}

    try:
        url = "https://opendart.fss.or.kr/api/corpCode.xml"
        res = _dart_request(url, {"crtfc_key": api_key}, timeout=30)
        debug_info["http_status"] = res.status_code
        debug_info["via_proxy"] = _DART_USE_PROXY
        debug_info["content_type"] = res.headers.get("Content-Type", "")
        res.raise_for_status()

        try:
            with zipfile.ZipFile(BytesIO(res.content)) as zf:
                xml_bytes = zf.read(zf.namelist()[0])
        except zipfile.BadZipFile:
            # DART가 zip 대신 에러 메시지(XML/텍스트)를 반환한 경우.
            # 보통 키 미승인/오타/사용한도초과일 때 이 분기로 들어온다.
            debug_info["step"] = "not_a_zip_file"
            debug_info["raw_response_preview"] = res.content[:300].decode("utf-8", errors="replace")
            _DEBUG_STORE["_dart_corpmap_debug"] = debug_info
            return {}

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

        debug_info["step"] = "success_from_api"
        debug_info["total_listed_companies"] = len(result)
        debug_info["samsung_005930_found"] = "005930" in result
        _DEBUG_STORE["_dart_corpmap_debug"] = debug_info
        return result
    except Exception as e:
        debug_info["step"] = "exception"
        debug_info["exception"] = f"{type(e).__name__}: {e}"
        _DEBUG_STORE["_dart_corpmap_debug"] = debug_info
        return {}


@st.cache_data(ttl=600, show_spinner=False)  # 10분 캐싱
def fetch_disclosure_list(code, days=90, page_count=30):
    """
    특정 종목코드의 최근 공시 목록을 반환.
    반환 형식: list of dict [{date, title, report_no, url, flag}, ...]
    """
    debug_info = {"step": "start", "requested_code": code}
    api_key = get_dart_api_key()
    if not api_key:
        debug_info["step"] = "no_api_key"
        _DEBUG_STORE[f"_dart_disclosure_debug_{code}"] = debug_info
        return []

    code = normalize_kr_code(code)
    corp_map = fetch_dart_corp_code_map()
    debug_info["corp_map_size"] = len(corp_map)
    corp_info = corp_map.get(code)
    if not corp_info:
        debug_info["step"] = "corp_code_not_found_in_map"
        _DEBUG_STORE[f"_dart_disclosure_debug_{code}"] = debug_info
        return []

    debug_info["corp_name"] = corp_info.get("corp_name")
    debug_info["corp_code"] = corp_info.get("corp_code")

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
        res = _dart_request(url, params, timeout=30)
        debug_info["http_status"] = res.status_code
        debug_info["raw_response_preview"] = res.text[:500]
        try:
            data = res.json()
        except Exception as e:
            debug_info["step"] = "json_decode_error"
            debug_info["json_decode_exception"] = f"{type(e).__name__}: {e}"
            _DEBUG_STORE[f"_dart_disclosure_debug_{code}"] = debug_info
            return []
        debug_info["response_keys"] = list(data.keys()) if isinstance(data, dict) else f"not_a_dict: {type(data).__name__}"
        debug_info["dart_status"] = data.get("status") if isinstance(data, dict) else None
        debug_info["dart_message"] = data.get("message") if isinstance(data, dict) else None
        debug_info["via_proxy"] = _DART_USE_PROXY

        if not isinstance(data, dict) or data.get("status") != "000":
            # DART status 코드 참고: 013=조회된 데이터 없음, 020=사용한도초과, 800=시스템점검 등
            debug_info["step"] = "dart_status_not_000"
            _DEBUG_STORE[f"_dart_disclosure_debug_{code}"] = debug_info
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
        debug_info["step"] = "success"
        debug_info["rows_found"] = len(rows)
        _DEBUG_STORE[f"_dart_disclosure_debug_{code}"] = debug_info
        return rows
    except Exception as e:
        debug_info["step"] = "exception"
        debug_info["exception"] = f"{type(e).__name__}: {e}"
        _DEBUG_STORE[f"_dart_disclosure_debug_{code}"] = debug_info
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
        period_choice = st.pills(
            "조회 기간",
            ["최근 30일", "최근 90일", "최근 180일", "최근 1년"],
            default="최근 90일",
            key=f"dart_period_{code}",
            label_visibility="collapsed",
        )
        if period_choice is None:  # pills는 다시 누르면 선택 해제가 되므로 기본값으로 되돌림
            period_choice = "최근 90일"
        days = {"최근 30일": 30, "최근 90일": 90, "최근 180일": 180, "최근 1년": 365}[period_choice]
    with col_refresh:
        if st.button("새로고침", key=f"dart_refresh_{code}", use_container_width=True):
            fetch_disclosure_list.clear()

    rows = run_with_progress("공시 데이터 조회 중...", fetch_disclosure_list, code, days)

    if not rows:
        st.caption("최근 공시 내역이 없거나 DART에 등록된 종목코드를 찾을 수 없습니다.")
        norm_code = normalize_kr_code(code)
        _corpmap_dbg = _DEBUG_STORE.get("_dart_corpmap_debug")
        _disclosure_dbg = _DEBUG_STORE.get(f"_dart_disclosure_debug_{norm_code}")
        if _corpmap_dbg or _disclosure_dbg:
            with st.expander("🔧 디버그 정보 (공시 조회 실패 원인 확인용)"):
                st.write("① 기업 고유번호(corp_code) 목록 조회 결과:")
                st.json(_corpmap_dbg or {"info": "호출 안 됨"})
                st.write("② 공시 목록 조회 결과:")
                st.json(_disclosure_dbg or {"info": "호출 안 됨"})
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


def _parse_screener_page_html(res_text, sosok):
    """네이버 시가총액 페이지 HTML → DataFrame 파싱 (fetch_page_data와 마지막페이지
    사전탐지(_detect_screener_last_page)가 이 로직을 공유하기 위해 분리함)."""
    code_matches = re.findall(r'href="/item/main\.naver\?code=(\d+)" class="tltle">(.*?)</a>', res_text)
    name_to_code = {name: code for code, name in code_matches}
    if not name_to_code: return None
    dfs = pd.read_html(io.StringIO(res_text))
    main_df = next((df for df in dfs if '종목명' in df.columns), None)
    if main_df is None or main_df.empty: return None
    main_df = main_df.dropna(subset=['종목명'])
    main_df['종목코드'] = main_df['종목명'].map(name_to_code)
    main_df['시장'] = "코스피" if sosok == 0 else "코스닥"
    return main_df

def _extract_screener_current_page(res_text):
    """[진단용] 응답 HTML의 네이버 페이지네이터에서 실제 활성 페이지 번호를 추출한다.
    네이버가 마크업을 바꿔서 못 찾으면 None을 반환하며, 호출부는 이 경우 검증을
    건너뛴다 — 이 추출 실패 자체가 정상 페이지를 실패로 오판시키지 않도록 하기 위함."""
    m = re.search(r'<td class="on">\s*<a[^>]*>(\d+)</a>', res_text)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None
    return None

def fetch_page_data(sosok, page, headers, cookies):
    time.sleep(random.uniform(0.1, 0.3))
    url = f"https://finance.naver.com/sise/sise_market_sum.naver?sosok={sosok}&page={page}"
    try:
        res = requests.get(url, headers=headers, cookies=cookies, timeout=10)
        res.encoding = 'euc-kr'  # 네이버금융(finance.naver.com)은 euc-kr 고정 — apparent_encoding 추측에 의존하면 특정 종목명 바이트 패턴에서 오탐(예: 키릴 계열로 오판)해 파싱이 깨진다

        # ── [진단] 상태코드가 200이 아닌 경우 원인 기록 ──────────────────────
        # 기존에는 status_code를 전혀 확인하지 않아서, 네이버가 429/403/503 등을
        # 줘도 그냥 파싱 실패로 뭉개져서 "왜 실패했는지"를 알 수 없었다.
        if res.status_code != 200:
            fails = _DEBUG_STORE.setdefault("_screener_fetch_failures", [])
            fails.append({
                "요청": (sosok, page), "원인": f"HTTP {res.status_code}",
                "시각": datetime.datetime.now().strftime("%H:%M:%S"),
            })

        # ── [진단] "실패"로는 안 잡히지만 요청한 페이지와 다른 내용이 온 경우 로그 ──
        # 레이트리밋/캐시 등으로 엉뚱한 페이지 내용이 200 OK로 오면 기존 로직은 이걸
        # 그냥 "성공"으로 처리해서 조용히 병합해버린다. 아직 이 불일치를 페이지 실패로
        # 처리하지는 않는다(마크업 추정에 대한 확신이 100%가 아니라, 이 체크 때문에
        # 정상 페이지까지 실패 처리되는 부작용을 피하려는 것). 우선 얼마나 자주
        # 발생하는지 _DEBUG_STORE에 쌓아서 다음 스캔에서 확인한다.
        returned_page = _extract_screener_current_page(res.text)
        if returned_page is not None and returned_page != page:
            mismatches = _DEBUG_STORE.setdefault("_screener_page_mismatches", [])
            mismatches.append({
                "요청": (sosok, page),
                "실제응답페이지": returned_page,
                "시각": datetime.datetime.now().strftime("%H:%M:%S"),
            })
            # 요청한 페이지가 실제보다 커서(존재하지 않는 페이지) 마지막 페이지로
            # 클램프되어 온 경우 → 진짜 실패가 아니라 "범위 초과"이므로 별도 표시.
            # (사전 감지가 어떤 이유로든 빗나갔을 때를 대비한 이중 안전장치)
            if returned_page < page:
                _DEBUG_STORE.setdefault("_screener_overflow_pages", set()).add((sosok, page))

        parsed = _parse_screener_page_html(res.text, sosok)
        if parsed is None:
            fails = _DEBUG_STORE.setdefault("_screener_fetch_failures", [])
            # ── [진단] 왜 파싱이 실패했는지 원인을 눈으로 볼 수 있게 스냅샷 저장 ──
            # HTTP 200인데도 파싱이 실패하는 건 네트워크 문제가 아니라 정규식/파싱
            # 로직이 실제 HTML 구조와 안 맞는 경우일 가능성이 높다. 얼마나 안 맞는지
            # 판단할 수 있도록 최소한의 단서를 남긴다: 페이지 길이, 핵심 키워드 존재
            # 여부, class="tltle" 요구 없이 느슨하게 찾은 종목코드 개수, 실제 HTML
            # 일부 발췌.
            loose_codes = re.findall(r'href="/item/main\.naver\?code=(\d+)"', res.text)
            snippet_idx = res.text.find('종목명')
            if snippet_idx == -1:
                snippet_idx = res.text.find('<table')
            snippet = res.text[max(0, snippet_idx - 100): snippet_idx + 500] if snippet_idx != -1 else res.text[:500]
            fails.append({
                "요청": (sosok, page),
                "원인": f"HTTP {res.status_code}, 종목테이블 파싱 실패",
                "응답길이": len(res.text),
                "'종목명'문자열있음": '종목명' in res.text,
                "'tltle'문자열있음": 'tltle' in res.text,
                "느슨한매칭_종목코드개수": len(loose_codes),
                "html_snippet": snippet,
                "시각": datetime.datetime.now().strftime("%H:%M:%S"),
            })
        return parsed
    except requests.exceptions.Timeout:
        _DEBUG_STORE.setdefault("_screener_fetch_failures", []).append({
            "요청": (sosok, page), "원인": "타임아웃(10초)", "시각": datetime.datetime.now().strftime("%H:%M:%S"),
        })
        return None
    except Exception as e:
        _DEBUG_STORE.setdefault("_screener_fetch_failures", []).append({
            "요청": (sosok, page), "원인": f"예외: {type(e).__name__}", "시각": datetime.datetime.now().strftime("%H:%M:%S"),
        })
        return None

def _detect_screener_last_page_by_probe(headers, cookies, sosok, default_last=44):
    """실제 마지막 페이지를 확실하게 찾기 위해, 절대 존재하지 않을 만큼 큰 페이지
    번호(999)를 일부러 요청한다. 네이버는 이런 초과 요청에도 에러를 내지 않고
    실제 마지막 페이지로 클램프해서 응답하며, 페이지네이터의 활성 페이지(class="on")
    표시도 그 진짜 마지막 페이지 번호를 그대로 보여준다 — 이건 실제 진단 로그로
    확인된 동작이다(코스닥 38~44 요청 → 매번 '실제응답페이지: 37'로 관측됨).
    이전에 시도했던 '맨뒤(pgRR)' 링크 파싱은 마크업 추정이 틀려 항상 실패했었다."""
    try:
        res = requests.get(
            f"https://finance.naver.com/sise/sise_market_sum.naver?sosok={sosok}&page=999",
            headers=headers, cookies=cookies, timeout=10
        )
        res.encoding = 'euc-kr'  # 네이버금융(finance.naver.com)은 euc-kr 고정 — apparent_encoding 추측에 의존하면 특정 종목명 바이트 패턴에서 오탐(예: 키릴 계열로 오판)해 파싱이 깨진다
        detected = _extract_screener_current_page(res.text)
        return detected if detected else default_last
    except Exception:
        return default_last

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
    time.sleep(0.15)

    field_url = "https://finance.naver.com/sise/field_submit.naver?menu=market_sum&returnUrl=https%3A%2F%2Ffinance.naver.com%2Fsise%2Fsise_market_sum.naver&fieldIds=per&fieldIds=pbr&fieldIds=roe&fieldIds=dividend&fieldIds=property_total&fieldIds=debt_total&fieldIds=high52"
    try:
        session.get(field_url, headers=headers, timeout=10)
    except Exception:
        pass
    cookies = session.cookies.get_dict()

    # ── 실제 마지막 페이지 자동 감지 (하드코딩 44 제거) ──────────────────────
    # 문제: range(1, 45)로 코스피/코스닥 둘 다 무조건 44페이지까지 요청했는데,
    # 코스닥은 상장 종목 수가 더 적어서 실제 마지막 페이지가 44보다 작다(진단 결과: 37).
    # 존재하지 않는 페이지를 요청하면 네이버가 200 OK를 주지만 종목 테이블은 비어 있어
    # 매 스캔마다 3라운드 재시도를 다 태우고도 결국 "실패 페이지"로 잡혔다.
    # _detect_screener_last_page_by_probe로 실제 마지막 페이지를 구하고, 그 이후
    # 페이지는 애초에 요청 목록에서 제외한다. 1페이지 응답은 그대로 결과에 재사용.
    _DEBUG_STORE["_screener_page_mismatches"] = []
    _DEBUG_STORE["_screener_overflow_pages"] = set()
    _DEBUG_STORE["_screener_fetch_failures"] = []

    all_data = []
    last_page_by_sosok = {}
    for sosok in [0, 1]:
        try:
            res0 = requests.get(
                f"https://finance.naver.com/sise/sise_market_sum.naver?sosok={sosok}&page=1",
                headers=headers, cookies=cookies, timeout=10
            )
            res0.encoding = 'euc-kr'  # 네이버금융 euc-kr 고정
            df0 = _parse_screener_page_html(res0.text, sosok)
            if df0 is not None and not df0.empty:
                all_data.append(df0)
        except Exception:
            pass
        last_page_by_sosok[sosok] = _detect_screener_last_page_by_probe(headers, cookies, sosok, default_last=44)
        _DEBUG_STORE[f"_screener_last_page_sosok{sosok}"] = last_page_by_sosok[sosok]

    # 코스피를 전부 먼저, 코스닥을 나중에 순서대로 나열하면(기존 방식) 같은 세션
    # 쿠키로 나가는 요청 중 코스닥 쪽이 항상 시간상 뒤에 실행되어, 네이버가 세션
    # 단위로 누적 요청 수를 추적해 레이트리밋을 건다면 코스닥에만 실패가 몰릴 수
    # 있다(실제로 관측된 패턴과 일치). 두 시장 페이지를 번갈아 섞어서 제출 순서상
    # 어느 한쪽에만 부하가 쏠리지 않게 한다.
    _urls_0 = [(0, page) for page in range(2, last_page_by_sosok[0] + 1)]
    _urls_1 = [(1, page) for page in range(2, last_page_by_sosok[1] + 1)]
    urls = [u for pair in zip_longest(_urls_0, _urls_1) for u in pair if u is not None]
    total_pages = len(urls) + 2  # 이미 처리한 1페이지 2건 포함
    completed = 2  # 위에서 이미 처리한 1페이지 2건
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
        _retry_executor = get_shared_executor()
        # ⚠️ [버그 수정 2026-08-18] retry_workers([2,1,1])를 선언만 해두고 실제로는
        # 안 써서, 재시도 라운드마다 failed_pages 전체를 한꺼번에 다시 던졌었다.
        # 그러면 "네이버 레이트리밋 때문에 실패한 페이지들"을 재시도할 때 똑같이
        # 동시에 몰아서 요청하게 되어, 레이트리밋을 다시 유발해 재시도가 재시도를
        # 반복해서 부르는 상황이 나올 수 있었다. 라운드마다 retry_workers[round]개씩
        # 청크로 나눠서 순차적으로 처리(청크 안에서만 동시 실행)하도록 고쳤다 —
        # 라운드가 진행될수록(2→1→1) 동시 요청 수가 점점 줄어드는 게 원래 의도였다.
        _chunk_size = retry_workers[retry_round]
        _failed_list = list(failed_pages)
        for _i in range(0, len(_failed_list), _chunk_size):
            _chunk = _failed_list[_i:_i + _chunk_size]
            _retry_processed = set()
            future_to_url = {_retry_executor.submit(fetch_page_data, s, p, headers, cookies): (s, p) for s, p in _chunk}
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
                for s, p in _chunk:
                    if (s, p) not in _retry_processed:
                        still_failed.append((s, p))
            finally:
                for f in future_to_url:
                    f.cancel()
        failed_pages = still_failed
        retry_round += 1

    # 범위 초과(존재하지 않는 페이지라 마지막 페이지로 클램프된 경우)로 확인된 건
    # 진짜 실패가 아니므로 사용자 경고 대상에서 제외한다.
    _overflow = _DEBUG_STORE.get("_screener_overflow_pages", set())
    failed_pages = [p for p in failed_pages if p not in _overflow]

    # ── [세션 프리징 버그 수정] session_state 대신 _DEBUG_STORE에 기록 ──────────
    # 이 제너레이터는 _unified_scan_worker(오케스트레이션 백그라운드 스레드)에서
    # 직접 호출된다. 파일 상단에 이미 문서화된 규칙(백그라운드 스레드에서 session_state를
    # 직접 건드리면 메인 스크립트 실행 스레드가 영원히 멈출 수 있다)을 그대로 어기고
    # 있던 부분 — 같은 함수의 page_mismatches/fetch_failures는 이미 _DEBUG_STORE를
    # 쓰고 있었는데 missing_pages만 예외로 session_state를 직접 썼다. 이 불일치가
    # "새로고침도 재시도도 안 통하고 오직 앱 리붓만 통하는" 세션 프리징의 유력한
    # 원인이었을 가능성이 높다. 화면 표시는 메인 스레드(run_unified_market_scan_async)가
    # 완료 시점에 이 값을 읽어서 처리한다.
    if failed_pages:
        _DEBUG_STORE["_screener_missing_pages"] = failed_pages
    else:
        _DEBUG_STORE["_screener_missing_pages"] = []

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
        # ⚠️ [버그 수정] 예전엔 mask_high에 안 걸리는(52주고점 데이터를 못 구한) 종목도
        # 고점대비(%)를 0.0으로 채웠다. 그러면 '52주 고점을 모른다'와 '지금 딱 52주
        # 고점이다(0% 하락)'가 똑같은 0.0으로 뭉개져서, 다음 단계(get_ai_diagnosis_inputs
        # → calc_risk_score)에서 진짜 데이터를 '데이터 없음'으로 오인하는 원인이 됐다.
        # 실측: 삼성전자 리스크 점수가 실제 하락폭(-36.5%, 감점 12.6점)이 아니라 데이터
        # 없음 취급(중립 감점 5.0점)으로 계산돼 -46.7점이어야 할 게 -39.1점으로 나왔다.
        # NaN으로 남겨서 '모른다'를 명확히 구분한다.
        final_df['고점대비(%)'] = np.nan
        final_df.loc[mask_high, '고점대비(%)'] = ((final_df.loc[mask_high, '현재가'] - final_df.loc[mask_high, '52주고점']) / final_df.loc[mask_high, '52주고점']) * 100
        final_df = final_df[['종목코드', '종목명', '시장', '현재가', '52주고점', '고점대비(%)', 'PER', 'PBR', '배당수익률', 'ROE', '부채비율']]
    else:
        final_df = final_df[['종목코드', '종목명', '시장', '현재가', 'PER', 'PBR', '배당수익률', 'ROE', '부채비율']]

    # ── 병합 후 종목코드 중복 검사 ────────────────────────────────────────────
    # "실패 페이지" 경고에는 안 잡히지만, 레이트리밋/캐시 등으로 엉뚱한 페이지
    # 내용이 200 OK로 와서 조용히 병합되는 경우 여기서 중복 종목코드로 드러난다.
    # 이 경우 전에는 아무 경고 없이 중복 행이 그대로 섞여 들어갔다.
    dup_mask = final_df['종목코드'].duplicated(keep=False)
    # ── [세션 프리징 버그 수정] 여기도 session_state 대신 _DEBUG_STORE 사용 (위 missing_pages와 동일한 이유) ──
    if dup_mask.any():
        _DEBUG_STORE["_screener_dup_codes"] = sorted(final_df.loc[dup_mask, '종목코드'].dropna().unique().tolist())
    else:
        _DEBUG_STORE["_screener_dup_codes"] = []

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
    # ⚠️ [CSV 업로드 기능 제거] 예전엔 여기서 관리자가 수동 업로드한 KRX 52주 고점
    # CSV를 merge_high52로 붙여줬는데, 스크리너 스캔 자체가 네이버에서
    # fieldIds=high52로 52주고점을 이미 직접 받아와 '52주고점' 컬럼에 채워주고
    # 있어서(중복 데이터) 그 기능은 완전히 제거했다. 이제 세 경로 모두 df를
    # 그대로 반환하면 된다.
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



def check_naver_52w_robust(row_dict):
    code = str(row_dict['종목코드']).replace('.0','').zfill(6)
    mkt = row_dict.get('시장', '코스피')
    
    price = float(str(row_dict.get('현재가', 0)).replace(',', ''))
    high = 0.0

    if '52주고점' in row_dict and pd.notna(row_dict['52주고점']) and float(row_dict['52주고점']) > 0:
        high = float(row_dict['52주고점'])
    # ⚠️ [CSV 업로드 기능 제거] 예전엔 여기서 high52_map(수동 업로드한 KRX CSV)을
    # 폴백으로 썼다. 그런데 스크리너 자체 스캔이 네이버에서 fieldIds=high52로 이미
    # 52주고점을 직접 받아오고 있어서(위 if문에서 처리), CSV는 사실상 그 값을
    # 다시 덮어쓰기만 하는 중복 데이터였다. row_dict에 52주고점이 없는 경우엔
    # (high가 0.0으로 남아) 곧바로 아래 네이버 실시간 API 경로로 넘어간다.

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

# ── [스캔 버튼 멈춤 수정] 전체 시장 스캔을 백그라운드 스레드 + 논블로킹 폴링으로 전환 ──
# 문제: 기존 run_unified_market_scan()은 fetch_screener_data_generator()를 "메인
# 스크립트 실행 스레드"에서 그대로 for문으로 소비했다. 내부적으로 1단계만 최대 35초
# 대기 + 실패 페이지 재시도 최대 3라운드(라운드마다 최대 2~10초 대기 + 최대 18초
# 응답 대기)가 있어서, 최악의 경우 1분~1분 반 가까이 메인 스레드가 통째로 막혔다.
# 이 동안 Streamlit은 같은 세션에서 오는 어떤 클릭(탭 이동 포함)도 받지 못했고,
# "멈춘 줄 알고" 사용자가 반복 클릭/새로고침을 하면 스캔이 중복 실행되어 공유
# 스레드풀에 부하가 더 쏠리는 악순환까지 생겼다(스레드 좀비 누적 → 결국 프로세스
# 전체가 응답 없음 상태로 이어지는 원인 중 하나).
#
# 해결: 다른 비동기 로딩 지점(render_async_multi)과 동일한 철학으로, 실제 스캔
# 작업(1단계 스크리너 스캔 + 2단계 52주 고점 매칭)을 오케스트레이션 풀의 백그라운드
# 스레드에 통째로 던지고, 메인 스크립트는 st.fragment(run_every=...)로 진행률만
# 짧은 간격(0.4초)으로 폴링한다. 백그라운드 스레드 안에서는 st.progress/st.error 같은
# 위젯 호출이 안전하지 않으므로, 진행률/에러/경고는 전부 모듈 전역 dict
# (_SCAN_JOB_STATE)에 기록해두고 메인 스레드가 폴링 시점에 그 값을 읽어 그린다.
# session_state 최종 반영(shared_screener_df / reco_raw_data)도 반드시 메인
# 스레드에서만 수행해서, "백그라운드 스레드가 세션 상태를 직접 확정 짓는" 상황을
# 피한다.
# ── [핵심 버그 수정] "스캔 실패: 알 수 없음, 0%"가 항상 뜨던 진짜 원인 ──────────
# 이전에는 이 줄이 `_SCAN_JOB_STATE = {}`처럼 평범한 전역 변수였다. Streamlit은
# 상호작용이 있을 때마다(특히 아래 폴링 프래그먼트가 거는 st.rerun()마다) 스크립트
# 파일 전체를 처음부터 다시 실행하므로, 이 대입문도 매번 다시 실행되어 매 rerun마다
# _SCAN_JOB_STATE가 새로운 빈 딕셔너리로 초기화되고 있었다. 반면 백그라운드에서
# 실제로 스캔을 수행하는 _unified_scan_worker 스레드는 자신이 "시작될 때(=이전
# rerun)의" _SCAN_JOB_STATE 객체에 진행률을 계속 기록한다. 그 결과 폴링 후 다시
# 실행된 스크립트가 state = _SCAN_JOB_STATE.get(job_id, {})를 읽으면, 실제 진행
# 상황이 담긴 "옛날" 객체가 아니라 방금 새로 만들어진 "텅 빈" 객체를 보게 되어
# 항상 text="알 수 없음", pct=0으로 나타났다 — 스캔이 실제로는 잘 진행/완료되고
# 있었어도 절대 그 사실을 알 수 없었던 것.
# 해결: 아래 스레드풀들과 동일하게 @st.cache_resource로 감싸서, 이 줄이 매
# rerun마다 다시 실행되더라도 항상 "동일한" 딕셔너리 객체를 반환하게 한다.
@st.cache_resource(show_spinner=False)
def _get_scan_job_state_store():
    return {}

_SCAN_JOB_STATE = _get_scan_job_state_store()

def _unified_scan_worker(job_id):
    """run_unified_market_scan()의 1+2단계 로직을 그대로 수행하되, st.* 위젯 호출
    대신 _SCAN_JOB_STATE[job_id]에 진행률/결과를 기록한다. 오케스트레이션 풀의
    백그라운드 스레드에서 submit_with_ctx로 실행되는 것을 전제로 한다."""
    state = _SCAN_JOB_STATE[job_id]

    def set_progress(text, pct):
        state["text"] = text
        state["pct"] = pct

    # 1단계: 전체 시장 스캔 (종목 스크리너 데이터)
    set_progress("[1/2] 전체 시장 데이터 스캔 준비 중...", 0)
    try:
        fetch_and_cache_screener_data.clear()
        temp_df = pd.DataFrame()
        for status_msg, pct in fetch_screener_data_generator():
            if isinstance(status_msg, str):
                set_progress(f"[1/2] 전체 시장 스캔 중: {status_msg}", pct)
            else:
                temp_df = status_msg

        if temp_df.empty:
            state["done"] = True
            state["success"] = False
            state["error"] = "통신 지연으로 시장 스캔에 실패했습니다. 다시 시도해주세요."
            return

        temp_df = _safe_save_screener_df(temp_df, "saved_screener_data.csv")
        state["screener_df"] = temp_df

        # fetch_screener_data_generator 내부에서 이미 _DEBUG_STORE에 채워둔
        # 진단 정보를 그대로 옮겨 담아둔다 (최종 표시는 메인 스레드에서).
        # ⚠️ 이 함수 전체가 오케스트레이션 백그라운드 스레드에서 실행되므로,
        # 여기서는 절대 st.session_state를 읽거나 쓰지 않는다 — session_state
        # 반영은 메인 스레드(run_unified_market_scan_async)에서만 한다.
        state["missing_pages"] = list(_DEBUG_STORE.get("_screener_missing_pages") or [])
        state["dup_codes"] = list(_DEBUG_STORE.get("_screener_dup_codes") or [])
        state["page_mismatches"] = list(_DEBUG_STORE.get("_screener_page_mismatches") or [])
        state["fetch_failures"] = list(_DEBUG_STORE.get("_screener_fetch_failures") or [])
        _DEBUG_STORE["_screener_missing_pages"] = []
        _DEBUG_STORE["_screener_dup_codes"] = []
        _DEBUG_STORE["_screener_page_mismatches"] = []
        _DEBUG_STORE["_screener_fetch_failures"] = []
    except Exception as e:
        state["done"] = True
        state["success"] = False
        state["error"] = f"스캔 실패: {e}"
        return

    # 2단계: 52주 고점 매칭 → 추천 종목 후보 산출
    df = temp_df.copy()
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
        set_progress("✨ 시장 스캔 완료!", 100)
        state["done"] = True
        state["success"] = True
        state["reco_df"] = None
        state["warning"] = "현재 시장 데이터 기준, 최소 요건(D급)을 통과한 종목조차 없습니다. 추천 종목 후보 산출은 건너뜁니다."
        return

    # ⚠️ [후보 확대: 150 → 300] ROE 상위 150개까지만 후보로 넘기다 보니, 1단계
    # 재무 조건(PER≤40, PBR≤4.0, ROE≥0, 부채비율≤300)은 통과했는데도 ROE 순위가
    # 151~300위 사이라는 이유만으로 삼성전자 같은 대형 우량주가 통째로 걸러지는
    # 경우가 있었다. 300개로 넓혀서 이런 종목들도 2단계(52주 고점 매칭) 이후
    # 추천 종목 탭에서 AI 점수로 다시 경쟁할 기회를 준다.
    # ⚠️ 트레이드오프: 2단계에서 네이버 API로 개별 조회해야 하는 종목 수가 2배로
    # 늘어나는데, 전체 시간 예산(25초, concurrent.futures.as_completed의 timeout)은
    # 그대로라서 스캔이 몰리는 시간대엔 일부 종목이 이번 스캔에서 타임아웃으로
    # 누락될 수 있다(다음 스캔에서 다시 시도되면 잡힘).
    val_df = val_df.sort_values('ROE', ascending=False).head(300)
    rows = []
    dict_records = val_df.to_dict('records')
    total = len(dict_records)
    progress_text = "⚡ 네이버 실시간 API 스캔 중..."
    completed = 0

    # ── [세션 프리징 예방] check_naver_52w_robust가 부르는 st.cache_data 함수를
    # ScriptRunContext 없는 스레드에서 호출하면 내부 락에 걸려 영원히 대기할 수
    # 있다는 게 이미 다른 곳(대시보드 등)에서 확인된 문제라, 여기도 반드시
    # submit_with_ctx로 컨텍스트를 심어서 제출한다.
    _executor = get_shared_executor()
    _futures = {submit_with_ctx(_executor, check_naver_52w_robust, r): r for r in dict_records}
    try:
        for future in concurrent.futures.as_completed(_futures, timeout=25):
            completed += 1
            set_progress(f"[2/2] {progress_text} ({completed}/{total})", int((completed / total) * 100))
            try:
                res = future.result(timeout=8)
            except Exception:
                res = None
            if res:
                rows.append(res)
    except concurrent.futures.TimeoutError:
        pass  # 전체 상한(25초) 초과 → 지금까지 모인 결과로 계속 진행
    finally:
        for f in _futures:
            f.cancel()

    # ⚠️ [기능 제거 이력] 한때 여기서 후보마다 오늘 누적 거래대금을 동기 조회해
    # 즉시 걸러내려 한 적이 있었으나, 스캔 시간 예산을 갉아먹는 문제가 있어 이후
    # 지연 계산 방식으로 옮겼었다. 지금은 그 최소 거래대금 필터 기능 자체를 완전히
    # 제거했다(장 시작 직후엔 모든 종목의 누적 거래대금이 낮게 나와 판단 근거로
    # 부적절했고, 종목마다 API 호출이 추가로 붙어 스캔 체감 속도에도 부담이었다).

    set_progress("✨ 스캔 완료! (스크리너 + 추천 종목 데이터가 함께 갱신되었습니다)", 100)
    state["done"] = True
    state["success"] = True
    state["reco_df"] = pd.DataFrame(rows) if rows else None



def run_unified_market_scan_async(job_key="unified_scan", overall_timeout=150):
    """run_unified_market_scan()의 논블로킹 버전. 버튼 클릭 시 이 함수를 호출하면
    백그라운드 스레드에서 스캔이 진행되는 동안에도 메인 스크립트가 절대 멈추지 않고,
    사용자는 다른 탭 이동이나 다른 버튼 클릭을 그대로 계속할 수 있다.
    (동일 세션 안에서 여러 곳에서 호출해도 job_key가 같으면 중복 스캔이 아니라
    이미 진행 중인 스캔의 진행률을 같이 보여준다.)"""
    jobs = st.session_state.setdefault("_scan_jobs", {})
    job = jobs.get(job_key)

    if job is None:
        job_id = f"{job_key}_{time.time()}"
        _SCAN_JOB_STATE[job_id] = {"text": "스캔 준비 중...", "pct": 0, "done": False}
        future = submit_with_ctx(get_orchestration_executor(), _unified_scan_worker, job_id)
        job = {"job_id": job_id, "future": future, "started_at": time.time(), "overall_timeout": overall_timeout}
        jobs[job_key] = job

    job_id = job["job_id"]
    future = job["future"]
    state = _SCAN_JOB_STATE.get(job_id, {})
    _elapsed = time.time() - job["started_at"]
    timed_out = _elapsed > job.get("overall_timeout", overall_timeout)
    # ── [임시 진단 로그] 150초 안전장치가 왜 발동을 안 하는지 원인 파악용.
    # 원인 파악되면 제거할 것.
    print(f"[DEBUG SCAN {datetime.datetime.now().strftime('%H:%M:%S')}] "
          f"job_id={job_id} elapsed={_elapsed:.1f}s timeout={job.get('overall_timeout', overall_timeout)} "
          f"timed_out={timed_out} future.done()={future.done()} pct={state.get('pct')}",
          file=sys.stderr, flush=True)

    # ── [진행률 정체 감지] "죽지도 않고 응답도 안 오는" 소켓/DNS 행에 대한 방어 ──
    # concurrent.futures 타임아웃은 대부분의 경우를 막아주지만, DNS 조회 단계처럼
    # socket.setdefaulttimeout도 적용 안 되는 지점에서 워커 스레드 자체가 통째로
    # 멎어버리면 future.done()이 영원히 False로 남는다. 이런 경우 overall_timeout까지
    # 마냥 기다리게 두는 대신, "진행률(%)이 stall_threshold초 이상 전혀 안 바뀌면"
    # 조기에 중단하고 다음 재시도를 위해 스레드풀 자체를 새로 갈아치운다.
    _now = time.time()
    _last_pct = state.get("pct", 0)
    if state.get("_last_pct_seen") != _last_pct:
        state["_last_pct_seen"] = _last_pct
        state["_last_pct_change_at"] = _now
    # 전역 폴링 fragment(_all_jobs_settled)가 이 job의 정체 여부를 판단할 수 있도록
    # job dict 자체에도 최신 정체-시각을 반영해둔다.
    job["_last_pct_change_at"] = state.get("_last_pct_change_at", job["started_at"])
    _stalled = (_now - state.get("_last_pct_change_at", job["started_at"])) > 50

    if not future.done() and not timed_out and not _stalled:
        # ── [진행률 표시 이원화 문제 수정] 예전에는 여기서 st.progress(...)를 직접
        # 그렸는데, 이 줄은 "전체 페이지가 다시 실행될 때"만 값이 갱신된다. 반면
        # 실제 살아있는 진행률(%)은 아래 전역 폴링 fragment가 0.4초마다 갱신하는
        # 배너 쪽에만 반영된다. 그 결과 화면에는 "예전 % 값에서 멈춰있는 바(여기)"와
        # "실시간으로 올라가는 배너(페이지 맨 아래, 전역 fragment)"가 동시에 보여서,
        # 사용자 입장에서는 "진행 표시가 위쪽에서 아래로 옮겨간 것처럼" 보이는
        # 혼란을 줬다. 정적인 바는 그리지 않고, 실시간 표시는 전역 fragment
        # 하나로 통일한다(각 페이지가 그 fragment를 어디서 호출하느냐로 위치를 정한다).
        # ── [fragment #10719 회피] 여기서도 자체 fragment를 만들지 않는다. job이
        # _scan_jobs에 남아있으면, 페이지 렌더링 뒤 호출되는 maybe_run_global_poller()의
        # 전역 fragment가 이어서 감시하고 완료 시 st.rerun()으로 갱신해준다.
        return

    # 완료(또는 상한 시간 초과 / 진행률 정체) → 결과를 메인 스레드에서 session_state에 반영
    print(f"[DEBUG SCAN {datetime.datetime.now().strftime('%H:%M:%S')}] "
          f"job_id={job_id} 정리 단계 진입 (future.done()={future.done()} timed_out={timed_out} stalled={_stalled}) "
          f"→ jobs.pop 실행", file=sys.stderr, flush=True)
    jobs.pop(job_key, None)

    if not state.get("success"):
        _last_text = state.get("text", "알 수 없음")
        _last_pct = state.get("pct", 0)
        if _stalled and not future.done():
            # ── [죽은 스레드풀 강제 교체] ────────────────────────────────
            # 이 future가 물려있던 풀(공유풀/오케스트레이션풀)에 진짜 좀비 스레드가
            # 있다는 뜻이므로, 다음 재시도가 또 같은 죽은 풀 뒤에 줄서지 않도록
            # 지금 바로 두 풀을 전부 새것으로 교체해둔다. 이미 던져진 이 future
            # 자체는 복구가 안 되지만(파이썬은 실행 중 스레드를 못 죽인다), 최소한
            # "다시 시도" 버튼을 눌렀을 때는 깨끗한 풀에서 새로 시작하게 된다.
            print(f"[SCAN STALL {datetime.datetime.now().strftime('%H:%M:%S')}] "
                  f"진행률이 50초 이상 멈춰 강제 종료 후 스레드풀 교체 "
                  f"(마지막 상태: {_last_text} {_last_pct}%)", file=sys.stderr, flush=True)
            try:
                _get_shared_executor_raw.clear()
            except Exception:
                pass
            try:
                _get_orchestration_executor_raw.clear()
            except Exception:
                pass
            future.cancel()
            st.error(
                f"스캔 실패: 진행이 멈춰서 중단했습니다 (마지막 상태: {_last_text}, {_last_pct}%). "
                "네이버 서버 응답이 완전히 끊긴 것으로 보입니다. 스레드풀을 새로 정리했으니 "
                "잠시 후 다시 시도해주세요."
            )
        else:
            st.error(
                state.get("error")
                or f"스캔 실패: 시간이 너무 오래 걸려 중단했습니다 (마지막 상태: {_last_text}, {_last_pct}%). 다시 시도해주세요."
            )
        _SCAN_JOB_STATE.pop(job_id, None)
        return

    screener_df = state.get("screener_df")
    if screener_df is not None:
        st.session_state['shared_screener_df'] = screener_df

    if state.get("missing_pages"):
        st.warning(f"⚠️ 이번 스캔에서 끝내 실패한 페이지 (시장구분, 페이지번호): {state['missing_pages']}")
    if state.get("dup_codes"):
        st.warning(f"⚠️ 병합 결과에서 중복된 종목코드 발견 (엉뚱한 페이지 내용이 섞였을 가능성): {state['dup_codes']}")
    if state.get("page_mismatches"):
        st.warning(f"⚠️ 요청한 페이지와 실제 응답 페이지가 다른 경우 {len(state['page_mismatches'])}건 발견: {state['page_mismatches']}")
    if state.get("fetch_failures"):
        from collections import Counter
        reason_counts = Counter(f["원인"] for f in state["fetch_failures"])
        st.warning(
            f"🔍 [진단] 이번 스캔 중 발생한 개별 요청 실패 {len(state['fetch_failures'])}건 "
            f"(재시도로 회복된 것 포함) — 원인별 집계: {dict(reason_counts)}"
        )
        with st.expander("실패 상세 로그 보기"):
            st.write(state["fetch_failures"])

    reco_df = state.get("reco_df")
    if reco_df is not None and not reco_df.empty:
        st.session_state['reco_raw_data'] = reco_df
        try:
            reco_df.to_csv(RECO_PATH, index=False, encoding='utf-8-sig')
        except Exception:
            pass
        # ⚠️ [스캔 완료 시 AI 점수 자동 일괄 계산] 스캔 한 번으로 스크리너+추천
        # 후보가 갱신될 때, 사용자가 추천 종목 탭에 들어가 "AI 점수 일괄 계산"
        # 버튼을 따로 또 누르지 않아도 되도록 여기서 플래그만 켜둔다. 실제 계산
        # 제출/진행은 무거우니 스캔 자체(_unified_scan_worker)에 합쳐 넣지 않고,
        # 기존에 이미 검증된 배치 계산 경로(_render_ai_grade_filter_and_score →
        # render_async_multi, 30개씩·정체 감지·디스크 캐시 포함)를 그대로
        # 재사용한다. 대시보드/스크리너에 계속 머물러 있어도 진행되도록
        # maybe_kickoff_ai_bulk_scan()이 페이지와 무관하게 이어서 처리한다.
        st.session_state['_reco_ai_bulk_scan'] = True
    else:
        st.session_state.pop('reco_raw_data', None)
        if os.path.exists(RECO_PATH):
            try:
                os.remove(RECO_PATH)
            except Exception:
                pass
        # 후보가 아예 없어지면 이전 스캔의 AI/유동성 디스크 캐시는 어차피
        # 서명(_candidates_signature) 불일치로 다음에도 안 쓰이지만, 고아 파일로
        # 계속 남는 걸 막기 위해 같이 정리한다.
        for _stale_path in (AI_SCORE_CACHE_PATH, LIQ_VALUE_CACHE_PATH):
            if os.path.exists(_stale_path):
                try:
                    os.remove(_stale_path)
                except Exception:
                    pass
        st.warning(state.get("warning") or "분석 결과 고점 대비 유의미하게 하락한 종목이 없습니다.")

    st.success("✨ 스캔 완료! (스크리너 + 추천 종목 데이터가 함께 갱신되었습니다)")
    _SCAN_JOB_STATE.pop(job_id, None)
    st.rerun()


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

def _probability_tier(pct):
    """확률 구간별로 색상·이모지·감성 라벨을 매핑. (재미 요소 — 투자 조언 아님)"""
    if pct >= 60:
        return "#DC2626", "🔥", "꽤 유력해요"
    elif pct >= 35:
        return "#D97706", "⚡", "해볼 만해요"
    elif pct >= 15:
        return "#2563EB", "🤔", "쉽지 않아요"
    else:
        return "#64748B", "🥶", "희박해요"

def _format_probability_fun_card(result, target_price, target_src="목표가", current_price=None):
    """AI 확률분석 탭 전용 — 30/90/180일 도달확률을 밝은 배경의 카드 3개로
    나란히 보여주는 디자인. (재미 요소 — 투자 조언 아님)

    🔧 [2026-08-19] current_price를 옵션으로 받아 헤더에 "현재가 → 목표가
    (등락률)" 형태로 같이 보여준다. 목표가 하나만 덩그러니 있으면 "지금 얼마인데
    몇 % 움직여야 하는 목표인지"를 알 수 없어서, 나중에 컨센서스 목표가를 자동
    반영하게 되더라도(사용자 의도) 항상 현재가 대비 맥락을 함께 볼 수 있게 한다.
    호출부가 안 넘겨주면(None) 기존처럼 목표가만 표시 — 하위 호환 유지.

    ⚠️ 다른 탭(관심종목·전략계산)에서 쓰는 _format_hit_probability_badge와는 별개다
    (그쪽은 여러 종목이 한 화면에 쭉 나열되는 목록형이라 지금처럼 큰 카드를 쓰면
    화면이 너무 길어진다 — 이 탭은 종목 하나만 크게 보여주는 상세 페이지라서
    카드를 키워도 괜찮다).

    🎨 [2026-08-19 디자인 변경] 진한 다크 그라디언트 카드는 색이 너무 강하다는
    피드백을 받아, 흰 배경 카드 + 셋 중 확률이 가장 높은 구간만 컬러 테두리·
    컬러 텍스트로 강조하고 나머지 둘은 무채색(회색)으로 눌러주는 방식으로 바꿨다
    (참고 이미지: 매수 타점 카드에서 1차 진입만 보라색으로 강조하고 2·3차는 회색인
    패턴과 동일한 아이디어). 강조 대상은 셋 중 가장 확률이 높은 기간(best_h) —
    "이 종목은 어느 기간을 노리는 게 그나마 승산이 높은지"를 한눈에 보여준다.

    ⚠️ [버그 수정 이력] f-string은 반드시 줄 앞 들여쓰기 없이 이어붙인다.
    st.markdown의 마크다운 파서가 줄 앞 공백 4칸 이상을 "코드 블록"으로 해석해서
    HTML이 렌더링되지 않고 태그가 그대로 텍스트로 찍히는 문제가 있었기 때문.
    """
    if not result or not target_price or target_price <= 0:
        return ""
    probs = result["probs"]
    horizon_labels = {30: "30일 후", 90: "90일 후", 180: "180일 후"}
    horizons = (30, 90, 180)
    best_h = max(horizons, key=lambda h: probs.get(h, 0))

    cards_html = ""
    for h in horizons:
        pct = probs.get(h, 0)
        color, emoji, label = _probability_tier(pct)
        is_best = (h == best_h)
        border = f"1.5px solid {color}" if is_best else "1px solid #E2E8F0"
        bg = f"{color}0D" if is_best else "#FFFFFF"
        label_color = color if is_best else "#94A3B8"
        value_color = color if is_best else "#0F172A"
        sub_color = color if is_best else "#94A3B8"
        cards_html += (
            f'<div style="flex:1; text-align:center; padding:16px 10px; background:{bg}; '
            f'border:{border}; border-radius:12px;">'
            f'<div style="font-size:11.5px; color:{label_color}; font-weight:700; margin-bottom:6px;">'
            f'{horizon_labels[h]}</div>'
            f'<div style="font-size:24px; font-weight:800; color:{value_color};">{pct:.0f}%</div>'
            f'<div style="font-size:11px; color:{sub_color}; margin-top:6px; font-weight:600;">{emoji} {label}</div>'
            '</div>'
        )

    if current_price and current_price > 0:
        _diff_pct = (target_price - current_price) / current_price * 100
        _diff_sign = "+" if _diff_pct >= 0 else ""
        _diff_color = "#DC2626" if _diff_pct >= 0 else "#2563EB"
        header_html = (
            f'<span style="color:#0F172A; font-weight:700;">{current_price:,.0f}원</span>'
            f'<span style="color:#CBD5E1; margin:0 6px;">→</span>'
            f'<span style="color:#0F172A; font-weight:700;">{target_price:,.0f}원</span>'
            f'<span style="color:#94A3B8;">({target_src})</span>'
            f'<span style="color:{_diff_color}; font-weight:700; margin-left:6px;">{_diff_sign}{_diff_pct:.1f}%</span>'
        )
    else:
        header_html = f'{target_price:,.0f}원({target_src})'

    return (
        '<div style="margin-top:8px; padding:14px 16px; background:#F8FAFC; '
        'border:1px solid #E2E8F0; border-radius:14px;">'
        f'<div style="font-size:12.5px; color:#64748B; font-weight:700; margin-bottom:10px;">'
        f'🎲 {header_html} 도달 확률 · 종가 기준 통계 추정</div>'
        f'<div style="display:flex; gap:10px;">{cards_html}</div>'
        '</div>'
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
    df_annual, df_quarter, df_dividend = fetch_financial_data(code)

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

        if info.get("consensus_note"):
            _cdbg = _DEBUG_STORE.get(f"_consensus_debug_{code}")
            if _cdbg:
                with st.expander("🔧 디버그 정보 (컨센서스 조회 실패 원인 확인용)"):
                    st.json(_cdbg)

        # ── 최근 수급 동향 (외국인 / 기관 / 개인 추정 순매매) ────────────────────
        st.markdown("<h4 style='font-size:16px; margin:20px 0 4px 0;'>📊 최근 수급 동향</h4>", unsafe_allow_html=True)
        st.markdown(
            "<p style='font-size:12px; color:#64748B; margin-bottom:10px;'>"
            "외국인·기관이 동반 순매수로 돌아서는 구간은 통상 긍정적인 수급 신호로 해석됩니다. "
            "단, <b>개인 순매매는 네이버가 별도 제공하지 않아 (외국인+기관)의 반대부호로 추정한 값</b>입니다."
            "</p>",
            unsafe_allow_html=True,
        )

        period_choice = st.pills(
            "조회 기간",
            ["2주 (14일)", "4주 (1개월)", "24주 (6개월)"],
            default="4주 (1개월)",
            key=f"trend_period_{code}",
            label_visibility="collapsed",
        )
        if period_choice is None:
            period_choice = "4주 (1개월)"
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

            def custom_formatter(val, col_name, is_est=False):
                try:
                    clean_val = str(val).replace(',', '').strip()
                    f_val = float(clean_val)
                    if pd.isna(f_val) or clean_val == '-' or clean_val == 'nan': return "-"
                    prefix = "≈ " if is_est else ""
                    if '성장률' in col_name:
                        if f_val > 0: return f"{prefix}🔺 +{f_val:.1f}%"
                        elif f_val < 0: return f"{prefix}🔻 {f_val:.1f}%"
                        else: return f"{prefix}0.0%"
                    if col_name in ['매출액', '영업이익', '당기순이익']:
                        v_int = int(round(f_val))
                        is_minus = v_int < 0
                        abs_v = abs(v_int)
                        cho = abs_v // 10000
                        uk  = abs_v % 10000
                        formatted_num = f"{v_int:,}"
                        if cho > 0: return f"{prefix}{formatted_num} ({'-' if is_minus else ''}{cho}조 {uk:,}억)" if uk > 0 else f"{prefix}{formatted_num} ({'-' if is_minus else ''}{cho}조)"
                        return f"{prefix}{formatted_num} ({'-' if is_minus else ''}{uk:,}억)"
                    elif col_name in ['영업이익률', '순이익률', 'ROE', '부채비율']: return f"{prefix}{f_val:.1f}%"
                    elif col_name in ['PER', 'PBR']: return f"{prefix}{f_val:.2f}배"
                    return f"{prefix}{f_val:,}"
                except: return str(val)

            def format_and_style(input_df):
                display_df = input_df.copy()
                # '_est_cols'는 화면에 그대로 노출할 컬럼이 아니라, 어떤 셀이 마감 전
                # 기간이라 직전 실제값으로 근사 대체됐는지 표시하기 위한 내부 메타데이터다.
                # 여기서 꺼내 쓰고 실제 표에서는 제거한다.
                if '_est_cols' in display_df.columns:
                    est_series = display_df['_est_cols'].fillna('').astype(str)
                    display_df = display_df.drop(columns=['_est_cols'])
                else:
                    est_series = pd.Series([''] * len(display_df), index=display_df.index)
                for col in display_df.columns[1:]:
                    est_flags = [col in e.split(',') for e in est_series]
                    display_df[col] = [
                        custom_formatter(v, col, is_est=flag)
                        for v, flag in zip(display_df[col], est_flags)
                    ]
                def style_cells(val):
                    if '≈' in str(val): return 'color: #64748B; font-style: italic;'
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
                    # ── [분기(E) 컬럼 근사치 표시 안내 — 2026-08-06] ──────────────────
                    # "2026/06(E)"처럼 아직 마감되지 않은 분기는 컨센서스 원본 자체가
                    # 매출액·영업이익률 등만 먼저 제공하고, ROE·PER·PBR·부채비율(그리고
                    # 이로부터 역산 가능한 영업이익 금액)은 분기 마감 전까지 아예
                    # 제공하지 않는 경우가 흔하다. 이제는 빈 칸으로 두는 대신 직전
                    # 실제(마감) 분기 값을 '≈' 표시로 근사해서 채워 보여준다.
                    _latest_q = str(df_quarter['연도/분기'].iloc[-1])
                    if '(E)' in _latest_q:
                        st.caption(
                            f"💡 **{_latest_q}**는 아직 마감되지 않은 분기(컨센서스 추정치)입니다. "
                            "매출액·영업이익률 등은 컨센서스 제공값을 그대로 표시하고, "
                            "ROE·PER·PBR·부채비율처럼 분기 컨센서스에 아직 없는 값은 "
                            "직전 실제(마감) 분기 값을 **'≈'** 표시로 근사해 보여줍니다."
                        )
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
            st.caption("ℹ️ 연간/분기 실적 데이터를 불러오지 못했습니다. (FnGuide·네이버 두 소스 모두 실패)")
            _fdbg = _DEBUG_STORE.get(f"_fnguide_debug_{code}")
            _ndbg = _DEBUG_STORE.get(f"_naver_wise_debug_{code}")
            if _fdbg or _ndbg:
                with st.expander("🔧 디버그 정보 (실적 데이터 실패 원인 확인용)"):
                    st.write("① FnGuide 조회 결과:")
                    st.json(_fdbg or {"info": "호출 안 됨"})
                    st.write("② 네이버(WiseReport) 폴백 조회 결과:")
                    st.json(_ndbg or {"info": "호출 안 됨 (FnGuide가 성공했거나 폴백 로직 미도달)"})

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
# ⚠️ [보안 수정 2026-08-18] SESSION_SECRET 미설정 시 하드코딩된 고정 문자열
# ("insecure-default-please-set-SESSION_SECRET")을 그대로 서명 키로 써왔다.
# 이 문자열은 소스코드에 그대로 노출돼 있으므로, 배포 시 SESSION_SECRET을
# 설정하지 않으면 누구나 그 문자열로 make_session_token()과 똑같은 HMAC을
# 직접 계산해서 임의의 아이디에 대한 로그인 토큰을 위조할 수 있었다(=인증
# 우회). 해결: 설정이 없으면 앱 프로세스당 한 번만 무작위 시크릿을 생성해
# st.cache_resource로 캐싱해서 쓴다. 위조는 막히지만, 그 대가로 앱이
# 재시작될 때마다(예: 배포 재시작) 발급됐던 세션 토큰은 전부 무효화되어
# 사용자가 다시 로그인해야 한다 — 위조 가능한 상태보다는 훨씬 안전한
# 트레이드오프라 이렇게 처리한다. 근본 해결은 배포 환경에 SESSION_SECRET을
# 직접 설정하는 것이며, 이 폴백은 그걸 깜빡했을 때의 안전망일 뿐이다.
@st.cache_resource(show_spinner=False)
def _get_process_random_session_secret():
    return secrets.token_hex(32)

def _get_session_secret():
    try:
        return str(st.secrets["SESSION_SECRET"])
    except Exception:
        env_secret = os.environ.get("SESSION_SECRET")
        if env_secret:
            return env_secret
        return _get_process_random_session_secret()

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


def is_admin_user():
    """현재 로그인한 사용자가 관리자인지 확인.

    secrets.toml에 다음처럼 등록해서 관리한다 (쉼표로 여러 명 등록 가능):
        ADMIN_USERNAMES = "내아이디,다른관리자아이디"
    등록돼 있지 않으면 아무도 관리자가 아닌 것으로 간주(보수적 기본값).
    """
    current = st.session_state.get("auth_user")
    if not current:
        return False
    try:
        raw = st.secrets.get("ADMIN_USERNAMES", "")
    except Exception:
        raw = ""
    admin_ids = {u.strip() for u in str(raw).split(",") if u.strip()}
    return current in admin_ids


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
    _futures = {submit_with_ctx(_executor, fetch_live_price_change, code): code for code in codes}
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
        # [저장 버튼 클릭 시 멈춤 대응] render_async_multi의 자동 폴링(0.4초 간격)은
        # 아직 안 끝났으면 st.fragment 안에서 st.rerun()으로 "전체 페이지"를 강제로
        # 다시 실행시킨다. 이건 신규 종목처럼 보여줄 데이터가 아예 없을 때는 필요하지만,
        # 이미 캐시된 값이 있고 그저 55초 신선도가 지나서 "슬쩍 갱신"하는 상황에서까지
        # 이 강제 리런 루프를 돌리면, 하필 사용자가 매수가/수량을 입력하고 저장 버튼을
        # 누르는 시점과 겹칠 때 백그라운드 강제 리런이 진행 중인 상호작용과 부딪혀
        # 세션이 꼬이는 것으로 추정되는 멈춤 현상이 있었다.
        # 그래서 "완전히 처음 보는 신규 종목"만 어쩔 수 없이 기다리게 하고,
        # "이미 있던 종목의 신선도 갱신"은 대기/강제리런 없이 조용히 백그라운드에
        # 던져두기만 했다가 다음 자연스러운 재실행(다른 버튼 클릭 등) 때 반영한다.
        _WL_PREFETCH_REFRESH_SEC = 55
        _wl_cache_key = "_wl_prefetch_cache"
        _wl_cache = st.session_state.setdefault(
            _wl_cache_key,
            {"results": {"price": {}, "spark": {}, "ai": {}, "hitprob": {}}, "ts": {}},
        )
        _wl_now = time.time()

        _wl_missing_codes = [c for c in _wl_codes if c not in _wl_cache["ts"]]
        _wl_stale_codes = [
            c for c in _wl_codes
            if c not in _wl_missing_codes and (_wl_now - _wl_cache["ts"][c]) >= _WL_PREFETCH_REFRESH_SEC
        ]

        if _wl_missing_codes:
            def _submit_wl_jobs():
                _wl_executor = get_shared_executor()
                return {c: submit_with_ctx(_wl_executor, _wl_prefetch_one, c) for c in _wl_missing_codes}

            def _collect_wl_results(futures):
                out = {"price": {}, "spark": {}, "ai": {}, "hitprob": {}}
                for _code, f in futures.items():
                    if f.done():
                        try:
                            _c, _price_info, _spark, _ai_score, _hp_html = f.result(timeout=0.1)
                            out["price"][_c] = _price_info
                            out["spark"][_c] = _spark
                            out["ai"][_c] = _ai_score
                            if _hp_html:
                                out["hitprob"][_c] = _hp_html
                        except Exception:
                            pass
                return out

            _wl_new_results, _wl_ready = render_async_multi(
                job_key="watchlist_prefetch_new",
                submit_fn=_submit_wl_jobs,
                collect_fn=_collect_wl_results,
                default_result={"price": {}, "spark": {}, "ai": {}, "hitprob": {}},
                spinner_text=f"신규 관심종목 {len(_wl_missing_codes)}건 시세 조회 중...",
                overall_timeout=15,
            )
            if not _wl_ready:
                return  # 처음 보는 종목이라 보여줄 게 없을 때만 대기

            for _key in ("price", "spark", "ai", "hitprob"):
                _wl_cache["results"][_key].update(_wl_new_results.get(_key, {}))
            for _c in _wl_missing_codes:
                _wl_cache["ts"][_c] = _wl_now

        if _wl_stale_codes:
            # 강제 리런/대기 없이 그냥 백그라운드에 던져만 놓는다.
            _wl_bg_jobs = st.session_state.setdefault("_wl_bg_jobs", {})
            _wl_executor = get_shared_executor()
            for c in _wl_stale_codes:
                if c not in _wl_bg_jobs or _wl_bg_jobs[c].done():
                    _wl_bg_jobs[c] = submit_with_ctx(_wl_executor, _wl_prefetch_one, c)

        # 예전에 백그라운드로 던져놓은 작업 중 그 사이 끝난 게 있으면 조용히 캐시에 반영
        _wl_bg_jobs = st.session_state.get("_wl_bg_jobs", {})
        for c in list(_wl_bg_jobs.keys()):
            f = _wl_bg_jobs[c]
            if f.done():
                try:
                    _c, _price_info, _spark, _ai_score, _hp_html = f.result(timeout=0.1)
                    _wl_cache["results"]["price"][_c] = _price_info
                    _wl_cache["results"]["spark"][_c] = _spark
                    _wl_cache["results"]["ai"][_c] = _ai_score
                    if _hp_html:
                        _wl_cache["results"]["hitprob"][_c] = _hp_html
                    _wl_cache["ts"][_c] = time.time()
                except Exception:
                    pass
                del _wl_bg_jobs[c]

        # 관심종목에서 삭제된 종목의 캐시/백그라운드 작업은 정리(메모리 누적 방지)
        _wl_codes_set = set(_wl_codes)
        for _key in ("price", "spark", "ai", "hitprob"):
            _wl_cache["results"][_key] = {
                c: v for c, v in _wl_cache["results"][_key].items() if c in _wl_codes_set
            }
        _wl_cache["ts"] = {c: v for c, v in _wl_cache["ts"].items() if c in _wl_codes_set}

        wl_price_cache = _wl_cache["results"]["price"]
        sparkline_cache = _wl_cache["results"]["spark"]
        ai_score_cache = _wl_cache["results"]["ai"]
        hit_prob_cache = _wl_cache["results"]["hitprob"]


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
    # ── [워치독 통합] 개별 arm/cancel은 파일 상단의 영구(repeat=True) 워치독과
    # 충돌하므로 여기서 더 이상 걸지 않는다 (자세한 이유는 파일 상단 주석 참고).
    _main_impl()


def _main_impl():
    if 'auth_user' not in st.session_state:
        st.session_state.auth_user = None

    # 🔁 F5 새로고침 대응: session_state는 새로고침 시 초기화되지만
    # URL 쿼리파라미터는 유지되므로, 그 안의 서명 토큰으로 로그인 상태를 복원한다.
    if not st.session_state.auth_user:
        _token = st.query_params.get("session_token")
        if _token:
            _restored_user = verify_session_token(_token)
            if _restored_user:
                st.session_state.auth_user = _restored_user

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
            ("AI 확률분석", ":material/percent:"),
            ("실시간 배당 순위", ":material/payments:"),
        ]),
        ("MY PAGE", [
            ("관심종목", ":material/bookmark:"),
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
                st.button(
                    label,
                    icon=icon,
                    key=f"nav_{label}",
                    type="primary" if is_selected else "secondary",
                    use_container_width=True,
                    # 🔧 [사이드바 색상 한 박자 밀리는 현상 대응] 예전에는
                    # `if st.button(...): session_state 갱신` 방식이었다. 이 경우
                    # "이 버튼 자신의 type(primary/secondary)"은 이미 클릭을 처리하기
                    # *전에* 계산된 is_selected로 그려져 버린 뒤라, 클릭한 바로 그
                    # 렌더링에서는 메인 콘텐츠만 바뀌고 버튼 색은 다음 클릭이 와야
                    # 비로소 새 상태를 반영했다. on_click 콜백은 스크립트가 처음부터
                    # 다시 그려지기 *전에* 먼저 실행되므로, session_state가 미리
                    # 갱신된 채로 이번 렌더링의 모든 버튼이 그려져 색상이 항상 맞다.
                    on_click=lambda _label=label: st.session_state.__setitem__("current_page", _label),
                )

        _show_debug_memory()

    selected = st.session_state.current_page

    # ── [원인 진단용 임시 로그] ────────────────────────────────────────
    # 멈춤 현상이 완전히 해결됐다고 확신이 들 때까지만 남겨둔다. 로그의 마지막
    # "진입"만 있고 "완료"가 없는 페이지가 바로 멈춘 지점이다.
    print(f"[DEBUG {datetime.datetime.now().strftime('%H:%M:%S')}] 페이지 진입: {selected}", file=sys.stderr, flush=True)

    # ── [AI 점수 일괄 계산 이어가기 — 반드시 페이지 렌더링보다 먼저] ─────────────
    # 대시보드/종목 스크리너 페이지는 자기 렌더링 함수 안에서 스스로
    # maybe_run_global_poller()를 호출해 전역 폴링 fragment를 띄운다. 그 호출
    # 시점에 _bg_jobs가 비어있으면 fragment 자체가 아예 안 뜬다(has_pending=False
    # 이면 마운트를 건너뜀). 그래서 AI 점수 일괄 계산 job은 각 페이지가 자기
    # 폴러를 부르기 "전에" 먼저 _bg_jobs에 등록돼 있어야, 그 페이지의 폴러가
    # 이 job도 함께 보고 fragment를 정상적으로 띄워준다. (스캔 완료 시점에
    # run_unified_market_scan_async()가 플래그를 켠 직후 st.rerun()으로 다음
    # 실행으로 넘기므로, 다음 실행의 이 시점엔 플래그가 이미 반영돼 있다.)
    if selected != "추천 종목":
        maybe_kickoff_ai_bulk_scan()

    # ── [워치독 통합] 개별 arm/cancel은 파일 상단의 영구(repeat=True) 워치독과
    # 충돌하므로 여기서 더 이상 걸지 않는다 (자세한 이유는 파일 상단 주석 참고).
    # "진입"/"완료" 로그 자체는 여전히 유용하므로(어느 페이지에서 안 돌아오는지
    # 특정 가능) 그대로 남겨둔다.
    if   selected == "대시보드 홈":      render_dashboard()
    elif selected == "추천 종목":        render_recommendations()
    elif selected == "종목 스크리너":    render_screener()
    elif selected == "기업 재무 분석":   render_fnguide()
    elif selected == "AI 확률분석":      render_ai_probability()
    elif selected == "실시간 배당 순위": render_dividend()
    elif selected == "관심종목":         render_watchlist()
    elif selected == "비밀번호 변경":     render_change_password()

    print(f"[DEBUG {datetime.datetime.now().strftime('%H:%M:%S')}] 페이지 렌더링 완료: {selected}", file=sys.stderr, flush=True)

    # ── [전역 폴링 fragment 호출 — "이번 스크립트 실행"에 딱 한 번만] ───────────
    # 위에서 렌더링한 페이지가 render_async_multi/run_unified_market_scan_async를
    # 통해 _bg_jobs 또는 _scan_jobs에 작업을 남겨뒀을 수 있다. 그 작업들의 완료
    # 여부를 감시하는 주기적(run_every) fragment는 이번 스크립트 실행 동안 반드시
    # 딱 한 번만 호출돼야 한다 — 같은 실행 안에서 두 번 호출되면 그 순간 화면에
    # run_every fragment가 2개 동시에 떠서 Streamlit 버그 #10719("does not exist
    # anymore")가 재현될 수 있기 때문이다.
    # [진행률 배너 위치 문제 수정] 예전에는 이 중앙 호출 단 한 곳에서만 불렀는데,
    # 그 위치가 "어떤 페이지든 다 그린 뒤"라서 실시간 진행률 배너가 항상 그 페이지
    # 맨 아래(스캔 버튼과 동떨어진 곳)에만 나타났다. 대시보드와 종목 스크리너는
    # 스캔 버튼 바로 아래에서 보이도록 각자의 render 함수 안에서 직접 이 fragment를
    # 이미 호출해두었으므로, 여기서 또 부르면 "한 실행에 두 번 호출"이 되어 버린다.
    # 그래서 이 두 페이지일 때는 여기서 건너뛴다.
    if selected not in ("대시보드 홈", "종목 스크리너"):
        maybe_run_global_poller()


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
            # render_async_multi의 짧은 재사용 캐시도 같이 비워야, 새로고침 직후에
            # 방금까지 쓰던 옛 결과가 다시 그대로 재사용되는 일이 없다.
            st.session_state.get("_bg_job_results", {}).pop("dashboard_main_data", None)
            st.rerun()
    with col_scan:
        if DEBUG_DISABLE_DASHBOARD_SCAN:
            st.button(
                "종목 스캔 (임시 비활성화 · 테스트 중)",
                use_container_width=True,
                key="dash_unified_scan_btn",
                disabled=True,
                help="원인 파악을 위해 임시로 꺼둔 상태입니다. DEBUG_DISABLE_DASHBOARD_SCAN 값을 False로 되돌리면 다시 켜집니다.",
            )
            _dash_scan_clicked = False
        else:
            _dash_scan_clicked = st.button("종목 스캔 (스크리너+추천)", use_container_width=True, key="dash_unified_scan_btn")
    if not DEBUG_DISABLE_DASHBOARD_SCAN and (_dash_scan_clicked or "unified_scan" in st.session_state.get("_scan_jobs", {})):
        run_unified_market_scan_async()
    with col_rate_strip:
        render_rate_strip()

    # ── [로딩 배너 위치를 화면 상단으로 — 2026-08-06] ─────────────────────────
    # 사용자가 원하는 위치는 스캔 버튼 바로 아래(화면 상단)다. 그런데 poller
    # 호출 자체를 여기로 옮기면 예전에 이미 한 번 겪었던 무한 리렌더 루프
    # 버그가 재발한다 — 바로 아래 [무한 리렌더 루프 버그 수정] 주석 참고:
    # 완료됐지만 아직 수거(pop)되지 않은 job이 있을 때 poller가 여기서 먼저
    # _all_jobs_settled()=True를 보고 st.rerun()을 던지면, 그 job을 실제로
    # 수거하는 뒤쪽 render_async_multi() 호출까지 스크립트가 도달하지 못해서
    # job이 "완료됐는데 안 치워진" 채로 영원히 남아 매 실행마다 즉시
    # rerun→중단이 반복된다.
    #
    # 해결: "호출 위치"와 "화면에 그려지는 위치"를 분리한다. st.empty()로
    # 화면상의 자리(placeholder)만 여기(상단)에 미리 잡아두고, 실제
    # maybe_run_global_poller() 호출은 여전히 함수 맨 끝 — 모든
    # render_async_multi() 호출(=job 수거 지점)이 끝난 뒤 — 에서 한다. 그때
    # with _poller_slot.container(): 로 감싸서 호출하면, 실제 실행은 뒤에서
    # 하면서도 화면에는 이 placeholder 자리(=상단)에 그려진다.
    # ⚠️ 절대 이 지점에서 바로 maybe_run_global_poller()를 호출하지 말 것.
    _poller_slot = st.empty()

    # ── [무한 리렌더 루프 버그 수정 — 2026-08-06] ─────────────────────────────
    # 문제: maybe_run_global_poller()가 예전에는 여기(스캔 버튼 바로 아래)에서
    # 호출됐다. 그런데 실제 백그라운드 작업(dashboard_main_data, investor_monthly_*)은
    # 이 지점보다 한참 "뒤"의 render_async_multi() 호출에서 생성되고, 그 작업이
    # 끝났을 때 _bg_jobs에서 지우는(jobs.pop) 것도 바로 그 뒤쪽 호출이 담당한다.
    #
    # 그 결과 이런 일이 벌어졌다: 예를 들어 수급동향 토글을 열어 investor_monthly_*
    # 작업이 백그라운드에서 완료되면(future.done()=True), 그 job은 아직 여기
    # 위쪽(이 지점)에서는 전혀 손대지 않은 채로 _bg_jobs에 "완료된 채로" 남아있는
    # 상태였다. 다음 스크립트 실행이 이 지점에 도달하면 maybe_run_global_poller()는
    # "_bg_jobs가 비어있지 않다"는 이유만으로 전역 폴링 fragment를 새로 띄웠고,
    # 그 fragment는 즉시 "어차피 다 끝났다(_all_jobs_settled()=True)"고 판단해서
    # *그 자리에서 바로* st.rerun()을 호출했다. st.rerun()은 예외를 던져 현재
    # 스크립트 실행을 즉시 중단시키므로, 스크립트는 실제로 job을 수거(pop)하는
    # 뒤쪽 코드까지 끝내 도달하지 못했다. 그래서 job은 "완료됐지만 안 치워진" 채로
    # 영원히 남고, 이 패턴이 매 실행마다 반복되며 초당 수 회의 전체 페이지
    # 리렌더 무한 루프가 됐다 (로그에서 "페이지 진입"만 잔뜩 찍히고 "페이지
    # 렌더링 완료"는 거의 안 찍힌 이유).
    #
    # 해결: maybe_run_global_poller() "호출"은 이 함수(render_dashboard) 안의 모든
    # render_async_multi() 호출(=job 생성/수거 지점)보다 "뒤"에서, 딱 한 번만
    # 한다 (아래 함수 맨 끝). 이러면 이번 스크립트 실행에서 이미 끝난 job은
    # poller가 보기 전에 먼저 수거돼서 _bg_jobs에서 사라지고, poller는 "진짜로
    # 아직 안 끝난" 작업이 있을 때만 fragment를 띄우게 된다. 화면상 위치만
    # 위 _poller_slot 덕분에 상단(스캔 버튼 바로 아래)으로 옮겨진다.
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
            "indices":    submit_with_ctx(_dash_executor, fetch_market_index_table),
            "sparklines": submit_with_ctx(_dash_executor, fetch_sparkline_data),
            "trend":      submit_with_ctx(_dash_executor, fetch_investor_trend),
            "df_sector":  submit_with_ctx(_dash_executor, fetch_sector_ranking),
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
    # ── [로딩 배너 위치 통일 — 2026-08-06] ──────────────────────────────────
    # 예전에는 여기서 바로 poller를 켜고 return 해버렸다. 그러면 이 시점 이후의
    # 모든 렌더링(시장 지수/수급 동향/핫 섹터/데이터 안내)이 통째로 스킵되면서
    # 페이지가 "버튼 줄 + 로딩 배너"만 남은 아주 짧은 상태가 되고, 그 결과 로딩
    # 배너가 (원래는 맨 아래인) 데이터 안내 박스 자리가 아니라 화면 훨씬 위쪽에,
    # 그리고 다른 내용이 없어 보이니 사실상 "화면 맨 아래"처럼 나타났다.
    # 해결: 여기서 return 하지 않고 그냥 계속 진행한다. _dash_results는 이미
    # default_result(빈 dict/DataFrame)로 채워져 있어서 아래 렌더링 코드가 전부
    # "데이터 없음" 형태로 안전하게 그려진다. poller 호출도 더 이상 여기서 하지
    # 않고, 함수 맨 끝(데이터 안내 박스 바로 아래) 단 한 곳에서만 한다 — 그래야
    # 로딩 배너가 항상 같은 자리에 뜬다.

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
            # [환율/금/원유 카드에서 HTML 태그가 그대로 텍스트로 보이는 문제 수정]
            # show_volume=False일 때 vol_html이 빈 문자열이 되는데, 위 f-string은
            # Python 소스 들여쓰기가 그대로 살아있는 여러 줄짜리 문자열이라
            # "공백만 있는 줄"(빈 vol_html 자리) 바로 다음에 "4칸 이상 들여쓰인 줄"
            # (chart_section)이 오게 된다. 마크다운 문법에서는 이 조합을 "들여쓰기
            # 코드블록"으로 해석해서, 그 아래 HTML을 그대로 이스케이프된 텍스트로
            # 보여준다(정확히 스크린샷에서 보인 증상). 줄마다 앞뒤 공백을 지워서
            # 이 오인식 자체가 안 일어나게 한다.
            html_stripped = "\n".join(line.strip() for line in html.split("\n"))
            st.markdown(html_stripped, unsafe_allow_html=True)

    c1, c2, c3 = st.columns(3)
    for col, key in [(c1, "kospi"), (c2, "kosdaq"), (c3, "nasdaq")]:
        render_index_card(col, key, indices.get(key, {}), show_volume=True)

    # 코스피/코스닥 거래량 정상 확인 완료 (진단용 expander 제거됨)

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
            # ── [Streamlit 확인된 버그(#10719) 회피] 동시에 여러 개의 run_every
            # fragment가 뜨면 "fragment does not exist anymore" 에러와 함께 세션이
            # 멈추는 게 Streamlit 팀이 인정한 버그다. render_async_multi가 토글마다
            # 하나씩 주기적 fragment를 만들기 때문에, 코스피/코스닥을 동시에 열어두면
            # 그 순간 2개가 동시에 돌면서 이 버그에 걸린다. 그래서 한 번에 하나만
            # 열리도록 강제한다(새로 여는 시장 외에는 전부 닫음).
            #
            # ── [토글 반복 클릭 시 멈춤 추가 대응] ────────────────────────────
            # 위 대응만으로는 "아직 데이터 로딩(=백그라운드 job)이 안 끝난 시장을
            # 닫자마자 바로 다른 시장을 여는" 경우가 막히지 않는다. 이때 방금 닫은
            # 시장의 render_async_multi job이 st.session_state["_bg_jobs"]에 그대로
            # 남아있으면, 그 job에 딸려있던 주기적(run_every) 프래그먼트가 화면에서는
            # 사라졌는데도 백엔드에서는 계속 스케줄링되고 있을 수 있다. 여기서 곧바로
            # 다른 시장을 열어 새 프래그먼트가 또 뜨면 두 프래그먼트가 겹치면서
            # 위와 동일한 Streamlit 버그(#10719)에 걸릴 수 있다. 그래서 시장을 닫는
            # 순간, 그 시장에 대해 진행 중이던 백그라운드 job을 명시적으로 지워서
            # "닫혔는데 뒤에서 계속 폴링되는" 상태 자체를 없앤다.
            opening = not is_open
            if opening:
                st.session_state["investor_open"] = {k: False for k in st.session_state["investor_open"]}
            else:
                st.session_state.get("_bg_jobs", {}).pop(f"investor_monthly_{market_key}", None)
            st.session_state["investor_open"][market_key] = opening
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

        if is_open and "unified_scan" in st.session_state.get("_scan_jobs", {}):
            # ── [Streamlit 확인된 버그(#10719) 회피] 전체 시장 스캔이 진행 중이면
            # 이미 스캔 진행률용 주기적(run_every) fragment가 하나 떠 있는 상태다.
            # 여기서 또 render_async_multi가 두 번째 주기적 fragment를 만들면,
            # 두 fragment의 자동 재실행 타이밍이 겹치면서 "does not exist anymore"
            # 에러와 함께 세션이 멈추는 Streamlit 자체 버그(github.com/streamlit/
            # streamlit/issues/10719)에 걸린다. 그래서 스캔이 끝날 때까지는 이
            # 수급 추이 조회 자체를 잠깐 미뤄둔다(스캔이 끝나면 자연히 다시 열림).
            st.markdown(
                '<div style="border:1px solid #E2E8F0;border-top:none;border-radius:0 0 8px 8px;'
                'padding:12px 16px;font-size:12px;color:#94A3B8;">⏳ 전체 시장 스캔이 진행 중이라 잠시 후 표시됩니다.</div>',
                unsafe_allow_html=True
            )
        elif is_open:
            # ── [수급 토글 반복 클릭 시 완전 멈춤 수정] ──────────────────────────
            # 이전에는 run_with_progress(...)가 fetch_investor_trend_monthly를
            # "메인 스크립트 스레드에서 직접" 동기 호출했다. 이 함수는 내부적으로
            # 22개의 날짜별 요청을 공유 스레드풀에 던지고 최대 12초간 그 자리에서
            # 기다리는데, 그동안 Streamlit은 새로운 클릭(토글을 또 누르는 것 포함)을
            # 전혀 받아줄 수 없다. 사용자가 토글을 반복해서 누르면 이런 블로킹 호출이
            # 계속 쌓이고, 공유 풀(32개 워커)에 요청이 몰리면서 결국 앱 전체가
            # 완전히 멈추는(리붓 전에는 회복 안 되는) 상태로 이어졌다.
            # 해결: 대시보드의 다른 카드들과 동일하게 render_async_multi(논블로킹 +
            # 짧은 간격 폴링) 패턴으로 바꾼다. 이러면 메인 스레드가 절대 막히지 않고,
            # 토글을 반복해서 눌러도 매번 즉시 다음 상호작용을 받을 수 있다. 또한
            # render_async_multi에 이미 추가된 "짧은 재사용 캐시" 덕분에, 같은 시장을
            # 짧은 시간 안에 다시 열었다 닫았다 해도 재조회 자체가 일어나지 않는다.
            _monthly_result, _monthly_ready = render_async_multi(
                job_key=f"investor_monthly_{market_key}",
                submit_fn=lambda: {"monthly": submit_with_ctx(get_orchestration_executor(), fetch_investor_trend_monthly, sosok)},
                collect_fn=lambda futures: {
                    "monthly": (futures["monthly"].result(timeout=0.1) if futures["monthly"].done() else None) or []
                },
                default_result={"monthly": []},
                spinner_text=f"{market_label} 월별 수급 불러오는 중...",
                overall_timeout=15,
            )
            # ── [로딩 배너 위치 통일 — 2026-08-06] ──────────────────────────
            # 예전에는 여기서 바로 return 해서 이후 렌더링(핫 섹터, 데이터 안내,
            # 그리고 함수 맨 끝의 poller 호출)까지 전부 건너뛰었다. 그러면 이
            # 토글이 로딩 중인 동안은 poller 자체가 아예 켜지지 않아서(함수 밖
            # _main_impl()도 대시보드 홈 페이지에서는 poller를 안 켜주므로) 화면이
            # 자동으로 갱신되지 않는 문제도 있었다. 이제는 이 섹션만 "불러오는
            # 중" placeholder로 대체하고 계속 진행해서, 로딩 배너가 함수 맨 끝
            # (데이터 안내 박스 바로 아래) 한 곳에서만 뜨도록 통일한다.
            if not _monthly_ready:
                st.markdown(
                    '<div style="border:1px solid #E2E8F0;border-top:none;border-radius:0 0 8px 8px;'
                    'padding:12px 16px;font-size:12px;color:#94A3B8;">⏳ 월별 수급 데이터를 불러오는 중입니다...</div>',
                    unsafe_allow_html=True
                )
                monthly = []
            else:
                monthly = _monthly_result.get("monthly") or []
            if _monthly_ready and not monthly:
                st.markdown(
                    '<div style="border:1px solid #E2E8F0;border-top:none;border-radius:0 0 8px 8px;'
                    'padding:12px 16px;font-size:12px;color:#94A3B8;">데이터를 불러올 수 없습니다.</div>',
                    unsafe_allow_html=True
                )
            elif _monthly_ready:
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

    # ── [로딩 배너 위치를 화면 상단으로 — 2026-08-06] ─────────────────────────
    # 여기 도달했다는 것은 이 함수 안의 모든 render_async_multi() 호출(=job
    # 수거 지점)을 이미 다 지나온 뒤라는 뜻이므로, 지금 poller를 켜는 것은
    # 안전하다(위쪽 "무한 리렌더 루프 버그 수정" 주석 참고). 실제 "호출"은
    # 여기서 하지만, _poller_slot.container()로 감싸서 화면에는 함수 상단에서
    # 미리 잡아둔 자리(스캔 버튼 바로 아래)에 그려지도록 한다.
    with _poller_slot.container():
        maybe_run_global_poller()

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
    """관심종목 카드 등에서 재사용: 종목코드만으로 AI 종합점수(0~1000)를 계산. 실패 시 None.
    screener_df를 미리 전달하면(예: 관심종목 병렬조회) 백그라운드 스레드에서 다시
    load_screener_df()를 호출하지 않아도 되어 session_state 접근을 피할 수 있다.

    ⚠️ [세분화 리뉴얼] 배점 체계를 4항목(건전성/성장성/수익성/배당) 뭉뚱그린 점수에서
    8항목(추세/수급/거래량/재무/밸류/모멘텀/AI패턴/리스크) 세부 배점으로 교체했다
    (calc_ai_scores_detailed). '왜 이 점수인지' 항목별로 바로 보이도록 하기 위함."""
    try:
        df_annual_ai, _, _ = fetch_financial_data(code)
        per_ai, pbr_ai, roe_ai, debt_ai, drop_pct_ai, div_ai = get_ai_diagnosis_inputs(code, df_annual_ai, screener_df=screener_df)
        return calc_ai_scores_detailed(code, per_ai, pbr_ai, roe_ai, debt_ai, drop_pct_ai, div_ai, df_annual=df_annual_ai)["total"]
    except Exception:
        return None

# ═══════════════════════════════════════════════════════════════════════
# 🔬 [AI 종합점수 세분화] 8개 항목 배점 체계
# ─────────────────────────────────────────────────────────────────────
# TradingView Technical Rating(추세/모멘텀 계열 지표 종합), TrendSpider(추세·모멘텀·
# 거래량·돌파 점수를 각각 분리), MarketSmith(RS Rating·SMR Rating 등 항목별 등급)의
# "카테고리를 잘게 쪼개서 각각 보여준다"는 아이디어를 참고해, 기존 4항목(건전성/
# 성장성/수익성/배당)을 아래 8항목으로 재구성했다.
#
#   추세(0~20) · 수급(0~20) · 거래량(0~10) · 재무(0~15) ·
#   밸류(0~10) · 모멘텀(0~10) · AI패턴(0~10) · 리스크(-5~0)
#
# ⚠️ 'AI패턴' 항목은 이름과 달리 실제 머신러닝 유사도 검색이 아니라, 과거 급등주에서
# 자주 관찰되는 규칙(직전 저항선 돌파 + 거래량 동반 + 단기 상승 우위)을 조합한
# 근사 휴리스틱이다. UI에도 이 점을 명시해 과장된 신뢰를 주지 않도록 한다.
#
# 각 항목은 가격/거래량/수급 데이터 조회가 실패해도(네트워크 지연 등) 전체 계산이
# 막히지 않도록 항목별로 try/except를 걸고, 실패 시에는 극단값 대신 "중립값"으로
# 대체한다(예: 거래량 데이터가 없으면 만점/0점이 아니라 중간값 5점).
# ═══════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=1800, show_spinner=False)
def fetch_price_history_for_score(code, period="1y"):
    """추세/거래량/모멘텀/패턴/리스크 점수 계산용 일봉 OHLCV(종가·거래량·고가).
    코스피(.KS) 우선 조회 후 실패하면 코스닥(.KQ)으로 폴백. 실패 시 빈 DataFrame."""
    code = normalize_kr_code(code)
    if _is_invalid_kr_code(code):
        return pd.DataFrame()
    try:
        import yfinance as yf
        df = call_with_timeout(
            lambda: yf.Ticker(f"{code}.KS").history(period=period, interval="1d", timeout=8),
            timeout=10,
        )
        if df is None or df.empty:
            df = call_with_timeout(
                lambda: yf.Ticker(f"{code}.KQ").history(period=period, interval="1d", timeout=8),
                timeout=10,
            )
        if df is None or df.empty:
            return pd.DataFrame()
        df = df[["Close", "Volume", "High"]].dropna(subset=["Close"])

        # ── [버그 수정: 당일 거래량 0으로 잡혀 거래량/추세/모멘텀 점수가 왜곡되는 문제] ──
        # 코스피/코스닥 종목을 yfinance로 조회하면, 장중이거나 당일 데이터가 아직
        # 완전히 반영되기 전에는 가장 최근(오늘) 봉의 Volume이 0 또는 NaN으로 채워진
        # 채 내려오는 경우가 흔하다(야후가 KRX 거래량 집계를 종가 확정 후에야 채워
        # 넣는 것으로 추정됨). 이 상태를 그대로 쓰면 "오늘 거래량 0 ÷ 20일 평균" = 0점
        # 으로 계산되어, 실제로는 정상 거래되는 날에도 거래량 점수가 0으로 나오고
        # (실측: 삼성전자 거래량 0/10), 같은 미확정 봉의 종가를 쓰는 추세·모멘텀
        # 점수까지 함께 낮게 왜곡됐다. 마지막 봉이 거래량 0/NaN이면 아직 확정되지
        # 않은 데이터로 보고 제거하고, 그 앞의 확정된 거래일 데이터로 계산한다.
        if not df.empty and (pd.isna(df["Volume"].iloc[-1]) or df["Volume"].iloc[-1] == 0):
            df = df.iloc[:-1]

        # ── [버그 수정: yfinance 가격 자체가 실제 주가와 몇 배씩 어긋나는 문제] ──────
        # 실측 사례(2026-08): 삼성전자(005930)를 조회하면 yfinance가 최근 종가를
        # 1,458,000원으로 내려줬다. 그런데 실제 삼성전자는 2018년 50:1 액면분할
        # 이후 2026년 8월 기준 20만원대 후반~30만원대에서 거래 중이라(2026-05-04
        # 장중 23만1000원으로 액면분할 후 최고가 경신), 실제가와 4~5배 이상
        # 차이난다. 즉 yfinance가 이 종목에 대해 (액면분할 조정 오류 등으로 추정되는)
        # 잘못된 가격 계열을 내려주고 있었다는 뜻이다. 이 가격을 그대로 쓰면 52주
        # 고점대비/추세/모멘텀 점수가 전부 허구의 폭락으로 계산된다.
        #
        # 이미 이 앱에는 네이버 금융에서 실시간가를 가져오는 fetch_current_price_info
        # (신뢰도 높은 소스, 60초 캐시)가 있으므로, yfinance 마지막 종가를 그 값과
        # 대조해서 2배 넘게 어긋나면 "이 종목은 yfinance 가격 데이터를 신뢰할 수
        # 없다"고 보고 통째로 버린다(빈 DataFrame → 각 점수 함수가 중립값으로 대체).
        # 어설프게 배율을 추정해서 보정하지 않는 이유: 어긋난 배율이 항상 깔끔한
        # 50배 같은 값이 아닐 수 있어(실측 사례는 약 5배), 잘못 보정하면 틀린
        # 데이터를 "그럴듯하게 틀린" 데이터로 바꿔 오히려 알아채기 더 어려워진다.
        if not df.empty:
            try:
                ref_price = fetch_current_price_info(code).get("price")
                last_close = df["Close"].iloc[-1]
                if ref_price and last_close and ref_price > 0:
                    ratio = last_close / ref_price
                    if ratio > 2.0 or ratio < 0.5:
                        return pd.DataFrame()
            except Exception:
                pass

        return df
    except Exception:
        return pd.DataFrame()


def _lerp_score(x, points):
    """구간별 '계단식' 점수 대신, 지정한 (x좌표, 점수) 앵커 사이를 선형보간해
    연속값을 반환한다. 이렇게 하면 같은 구간 안에 있다는 이유만으로 서로 다른
    입력값(예: -12% 하락과 -34% 하락)이 완전히 동일한 점수로 뭉개지는 문제가
    없어지고, 입력값이 조금만 달라져도 점수가 그에 비례해 미세하게 움직인다.

    points: [(x0,score0), (x1,score1), ...] — x좌표 오름차순으로 정렬해서 전달.
    x가 points의 범위를 벗어나면 양 끝 점수로 고정(clamp)한다."""
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
    """📈 추세 점수 (0~200) = MA 정배열 강도(0~130) + 추세 지속성(0~70)

    ⚠️ [1000점 리뉴얼] 기존엔 'ma5>ma20>ma60'이면 무조건 10점, 아니면 6점 또는 2점 이런 식으로
    딱 3단계로만 나뉘어 있었다. 그래서 정배열이 아주 살짝 무너진 경우와 완전히 역배열인 경우가
    같은 점수로 뭉개졌다. 지금은 이동평균선 사이의 실제 이격도(%)를 연속값으로 계산해서
    _lerp_score로 부드럽게 보간한다 — 정배열이 강할수록, 눌림 없이 버틴 날이 많을수록
    점수가 촘촘하게 올라간다.

    ⚠️ [방향 충돌 수정] 원래 여기 있던 '52주 신고가 근접도(0~60)'는 "52주 고점에
    가까울수록 가점"이었는데, 리스크 점수 쪽의 "52주 고점 대비 하락폭이 클수록
    감점"과 사실상 같은 정보(52주 고점 대비 현재 위치)를 담당하고 있었다. 문제는
    이 앱의 추천 엔진 목표 자체가 "52주 고점 대비 유의미하게 하락해 안전마진이
    확보된 종목을 찾는 것"인데, 정작 점수 시스템은 "많이 빠진 종목"에게 추세에서
    가점을 못 주고 리스크에서 감점까지 이중으로 주고 있어 엔진 철학과 배점이
    반대로 가는 구조였다. 52주 고점 대비 위치는 이제 리스크 점수 한 곳에서만
    다루도록 여기서는 제거하고, 비어난 60점만큼을 MA 정배열(100→130)·지속성
    (40→70)에 비례 재배분했다 — '현재 추세가 얼마나 강하고 꾸준한가'라는 이 함수
    본연의 역할에만 집중한다."""
    try:
        if df_price is None or df_price.empty:
            return 100.0
        closes = df_price["Close"].dropna()
        if len(closes) < 20:
            return 100.0

        ma5 = closes.tail(5).mean()
        ma20 = closes.tail(20).mean()
        ma60 = closes.tail(60).mean() if len(closes) >= 60 else ma20

        # MA 정배열 강도(%): ma5 vs ma20, ma20 vs ma60의 이격도를 합산 — 값이 클수록 강한 정배열
        gap_short = (ma5 - ma20) / ma20 * 100 if ma20 else 0
        gap_long = (ma20 - ma60) / ma60 * 100 if ma60 else 0
        align_pct = gap_short + gap_long
        s_align = _lerp_score(align_pct, [(-8, 13), (0, 52), (3, 91), (8, 130)])

        recent20 = closes.tail(20)
        ma20_series = closes.rolling(20).mean().reindex(recent20.index)
        above_ratio = (recent20 > ma20_series).sum() / len(recent20)
        s_persist = _lerp_score(above_ratio, [(0, 0), (0.3, 17.5), (0.5, 35), (0.8, 70), (1.0, 70)])

        return round(max(0, min(200, s_align + s_persist)), 1)
    except Exception:
        return 100.0


def calc_volume_score(df_price, exclude_today=False):
    """📊 거래량 점수 (0~100) = 당일 거래량 스파이크(0~60) + 최근 5일 평균 지속성(0~40)
       각각에 '같은 기간 주가가 오른 방향이었는지'를 반영한 방향 배수(0.1~1.0)를 곱한다.

    ⚠️ [버그 수정 1] 기존에는 ratio < 0.7(오늘 거래량이 20일 평균의 70% 미만)이면
    무조건 0점으로 처리했다. 이 구간이 너무 넓어서(0%~69%가 전부 0점), 실제로는
    "약간 조용한 날"과 "거래가 사실상 없는 날"이 구분되지 않고 똑같이 0점으로
    표시되는 문제가 있었다. 특히 삼성전자·SK하이닉스처럼 평소 거래량 절대량이
    크고 안정적인 초대형주는 평균 대비 50~65% 수준으로만 줄어도 이 넓은 0점
    구간에 자주 걸려, 다른 종목과 비교했을 때 데이터가 안 들어오는 것처럼
    보이는 원인이 됐다.

    ⚠️ [1000점 리뉴얼] 기존 9단계 계단(ratio 0.4/0.6/0.8/1.0/1.2/1.5/2/3배 경계마다
    점수가 뚝뚝 끊김)을 그대로 두면 배율이 조금만 달라도 같은 계단에 갇혀 점수가
    안 움직였다. _lerp_score로 같은 경계값들 사이를 선형보간해서, ratio가 1.3배든
    1.45배든(둘 다 예전엔 '5점' 계단에 갇힘) 배율에 비례해 촘촘하게 점수가 갈리도록 했다.

    ⚠️ [입체화] 예전엔 '당일 거래량 ÷ 20일 평균' 하나만 봐서, 하루 반짝 터진 거래량과
    며칠째 이어지는 거래 증가세가 똑같이 취급됐다. 지금은 당일 스파이크(0~60)와
    최근 5일 평균의 지속성(0~40)을 나눠서 합산한다 — 하루짜리 반짝 거래량은 스파이크
    점수만 받고, 여러 날 이어지는 거래 증가는 지속성 점수까지 추가로 받는다.

    ⚠️ [버그 수정 2] 장중(당일 미마감)에 조회하면 df_price의 마지막 봉은 "오늘"의
    아직 다 쌓이지 않은 누적 거래량이다. 이걸 하루 전체가 확정된 20일 평균과
    비교하면, 장이 끝나기 전까지는 구조적으로 항상 낮은(심하면 0점) 점수가 나올
    수밖에 없다 — 실제로 거래가 부진해서가 아니라 비교 대상 자체가 불공정한
    것이다(실측: 삼성전자 장중 조회 시 오늘거래량/20일평균 = 11%). exclude_today=True
    로 호출되면 마지막(당일) 봉을 빼고 확정된 전날까지의 데이터로 계산한다.

    ⚠️ [방향성 결합 — NEW] 기존에는 거래량이 '얼마나' 튀었는지만 보고, 그 거래량이
    주가 상승과 함께 터진 건지 급락과 함께 터진 건지는 전혀 구분하지 않았다. 그
    결과 하한가를 맞으며 거래량이 폭발한 종목도 상한가를 가며 거래량이 폭발한
    종목과 동일하게 만점 근처를 받아, 실제로는 나쁜 신호(패닉셀)인데 AI 종합점수를
    깎기는커녕 오히려 끌어올리는 왜곡이 있었다. calc_breakout_score(저항선 돌파
    점수)는 이미 '가격 상승 + 거래량'을 같이 봐야 의미 있다는 전제로 설계돼 있었는데,
    거래량 항목 자체에는 그 전제가 빠져 있었던 것이다. 지금은 스파이크(당일 등락률
    기준)와 지속성(최근 5일 누적 등락률 기준) 각각에, 같은 기간 주가 방향을 반영한
    배수(0.1~1.0, _lerp_score로 연속 보간)를 곱한다 — 상승 동반 거래량은 원점수를
    거의 그대로 인정받고, 하락 동반 거래량은 최대 90%까지 깎인다. 완전히 0으로
    죽이지 않는 이유는, 방향이 애매하거나(보합) 하락 중이어도 '거래 자체가
    활발했다'는 유동성 정보에는 최소한의 참고 가치가 있기 때문이다. Close 컬럼이
    없거나 방향 계산이 실패하면 배수를 1.0(기존과 동일)으로 두고 안전하게
    원래 로직으로 폴백한다."""
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

        # ── 방향 배수: 상승 동반 거래량만 원점수를 거의 그대로 인정, 하락 동반은 감쇠 ──
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
    """🚀 모멘텀 점수 (0~100) = 단기 5일(0~20) + 중기 20일(0~40) + 장기 60일(0~20)
       + 코스피 대비 상대강도 RS 보너스(0~20)

    ⚠️ [1000점 리뉴얼] 기존엔 ret20 < -10%면 무조건 base=0으로 죽어서, -10.1%짜리
    하락과 -34%짜리 폭락이 똑같은 0점으로 표시됐다(실측: 삼성전자 -17.7%, SK하이닉스
    -34.1%가 둘 다 모멘텀 0/10). 지금은 -30% 지점까지는 하락폭에 비례해 점수가 계속
    줄어들고, 그 밑으로 더 빠지면 0에서 바닥을 잡는다.

    ⚠️ [입체화] 예전엔 20일 수익률 하나로만 모멘텀을 판단해서, '최근 5일은 반등 중인데
    20일 전체로는 마이너스'인 종목과 '5일도 20일도 계속 빠지는' 종목이 구분되지 않았다.
    지금은 5일(단기)·20일(중기)·60일(장기) 수익률을 각각 채점해서 더한다 — 단기 반등
    조짐이나 장기 추세 이탈을 20일 수익률 하나보다 입체적으로 잡아낸다. 데이터가 모자라
    특정 기간을 못 구하면(예: 상장 60일 미만) 그 구간만 중립값으로 채운다."""
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

        rs_bonus = 10.0  # 코스피 비교 데이터가 없을 때의 중립값
        if ret20 is not None and kospi_closes and len(kospi_closes) >= 21:
            kospi_ret20 = (kospi_closes[-1] / kospi_closes[-21] - 1) * 100
            diff = ret20 - kospi_ret20
            rs_bonus = _lerp_score(diff, [(-15, 0), (0, 10), (10, 20)])

        return round(max(0, min(100, s5 + s20 + s60 + rs_bonus)), 1)
    except Exception:
        return 50.0


def _investor_side_score(buy_days, n, net_sum, avg_volume=None):
    """기관 또는 외국인 한쪽의 수급 점수(0~100)
       = 순매수일 비율 기반 지속성(0~80) + 순매수 강도(0~20, NEW)

    ⚠️ [1000점 리뉴얼] 기존엔 순매수일 비율이 5단계 계단(0.3/0.5/0.7 경계)으로만
    나뉘어 있었다. 지금은 net_sum 부호로 어느 커브를 쓸지 고른 뒤, 그 안에서
    비율(ratio)에 비례해 연속적으로 점수를 매긴다 — 순매수 68%와 72%가 예전엔
    같은 계단(5점)에 갇혔지만 이제는 미세하게 갈린다.

    ⚠️ [버그 수정] '순매수한 날의 비율'과 '순매수 금액(규모)의 크기'가 구분이 안
    됐다. 20일 중 15일 순매수라는 결과만 같으면, 하루 평균 거래량의 0.5%만
    순매수한 종목과 하루 평균 거래량의 8%씩 순매수한 종목이 거의 같은 점수를
    받았다 — 실제 수급 강도는 후자가 훨씬 강한데도 구분이 안 됨. 이 함수는 네이버
    페이지에서 '수량(주)'만 주고 금액(원)은 안 주기 때문에, 종목마다 다른 거래량
    규모를 감안해 절대 수량 대신 '평균 거래량 대비 하루 평균 순매수 비중(%)'을
    강도 지표로 쓴다 — 대형주/소형주 상관없이 같은 기준으로 비교 가능하다.
    avg_volume(최근 N일 평균 거래량)을 못 구하면 강도 항목은 중립값(10)으로 채운다."""
    if n <= 0:
        return 50.0
    ratio = buy_days / n

    # 지속성(0~80): 기존 커브를 그대로 쓰되, 강도(0~20)를 더할 자리를 만들기 위해
    # 만점을 100→80으로 비례 축소했다 (순서/구간 경계는 기존과 동일하게 유지).
    if net_sum > 0:
        s_persist = _lerp_score(ratio, [(0, 24), (0.3, 32), (0.5, 56), (0.7, 80), (1.0, 80)])
    else:
        s_persist = _lerp_score(ratio, [(0, 0), (0.3, 0), (0.5, 24), (0.7, 40), (1.0, 48)])

    if avg_volume and avg_volume > 0 and n > 0:
        avg_daily_net = net_sum / n
        intensity_pct = avg_daily_net / avg_volume * 100
        s_intensity = _lerp_score(intensity_pct, [(-10, 0), (-3, 6), (0, 10), (3, 16), (10, 20)])
    else:
        s_intensity = 10.0  # 거래량 기준을 못 구했을 때의 중립값

    return round(max(0, min(100, s_persist + s_intensity)), 1)


def calc_flow_score(code, days=20, df_price=None):
    """💰 수급 점수 (0~200) = 기관 순매수 점수(0~100) + 외국인 순매수 점수(0~100)

    ⚠️ df_price(최근 가격/거래량 데이터)를 전달하면 같은 기간의 평균 거래량을 구해
    순매수 강도 계산에 쓴다. 이미 calc_ai_scores_detailed에서 다른 점수 계산에
    쓰려고 조회해둔 데이터를 재사용하는 것이라 추가 호출 비용은 없다."""
    try:
        df = fetch_investor_trend_by_code(code, days=days)
        if df.empty or '기관순매매' not in df.columns or '외국인순매매' not in df.columns:
            return 100.0
        inst = df['기관순매매']
        frgn = df['외국인순매매']
        n = len(df)

        avg_volume = None
        if df_price is not None and not df_price.empty and "Volume" in df_price.columns:
            vol_tail = df_price["Volume"].dropna().tail(days)
            if len(vol_tail) >= 5:
                avg_volume = vol_tail.mean()

        inst_score = _investor_side_score(int((inst > 0).sum()), n, inst.sum(), avg_volume)
        frgn_score = _investor_side_score(int((frgn > 0).sum()), n, frgn.sum(), avg_volume)
        return round(max(0, min(200, inst_score + frgn_score)), 1)
    except Exception:
        return 100.0


def calc_financial_score_detailed(df_annual, roe, debt=None, return_breakdown=False):
    """💹 재무 점수 (0~180) = 수익성(0~60) + 성장성(0~70) + 건전성(0~50)

       수익성(60)  = ROE(0~35) + 영업이익률(0~15) + 순이익률(0~10)
       성장성(70)  = 매출액 YoY(0~20) + 영업이익 YoY(0~30) + EPS YoY(0~20)
       건전성(50)  = 부채비율(0~50)

    ⚠️ [세분화] 예전엔 'ROE 하나(80점)'가 재무 점수의 절반 가까이를 차지해서, ROE는
    높은데 영업이익률·매출 성장은 꺾이고 있는 종목과 실제로 전방위로 탄탄한 종목이
    구분되지 않았다. 지금은 '돈을 잘 버는가(수익성)·성장하고 있는가(성장성)·재무가
    튼튼한가(건전성)'를 각각 별도 하위 점수로 나눠서 어느 축에서 약한지 바로 보이게
    했다.

    ⚠️ [데이터 한계로 인한 의도적 축소] 원래 구상은 '현금흐름(영업현금흐름/FCF)'까지
    네 번째 축으로 넣는 것이었지만, 이 코드베이스가 실제로 가져오는 재무 데이터
    (FnGuide/네이버 WiseReport)에는 현금흐름표·유동비율·이자보상배율이 아예 없다.
    없는 데이터를 있는 것처럼 흉내내는 대신, 현재 확보 가능한 지표(매출액·영업이익·
    당기순이익·영업이익률·순이익률·ROE·부채비율·EPS)만으로 정직하게 3축을 구성했다.
    현금흐름을 실제로 넣으려면 DART 재무제표(현금흐름표) API 연동이 먼저 필요하다.

    ⚠️ [1000점 리뉴얼] ROE 12%와 14%가 예전엔 같은 계단(4점)에 갇혔다. 지금도
    모든 하위 지표는 실제 %값을 그대로 _lerp_score에 넣어 연속적으로 채점한다.

    ⚠️ [카테고리 정합성 유지] 부채비율은 '리스크' 감점과 '재무' 가점에 동시에 쓰인다.
    '재무 건전성 가점'과 '변동성·부실 리스크 감점'은 서로 다른 관점이라 중복이
    아니라 상호 보완으로 본다(자세한 이유는 calc_risk_score 참고).

    return_breakdown=True로 호출하면 총점 대신 하위 카테고리 점수까지 담은 dict를
    반환한다 (UI에서 '재무 몇 점 중 성장성이 특히 약하다' 같은 설명에 사용)."""
    try:
        # ── 수익성 (0~60) ──
        if roe is None or roe == -999:
            s_roe = 17.5  # 데이터 없을 때의 중립값 (35점 만점의 절반)
        else:
            s_roe = _lerp_score(roe, [(-10, 0), (0, 4.4), (5, 8.75), (10, 17.5), (15, 26.25), (20, 35)])

        s_op_margin, s_ni_margin = 7.5, 5.0  # 데이터 없을 때의 중립값

        op_margin = ni_margin = None
        if df_annual is not None and not df_annual.empty:
            if '영업이익률' in df_annual.columns:
                op_margin = _to_float_safe(df_annual.iloc[-1]['영업이익률'])
            if '순이익률' in df_annual.columns:
                ni_margin = _to_float_safe(df_annual.iloc[-1]['순이익률'])

        if op_margin is not None:
            s_op_margin = _lerp_score(op_margin, [(-10, 0), (0, 3), (5, 8), (10, 12), (15, 15), (25, 15)])
        if ni_margin is not None:
            s_ni_margin = _lerp_score(ni_margin, [(-10, 0), (0, 2), (5, 5), (10, 8), (15, 10), (25, 10)])

        s_profitability = s_roe + s_op_margin + s_ni_margin

        # ── 성장성 (0~70) ──
        s_revenue, s_op, s_eps = 10.0, 15.0, 10.0  # 데이터 없을 때의 중립값

        if df_annual is not None and not df_annual.empty and len(df_annual) >= 2:
            if '매출액' in df_annual.columns:
                prev_rev = _to_float_safe(df_annual.iloc[-2]['매출액'])
                latest_rev = _to_float_safe(df_annual.iloc[-1]['매출액'])
                if prev_rev not in (None, 0) and latest_rev is not None:
                    rev_growth = (latest_rev - prev_rev) / abs(prev_rev) * 100
                    s_revenue = _lerp_score(rev_growth, [(-20, 0), (0, 5), (10, 12), (20, 20)])

            if '영업이익' in df_annual.columns:
                prev = _to_float_safe(df_annual.iloc[-2]['영업이익'])
                latest = _to_float_safe(df_annual.iloc[-1]['영업이익'])
                if prev not in (None, 0) and latest is not None:
                    growth = (latest - prev) / abs(prev) * 100
                    s_op = _lerp_score(growth, [(-30, 0), (0, 6), (10, 18), (20, 30)])

            eps_col = next((c for c in df_annual.columns if 'EPS' in str(c)), None)
            if eps_col:
                prev_eps = _to_float_safe(df_annual.iloc[-2][eps_col])
                latest_eps = _to_float_safe(df_annual.iloc[-1][eps_col])
                if latest_eps is not None and latest_eps > 0:
                    if prev_eps not in (None, 0):
                        eps_growth = (latest_eps - prev_eps) / abs(prev_eps) * 100
                        s_eps = _lerp_score(eps_growth, [(-20, 0), (0, 10), (15, 20)])
                    else:
                        s_eps = 15.0  # 직전 EPS가 없어 증가율은 못 구하지만 현재 EPS는 흑자
                elif latest_eps is not None:
                    s_eps = 0.0

        s_growth = s_revenue + s_op + s_eps

        # ── 건전성 (0~50) ──
        if debt is None or debt < 0:
            s_debt = 25.0  # 데이터 없을 때의 중립값
        else:
            s_debt = _lerp_score(debt, [(0, 50), (30, 50), (60, 33.3), (100, 16.7), (150, 8.3), (300, 0)])

        s_stability = s_debt

        total = round(max(0, min(180, s_profitability + s_growth + s_stability)), 1)

        if return_breakdown:
            return {
                "total": total,
                "수익성": round(s_profitability, 1), "수익성_max": 60,
                "성장성": round(s_growth, 1), "성장성_max": 70,
                "건전성": round(s_stability, 1), "건전성_max": 50,
            }
        return total
    except Exception:
        if return_breakdown:
            return {"total": 85.0, "수익성": 28.0, "수익성_max": 60,
                    "성장성": 32.0, "성장성_max": 70, "건전성": 25.0, "건전성_max": 50}
        return 85.0


def calc_valuation_score_detailed(per, pbr, roe, div=None):
    """📉 밸류 점수 (0~120) = PER(0~50) + PBR(0~30) + PEG 근사(0~20, PER÷ROE)
       + 배당수익률(0~20, NEW)

    ⚠️ [1000점 리뉴얼] PER 8.5배와 11.5배가 예전엔 같은 계단(4점)에 갇혔다.
    지금은 PER·PBR·PEG 실제 값을 그대로 _lerp_score에 넣어 연속적으로 채점한다.

    ⚠️ [버그 수정] div(배당수익률)는 calc_ai_scores_detailed에 파라미터로 넘어오면서도
    정작 8개 항목 어디에도 쓰이지 않았다(화면 디버그 정보에는 표시되지만 점수엔 반영 안
    됐던 값). 배당수익률은 '싸게 사는지'를 보는 밸류 관점에도 들어맞는 지표라 여기에
    가산점으로 추가했다."""
    try:
        if per is None or per <= 0:
            s_per = 20.0
        else:
            s_per = _lerp_score(per, [(8, 50), (12, 40), (18, 30), (25, 10), (40, 0)])

        if pbr is None or pbr <= 0:
            s_pbr = 10.0
        else:
            s_pbr = _lerp_score(pbr, [(1, 30), (1.5, 20), (2.5, 10), (4, 0)])

        if per and per > 0 and roe and roe != -999 and roe > 0:
            peg = per / roe
            s_peg = _lerp_score(peg, [(0.5, 20), (1, 20), (1.5, 10), (2.5, 0)])
        else:
            s_peg = 10.0

        if div is None or div <= 0:
            s_div = 0.0
        else:
            s_div = _lerp_score(div, [(0, 0), (1, 5), (2, 10), (3.5, 15), (5, 20)])

        return round(max(0, min(120, s_per + s_pbr + s_peg + s_div)), 1)
    except Exception:
        return 60.0


def calc_pattern_score(df_price, volume_score):
    """🔥 AI 패턴 점수 (0~100, ⚠️ 규칙 기반 근사 휴리스틱 — 실제 ML 유사도 검색 아님)
    = 직전 20일 저항선(고점) 돌파(0~50) + 거래량 동반(0~30) + 최근 상승 우위(0~20)

    ⚠️ [1000점 리뉴얼] '돌파했다/안 했다', '거래량 동반이다/아니다'처럼 이분법으로
    끊던 걸, 저항선까지 남은 거리(%)와 거래량 점수(이미 연속값) 자체를 그대로
    보간에 사용해 연속적으로 채점하도록 바꿨다."""
    try:
        if df_price is None or df_price.empty or len(df_price) < 21:
            return 50.0
        closes = df_price["Close"].dropna()
        cur = closes.iloc[-1]
        prior20_high = closes.iloc[-21:-1].max()

        if prior20_high > 0:
            gap_to_high = (cur - prior20_high) / prior20_high * 100
            score_breakout = _lerp_score(gap_to_high, [(-10, 0), (0, 35), (5, 50)])
        else:
            score_breakout = 0.0

        score_volume = _lerp_score(volume_score, [(0, 0), (40, 10), (60, 30), (100, 30)])

        recent5 = closes.tail(6).diff().dropna()
        up_ratio = (recent5 > 0).sum() / len(recent5) if len(recent5) else 0.5
        score_updays = _lerp_score(up_ratio, [(0, 0), (0.5, 10), (0.6, 20)])

        return round(max(0, min(100, score_breakout + score_volume + score_updays)), 1)
    except Exception:
        return 50.0


def calc_risk_score(df_price, debt, drop_pct=None):
    """⚠️ 리스크 점수 (-80~0, 감점 전용)
       = 변동성(0~30 감점) + 부채비율(0~20 감점) + 52주 고점대비 하락폭(0~20 감점, NEW)
       + 최근 연속 하락일수(0~10 감점, NEW)

    ⚠️ [지표 교체] '20일 종가의 표준편차÷평균(CV)'은 사실 변동성보다 '추세가 얼마나
    강했는지'에 더 가까운 지표였다 — 하루하루는 잔잔해도 20일간 한 방향으로 꾸준히
    움직이기만 하면 가격이 평균에서 계속 멀어져서 CV가 높게 나온다(실측: 삼성전자가
    20일간 완만하게 -18% 정도만 빠졌는데도 CV 7.97%로 만점 문턱 6%를 이미 넘었다).
    반대로 하루하루 크게 출렁여도 20일 뒤 제자리로 돌아온 종목은 CV가 낮게 나와
    "진짜 변동성"과 "그냥 한 방향으로 흐른 추세"가 구분이 안 됐다. 지금은 종가 자체가
    아니라 '전일 대비 일별 등락률의 표준편차'를 쓴다 — 방향(추세)과 무관하게 하루하루
    얼마나 출렁였는지만 잡아내므로, 완만하게 꾸준히 빠진 종목은 감점이 줄고 급등락을
    반복하는 진짜 변동성 큰 종목만 감점이 크게 걸린다.

    ⚠️ [버그 수정] drop_pct(52주 고점 대비 하락률, 스크리너의 '고점대비(%)' 컬럼)는
    calc_ai_scores_detailed에 파라미터로 넘어오면서도 정작 8개 항목 어디에도 쓰이지
    않았다(화면 디버그 정보엔 표시되지만 점수엔 반영 안 됐던 값). '이미 많이 빠진
    종목일수록 추가 변동성·투자심리 리스크가 크다'는 관점에서 여기 반영했다.

    ⚠️ [방향 충돌 수정] 예전엔 calc_trend_score에도 '52주 고점 근접도(0~60, 고점에
    가까울수록 가점)'가 따로 있어서, 52주 고점 대비 위치라는 같은 정보를 추세(가점)와
    리스크(감점) 양쪽에서 반대 방향으로 이중 반영하고 있었다. 문제는 이 앱의 추천
    엔진 목표 자체가 '52주 고점 대비 하락해 안전마진이 확보된 종목 찾기'인데, 정작
    많이 빠진 종목이 추세에서 가점을 못 받고 여기서 감점까지 두 번 맞는 구조라 엔진
    철학과 배점이 어긋났었다. 이제 52주 고점 대비 위치는 여기(리스크) 한 곳에서만
    다루도록 trend에서 해당 항목을 제거했다 — 하락폭 반영은 여기 하나로 정리됐다.

    ⚠️ [의존성 제거] drop_pct는 원래 관리자가 KRX 52주 고점 CSV를 수동으로 업로드해야만
    채워지는 스크리너 값이라, 업로드를 안 했거나 스크리너에 없는 종목이면 늘 '데이터
    없음' 중립값(5점)으로 빠졌다. 그런데 이 함수는 df_price(최근 1년치 일봉)를 이미
    받고 있고, 여기서 직접 52주 고점을 계산할 수 있다 — 실제로 추세 점수·디버그
    패널은 이미 이 방법을 쓰고 있었다. 그래서 스크리너 값이 없을 땐 df_price의 최근
    1년 종가 최고치 기준으로 52주 고점대비를 자체 계산해서 대신 쓰도록 폴백을
    추가했다. 이제 리스크 점수의 하락폭 반영은 스크리너 업로드 여부와 무관하게
    항상 동작한다.

    ⚠️ [버그 수정] 부채비율 감점이 예전엔 0%부터 바로 시작해서([(0,0),(100,10),(200,20)]),
    부채비율 20~30%처럼 통상 '안정권'으로 보는 건전한 수준도 조금씩 감점을 먹고 있었다.
    재무 점수 쪽 부채 가점 곡선은 이미 '30% 이하는 우량'으로 보는데 리스크 쪽만 다른
    기준을 쓰던 불일치이기도 했다. 감점 없는 구간을 60%까지로 두고, 그 이상부터
    100%→8점, 150%→15점, 200% 이상→20점(만점)으로 상대적으로 타이트하게 올라간다.

    ⚠️ [입체화] 변동성·부채, 두 가지뿐이던 리스크 요인에 '최근 며칠째 연속으로
    빠지고 있는지'를 추가했다 — 단기 급락이 이어지는 종목을 더 민감하게 잡아낸다."""
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
            # 스크리너 값이 없으면 df_price(최근 1년 종가)로 직접 52주 고점대비를 계산해
            # 대신 쓴다. 디버그 패널의 "52주 고점 대비" 줄과 같은 기준(종가 최고가)을
            # 써서, 화면에 보이는 검증용 수치와 실제 계산에 쓰이는 값이 항상 일치하도록
            # 맞췄다.
            if closes is not None and len(closes) > 0:
                high52 = closes.max()
                if high52 > 0:
                    effective_drop_pct = (closes.iloc[-1] - high52) / high52 * 100

        if effective_drop_pct is None or effective_drop_pct == 0.0:
            penalty += 5.0  # df_price조차 없어서 진짜 아무 데이터도 못 구했을 때의 중립값
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


def _last_bar_is_today_kst(df_price):
    """df_price의 마지막 봉이 '오늘(한국시간 기준)' 날짜인지 확인한다.
    장이 아직 마감되지 않았다면 오늘 봉의 누적 거래량은 하루 전체가 확정된
    20일 평균과 비교하기에 본질적으로 불공정하게 낮을 수밖에 없다(아직 절반만
    쌓였을 뿐 거래가 부진한 게 아님). 이 함수는 그 가능성이 있는지만 판별하고,
    실제 처리(전날 데이터로 대체)는 calc_volume_score(exclude_today=True)에서 한다."""
    try:
        if df_price is None or df_price.empty:
            return False
        last_idx = df_price.index[-1]
        last_date = last_idx.date() if hasattr(last_idx, "date") else pd.Timestamp(last_idx).date()
        today_kst = pd.Timestamp.now(tz="Asia/Seoul").date()
        return last_date == today_kst
    except Exception:
        return False


def _ai_score_debug_info(df_price, kospi_closes=None, debt=None, drop_pct=None):
    """점수 산출에 실제로 쓰인 원본 수치를 사람이 읽을 수 있는 형태로 반환한다.
    '왜 0점/저점이 나왔는지' 코드를 다시 열어보지 않고도 화면에서 바로 확인하기 위한
    진단용 부가 정보. 계산 실패해도 앱 전체가 죽지 않도록 개별 항목마다 방어한다."""
    info = {}
    try:
        if df_price is None or df_price.empty:
            info["데이터"] = "가격 데이터를 가져오지 못했습니다 (조회 실패 또는, 야후 가격이 실제가와 크게 어긋나 안전상 자동 제외됨)."
            return info

        info["데이터 마지막 날짜"] = str(df_price.index[-1].date()) if hasattr(df_price.index[-1], "date") else str(df_price.index[-1])

        closes = df_price["Close"].dropna()
        if len(closes) >= 1:
            info["최근 종가"] = f"{closes.iloc[-1]:,.0f}"

        if len(closes) >= 20:
            ma5 = closes.tail(5).mean()
            ma20 = closes.tail(20).mean()
            ma60 = closes.tail(60).mean() if len(closes) >= 60 else ma20
            info["MA5 / MA20 / MA60"] = f"{ma5:,.0f} / {ma20:,.0f} / {ma60:,.0f}"
            high52 = closes.max()
            if high52 > 0:
                info["52주 고점 대비"] = f"-{(high52 - closes.iloc[-1]) / high52 * 100:.1f}%"

        if len(closes) >= 21:
            ret20 = (closes.iloc[-1] / closes.iloc[-21] - 1) * 100
            info["최근 20일 수익률"] = f"{ret20:+.1f}%"
            if kospi_closes and len(kospi_closes) >= 21:
                kospi_ret20 = (kospi_closes[-1] / kospi_closes[-21] - 1) * 100
                info["(참고) 같은 기간 코스피"] = f"{kospi_ret20:+.1f}%"

        today_incomplete = _last_bar_is_today_kst(df_price)
        if "Volume" in df_price.columns:
            vol = df_price["Volume"].dropna()
            vol_calc = vol.iloc[:-1] if (today_incomplete and len(vol) >= 6) else vol
            if len(vol_calc) >= 5:
                recent = vol_calc.iloc[-1]
                avg20 = vol_calc.tail(20).mean() if len(vol_calc) >= 20 else vol_calc.mean()
                if avg20 > 0:
                    label = "전일(확정) 거래량 / 20일 평균" if today_incomplete else "오늘 거래량 / 20일 평균"
                    info[label] = f"{recent:,.0f} / {avg20:,.0f}  (비율 {recent / avg20 * 100:.0f}%)"
        if today_incomplete:
            info["⏳ 참고"] = "오늘 장이 아직 진행 중일 수 있어, 거래량 점수는 전일까지의 확정 데이터로 계산했습니다."

        # ⚠️ [버그 수정] 리스크 점수(변동성/부채/하락폭/연속하락)에 실제로 쓰이는 원본
        # 수치가 이 패널에 하나도 없어서, "왜 리스크가 -39점인지" 화면에서 확인할 방법이
        # 없었다. 다른 항목들과 동일하게 계산 근거를 노출한다.
        if len(closes) >= 6:
            daily_ret = closes.tail(21).pct_change().dropna() * 100
            if len(daily_ret) >= 5:
                info["변동성(최근 20일 일별 등락률 표준편차)"] = f"{daily_ret.std():.2f}%"

        if len(closes) >= 6:
            diffs = closes.tail(6).diff().dropna()
            down_streak = 0
            for d in diffs.iloc[::-1]:
                if d < 0:
                    down_streak += 1
                else:
                    break
            info["최근 연속 하락일수"] = f"{down_streak}일"

        if debt is not None and debt >= 0:
            info["부채비율"] = f"{debt:.1f}%"
        if drop_pct is not None and drop_pct != 0.0:
            info["52주 고점대비(스크리너 기준)"] = f"{drop_pct:+.1f}%"

        # ⚠️ [투명성 강화] calc_risk_score 내부의 폴백 로직과 완전히 동일한 계산을 여기서도
        # 그대로 수행해서, "리스크 점수가 실제로 어떤 하락폭 값·어떤 출처를 썼는지"를
        # 화면에서 바로 확인할 수 있게 했다. 예전엔 최종 리스크 점수를 손으로 역산해야만
        # 폴백이 진짜 작동했는지 알 수 있었다.
        _effective_drop = drop_pct
        _drop_source = "스크리너"
        if _effective_drop is None or _effective_drop == 0.0:
            _drop_source = "df_price 폴백"
            if len(closes) > 0:
                _high52 = closes.max()
                if _high52 > 0:
                    _effective_drop = (closes.iloc[-1] - _high52) / _high52 * 100
        if _effective_drop is None or _effective_drop == 0.0:
            info["리스크에 실제로 쓰인 하락폭"] = "값 없음 → 중립값(5점 감점) 적용"
        else:
            info["리스크에 실제로 쓰인 하락폭"] = f"{_effective_drop:+.1f}%  (출처: {_drop_source})"
    except Exception:
        info["오류"] = "진단 정보 계산 중 문제가 발생했습니다."
    return info


def calc_ai_scores_detailed(code, per, pbr, roe, debt, drop_pct, div, df_annual=None, screener_df=None):
    """세분화된 AI 종합점수(0~1000). 8개 항목 배점을 그대로 합산한다.

        추세(0~200) · 수급(0~200) · 거래량(0~100) · 재무(0~180) ·
        밸류(0~120) · 모멘텀(0~100) · AI패턴(0~100) · 리스크(-80~0)

    ⚠️ [1000점 리뉴얼] 기존 100점 만점(각 항목 계단식 정수 점수)을 그대로 ×10 해서
    보여주기만 하면 눈금 자체는 예전과 똑같아서(여전히 10점 단위로만 움직임) 숫자만
    커 보이고 실제 정밀도는 늘지 않는다. 그래서 각 항목 계산 함수(calc_trend_score
    등) 내부를 전부 _lerp_score 기반 연속 보간으로 다시 짜서, 만점 자체도 ×10 하고
    동시에 입력값이 조금만 달라져도 점수가 소수점 단위로 촘촘하게 갈리도록 했다.

    ⚠️ [배점 재조정] 리스크를 제외한 7개 항목의 만점 합이 정확히 1000이 되도록
    재무(150→180)·밸류(100→120)를 조정했다. 예전엔 8개 만점을 그냥 더하면 95/100이라
    100점 만점을 이론상으로도 못 채웠는데, 지금은 리스크 감점이 0이면(=무위험) 정확히
    1000점이 최고점이 되도록 맞췄다.

    ⚠️ [버그 수정] div(배당수익률)·drop_pct(52주 고점대비 하락률)가 파라미터로 넘어오면서도
    정작 8개 항목 어디에도 쓰이지 않던 문제를 고쳤다 — div는 밸류 점수에, drop_pct는
    리스크 점수에 반영된다. debt(부채비율)도 기존엔 리스크 감점에서만 쓰였는데, 이제
    재무 점수의 가점 요소로도 반영된다(자세한 이유는 각 함수 docstring 참고).

    df_annual을 미리 전달하면(재무 데이터를 이미 조회한 경우) 재조회를 생략한다.

    ⚠️ [내부 호출 재병렬화 — 2026-08-18] 2026-08-12에는 이 4개 네트워크 호출
    (가격이력·스파크라인·재무·수급)을 get_orchestration_executor()로 동시에
    던졌다가, 그 풀이 다시 get_shared_executor()를 기다리고 AI 점수 배치
    (_score_one_for_ai_batch)는 get_shared_executor() 안에서 이 함수를 기다리는
    순환 대기(circular wait) 교착상태가 나서 순수 순차 호출로 되돌렸었다.
    하지만 순차 호출로 되돌린 대가로 종목 1개 계산 시간이 "4개 호출의 합"이
    되어(배치당 overall_timeout 10초를 거의 매번 넘겨버림), AI 종합점수
    일괄계산/점수구간 필터가 몇 분씩 걸리는 근본 원인이 됐다(실측 로그로 확인:
    같은 배치가 몇 번이고 처음부터 재시도되면서 done_count가 몇십 초씩 안 늘어남).
    이번엔 get_ai_score_inner_executor() — 공유 풀·오케스트레이션 풀과 완전히
    무관한 별도 풀 — 에 던진다. 이 풀은 그 어느 쪽도 기다리지 않으므로 예전과
    같은 순환 대기가 구조적으로 발생할 수 없다. 종목 1개 소요 시간이 (4개 호출의
    합) → (4개 중 가장 느린 것 하나)로 줄어든다.
    (수급 점수는 df_price로 평균거래량을 보정해 정확도를 살짝 높이던 부분을
    병렬화를 위해 포기했다 — 순매수 일수·규모 기반 본점수는 그대로고, 강도
    보정값만 약간 덜 정밀해진다.)"""
    code = normalize_kr_code(code)

    _ai_ex = get_ai_score_inner_executor()
    _f_price = _ai_ex.submit(fetch_price_history_for_score, code)
    _f_kospi = _ai_ex.submit(lambda: fetch_sparkline_data().get("kospi"))
    _f_flow = _ai_ex.submit(calc_flow_score, code, 20, None)
    _f_annual = _ai_ex.submit(fetch_financial_data, code) if df_annual is None else None

    try:
        df_price = _f_price.result(timeout=12)
    except Exception:
        df_price = pd.DataFrame()

    kospi_closes = None
    try:
        kospi_closes = _f_kospi.result(timeout=12)
    except Exception:
        pass

    try:
        flow = _f_flow.result(timeout=12)
    except Exception:
        flow = 100.0

    if _f_annual is not None:
        try:
            df_annual, _, _ = _f_annual.result(timeout=12)
        except Exception:
            df_annual = None

    today_incomplete = _last_bar_is_today_kst(df_price)

    trend      = calc_trend_score(df_price)
    volume     = calc_volume_score(df_price, exclude_today=today_incomplete)
    momentum   = calc_momentum_score(df_price, kospi_closes)
    pattern    = calc_pattern_score(df_price, volume)
    risk       = calc_risk_score(df_price, debt, drop_pct)
    financial  = calc_financial_score_detailed(df_annual, roe, debt)
    valuation  = calc_valuation_score_detailed(per, pbr, roe, div)

    total = int(round(max(0, min(1000, trend + flow + volume + financial + valuation + momentum + pattern + risk))))

    return {
        "total": total,
        "trend": trend,         "trend_max": 200,    "trend_min": 0,
        "flow": flow,           "flow_max": 200,     "flow_min": 0,
        "volume": volume,       "volume_max": 100,   "volume_min": 0,
        "financial": financial, "financial_max": 180, "financial_min": 0,
        "valuation": valuation, "valuation_max": 120, "valuation_min": 0,
        "momentum": momentum,   "momentum_max": 100, "momentum_min": 0,
        "pattern": pattern,     "pattern_max": 100,  "pattern_min": 0,
        "risk": risk,           "risk_max": 0,       "risk_min": -80,
        "debug": _ai_score_debug_info(df_price, kospi_closes, debt, drop_pct),
    }


def _ai_score_color(total):
    if total >= 850:   return "#7C3AED"
    elif total >= 700: return "#2563EB"
    elif total >= 550: return "#16A34A"
    elif total >= 400: return "#D97706"
    else:               return "#DC2626"

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

def _score_one_for_ai_batch(code, per, pbr, roe, debt, drop_pct, div):
    """AI 종합점수(1000점 만점 중 total)만 뽑아내는 배치 계산용 래퍼. 실패해도 배치
    전체가 죽지 않도록 예외를 삼키고 None을 반환한다."""
    try:
        return calc_ai_scores_detailed(code, per, pbr, roe, debt, drop_pct, div)["total"]
    except Exception:
        return None


# ── [캐시 영속화] AI 점수·유동성 조회 결과를 세션 메모리뿐 아니라 디스크에도
# 저장한다. reco_df를 CSV로 저장해두는 것과 같은 이유 — 브라우저 새로고침이나
# (특히 Streamlit Cloud에서 흔한) 앱 재시작으로 세션 메모리가 날아가도, 이미
# 계산해둔 종목까지 다시 계산하지 않게 하기 위함.
AI_SCORE_CACHE_PATH = "saved_ai_score_cache.json"
LIQ_VALUE_CACHE_PATH = "saved_liq_value_cache.json"

# ⚠️ [캐시 전략 변경 2026-08-18: 스캔 서명 단위 → 종목코드+TTL 단위] ─────────────
# 문제: 예전에는 캐시가 "이번 스캔 후보 300종목 조합 전체"를 md5로 묶은 서명
# (_candidates_signature) 단위로 저장/조회됐다. 그래서 새로 스캔을 돌려 후보가
# 단 1종목만 바뀌어도 서명 전체가 달라지고, 이전 스캔과 실제로는 280~290개가
# 겹치는데도 그 겹치는 종목의 점수까지 전부 버리고 처음부터 다시 계산해야 했다.
# 이게 "추천 종목 탭에서 스캔할 때마다 AI 점수 계산이 몇 분씩 걸리고 자꾸 멈춘
# 것처럼 보인다"는 문제의 가장 큰 원인이었다.
# 해결: 캐시를 종목코드를 키로 직접 저장하고, 각 항목에 계산 시각(ts)을 같이
# 남긴다. 새 스캔이 몇 번을 돌든, TTL(AI_SCORE_CACHE_TTL) 이내에 이미 계산해둔
# 종목이면 그대로 재사용한다. TTL은 점수 계산에 들어가는 하위 데이터(가격이력
# 30분/재무 1시간/수급 30분 캐시)와 보조를 맞춰, 원본 데이터가 갱신될 때쯤엔
# 점수도 자연스럽게 다시 계산되도록 30분으로 잡았다.
AI_SCORE_CACHE_TTL = 1800  # 30분


def _load_ai_score_disk_cache(path=AI_SCORE_CACHE_PATH):
    """{종목코드: {"score":.., "ts":..}} 형태의 디스크 캐시를 그대로 불러온다.
    예전 형식({"sig":.., "scores": {종목코드: 점수(숫자)}})이 파일에 남아있어도,
    값이 dict가 아니므로 자동으로 걸러져(=신선하지 않은 것으로 간주) 새로
    계산된다 — 별도 마이그레이션 없이 안전하게 자연 교체된다."""
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            raw = payload.get("scores", {}) if isinstance(payload, dict) else {}
            return {c: v for c, v in raw.items() if isinstance(v, dict) and "score" in v and "ts" in v}
    except Exception:
        pass
    return {}


def _save_ai_score_disk_cache(cache_dict, path=AI_SCORE_CACHE_PATH):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"scores": cache_dict}, f, ensure_ascii=False)
    except Exception:
        pass


def _ai_cache_entry_fresh(entry, ttl=AI_SCORE_CACHE_TTL):
    """캐시 항목이 존재하고 TTL 이내에 계산된 것인지."""
    return isinstance(entry, dict) and "ts" in entry and (time.time() - entry["ts"]) < ttl


def _flush_ai_score_partial(scores):
    """지금까지 새로 계산된 부분 결과를 디스크 캐시에 병합 저장한다(기존에 남아있는
    신선한 항목은 그대로 두고, 새로 계산된 것만 갱신). 배치 계산 도중(청크마다)
    호출해서, 세션이 끊기거나 계산이 중간에 멈춰도 그때까지 계산해둔 만큼은
    잃지 않게 한다. 파일 I/O만 하고 st.* API는 전혀 쓰지 않으므로 백그라운드
    스레드 안에서 직접 호출해도 안전하다(스크리너 스캔의 _safe_save_screener_df와
    동일한 이유)."""
    try:
        existing = _load_ai_score_disk_cache()
        now = time.time()
        for code, score in scores.items():
            existing[code] = {"score": score, "ts": now}
        _save_ai_score_disk_cache(existing)
    except Exception:
        pass


def _track_batch_progress_stall(tracker_key, done_count, stall_threshold=25):
    """[진행률 정체 감지] AI 종합점수/거래대금 배치 계산이 여러 rerun에 걸쳐
    점진적으로 채워지는 도중, done_count(완료 개수)가 stall_threshold초 이상
    전혀 늘지 않으면 '정체됨(stalled)'으로 판단해 True를 반환한다.

    ⚠️ [주의: 한계] 이 판정은 페이지 스크립트가 실제로 다시 실행돼야만(rerun)
    동작한다. 만약 브라우저 탭이 백그라운드로 밀리거나 웹소켓 연결 자체가
    끊겨서 재실행 자체가 아예 안 일어나는 상황이라면, 이 함수를 포함한 모든
    코드가 실행되지 않으므로 정체 안내 문구조차 띄울 수 없다 — 그런 경우는
    사용자가 직접 새로고침하는 것 외에는 서버 쪽 코드로 해결할 수 없다.
    이 함수는 어디까지나 "재실행은 계속 되는데 계산 자체가 같은 자리에서
    맴도는" 경우(예: 같은 종목이 매번 타임아웃되어 재시도만 반복)를 잡기 위함이다."""
    tracker = st.session_state.setdefault('_batch_progress_tracker', {})
    now = time.time()
    prev = tracker.get(tracker_key)
    if prev is None or prev.get('done') != done_count:
        tracker[tracker_key] = {'done': done_count, 'ts': now}
        return False
    return (now - prev['ts']) > stall_threshold





def _ai_bulk_score_worker(job_id, items):
    """AI 종합점수 배치를 하나의 지속적인 백그라운드 스레드 안에서 끝까지 이어서
    계산한다 — 종목 스크리너 스캔(_unified_scan_worker)과 동일한 패턴.

    ⚠️ [구조 변경 2026-08-18: 배치+전체rerun → 연속 job] 예전 방식은 30종목씩
    배치를 끊어서 "배치 하나 끝남(최대 18초) → 페이지 전체 재실행(st.rerun()) →
    다음 배치 제출"을 10번 가까이 반복해야 했다. 이 전체 재실행 의존성 때문에
    배치 사이마다 지연이 생기고, 웹소켓이 잠깐 끊기거나 브라우저 탭이 백그라운드로
    밀려 재실행 자체가 늦어지면 진행률이 그대로 멈춘 것처럼 보였다(사용자가
    "새로고침"을 직접 눌러줘야 했던 이유). 지금은 이 함수 하나가 스레드 안에서
    전체 종목을 계속 처리하며 _SCAN_JOB_STATE에 자기 진행률을 스스로 기록하므로,
    페이지가 재실행되지 않아도 전역 폴러(0.4초 간격)가 실시간으로 %를 보여줄 수
    있다.

    items: [{"code", "per", "pbr", "roe", "debt", "drop", "div"}, ...]
    """
    state = _SCAN_JOB_STATE[job_id]
    total = len(items)
    state.update({"total": total, "done": 0, "scores": {},
                   "text": "AI 종합점수 계산 준비 중...", "pct": 0})

    CHUNK = 30  # 공유 스레드풀(32개)을 한 job이 통째로 오래 독점하지 않도록 청크로 나눠 제출
    executor = get_shared_executor()
    for i in range(0, total, CHUNK):
        chunk = items[i:i + CHUNK]
        futures = {
            submit_with_ctx(
                executor, _score_one_for_ai_batch, it["code"], it["per"], it["pbr"],
                it["roe"], it["debt"], it["drop"], it["div"],
            ): it["code"]
            for it in chunk
        }
        pending = set(futures.keys())
        try:
            for future in concurrent.futures.as_completed(futures, timeout=20):
                code = futures[future]
                pending.discard(future)
                try:
                    state["scores"][code] = future.result(timeout=1)
                except Exception:
                    state["scores"][code] = None
                state["done"] += 1
                state["pct"] = int(state["done"] / total * 100) if total else 100
                state["text"] = f"AI 종합점수 계산 중 ({state['done']}/{total}건)"
        except concurrent.futures.TimeoutError:
            pass
        # 이 청크에서 20초 안에 못 끝난 종목은 건너뛴다(다음 전체 계산 때 디스크
        # 캐시에 없으므로 자연히 다시 시도됨). 큐 대기중이었다면 자리를 비워준다.
        for f in pending:
            f.cancel()
        # 청크가 끝날 때마다 디스크에도 반영 — 세션이 끊기거나 계산이 중간에
        # 멈춰도 그때까지 계산해둔 만큼은 잃지 않는다.
        _flush_ai_score_partial(state["scores"])

    state["text"] = "✅ AI 종합점수 계산 완료"
    state["pct"] = 100


def run_ai_bulk_score_async(job_key, items, overall_timeout=600, stall_threshold=45):
    """AI 종합점수 배치의 논블로킹 버전. run_unified_market_scan_async와 완전히
    같은 패턴(같은 _scan_jobs / _SCAN_JOB_STATE 저장소를 그대로 재사용) — 백그라운드
    에서 계속 진행되고, 메인 스크립트는 절대 멈추지 않는다. 같은 job_key로 다시
    호출하면 새 job을 또 만들지 않고 이미 진행 중인 job에 합류한다.

    반환값: (scores, ready, stalled)
      scores  : 지금까지 끝난 만큼(또는 최종)의 부분 결과 {종목코드: 점수 or None}
      ready   : True면 이 job은 이제 없음(완료/시간초과/정체로 종료됨). 남은 대상이
                있으면 호출부가 다음 호출에서 새 job으로 이어서 계산한다.
      stalled : 진행률이 stall_threshold초 이상 전혀 안 바뀌어 강제 종료됐는지 여부
                (호출부가 "계산이 멈춘 것 같다"는 경고 문구를 보여줄 때 사용)."""
    jobs = st.session_state.setdefault("_scan_jobs", {})
    job = jobs.get(job_key)

    if job is None:
        if not items:
            return {}, True, False
        job_id = f"{job_key}_{time.time()}"
        _SCAN_JOB_STATE[job_id] = {"text": "AI 종합점수 계산 준비 중...", "pct": 0,
                                     "done": 0, "total": len(items), "scores": {}}
        future = submit_with_ctx(get_orchestration_executor(), _ai_bulk_score_worker, job_id, items)
        job = {"job_id": job_id, "future": future, "started_at": time.time(), "overall_timeout": overall_timeout}
        jobs[job_key] = job

    job_id = job["job_id"]
    future = job["future"]
    state = _SCAN_JOB_STATE.get(job_id, {})
    partial = dict(state.get("scores", {}))

    now = time.time()
    timed_out = (now - job["started_at"]) > job.get("overall_timeout", overall_timeout)

    # ── [정체 감지] 스캔과 동일한 방식 — done 개수가 stall_threshold초 이상
    # 전혀 안 바뀌면(예: 남은 종목들이 전부 네트워크 응답 없이 걸려있는 상태)
    # 무작정 overall_timeout까지 기다리지 않고 조기에 포기한다.
    last_done = state.get("done", 0)
    if state.get("_last_done_seen") != last_done:
        state["_last_done_seen"] = last_done
        state["_last_done_change_at"] = now
    job["_last_pct_change_at"] = state.get("_last_done_change_at", job["started_at"])
    stalled = (now - state.get("_last_done_change_at", job["started_at"])) > stall_threshold

    if not future.done() and not timed_out and not stalled:
        return partial, False, False

    jobs.pop(job_key, None)
    force_stopped = stalled and not future.done()
    if force_stopped:
        # 좀비 스레드가 공유 풀에 쌓여있을 수 있으니(자세한 이유는 파일 상단
        # get_shared_executor 주석 참고) 다음 재시도가 깨끗한 풀에서 시작하도록 교체.
        try:
            _get_shared_executor_raw.clear()
        except Exception:
            pass
        future.cancel()
    _SCAN_JOB_STATE.pop(job_id, None)
    return partial, True, force_stopped


def _render_ai_grade_filter_and_score(display_df, source_df):
    """[AI 등급 필터] 저평가 등급(S~D) 필터와 완전히 독립된 두 번째 축으로 AI
    종합점수 등급을 추가한다. AND 조건으로 나란히 적용되며, 어느 한쪽 때문에
    다른 쪽 후보군이 미리 줄어들지 않는다.

    ⚠️ [설계 배경] AI 종합점수 하나 계산하려면 종목당 df_price(야후)·수급(네이버)·
    재무(크롤링) 이렇게 최소 3번의 외부 호출이 필요하다. 저평가 등급 필터처럼
    이미 받아온 데이터로 즉석 계산할 수 있는 게 아니라서, 후보 전체(많으면 300개
    안팎)에 한 번에 다 걸면 스캔 자체보다 훨씬 무거운 부하가 생긴다. 그래서
    "S/A/B만 먼저 거르고 그 안에서 AI 점수"가 아니라, 두 필터를 나란히 두되
    AI 점수는 종목코드별로 디스크 캐시(TTL 30분)에 한 번만 계산해 재사용하고,
    아직 신선한 캐시가 없는 종목만 백그라운드 job(run_ai_bulk_score_async)으로
    점진적으로 채운다.

    ⚠️ [캐시 전략 변경 2026-08-18] 예전엔 "이번 스캔 후보 조합 전체"를 서명으로
    묶어 캐시를 통째로 재사용/폐기했다. 지금은 종목코드 단위 TTL 캐시(자세한 이유는
    _load_ai_score_disk_cache 주석 참고)를 쓰므로, 이 함수는 더 이상 source_df로
    "새 스캔인지"를 판별할 필요가 없다. source_df는 인터페이스 호환을 위해 인자로
    남겨뒀다(현재는 내부에서 쓰지 않음).

    반환값: (score_map, still_loading, done_count, total_count, stalled)
      score_map     : {종목코드: AI총점 or None(계산 실패)} — 지금까지 확인된 것만
      still_loading : 아직 계산 안 끝난 종목이 남아있으면 True (안내 문구 표시용)
      stalled       : 계산이 정체돼 강제 종료됐으면 True (경고 문구 표시용)"""
    codes_needed = [str(c).zfill(6) for c in display_df['종목코드']]
    total_count = len(codes_needed)
    if total_count == 0:
        return {}, False, 0, 0, False

    disk_cache = _load_ai_score_disk_cache()
    score_map = {}
    stale_rows = []
    for _, row in display_df.iterrows():
        c = str(row['종목코드']).zfill(6)
        entry = disk_cache.get(c)
        if _ai_cache_entry_fresh(entry):
            score_map[c] = entry["score"]
        else:
            stale_rows.append(row)

    if not stale_rows:
        return score_map, False, total_count, total_count, False

    items = [
        {
            "code": str(r['종목코드']).zfill(6), "per": r['PER'], "pbr": r['PBR'],
            "roe": r['ROE'], "debt": r['부채비율'], "drop": r['고점 / 하락률'],
            "div": r.get('배당수익률', 0.0),
        }
        for r in stale_rows
    ]

    job_scores, ready, job_stalled = run_ai_bulk_score_async("ai_bulk_score", items)
    score_map.update(job_scores)

    done_count = len(score_map)
    still_loading = done_count < total_count

    # ── [진단 로그] 전체 스캔의 [DEBUG SCAN ...]과 동일한 취지 — 배치 계산이
    # 왜/언제 멈췄는지 다음에 로그만 보고도 재구성할 수 있게 남긴다.
    print(f"[DEBUG AI배치 {datetime.datetime.now().strftime('%H:%M:%S')}] "
          f"done={done_count}/{total_count} still_loading={still_loading} "
          f"ready={ready} stalled={job_stalled}", file=sys.stderr, flush=True)

    return score_map, still_loading, done_count, total_count, job_stalled


def render_ai_diagnosis(name, code, per, pbr, roe, debt, drop_pct, div, grade_label):
    # 💬 AI 코멘트(강점/약점 서술)는 기존 4항목(건전성/성장성/수익성/배당) 로직을 그대로 쓴다.
    # 화면에 보이는 점수판/총점은 아래 8항목 세분화(calc_ai_scores_detailed)가 "공식" 값이며,
    # legacy_scores는 코멘트 문장 생성용 내부 참고값일 뿐 화면에 별도로 노출하지 않는다.
    legacy_scores = calc_ai_scores(per, pbr, roe, debt, drop_pct, div)
    detailed = calc_ai_scores_detailed(code, per, pbr, roe, debt, drop_pct, div)
    total = detailed["total"]

    if total >= 850:   total_color = "#7C3AED"; total_label = "최우량"
    elif total >= 700: total_color = "#2563EB"; total_label = "우량"
    elif total >= 550: total_color = "#16A34A"; total_label = "양호"
    elif total >= 400: total_color = "#D97706"; total_label = "보통"
    else:               total_color = "#DC2626"; total_label = "주의"

    grade_badge = (
        f'<span style="font-size:11px; background:#EEF2FF; color:#4F46E5; '
        f'border-radius:4px; padding:2px 7px; margin-left:8px; font-weight:600;">'
        f'{grade_label}</span>'
    ) if grade_label else ""

    CATEGORY_HELP = {
        "추세":   "MA 정배열 강도(0~130) + 추세 지속성(0~70)을 더합니다. 이동평균선이 위로 잘 정렬돼 있고 눌림 없이 버틴 날이 많을수록 높습니다. (52주 고점 대비 위치는 '리스크' 항목에서 별도로 반영합니다.)",
        "수급":   "최근 20일간 기관·외국인 각각의 순매수일 비율(0~80)과 순매수 강도(0~20, 평균 거래량 대비 순매수 규모)를 더해 0~100점씩 채점합니다(최대 200). 두 주체 모두 꾸준히, 큰 규모로 순매수 중이면 높습니다.",
        "거래량": "당일 거래량이 20일 평균보다 얼마나 튀었는지(스파이크, 0~60) + 최근 5일 평균이 얼마나 이어지는지(지속성, 0~40)를 봅니다. 하루짜리 반짝 거래량과 며칠째 이어지는 증가세를 구분합니다.",
        "재무":   "수익성(0~60: ROE+영업이익률+순이익률) + 성장성(0~70: 매출·영업이익·EPS 증가율) + 건전성(0~50: 부채비율)을 더합니다. 돈을 잘 벌고, 성장하고, 빚이 적을수록 높습니다.",
        "밸류":   "PER(0~50) + PBR(0~30) + PEG 근사(PER÷ROE, 0~20) + 배당수익률(0~20)을 더합니다. 이익·자산 대비 저평가돼 있고 배당이 높을수록 높습니다.",
        "모멘텀": "5일(0~20) + 20일(0~40) + 60일(0~20) 수익률 + 코스피 대비 상대강도 RS 보너스(0~20)를 더합니다. 단기·중기·장기 모두 오르는 중이고 지수보다 잘 버틸수록 높습니다.",
        "AI패턴": "⚠️ 과거 급등 사례에서 흔히 보이는 규칙(저항선 돌파·거래량 동반·최근 상승 우위)을 조합한 근사 휴리스틱입니다. 실제 유사도 검색이나 매매 신호가 아닙니다.",
        "리스크": "감점 전용 항목입니다. 일별 등락률 변동성(0~30) + 부채비율(0~20, 60% 이하는 감점 없음) + 52주 고점대비 하락폭(0~20) + 최근 연속 하락일수(0~10)를 감점으로 뺍니다.",
    }

    def score_bar(score, max_val, min_val=0):
        rng = max_val - min_val
        ratio = (score - min_val) / rng if rng else 1.0
        pct = max(0, min(100, ratio * 100))
        if ratio >= 0.8:   bar_color = "#7C3AED"
        elif ratio >= 0.65: bar_color = "#2563EB"
        elif ratio >= 0.5:  bar_color = "#16A34A"
        elif ratio >= 0.35: bar_color = "#D97706"
        else:               bar_color = "#DC2626"
        # 세부 항목은 이제 연속값(소수점)이라, 소수 1자리까지 보여줘서 새로 생긴
        # 정밀도가 실제로 눈에 보이게 한다(예: "62.4/100" vs 예전의 "6/10").
        label = f"{score:.1f}/{max_val}" if min_val == 0 else f"{score:.1f}점"
        return (
            '<div style="display:flex; align-items:center; gap:10px; margin-bottom:8px;">'
            '<div style="width:52px; font-size:12px; color:#64748B; text-align:right;">' + label + '</div>'
            '<div style="flex:1; background:#F1F5F9; border-radius:4px; height:8px;">'
            '<div style="width:' + str(pct) + '%; background:' + bar_color + '; border-radius:4px; height:8px;"></div>'
            '</div></div>'
        )

    categories = [
        ("📈", "추세",     detailed["trend"],     detailed["trend_max"],     0),
        ("💰", "수급",     detailed["flow"],       detailed["flow_max"],       0),
        ("📊", "거래량",   detailed["volume"],     detailed["volume_max"],     0),
        ("💹", "재무",     detailed["financial"],  detailed["financial_max"],  0),
        ("📉", "밸류",     detailed["valuation"],  detailed["valuation_max"],  0),
        ("🚀", "모멘텀",   detailed["momentum"],   detailed["momentum_max"],   0),
        ("🔥", "AI패턴",   detailed["pattern"],    detailed["pattern_max"],    0),
        ("⚠️", "리스크",   detailed["risk"],       detailed["risk_max"],       detailed["risk_min"]),
    ]

    def _tooltip_label(icon, label):
        help_text = CATEGORY_HELP.get(label, "")
        return (
            '<span class="ai-tip-wrap">' + icon + ' ' + label +
            '<span class="ai-tip-icon">?</span>'
            '<span class="ai-tip-box">' + help_text + '</span>'
            '</span>'
        )

    cat_cards = "".join(
        '<div style="background:#FFFFFF; border:1px solid #E2E8F0; border-radius:8px; padding:10px 12px;">'
        f'<div style="font-size:11px; color:#94A3B8; margin-bottom:4px;">{_tooltip_label(icon, label)}</div>'
        + score_bar(val, max_v, min_v) +
        '</div>'
        for icon, label, val, max_v, min_v in categories
    )

    # ── [종합 판단 배지] 항목별 raw 점수만 나열하면 "846점이 좋은 건지" 한눈에
    # 안 들어온다. 각 항목이 자기 만점 대비 몇 %인지(score_bar와 동일 기준)를 계산해
    # 뚜렷하게 강하거나(≥70%) 약한(≤30%) 항목만 골라 "🟢 재무 안정적" 같은 한 줄
    # 배지로 요약한다 — 항목별 8줄짜리 막대를 다 안 봐도 강점/약점이 바로 보이게.
    _STRONG_LABEL = {"추세": "추세 강함", "수급": "수급 양호", "거래량": "거래량 활발",
                      "재무": "재무 안정적", "밸류": "밸류 매력적", "모멘텀": "모멘텀 강함",
                      "AI패턴": "패턴 긍정적", "리스크": "리스크 낮음"}
    _WEAK_LABEL = {"추세": "추세 약함", "수급": "수급 부진", "거래량": "거래량 저조",
                    "재무": "재무 부담", "밸류": "밸류 부담", "모멘텀": "모멘텀 약함",
                    "AI패턴": "패턴 약함", "리스크": "리스크 높음"}

    badges = []
    for icon, label, val, max_v, min_v in categories:
        rng = max_v - min_v
        ratio = (val - min_v) / rng if rng else 1.0
        pct = max(0, min(100, ratio * 100))
        if pct >= 70:
            badges.append(("#16A34A", "#F0FDF4", "🟢", _STRONG_LABEL.get(label, f"{label} 우수")))
        elif pct <= 30:
            badges.append(("#DC2626", "#FEF2F2", "🔴", _WEAK_LABEL.get(label, f"{label} 약함")))

    if badges:
        badge_html = "".join(
            f'<span style="display:inline-flex; align-items:center; gap:4px; font-size:11.5px; '
            f'font-weight:600; color:{fg}; background:{bg}; border-radius:12px; padding:3px 9px; '
            f'margin:3px 6px 0 0;">{emoji} {text}</span>'
            for fg, bg, emoji, text in badges
        )
        cat_cards += (
            '<div style="grid-column:1 / -1; margin-top:2px;">' + badge_html + '</div>'
        )

    TOOLTIP_CSS = (
        '<style>'
        '.ai-tip-wrap{position:relative; display:inline-flex; align-items:center; cursor:help;}'
        '.ai-tip-icon{margin-left:4px; font-size:9px; color:#94A3B8; border:1px solid #CBD5E1; '
        'border-radius:50%; width:12px; height:12px; display:inline-flex; align-items:center; '
        'justify-content:center; line-height:1;}'
        '.ai-tip-wrap .ai-tip-box{visibility:hidden; opacity:0; transition:opacity 0.15s ease; '
        'position:absolute; z-index:80; bottom:135%; left:0; width:210px; max-width:60vw; '
        'background:#0F172A; color:#F1F5F9; font-size:11px; font-weight:400; line-height:1.5; '
        'padding:8px 10px; border-radius:6px; box-shadow:0 6px 16px rgba(0,0,0,0.25); '
        'white-space:normal; text-align:left;}'
        '.ai-tip-wrap:hover .ai-tip-box{visibility:visible; opacity:1;}'
        '</style>'
    )

    html = (
        TOOLTIP_CSS +
        '<div style="background:#FAFBFF; border:1px solid #C7D2FE; border-radius:10px; padding:18px 20px; margin-top:12px;">'
        '<div style="display:flex; align-items:center; gap:12px; margin-bottom:14px;">'
        '<div style="text-align:center;">'
        '<div style="font-size:32px; font-weight:900; color:' + total_color + '; line-height:1;">' + str(total) + '</div>'
        '<div style="font-size:11px; color:#94A3B8;">/ 1000점</div>'
        '</div>'
        '<div>'
        '<div style="font-size:14px; font-weight:700; color:#0F172A;">AI 종합 점수' + grade_badge + '</div>'
        '<div style="font-size:12px; color:' + total_color + '; font-weight:600;">● ' + total_label + '</div>'
        '</div></div>'
        '<div style="display:grid; grid-template-columns:1fr 1fr; gap:10px;">'
        + cat_cards +
        '</div>'
        '<div style="margin-top:10px; font-size:10.5px; color:#94A3B8; line-height:1.6;">'
        '⚠️ AI패턴 점수는 과거 급등 사례에서 흔히 보이는 규칙(저항선 돌파·거래량 동반·상승 우위)을 '
        '조합한 근사치이며, 실제 유사도 검색이나 매매 신호가 아닙니다.'
        '</div>'
        '</div>'
    )
    st.markdown(html, unsafe_allow_html=True)

    debug_info = detailed.get("debug") or {}
    if debug_info:
        with st.expander("🔍 점수 산출 원본 수치 보기 (왜 이 점수인지 확인용)"):
            for k, v in debug_info.items():
                st.markdown(f"- **{k}**: {v}")

    scores = legacy_scores  # 아래 _build_ai_comment 호출부와의 변수명 호환

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
        if warn_text:
            st.markdown(f"<div style='font-size:12.5px; color:#B45309; line-height:1.5; margin-top:2px;'>{warn_text}</div>", unsafe_allow_html=True)

    if btn_scan or "unified_scan" in st.session_state.get("_scan_jobs", {}):
        run_unified_market_scan_async()

    _reco_df = load_reco_df()
    if not _reco_df.empty:
        st.markdown("<hr style='margin: 25px 0 20px 0; border-color: #E5E7EB;'>", unsafe_allow_html=True)
        st.markdown("<h4 style='font-size: 16px; margin-bottom:15px;'>🎛️ 추천 종목 제어판 (실시간 필터링)</h4>", unsafe_allow_html=True)

        market_filter = st.selectbox("시장 분류", ["전체", "코스피", "코스닥"], key="reco_mkt_filter")

        # ⚠️ [UI] st.pills는 버튼 너비/높이를 직접 지정하는 파라미터가 없고, 라벨
        # 텍스트 길이(+이모지 글리프 크기)에 따라 자동으로 크기가 정해진다. 등급
        # pills("💎 S급" 등, 2글자)와 AI 점수 pills("🚀 800~1000" 등)가 텍스트
        # 길이도 다르고 쓰인 이모지도 서로 달라서, 두 줄이 자연스럽게는 크기가
        # 안 맞았다. 두 그룹 모두 같은 최소 너비/높이를 갖도록 CSS로 강제한다.
        st.markdown("""
            <style>
            div[data-testid="stPills"] button {
                min-width: 92px;
                min-height: 34px;
                font-size: 13px;
            }
            </style>
        """, unsafe_allow_html=True)

        st.markdown("<div style='font-size:13px; font-weight:600; color:#475569; margin:14px 0 6px;'>📊 저평가 등급 필터</div>", unsafe_allow_html=True)
        selected_grade = st.pills(
            "등급 필터",
            ["전체보기", "💎 S급", "🥇 A급", "🥈 B급", "🥉 C급", "👀 D급"],
            default="전체보기",
            label_visibility="collapsed"
        )
        if selected_grade is None:
            selected_grade = "전체보기"

        # ── [AI 등급 필터] 저평가 등급(S~D, 위 selected_grade)과는 완전히 독립된
        # 두 번째 필터 축. "S/A/B만 먼저 거르고 그 안에서 AI 점수"가 아니라, 두
        # 필터를 나란히 두고 AND로 조합한다 — S/A/B급만 되면 후보 자체가 너무
        # 적어지는 문제를 피하기 위해, 등급 필터는 '전체보기'로 둔 채 AI 점수만으로도
        # 거를 수 있게 한다. (자세한 이유는 _render_ai_grade_filter_and_score 참고)
        # ⚠️ [UI] 등급 필터 pills(이모지+한글)와 시각적 통일감을 주기 위해 점수
        # 구간마다 등급처럼 느껴지는 이모지를 하나씩 붙였다(🚀최상~🌱입문).
        # ── [AI 점수 일괄 계산 버튼] ────────────────────────────────────────
        # 문제: 원래는 사용자가 "700~799" 같은 특정 점수 구간 pill을 눌러야만
        # 그제서야 후보 전체(최대 150종목)의 AI 종합점수 계산이 시작됐다. 이
        # 계산은 종목당 3번의 외부 호출이 필요한 무거운 배치라서, 사용자
        # 입장에서는 "필터 하나 눌렀을 뿐인데 몇 분씩 로딩"으로 느껴졌다.
        # 해결: "스캔 실행"과 별개로, 언제든 미리 눌러서 전체 후보의 AI
        # 점수를 백그라운드로 미리 계산해둘 수 있는 버튼을 추가한다. 완전히
        # 새 계산 경로가 아니라 기존 _render_ai_grade_filter_and_score를
        # 그대로 재사용하되, display_df(현재 필터링된 일부)가 아니라
        # _reco_df(전체 후보)를 넘겨서 계산 범위를 넓힌 것뿐이다 — 그래서
        # 한 번 눌러두면 이후 어떤 점수 구간 pill을 눌러도(또는 등급/시장
        # 필터를 바꿔도) 이미 캐시에 다 있어서 즉시 결과가 나온다.
        col_ai_label, col_ai_btn = st.columns([4, 1.6])
        with col_ai_label:
            st.markdown("<div style='font-size:13px; font-weight:600; color:#475569; margin:14px 0 6px;'>🤖 AI 종합점수 필터</div>", unsafe_allow_html=True)
        with col_ai_btn:
            st.markdown("<div style='margin-top:10px;'></div>", unsafe_allow_html=True)
            _bulk_ai_clicked = st.button(
                "🤖 AI 점수 일괄 계산",
                use_container_width=True,
                key="reco_ai_bulk_scan_btn",
                help="점수 구간을 고르지 않아도, 지금 보이는 전체 후보 종목의 AI 종합점수를 미리 백그라운드로 계산해둡니다. 계산해두면 이후 점수 구간 필터를 눌렀을 때 바로 결과가 나옵니다.",
            )
        if _bulk_ai_clicked:
            st.session_state['_reco_ai_bulk_scan'] = True
            # 새 일괄 계산을 시작하니, 이전 완료 표시는 지금 상황과 맞지 않게
            # 되므로 지운다(끝나면 아래에서 새로 채워짐).
            st.session_state.pop('_reco_ai_bulk_scan_done_info', None)

        ai_grade_filter = st.pills(
            "AI 등급 필터",
            ["전체보기", "🚀 800~1000", "🔥 700~799", "⭐ 600~699", "✨ 500~599", "🌱 400~499"],
            default="전체보기",
            label_visibility="collapsed",
            key="reco_ai_grade_pills",
            help="AI 종합점수(1000점 만점, 추세·수급·거래량·재무·밸류·모멘텀·패턴·리스크 종합)로 필터링합니다. 저평가 등급 필터와 별개로 동시에 적용됩니다.",
        )
        if ai_grade_filter is None:
            ai_grade_filter = "전체보기"

        # ── [추가 옵션] 등급 필터·AI 필터처럼 후보를 고르는 축이 아니라, 채점
        # 기준 자체를 조정하는 온오프 토글류라서 따로 묶었다.
        st.markdown("<div style='font-size:13px; font-weight:600; color:#475569; margin:14px 0 6px;'>⚙️ 추가 옵션</div>", unsafe_allow_html=True)
        strict_debt = st.toggle("부채비율 '엄격 기준' 적용 (권장)", value=True, help="해제 시 모든 등급의 부채비율 허들을 300%로 완화하여 더 많은 종목을 탐색합니다.")
        # ⚠️ [기능 제거 — 최소 거래대금 필터] 오늘 누적 거래대금은 조회 시점(특히 장
        # 시작 직후)에 따라 값이 왜곡되고(모든 종목이 낮게 나옴), 종목마다 네이버
        # realtime API를 추가로 한 번씩 더 호출해야 해서 스캔 체감 속도에도 영향이
        # 있었다. 실효성 대비 비용이 크다고 판단해 필터 자체(토글·조회 함수·배치
        # 계산 로직)를 제거했다.

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

        _ai_score_map = {}
        _ai_still_loading = False
        _ai_done, _ai_total = 0, 0
        _ai_stalled = False
        # ⚠️ [일괄 계산 버튼과의 연동] "AI 점수 일괄 계산" 버튼을 눌러두면
        # (_reco_ai_bulk_scan=True), 지금 선택된 점수 구간 pill과 무관하게
        # display_df(등급/시장 필터링된 일부)가 아니라 _reco_df(전체 후보)를
        # 넘겨서 계산 범위를 넓힌다. display_df는 항상 _reco_df의 부분집합이라
        # (8339줄에서 .copy()로 파생됨), 이렇게 넓혀서 계산해둔 캐시는 이후
        # 어떤 등급/시장/점수 필터 조합을 눌러도 그대로 재사용된다.
        _bulk_scan_active = st.session_state.get('_reco_ai_bulk_scan', False)
        _need_ai_calc = (ai_grade_filter != "전체보기") or _bulk_scan_active
        # ⚠️ [버그 수정] display_df가 이미 0행일 때 그 위에 .apply() 기반 필터를
        # 또 적용하면, pandas가 빈 Series의 dtype을 제대로 추론하지 못해 boolean
        # 마스크가 아닌 이상한 타입이 되고, 그걸로 인덱싱하면 행뿐 아니라 컬럼까지
        # 통째로 날아간다(실측: KeyError('고점 / 하락률') at sort_values). AI
        # 필터에서 이미 0건이 된 뒤 유동성 필터까지 체이닝될 때 재현됐다(예:
        # AI 800~1000 구간에 맞는 종목이 하나도 없는 상태에서 유동성 필터까지 켜진 경우).
        # 그래서 각 필터 전에 "이미 비어있지 않을 때만 적용"하도록 막았다. 단,
        # 일괄 계산 버튼은 display_df가 아니라 _reco_df 기준으로 도니까
        # display_df가 비어도(예: 등급 필터에 걸리는 종목이 하나도 없어도) 막지 않는다.
        if _need_ai_calc and (_bulk_scan_active or not display_df.empty) and not _reco_df.empty:
            _ai_calc_target_df = _reco_df if _bulk_scan_active else display_df
            _ai_score_map, _ai_still_loading, _ai_done, _ai_total, _ai_stalled = _render_ai_grade_filter_and_score(_ai_calc_target_df, _reco_df)
            if _bulk_scan_active and not _ai_still_loading:
                # 일괄 계산이 끝났으면 버튼 상태를 꺼서, 다음 rerun부터는 이
                # 무거운 전체 재계산을 매번 반복하지 않고 캐시만 조회한다.
                st.session_state['_reco_ai_bulk_scan'] = False
                # ⚠️ [완료 표시 추가] 예전엔 계산이 끝나는 순간 진행률 캡션이
                # 그냥 사라져서, 사용자가 "끝난 건지 그냥 멈춘 건지" 알 수 없었다
                # (실측: 80% 근처에서 화면이 안 바뀌는 걸 보고 아직도 도는 중인
                # 줄 알았는데 실제론 이미 끝나 있었던 사례). 완료된 시점의 건수·
                # 시각을 세션에 남겨서, 아래에서 "완료됨" 표시를 계속 보여준다.
                st.session_state['_reco_ai_bulk_scan_done_info'] = {
                    'count': _ai_total,
                    'ts': time.time(),
                }

        if ai_grade_filter != "전체보기" and not display_df.empty:
            # ⚠️ [배타적 구간으로 변경] 예전엔 ">= min_score"(누적, 상한 없음)라서
            # "600+"를 골라도 700점·800점짜리가 다 같이 나왔다. 지금은 각 pill이
            # 정확히 그 구간(예: 600~699)만 가리키도록 (min, max) 튜플로 바꿨다.
            # 맨 위 구간(800~1000)만 사실상 상한이 없는 것과 같은 효과를 내도록
            # max를 1000으로 둔다(1000점 만점이라 자연스러운 상한).
            _ai_score_bands = {
                "🌱 400~499": (400, 499), "✨ 500~599": (500, 599), "⭐ 600~699": (600, 699),
                "🔥 700~799": (700, 799), "🚀 800~1000": (800, 1000),
            }
            _ai_min_score, _ai_max_score = _ai_score_bands[ai_grade_filter]
            display_df = display_df[
                display_df['종목코드'].apply(
                    lambda c: (lambda v: v is not None and _ai_min_score <= v <= _ai_max_score)(_ai_score_map.get(str(c).zfill(6)))
                )
            ]

        username = st.session_state.get("auth_user")

        # ⚠️ [버그 수정: 계산 도중 결과가 나왔다 사라졌다 하는 현상] 예전엔
        # AI 점수/거래대금이 아직 다 안 끝난 상태에서도 "그 순간까지 확인된 것만"
        # 곧바로 걸러서 보여줬다. 그런데 이 배치 계산은 스레드풀 상황에 따라
        # 종목별 완료 순서가 들쭉날쭉하고, 타임아웃된 종목은 다음 rerun에서 다시
        # 계산되면서 실시간 시세가 살짝 달라져 경계값(예: 700점) 근처 종목이
        # 점수 구간을 넘나들 수 있다. 그 결과 사용자 입장에서는 "종목이 나왔다
        # 안 나왔다, 다른 종목으로 바뀌었다" 하는 것처럼 보여 혼란을 줬다.
        # 계산이 완전히 끝나기 전까지는 목록 자체를 그리지 않고, 다 끝난 뒤
        # "한 번에 정리된" 결과만 보여주도록 바꾼다.
        if _ai_still_loading and ai_grade_filter != "전체보기":
            # 사용자가 실제로 점수 구간(예: 700~799)을 골라서 그 결과를 봐야
            # 하는 상황이라, 계산이 안 끝나면 목록을 보여줄 수 없다 → 기존처럼
            # 화면을 막고 진행률만 보여준다.
            _ai_pct = int((_ai_done / _ai_total) * 100) if _ai_total else 0
            if _ai_stalled:
                st.warning(f"⚠️ AI 종합점수 계산이 멈춘 것 같습니다 ({_ai_done}/{_ai_total}건, {_ai_pct}%에서 진행이 없습니다). 네트워크 조회가 지연되고 있을 수 있어요.")
                if st.button("🔄 새로고침해서 이어서 계산하기", key="reco_ai_stall_refresh"):
                    st.rerun()
            else:
                # 위와 동일한 이유(정체 감지는 재실행이 계속 돼야만 작동) — 항상
                # 보이는 새로고침 버튼을 같이 둔다.
                _cap_col, _btn_col = st.columns([5, 1])
                with _cap_col:
                    st.info(f"⏳ AI 종합점수를 계산하는 중입니다 ({_ai_done}/{_ai_total}건, {_ai_pct}%). 완료되면 결과가 한 번에 표시됩니다. 게이지가 한동안 안 움직이면 오른쪽 새로고침을 눌러주세요...")
                with _btn_col:
                    if st.button("🔄 새로고침", key="reco_ai_filtered_manual_refresh"):
                        st.rerun()
            return
        elif _ai_still_loading and _bulk_scan_active:
            # "전체보기" 상태에서 [AI 점수 일괄 계산] 버튼만 눌러둔 경우 —
            # 지금 화면에는 아직 점수 필터가 안 걸려 있으니 계산이 끝날 때까지
            # 굳이 목록을 가릴 필요가 없다. 진행률만 살짝 보여주고 아래 목록은
            # 정상적으로 계속 렌더링한다.
            _ai_pct = int((_ai_done / _ai_total) * 100) if _ai_total else 0
            if _ai_stalled:
                st.warning(f"⚠️ AI 점수 일괄 계산이 멈춘 것 같습니다 ({_ai_done}/{_ai_total}건, {_ai_pct}%에서 진행이 없습니다). 네트워크 조회가 지연되고 있을 수 있어요.")
                if st.button("🔄 새로고침해서 이어서 계산하기", key="reco_ai_bulk_stall_refresh"):
                    st.rerun()
            else:
                # ⚠️ [버그 수정 2026-08-18] 정체(stalled) 판정은 "재실행이 계속
                # 일어나는데 진행이 없을 때"만 작동한다. 브라우저 탭이 백그라운드로
                # 밀리거나 웹소켓이 잠깐 끊겨 재실행 자체가 아예 안 일어나면(위
                # _track_batch_progress_stall 주석 참고), 이 판정 로직도 같이
                # 멈춰버려서 경고/새로고침 버튼이 뜰 기회조차 없다. 그 결과
                # 사용자는 게이지가 멈춘 걸 보고도 아무 액션도 취할 수 없었고,
                # 우연히 필터를 눌러야만(=강제 rerun) 빠져나올 수 있었다(실측
                # 사례: 06:34:53~06:36:05, 72초간 rerun 없음 → stalled 감지 자체가
                # 불가능했음). 해결: 정체 감지 여부와 무관하게, 계산이 진행 중인
                # 동안에는 항상 눈에 보이는 새로고침 버튼을 같이 노출해서, 게이지가
                # 안 움직이는 걸 본 사용자가 바로 누를 수 있는 명시적인 액션을 준다.
                _cap_col, _btn_col = st.columns([5, 1])
                with _cap_col:
                    st.caption(f"🤖 AI 점수 일괄 계산 중... ({_ai_done}/{_ai_total}건, {_ai_pct}%) — 계산되는 동안에도 아래 목록은 그대로 보실 수 있어요. 게이지가 한동안 안 움직이면 오른쪽 새로고침을 눌러주세요.")
                with _btn_col:
                    if st.button("🔄 새로고침", key="reco_ai_bulk_manual_refresh"):
                        st.rerun()
        elif st.session_state.get('_reco_ai_bulk_scan_done_info'):
            # ⚠️ [완료 표시] 계산이 끝난 뒤에도 (다음 스캔을 새로 누르기 전까지)
            # 계속 남아있는 안내 — "조용히 사라짐" 대신 "확실히 끝났다"는 걸
            # 알려준다.
            _done_info = st.session_state['_reco_ai_bulk_scan_done_info']
            _mins_ago = int((time.time() - _done_info['ts']) / 60)
            _time_str = "방금 전" if _mins_ago < 1 else f"{_mins_ago}분 전"
            st.caption(f"✅ AI 점수 일괄 계산 완료 ({_done_info['count']}건, {_time_str}) — 모든 점수 구간 필터를 바로 확인하실 수 있어요.")

        # ⚠️ [방어 코드] 위 필터링 과정에서 어떤 이유로든(pandas 버전 이슈 포함)
        # display_df가 컬럼까지 잃어버린 채로 넘어오는 상황을 대비해, sort_values를
        # 부르기 전에 반드시 empty 여부부터 확인한다 — 근본 원인이 또 있더라도
        # 화면이 통째로 죽는 것만은 막기 위함.
        if display_df.empty:
            st.info(f"현재 설정된 필터({market_filter}, {selected_grade}, AI {ai_grade_filter})에 부합하는 종목이 없습니다. 조건을 완화해보세요.")
        else:
            # ── [정렬 기준: AI 점수 구간 선택 시 점수 내림차순] ──────────────────
            # 기존에는 AI 점수 구간(예: 700~799)을 골라도 항상 '고점 / 하락률' 오름차순
            # 으로만 정렬되어, 같은 구간 안에서 점수가 높은 종목이 먼저 보인다는 보장이
            # 없었다. AI 점수는 위 필터링 단계(_ai_score_map)에서 이미 계산이 끝난
            # 상태라 여기서는 추가 API 호출·재계산 없이 그 값으로 정렬만 바꾸면 되므로
            # 속도에는 영향이 없다. 점수 구간을 고르지 않은 '전체보기'일 때는 기존과
            # 동일하게 하락률 기준 정렬을 유지한다(원래 화면 흐름 유지).
            if ai_grade_filter != "전체보기":
                display_df['_ai_score_sort'] = display_df['종목코드'].apply(
                    lambda c: _ai_score_map.get(str(c).zfill(6), -1)
                )
                display_df = display_df.sort_values('_ai_score_sort', ascending=False).drop(columns=['_ai_score_sort']).reset_index(drop=True)
            else:
                display_df = display_df.sort_values('고점 / 하락률', ascending=True).reset_index(drop=True)
            # ── [페이지네이션 제거: 한 번에 전체 표시] ────────────────────────
            # 기존에는 "결과 보기"를 누른 뒤에도 PAGE_SIZE(15)개씩 "더 보기"를 눌러야
            # 다음 종목들이 나오는 구조였다. 그런데 이 시점(정렬 이후)에는 AI 점수
            # 계산·거래대금 필터·정렬이 이미 다 끝난 상태라, "더 보기"를 눌러도 추가
            # 계산이나 조회가 발생하는 게 아니라 이미 준비된 데이터를 몇 개 더 그릴지
            # 결정만 할 뿐이었다. 그런데도 매번 st.rerun()이 발생해 화면이 깜빡였고,
            # 이미 내림차순(AI 점수) 정렬이 되어 있어 사용자 입장에서는 상위 종목이
            # 잘려 보이다 말다 하는 것도 어색했다. "결과 보기" 클릭 게이트(최초
            # 렌더링 부하 방지)는 그대로 유지하되, 클릭한 뒤에는 더 보기 없이 전체를
            # 한 번에 그린다.
            PAGE_SIZE = None
            total_n = len(display_df)
            # ⚠️ total_n을 시그니처에 넣지 않는다 — AI 등급 필터가 점진적으로 채워지는
            # 동안(백그라운드 계산이 끝날 때마다) total_n이 계속 바뀌는데, 그때마다
            # "결과 보기"가 리셋돼 화면이 계속 접혔다 펼쳐지는 부작용이 생긴다. 대신
            # len(_reco_df)(원본 후보 개수, 새 스캔이 돌기 전까진 불변)로 "새 스캔이
            # 돌았는지"만 판단한다.
            _reco_filter_sig = (market_filter, strict_debt, selected_grade, ai_grade_filter, len(_reco_df))
            if st.session_state.get('_reco_filter_sig') != _reco_filter_sig:
                st.session_state['_reco_filter_sig'] = _reco_filter_sig
                st.session_state['_reco_shown'] = False

            if not st.session_state.get('_reco_shown', False):
                # [클릭 게이트] 탭 진입/필터 변경 즉시 종목 카드를 전부 그리면(많을 때는
                # 수십~백 개) 매번 렌더링 부하가 커서 탭 전환이 버벅였다. 그래서 결과를
                # 미리 그리지 않고, 사용자가 직접 "결과 보기"를 눌렀을 때만 그리기 시작한다.
                if st.button(f"🔽 결과 보기 ({total_n}건)", use_container_width=True, key="reco_show_btn"):
                    st.session_state['_reco_shown'] = True
                    st.rerun()
            else:
                if st.button("🔼 결과 접기", key="reco_hide_btn"):
                    st.session_state['_reco_shown'] = False
                    st.rerun()

                # [전체 표시] 결과 보기를 누르면 페이지 단위로 끊지 않고 전체(total_n)를
                # 한 번에 그린다. 위에서 이미 정렬(AI 점수 구간 선택 시 점수 내림차순,
                # 전체보기 시 하락률 오름차순)이 끝난 상태라, 상위 종목부터 순서대로
                # 그대로 다 보여주면 된다.
                n_show = total_n
                page_df = display_df.iloc[:n_show]

                # ⚠️ [되돌림 — 2026-08-12] "AI 필터가 꺼져있어도 화면에 보이는
                # page_df만큼은 자동으로 배지를 계산해서 보여준다"는 기능을 넣었었는데,
                # 실측 로그에서 스캔이 끝난 뒤에도 '추천 종목' 페이지가 0.1~1초 간격의
                # 재실행 스팸을 계속 일으키며 '공유'/'오케스트레이션' 풀이 번갈아
                # "완전히 막혀있어 새 풀로 교체함" 상태에 빠지는 게 확인됐다. 페이지에
                # 진입할 때마다(=폴링 재실행마다) 배경 계산이 계속 재점화되면서
                # 스레드풀에 부담이 누적된 것으로 보인다. 그래서 AI 점수 배지는
                # 다시 "AI 등급 필터를 켰을 때만" 계산하도록 보수적인 방식으로
                # 되돌린다 — 화면에 자동으로 배지가 뜨는 편의는 포기하더라도,
                # 안정성을 우선한다.
                for _, row in page_df.iterrows():
                    name  = row['종목명']
                    code  = str(row['종목코드']).zfill(6)
                    market_str = row.get('시장', '')
                    price = row['현재가_num']
                    drop_pct = row['고점 / 하락률']
                    per, pbr, roe, debt = row['PER'], row['PBR'], row['ROE'], row['부채비율']
                    div = row.get('배당수익률', 0.0)
                    grade_label = row['등급']
                    source_badge = row.get('데이터출처', '🌐 실시간')
                    _ai_score_val = _ai_score_map.get(code)
                    # ⚠️ [UI 정리] 등급·AI점수·출처 배지 3개의 font-size/padding/
                    # border-radius가 제각각이라 크기가 안 맞아 보였다. 세 배지 모두
                    # 같은 크기 규격(10px / 2px 8px / 9px)으로 통일하고, 색상만
                    # 종류별로 다르게 줘서 구분한다. 순서도 "지금 이 카드를 판단하는 데
                    # 중요한 순서"로 맞췄다: 등급(1차 분류) → AI 종합점수(2차 판단
                    # 근거) → 데이터출처(참고용 메타정보, 가장 덜 중요).
                    _BADGE_BASE = "font-size:10px; font-weight:700; padding:2px 8px; border-radius:9px; margin-left:6px;"
                    grade_badge_html = f'<span style="{_BADGE_BASE} color:#111827; background:#FFFFFF; border:1px solid #D1D5DB;">{grade_label}</span>'
                    ai_score_badge_html = (
                        f'<span style="{_BADGE_BASE} color:#4F46E5; background:#EEF2FF; border:1px solid #C7D2FE;">'
                        f'🎯 AI {int(_ai_score_val)}</span>'
                        if _ai_score_val is not None else ''
                    )
                    source_badge_html = f'<span style="{_BADGE_BASE} font-weight:500; color:#64748B; background:#F1F5F9; border:1px solid #E2E8F0;">{source_badge}</span>'

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
                                        {grade_badge_html}{ai_score_badge_html}{source_badge_html}
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
                        # ── [버그 수정: 메인 렌더 스레드가 네트워크 호출로 최대 20~30초씩
                        # 멈추던 문제] st.expander 안의 코드는 접혀있어도(펼치지 않아도)
                        # 매 rerun마다 그대로 실행된다. 그런데 render_ai_diagnosis →
                        # calc_ai_scores_detailed → fetch_financial_data는 캐시가
                        # 비어있으면(@st.cache_data 미스) 네이버/FnGuide에 동기(requests.get,
                        # 최대 10초×여러 번)로 직접 접속한다. 리스트에 표시되는 종목 수만큼
                        # 이게 반복되면 "추천 종목" 페이지 전체가 그 시간만큼 멈춰버렸다
                        # (실측: 워치독 로그에서 메인 스레드가 fetch_naver_wisereport_data
                        # 내부 requests.get에 멈춰있는 게 확인됨).
                        #
                        # 내부적으로 다시 병렬화하는 건 이미 한 번 시도했다가 orchestration
                        # 풀↔shared 풀 순환 대기로 데드락이 나서 되돌린 이력이 있으므로
                        # (calc_ai_scores_detailed 문서 참고) 여기서도 병렬화 대신, 이미
                        # 이 파일의 배당 탭에서 쓰던 것과 동일한 패턴 — "명시적 클릭 전에는
                        # 무거운 네트워크 호출을 시작하지 않는다" — 을 그대로 적용한다.
                        #
                        # AI 종합점수 배치(_render_ai_grade_filter_and_score)가 백그라운드
                        # 스레드풀에서 이미 이 종목을 계산해뒀다면(=st.session_state의
                        # _reco_ai_score_cache에 존재) fetch_financial_data가 이미
                        # @st.cache_data에 데워져 있어 즉시(네트워크 왕복 없이) 렌더링되므로
                        # 클릭 없이 바로 보여준다. 아직 배치가 이 종목까지 못 왔다면, 버튼을
                        # 눌러야만 그 순간 딱 이 종목 하나에 대해서만 네트워크 호출이 발생한다.
                        _diag_ready_key = f"reco_diag_ready_{code}"
                        _ai_cache_now = st.session_state.get('_reco_ai_score_cache', {})
                        _diag_is_warm = (code in _ai_cache_now) or st.session_state.get(_diag_ready_key, False)

                        if _diag_is_warm:
                            render_ai_diagnosis(name, code, per, pbr, roe, debt, drop_pct, div, grade_label)
                        else:
                            st.info("⏳ 아직 AI 재무 데이터가 준비되지 않았습니다. 배치 계산이 끝날 때까지 기다리시거나, 아래 버튼으로 이 종목만 지금 바로 불러올 수 있어요 (몇 초 소요될 수 있습니다).")
                            if st.button(f"⚡ {name} AI 진단 지금 불러오기", key=f"reco_diag_load_{code}"):
                                st.session_state[_diag_ready_key] = True
                                st.rerun()
                        st.markdown("<hr style='margin:16px 0 12px 0; border-color:#E5E7EB;'>", unsafe_allow_html=True)

                        btn_key = f"reco_fn_{code}"
                        data_key = f"reco_fn_data_{code}"
                        if st.button(f"📊 실시간 재무 데이터 불러오기 (FnGuide)", key=btn_key):
                            with st.spinner(f"'{name}'의 최신 기업 개요와 재무제표를 가져오는 중입니다..."):
                                st.session_state[data_key] = True
                        if st.session_state.get(data_key):
                            draw_fnguide_details(code)


                # ⚠️ "더 보기" 버튼 제거됨 — 위에서 이미 n_show = total_n으로 전체를
                # 한 번에 그리므로 더 불러올 필요가 없다.

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
    col_tabs, col_scan_btn = st.columns([5, 1.6])
    with col_tabs:
        selected_preset = st.pills("필터 단계", preset_names, default=preset_names[0], key="screener_preset", label_visibility="collapsed")
        if selected_preset is None:
            selected_preset = preset_names[0]
    with col_scan_btn:
        st.markdown("<div style='height: 2px;'></div>", unsafe_allow_html=True)
        _screener_scan_clicked = st.button("실시간 데이터 ⚡초고속 스캔 실행", use_container_width=True, key="screener_scan_btn")
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

    st.markdown("""
        <div class="info-box-modern">
            • 네이버 금융 사이트를 스캔하여 시장 전 종목의 최신 지표를 가져옵니다.<br>
            • <b>4코어 안전 멀티스레딩</b> 기술이 적용되어 빠르면서도 네이버 서버 차단을 완벽히 회피합니다.
        </div>
    """, unsafe_allow_html=True)

    if _screener_scan_clicked or "unified_scan" in st.session_state.get("_scan_jobs", {}):
        run_unified_market_scan_async()

    # ── [진행률 배너 위치 요청] 스캔 버튼 바로 아래에서 실시간 진행률을 보여준다 ──
    # 예전에는 이 페이지에서 전역 폴링 fragment를 따로 호출하지 않고 _main_impl()의
    # 맨 끝 호출에만 의존했다. fragment는 코드에서 "호출된 그 자리"에 그려지므로,
    # 그 결과 진행률 배너가 페이지 맨 아래(스캔 버튼과 멀리 떨어진 곳)에 나타났다.
    # 대시보드와 동일한 방식으로, 여기서 스캔 버튼 바로 아래에 앞당겨 호출한다.
    # ⚠️ fragment는 세션당 "이번 스크립트 실행에서 딱 한 번만" 호출돼야 하므로
    # (안 그러면 Streamlit 버그 #10719 재발), 이 페이지가 선택된 경우 _main_impl()의
    # 맨 끝 호출은 건너뛰도록 처리해뒀다 (아래 _main_impl 참고).
    # (run_unified_market_scan_async()가 막 job을 만들었거나 아직 진행 중일 때만
    # 여기 도달하므로 안전하다 — 방금 완료된 경우엔 그 함수 안에서 이미 결과를
    # 반영하고 st.rerun()까지 호출해버려서 아예 이 아래 코드에 도달하지 않는다.)
    maybe_run_global_poller()

    df = load_screener_df()

    if not df.empty:
        st.markdown("<hr style='margin: 30px 0 20px 0; border-color: #E5E7EB;'>", unsafe_allow_html=True)

        ETF_KEYWORDS = 'TIGER|KODEX|ARIRANG|KBSTAR|HANARO|KOSEF|TREX|ACE|SOL|RISE|ETF|인버스|레버리지|선물|리츠|REIT|인덱스|TR$'
        df = df[~df['종목명'].str.contains(ETF_KEYWORDS, regex=True, case=False, na=False)]

        col_tools1, col_tools2 = st.columns([3, 2])
        with col_tools1:
            with st.container(key="market_filter_box"):
                market_filter = st.pills("시장", ["전체", "코스피", "코스닥"], default="전체", key="screener_market", label_visibility="collapsed")
                if market_filter is None:
                    market_filter = "전체"
            
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
        res.encoding = 'euc-kr'  # 네이버금융(finance.naver.com)은 euc-kr 고정 — apparent_encoding 추측에 의존하면 특정 종목명 바이트 패턴에서 오탐(예: 키릴 계열로 오판)해 파싱이 깨진다
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

def render_ai_probability():
    """[신규 탭 뼈대 — 2026-08-18] "AI 확률분석"

    목표: 특정 목표가에 도달할 확률을 "딥하게" 보여주는 탭. 기존에 흩어져 있던
    두 엔진을 재사용해서 나란히 보여주는 걸 1단계로 잡았다:

      1) render_ai_diagnosis — 기업 재무 분석 탭에서 쓰는 것과 완전히 동일한
         AI 종합점수 카드(0~1000점, 8개 항목 막대그래프+툴팁, 강점/약점 배지,
         AI 코멘트). PER/PBR/ROE/부채비율/52주 고점대비 낙폭/배당/거래량/수급/
         추세/모멘텀/패턴을 전부 반영한다.
      2) estimate_target_hit_probability — 이미 있는 변동성 기반 몬테카를로
         확률(재무와 무관, 순수 통계). 이 탭에서는 게이지 바 + 감성 라벨을
         붙인 _format_probability_fun_card로 좀 더 눈에 띄게 표시한다.

    ⚠️ [의도적 미구현 — 다음 단계 TODO] 지금은 이 두 숫자를 "나란히" 보여줄
    뿐, 하나로 합치지 않는다. AI 종합점수가 높다고 그 방향(상승)에 가중치를
    주는 건 매력적이지만, 근거 없이 임의로 섞으면 "정교해 보이는데 실제로는
    감으로 만든 숫자"가 된다(사용자와 상의한 내용). 나중에 실제로 합치려면:
      - AI 점수 → 드리프트(방향성) 보정치로 변환하는 공식에 대한 근거가 있어야
        하고 (예: 점수 구간별로 과거 N개월 후 실제 수익률 분포를 집계해서
        캘리브레이션)
      - 그 보정이 실제로 맞았는지 백테스트로 검증하는 절차가 필요하다.
    이 두 가지가 준비되기 전까지는 "각자 다른 근거로 계산된 두 참고 지표"로
    분리해서 보여주는 게 사용자를 오도하지 않는 방법이다.
    """
    st.header(
        "AI 확률분석",
        help="""💡 **[AI 확률분석 안내]**\n\n특정 목표가에 도달할 가능성을 두 가지 서로 다른 방식으로 참고할 수 있게 보여줍니다.\n\n1) AI 종합점수 (0~1000점) — PER·PBR·ROE·부채비율·거래량·수급·추세·모멘텀 등 펀더멘털/기술적 지표를 종합한 점수\n2) 통계적 도달확률 — 최근 1년 변동성을 이용한 몬테카를로 시뮬레이션 (재무 내용과는 무관한 순수 통계치)\n\n⚠️ 두 지표는 아직 하나로 합쳐지지 않은 별도의 참고 정보이며, 투자 조언이나 수익을 보장하지 않습니다."""
    )
    st.markdown(
        "<p style='font-size:12px; color:#94A3B8; margin-top:-8px;'>"
        "🎲 통계적 도달확률은 재무제표·실적을 반영하지 않은, 순수 변동성 기반 참고치입니다. "
        "투자 판단은 본인 책임하에 신중히 결정해주세요.</p>",
        unsafe_allow_html=True
    )
    st.markdown("<hr style='margin: 10px 0 25px 0; border-color: #E5E7EB;'>", unsafe_allow_html=True)

    # ── 종목 검색 (기업 재무 분석 탭과 동일 패턴) ──────────────────────────
    # 🔧 [엔터 검색 지원 — 2026-08-19] 예전에는 st.text_input + st.button을 그냥
    # 나란히 뒀는데, 이 조합은 Streamlit이 "버튼 클릭"만 제출로 인식해서 검색창에
    # 값을 입력하고 Enter를 쳐도 아무 반응이 없었다(포커스만 벗어날 뿐 재실행은
    # 되지만 search_btn은 여전히 False). st.form으로 감싸면 그 안의 텍스트 입력에서
    # Enter를 눌러도 폼 전체가 제출된 것으로 처리되어(=form_submit_button을 누른 것과
    # 동일하게 취급), 버튼을 직접 클릭하지 않아도 검색이 실행된다.
    with st.form("aiprob_search_form", clear_on_submit=False, border=False):
        col1, col2, col3 = st.columns([1.6, 1, 3.4])
        with col1:
            query = st.text_input(
                "종목코드 또는 종목명 입력",
                placeholder="예: 005930, 삼성전자",
                label_visibility="collapsed",
                key="aiprob_query_input"
            )
        with col2:
            search_btn = st.form_submit_button("🔍 조회", use_container_width=True)

    if search_btn and query:
        resolved_code, resolved_name, candidates = resolve_stock_query(query)
        st.session_state.pop('aiprob_not_found', None)
        if candidates:
            st.session_state['aiprob_candidates'] = candidates
        elif resolved_code:
            st.session_state['aiprob_code'] = resolved_code
            st.session_state.pop('aiprob_candidates', None)
        else:
            st.session_state.pop('aiprob_candidates', None)
            st.session_state['aiprob_not_found'] = query

    if st.session_state.get('aiprob_candidates'):
        candidates = st.session_state['aiprob_candidates']
        options = [f"{c['name']} ({c['code']}) · {c['market']}" for c in candidates]
        col_pick, col_pick_btn, _ = st.columns([2.6, 1, 3.4])
        with col_pick:
            picked = st.selectbox(
                "검색 결과가 여러 건입니다. 종목을 선택해주세요.",
                options,
                label_visibility="visible",
                key="aiprob_pick_select"
            )
        with col_pick_btn:
            st.markdown("<div style='height:28px;'></div>", unsafe_allow_html=True)
            if st.button("이 종목 조회", use_container_width=True, key="aiprob_pick_confirm"):
                picked_idx = options.index(picked)
                st.session_state['aiprob_code'] = candidates[picked_idx]['code']
                st.session_state.pop('aiprob_candidates', None)
                st.rerun()

    if st.session_state.get('aiprob_not_found'):
        st.warning(f"'{st.session_state['aiprob_not_found']}'에 해당하는 종목을 찾을 수 없습니다. 정확한 종목명 또는 6자리 종목코드로 다시 검색해주세요.")

    active_code = st.session_state.get('aiprob_code', '')
    if not active_code:
        return

    code = active_code

    # 🐛 [버그 수정: 코스닥 종목이 "가격 데이터 부족"으로 잘못 표시되던 문제]
    # estimate_target_hit_probability에 market_hint를 안 넘기면(=None) 내부적으로
    # 코스피(.KS) 접미사부터 시도한다. 코스닥 종목(예: 파이오링크)은 이 첫 시도가
    # 항상 실패하고 두 번째 시도(.KQ)로 넘어가는데, 그 사이 지연·야후 쪽 일시적
    # 실패가 겹치면 실제로는 데이터가 있는데도 "가격 데이터가 부족합니다"로
    # 잘못 뜬다. 관심종목 탭(4851번 줄 부근)과 동일하게 screener_df에서
    # 종목코드→시장 매핑을 만들어 넘겨줘서, 코스닥 종목은 처음부터 .KQ를
    # 먼저 시도하도록 고친다.
    _aiprob_screener_df = load_screener_df()
    if _aiprob_screener_df is not None and not _aiprob_screener_df.empty and '시장' in _aiprob_screener_df.columns:
        _aiprob_market_map = dict(zip(_aiprob_screener_df['종목코드'], _aiprob_screener_df['시장']))
    else:
        _aiprob_market_map = {}
    market_hint = _aiprob_market_map.get(code)
    cache_key = f'aiprob_result_{code}'

    if search_btn or cache_key not in st.session_state:
        def _do_aiprob_fetch():
            _price_info = fetch_current_price_info(code)
            _info = fetch_company_info_fnguide(code)
            _df_annual, _, _ = fetch_financial_data(code)
            _per_ai, _pbr_ai, _roe_ai, _debt_ai, _drop_pct_ai, _div_ai = get_ai_diagnosis_inputs(code, _df_annual)
            return {
                "price_info": _price_info,
                "name": _info.get('name') or code,
                "per_ai": _per_ai, "pbr_ai": _pbr_ai, "roe_ai": _roe_ai,
                "debt_ai": _debt_ai, "drop_pct_ai": _drop_pct_ai, "div_ai": _div_ai,
            }

        with st.spinner("AI 종합점수를 계산하는 중입니다..."):
            _result = call_with_timeout(_do_aiprob_fetch, timeout=25)

        if _result is None:
            st.error("⏱️ 데이터 조회가 너무 오래 걸려 중단했습니다. 잠시 후 다시 시도해주세요.")
            st.stop()

        st.session_state[cache_key] = _result

    cached = st.session_state[cache_key]
    price_info = cached['price_info']
    current_price = price_info.get('price')

    if not current_price:
        st.warning("현재가를 조회하지 못했습니다. 종목코드를 다시 확인해주세요.")
        return

    # ── 목표가 도달 확률 (통계 기반) — 이 탭의 메인 지표라 상단에 배치 ──────
    st.markdown("<h4 style='font-size:16px; margin-bottom:4px;'>🎲 목표가 도달 확률 (통계 기반)</h4>", unsafe_allow_html=True)
    st.markdown(
        "<p style='font-size:12px; color:#64748B; margin-bottom:12px;'>"
        "최근 1년 변동성을 이용한 몬테카를로 시뮬레이션입니다. 아래 AI 종합점수와는 "
        "별개로 계산되며, 방향성(상승/하락)을 예단하지 않습니다.</p>",
        unsafe_allow_html=True
    )

    # 💡 [현재가 표시 — 2026-08-19] 목표가 입력란 바로 위에 지금 현재가가 얼마인지
    # (전일대비 등락 포함) 보여준다. 목표가만 덩그러니 있으면 "지금 얼마인데 이
    # 목표가를 잡은 건지" 감이 안 와서, 나중에 목표가 기본값을 컨센서스로 자동
    # 채우게 되더라도 항상 현재가를 같이 보고 판단할 수 있게 한다.
    _diff = price_info.get('diff')
    _diff_pct = price_info.get('diff_pct')
    if _diff is not None and _diff_pct is not None:
        _status = price_info.get('status', 'neutral')
        _p_color = {"up": "#DC2626", "down": "#2563EB", "neutral": "#64748B"}[_status]
        _p_arrow = {"up": "▲", "down": "▼", "neutral": "-"}[_status]
        _p_sign = "+" if _diff >= 0 else ""
        _price_line = (
            f"<span style='font-size:20px; font-weight:800; color:#0F172A;'>{int(current_price):,}원</span> "
            f"<span style='font-size:13px; font-weight:700; color:{_p_color};'>{_p_arrow} {_p_sign}{_diff:,.0f} ({_p_sign}{_diff_pct:.2f}%)</span>"
        )
    else:
        _price_line = f"<span style='font-size:20px; font-weight:800; color:#0F172A;'>{int(current_price):,}원</span>"
    st.markdown(
        f"<div style='margin-bottom:10px;'>"
        f"<span style='font-size:12px; color:#94A3B8; display:block; margin-bottom:2px;'>현재가</span>"
        f"{_price_line}</div>",
        unsafe_allow_html=True
    )

    default_target, target_src = estimate_simple_target_price(current_price)
    # 🐛 [버그 수정] key가 고정값(aiprob_target_input_field)이면, 종목을 바꿔도
    # Streamlit이 이전 위젯 상태를 그대로 재사용해서 목표가 입력란이 이전
    # 종목 값 그대로 남아있었다. key에 종목코드를 포함시키면 종목이 바뀔 때
    # 자동으로 새 위젯(=새 기본값)으로 취급되어 매번 그 종목에 맞는 기본
    # 목표가로 리셋된다.
    # 💬 [자동 콤마 포맷] 값을 바꿀 때(on_change)마다 숫자만 추출해 다시
    # "1,234" 형태로 저장한다. 관심종목 탭의 목표가 입력란과 동일한 패턴.
    _tgt_key = f"aiprob_target_input_{code}"
    if _tgt_key not in st.session_state:
        st.session_state[_tgt_key] = f"{default_target:,.0f}"

    def _fmt_aiprob_target(k=_tgt_key):
        digits = re.sub(r"[^\d]", "", str(st.session_state.get(k, "")))
        st.session_state[k] = f"{int(digits):,}" if digits else ""

    st.text_input(
        "목표가 직접 입력 (원)",
        key=_tgt_key,
        on_change=_fmt_aiprob_target,
        placeholder="예: 300,000"
    )
    try:
        target_price = int(re.sub(r"[^\d]", "", st.session_state.get(_tgt_key, "")))
    except Exception:
        target_price = default_target

    if target_price and target_price > 0:
        result = estimate_target_hit_probability(code, market_hint, target_price)
        badge_html = _format_probability_fun_card(result, target_price, target_src="목표가", current_price=current_price)
        if badge_html:
            st.markdown(badge_html, unsafe_allow_html=True)
        else:
            st.info("확률 계산에 필요한 가격 데이터가 부족합니다 (상장 1년 미만 종목 등).")

    # TODO(다음 단계): AI 종합점수를 드리프트 보정치로 반영해 "AI가 종합적으로
    # 판단한 확률" 하나로 합치기 — 캘리브레이션·백테스트 설계 후 진행.

    st.markdown("<hr style='margin:20px 0 16px 0; border-color:#E5E7EB;'>", unsafe_allow_html=True)

    # ── AI 종합점수 (기업 재무 분석 탭과 동일한 카드 UI 재사용) ────────────
    st.markdown("<h4 style='font-size:16px; margin-bottom:4px;'>🤖 AI 종합점수</h4>", unsafe_allow_html=True)
    st.markdown(f"<div style='color:#64748B; font-size:13px; margin-bottom:8px;'>현재가 {int(current_price):,}원 기준</div>", unsafe_allow_html=True)
    render_ai_diagnosis(
        cached['name'], code,
        cached['per_ai'], cached['pbr_ai'], cached['roe_ai'], cached['debt_ai'],
        cached['drop_pct_ai'], cached['div_ai'], ""
    )


def render_fnguide():
    st.header(
        "기업 재무 분석",
        help="""💡 **[기업 재무 분석 안내]**\n\n특정 종목의 상세한 재무 상태를 분석합니다.\n\nFnGuide 기반의 최신 연간/분기 실적 흐름, 매출 및 이익 성장률(YoY, QoQ), 증권사 목표주가 컨센서스와 통합 AI 종합 진단 결과를 한눈에 확인할 수 있습니다."""
    )
    st.markdown("<hr style='margin: 10px 0 25px 0; border-color: #E5E7EB;'>", unsafe_allow_html=True)

    # 🔧 [엔터 검색 지원 — 2026-08-19] st.form으로 감싸면 안의 텍스트 입력에서
    # Enter를 눌러도 폼 제출(=버튼 클릭과 동일)로 처리된다. 자세한 이유는
    # render_ai_probability()의 동일 패턴 주석 참고.
    with st.form("fnguide_search_form", clear_on_submit=False, border=False):
        col1, col2, col3 = st.columns([1.6, 1, 3.4])
        with col1:
            query = st.text_input(
                "종목코드 또는 종목명 입력",
                placeholder="예: 005930, 삼성전자",
                label_visibility="collapsed",
                key="fnguide_query_input"
            )
        with col2:
            search_btn = st.form_submit_button("🔍 조회", use_container_width=True)

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

        # 🐛 [버그 수정: 코스닥 종목 목표가 도달 확률이 "데이터 부족"으로 잘못 표시되던 문제]
        # 아래쪽 render_hit_probability_badge(code, None, ...) 호출들이 market_hint를
        # 항상 None으로 넘겨서, 코스닥 종목은 estimate_target_hit_probability 내부에서
        # 코스피(.KS) 접미사부터 헛시도한 뒤에야 코스닥(.KQ)으로 재시도한다. 이 지연·
        # 일시적 실패가 겹치면 실제로는 데이터가 있는데도 "가격 데이터가 부족합니다"로
        # 잘못 뜬다. screener_df에서 종목코드→시장 매핑을 만들어 넘겨줘서 처음부터
        # 맞는 접미사로 시도하도록 고친다. (render_ai_probability의 동일 수정 참고)
        _fnguide_screener_df = load_screener_df()
        if _fnguide_screener_df is not None and not _fnguide_screener_df.empty and '시장' in _fnguide_screener_df.columns:
            _fnguide_market_map = dict(zip(_fnguide_screener_df['종목코드'], _fnguide_screener_df['시장']))
        else:
            _fnguide_market_map = {}
        market_hint = _fnguide_market_map.get(code)

        cache_key = f'fnguide_result_{code}'

        if search_btn or cache_key not in st.session_state:
            # [재무분석 버튼 클릭 시 멈춤 대응] 이 블록만 유일하게 call_with_timeout 같은
            # 보호장치 없이 fetch_company_info_fnguide → fetch_financial_data를 메인
            # 스레드에서 순차적으로 직접 호출하고 있었다. 개별 요청엔 timeout이 있지만
            # 여러 개가 순서대로 실행되니 다 더해지고, FnGuide·네이버 같은 국내 사이트는
            # 해외 리전 서버에서 접속이 느려지는 경우가 잦아서(코드 곳곳의 DART 우회
            # 프록시 사례처럼) 체감상 "완전히 멈춘" 것처럼 보일 수 있었다.
            # 전체를 call_with_timeout으로 감싸서 상한(25초)을 명확히 강제한다.
            def _do_fnguide_fetch():
                _info = fetch_company_info_fnguide(code)
                _df_annual, _, _ = fetch_financial_data(code)
                _per_ai, _pbr_ai, _roe_ai, _debt_ai, _drop_pct_ai, _div_ai = get_ai_diagnosis_inputs(code, _df_annual)
                return {
                    'info': _info,
                    'per_ai': _per_ai, 'pbr_ai': _pbr_ai, 'roe_ai': _roe_ai,
                    'debt_ai': _debt_ai, 'drop_pct_ai': _drop_pct_ai, 'div_ai': _div_ai,
                }

            with st.spinner("에프앤가이드(FnGuide) 서버에서 데이터를 분석 중입니다..."):
                _fnguide_result = call_with_timeout(_do_fnguide_fetch, timeout=25)

            if _fnguide_result is None:
                st.error("⏱️ 데이터 조회가 너무 오래 걸려 중단했습니다. FnGuide/네이버 서버 응답이 느리거나 네트워크가 불안정한 것 같습니다. 잠시 후 다시 시도해주세요.")
                st.stop()

            st.session_state[cache_key] = _fnguide_result

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
        preset_choice = st.pills(
            "분할 비중 프리셋",
            list(_PRESETS.keys()),
            default=list(_PRESETS.keys())[0],
            key=f"preset_{code}",
            help="3차까지 하락이 왔을 때 실탄이 가장 많은 공격형(20:30:50)이 평균단가 절감 효과가 가장 큽니다."
        )
        if preset_choice is None:
            preset_choice = list(_PRESETS.keys())[0]

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
                help="실시간 현재가가 자동으로 입력됩니다. 이 값이 곧 '현재가' 기준으로 쓰여서, "
                     "직접 원하는 가격(예: 20만원 → 40만원)으로 바꾼 뒤 '전략 계산'을 누르면 "
                     "목표가·2/3차 진입가·손절가가 전부 그 값 기준으로 다시 계산됩니다.",
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

        # ── [버그 수정: 목표가 직접 입력 시 계산 카드 전체가 사라지던 문제] ──────────
        # st.button()은 "클릭된 바로 그 rerun"에서만 True이고, 그 다음 rerun(예: 아래
        # '목표가 직접 입력' 필드에 값을 넣고 엔터/포커스아웃 하는 순간)부터는 다시
        # False로 돌아간다. 그런데 계산 카드 전체가 `if calc_btn and ...`에만 기대고
        # 있었어서, 계산 결과를 본 뒤 목표가를 입력하면(=새 rerun 발생) 카드 자체가
        # 통째로 사라지는 것처럼 보였다(실제로는 아직 계산도 안 끝났는데 사라진 게
        # 아니라, 카드를 그리는 조건 자체가 다시 False가 된 것).
        # 해결: "계산 버튼을 눌렀었는지"를 세션 상태에 종목별로 기억해두고, 이후
        # rerun에서는 그 기억을 기준으로 카드를 계속 그린다. 진입가를 바꿔서 다시
        # 계산하고 싶으면 버튼을 다시 누르면 되고(그때 값이 최신 entry1_input으로
        # 갱신됨), 목표가 직접입력처럼 카드와 무관한 값을 바꾸는 것만으로는 카드가
        # 사라지지 않는다.
        _calc_shown_key = f"calc_shown_{code}"
        if calc_btn:
            st.session_state[_calc_shown_key] = True

        if st.session_state.get(_calc_shown_key) and entry1_input > 0:
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

            # ── [버그 수정: "1차 진입가"를 사용자가 바꿔도 PBR 기반 계산에는 반영 안 되던 문제] ──
            # PER 기반 목표가(target_price = e1 × 15/PER)는 이미 entry1_input(e1)을 그대로
            # 쓰고 있어서 사용자가 진입가를 20만→40만으로 바꾸면 즉시 반영됐다. 그런데
            # PBR 기반 경로(2·3차 진입가의 펀더멘털 근거, PBR 목표가, PBR 손절가, BPS 표시)는
            # 이 입력값 대신 _fetch_cur_price_for_fill()로 매번 새로 "실시간" 현재가를 다시
            # 가져와서 썼다. 그래서 PBR 방식으로 목표가가 뜨는 종목에서는 사용자가 "1차
            # 진입가"를 바꿔도 그 아래 계산들이 바뀌지 않는 것처럼 보였다.
            # 해결: e1(=entry1_input, 사용자가 직접 수정 가능한 값)을 "현재가" 기준으로
            # 통일해서 쓴다. 이러면 20만원을 40만원으로 바꿔 계산 버튼을 누르면, PER/PBR
            # 어느 경로든 그 값 기준으로 목표가·진입가·손절가가 전부 재계산된다.
            e1 = entry1_input
            _cur_price = e1

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

            _prob_html2 = render_hit_probability_badge(code, market_hint, target_price, target_src)
            if _prob_html2:
                st.markdown(_prob_html2, unsafe_allow_html=True)

            # ── [신규] 원하는 목표가를 직접 입력해서 도달 확률 확인 ──────────────────
            # 위 배지는 PER15×/PBR1.3×/컨센서스 등으로 "자동 계산된" 목표가 하나만
            # 보여준다. 하지만 "이 종목이 60만원 갈 확률은?"처럼 원하는 가격을 직접
            # 넣어보고 싶을 수 있다(예: 삼성SDI 현재가 49.5만원인데 60만원 도달 확률이
            # 궁금한 경우). 관심종목 탭에 이미 있는 "목표가 직접 입력" 패턴을 그대로
            # 가져와서, 여기 상세 분석 화면에서도 같은 방식으로 쓸 수 있게 한다.
            _custom_tgt_key = f"fnguide_custom_target_{code}"
            if _custom_tgt_key not in st.session_state:
                st.session_state[_custom_tgt_key] = ""

            def _fmt_custom_target(k=_custom_tgt_key):
                digits = re.sub(r"[^\d]", "", str(st.session_state.get(k, "")))
                st.session_state[k] = f"{int(digits):,}" if digits else ""

            st.markdown("<div style='margin-top:14px;'></div>", unsafe_allow_html=True)
            _ct1, _ct2, _ct3 = st.columns([1, 0.5, 1.8])
            with _ct1:
                st.text_input(
                    "🎯 원하는 목표가로 직접 확률 확인 (원)",
                    key=_custom_tgt_key,
                    on_change=_fmt_custom_target,
                    placeholder="예: 600,000",
                )
            with _ct2:
                st.markdown("<div style='padding-top:28px;'>", unsafe_allow_html=True)
                # ⚠️ [버그 수정: StreamlitAPIException] on_change처럼 위젯 생성 "이전"에
                # 실행되는 콜백이 아니라, 버튼 클릭 후 본문 코드에서 `_fmt_custom_target()`을
                # 직접 호출했더니 — 이미 위쪽에서 text_input(key=_custom_tgt_key)이 그 rerun에
                # 그려진 뒤라서 "위젯 생성 후에는 같은 키의 session_state를 직접 수정할 수
                # 없다"는 Streamlit 규칙에 걸려 예외가 났다(엔터=on_change는 위젯 재생성
                # 전에 실행되어 허용되지만, 버튼 클릭 후 본문에서의 수동 호출은 허용되지
                # 않음). on_click도 on_change와 동일하게 "위젯 재생성 전" 단계에서 실행되므로,
                # 버튼에 on_click으로 넘겨 같은 방식으로 처리하도록 수정.
                st.button(
                    "확인",
                    key=f"fnguide_custom_target_btn_{code}",
                    use_container_width=True,
                    on_click=_fmt_custom_target,
                )
                st.markdown("</div>", unsafe_allow_html=True)
            with _ct3:
                st.markdown(
                    "<div style='padding-top:28px; font-size:11px; color:#94A3B8;'>"
                    "원하는 가격을 입력하고 엔터 또는 '확인'을 누르면 30·90·180일 도달 확률을 바로 계산해 보여줍니다."
                    "</div>",
                    unsafe_allow_html=True,
                )

            _custom_digits = re.sub(r"[^\d]", "", str(st.session_state.get(_custom_tgt_key, "")))
            _custom_target = int(_custom_digits) if _custom_digits else 0
            if _custom_target > 0:
                _prob_html_custom = render_hit_probability_badge(code, market_hint, _custom_target, "직접 입력")
                if _prob_html_custom:
                    st.markdown(_prob_html_custom, unsafe_allow_html=True)


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

    col_refresh, col_scan, col_caption2, col_toggle = st.columns([1.5, 1.5, 4, 1.5])
    with col_refresh:
        if st.button("데이터 새로고침", use_container_width=True):
            fetch_dividend_ranking.clear()
            st.session_state["dividend_scanned"] = True
    with col_scan:
        if not st.session_state.get("dividend_scanned"):
            if st.button("🔍 배당 데이터 조회", key="dividend_manual_scan_btn", type="primary", use_container_width=True):
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
        st.info("💡 아직 배당 데이터를 조회하지 않았습니다. 위 [배당 데이터 조회] 버튼을 눌러 조회해주세요. (약 5~15초 소요)")
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