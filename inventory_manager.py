import os
import concurrent.futures
import random
import time
import datetime
import re
import io
import html as html_lib
import logging
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("inventory_manager")

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
from streamlit_option_menu import option_menu
import pandas as pd
import requests

# =========================
# ⚙️ 페이지 설정
# =========================
st.set_page_config(page_title="Inventory Manager", page_icon="📦", layout="wide")

# =========================
# 🕸️ 데이터 처리 엔진
# =========================
def normalize_kr_code(code):
    return re.sub(r"\D", "", str(code)).zfill(6)[:6]

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

@st.cache_data(ttl=60, show_spinner=False)
def fetch_market_index_table():
    targets = {
        "kospi":  {"symbol": "^KS11",  "name": "KOSPI",  "subtitle": "한국 코스피"},
        "kosdaq": {"symbol": "^KQ11",  "name": "KOSDAQ", "subtitle": "한국 코스닥"},
        "nasdaq": {"symbol": "^IXIC",  "name": "NASDAQ", "subtitle": "미국 나스닥"},
        "usdkrw": {"symbol": "KRW=X",  "name": "USD/KRW", "subtitle": "원/달러 환율"},
        "gold":   {"symbol": "GC=F",   "name": "Gold",    "subtitle": "금 선물"},
        "wti":    {"symbol": "CL=F",   "name": "WTI Crude", "subtitle": "서부텍사스산 원유"},
    }
    
    def get_data(key, meta):
        try:
            import yfinance as yf
            ticker = yf.Ticker(meta["symbol"])
            info = ticker.fast_info
            price = info.last_price
            prev = info.previous_close
            vol = getattr(info, "three_month_average_volume", None) or getattr(info, "regular_market_volume", None)
            
            entry = {
                "name": meta["name"], "subtitle": meta["subtitle"],
                "value": "-", "change": "-", "change_pct": "-",
                "status": "neutral", "volume": "-"
            }
            if price and prev:
                diff = price - prev
                diff_pct = diff / prev * 100
                sign = "+" if diff >= 0 else ""
                
                if key == "usdkrw": entry["value"] = f"{price:,.1f}"
                else: entry["value"] = f"{price:,.2f}"
                
                entry["change"] = f"{sign}{diff:,.2f}"
                entry["change_pct"] = f"{sign}{diff_pct:.2f}%"
                entry["status"] = "up" if diff > 0 else ("down" if diff < 0 else "neutral")
            
            if vol and key not in ["usdkrw"]: entry["volume"] = f"{int(vol):,}"
            else: entry["volume"] = "N/A"
            return key, entry
        except Exception as e:
            logger.warning(f"[fetch_market_index_table] '{key}'({meta['symbol']}) 조회 실패: {e}")
            return key, None

    result = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        futures = {executor.submit(get_data, k, m): k for k, m in targets.items()}
        for future in concurrent.futures.as_completed(futures):
            k, entry = future.result()
            if entry: result[k] = entry
            
    return {k: result.get(k, {"name": targets[k]["name"], "value": "-", "status": "neutral"}) for k in targets.keys()}

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
        except Exception as e:
            logger.warning(f"[fetch_sector_ranking] '{name}'({code}) 조회 실패: {e}")
        return None
        
    rows = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(get_data, n, c) for n, c in sector_etfs]
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res: rows.append(res)
            
    if not rows: return pd.DataFrame()
    return pd.DataFrame(rows).sort_values("등락률_num", ascending=False).reset_index(drop=True)

@st.cache_data(ttl=600, show_spinner=False)
def fetch_dividend_ranking():
    base_url = "https://finance.naver.com/sise/dividend_list.naver"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

    def fetch_page(page):
        try:
            res = requests.get(f"{base_url}?page={page}", headers=headers, timeout=10)
            res.encoding = 'euc-kr'
            dfs = pd.read_html(io.StringIO(res.text))
            for df in dfs:
                if any('종목명' in str(c) for c in df.columns.to_flat_index()):
                    page_df = df.dropna()
                    return page_df if not page_df.empty else None
        except Exception as e:
            logger.warning(f"[fetch_dividend_ranking] {page}페이지 조회 실패: {e}")
        return None

    try:
        # 1페이지로 전체 페이지 수 파악
        import re as _re
        res0 = requests.get(base_url, headers=headers, timeout=10)
        res0.encoding = 'euc-kr'
        page_nums = [int(p) for p in _re.findall(r'[?&]page=(\d+)', res0.text)]
        max_page = max(page_nums) if page_nums else 10
        max_page = min(max_page, 15)  # 안전 상한선 15페이지(450개)

        all_pages = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(fetch_page, p): p for p in range(1, max_page + 1)}
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result is not None:
                    all_pages.append(result)

        if not all_pages:
            return pd.DataFrame()
        result = pd.concat(all_pages, ignore_index=True)
        result = result.drop_duplicates()
        return result
    except Exception as e:
        logger.warning(f"[fetch_dividend_ranking] 전체 조회 실패: {e}")
        return pd.DataFrame()

def _parse_consensus_table(html_text):
    result = {"opinion": "", "opinion_score": "", "target": "", "analyst_count": ""}
    tbl_match = re.search(r'<th[^>]*>투자의견</th>.*?<tbody>(.*?)</tbody>', html_text, re.DOTALL)
    if not tbl_match:
        return result

    tbody = tbl_match.group(1)
    tds = re.findall(r'<td[^>]*>(.*?)</td>', tbody, re.DOTALL)
    tds_clean = [re.sub(r'<[^>]+>', '', td).strip().replace(',', '') for td in tds]
    if len(tds_clean) < 2:
        return result

    try:
        op_val = float(tds_clean[0])
        if op_val > 0:
            result["opinion_score"] = f"{op_val:.1f} / 5.0"
            if op_val >= 4.5:   result["opinion"] = "🔥 강력매수"
            elif op_val >= 3.5: result["opinion"] = "👍 매수"
            elif op_val >= 2.5: result["opinion"] = "✋ 중립"
            elif op_val >= 1.5: result["opinion"] = "👎 매도"
            else:               result["opinion"] = "💀 강력매도"
    except ValueError:
        pass

    tg_raw = re.sub(r'[^\d]', '', tds_clean[1])
    if tg_raw:
        result["target"] = f"{int(tg_raw):,} 원"

    if len(tds_clean) >= 5 and re.sub(r'[^\d]', '', tds_clean[4]):
        result["analyst_count"] = f"추정기관 {tds_clean[4]}곳"

    return result

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_company_info_fnguide(code):
    code = normalize_kr_code(code)
    data = {"name": "알 수 없음", "summary": "제공된 기업개요가 없습니다.", "opinion": "📭 분석의견 없음", "target": "데이터 없음", "opinion_score": "", "analyst_count": "", "consensus_note": ""}

    fn_headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://comp.fnguide.com/',
        'Accept-Language': 'ko-KR,ko;q=0.9',
    }
    naver_headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://finance.naver.com/',
    }

    try:
        fn_url = f"https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?pGB=1&gicode=A{code}"
        res = requests.get(fn_url, headers=fn_headers, timeout=8)
        res.encoding = res.apparent_encoding or 'utf-8'
        html = res.text
        name_match = re.search(r'id="giName"[^>]*>(.*?)</h1>', html)
        if name_match:
            name_text = re.sub(r'<[^>]+>', '', name_match.group(1))
            data["name"] = html_lib.unescape(name_text).strip()
        summary_match = re.search(r'id="bizSummaryContent"[^>]*>(.*?)</ul>', html, re.DOTALL)
        if summary_match:
            text = re.sub(r'<.*?>', ' ', summary_match.group(1))
            text = html_lib.unescape(text)
            text = re.sub(r'\s+', ' ', text).strip()
            if text:
                data["summary"] = text
    except Exception as e:
        logger.warning(f"[fetch_company_info_fnguide] {code} FnGuide 기업개요 조회 실패: {e}")

    if data["name"] == "알 수 없음":
        try:
            nv_url = f"https://finance.naver.com/item/main.naver?code={code}"
            res = requests.get(nv_url, headers=naver_headers, timeout=6)
            res.encoding = 'euc-kr'
            html = res.text
            name_match = re.search(r'<title>(.*?)\s*::', html)
            if name_match:
                data["name"] = html_lib.unescape(name_match.group(1)).strip()
            if data["summary"] == "제공된 기업개요가 없습니다.":
                summary_match = re.search(r'class="summary_info"[^>]*>(.*?)</p>', html, re.DOTALL)
                if summary_match:
                    text = re.sub(r'<[^>]+>', '', summary_match.group(1))
                    text = html_lib.unescape(text)
                    text = re.sub(r'\s+', ' ', text).strip()
                    if text:
                        data["summary"] = text
        except Exception as e:
            logger.warning(f"[fetch_company_info_fnguide] {code} 네이버 보조 조회 실패: {e}")

    consensus = {"opinion": "", "opinion_score": "", "target": "", "analyst_count": ""}
    fetch_failed = False
    try:
        fn_url2 = f"https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?pGB=1&gicode=A{code}"
        res2 = requests.get(fn_url2, headers=fn_headers, timeout=8)
        res2.encoding = res2.apparent_encoding or 'utf-8'
        consensus = _parse_consensus_table(res2.text)
    except Exception as e:
        logger.warning(f"[fetch_company_info_fnguide] {code} 컨센서스(PC) 조회 실패: {e}")
        fetch_failed = True

    if not consensus["opinion"] and not consensus["target"]:
        try:
            fn_mobile_url = f"https://m.comp.fnguide.com/m2/company_03.asp?pGB=1&gicode=A{code}"
            res3 = requests.get(fn_mobile_url, headers=fn_headers, timeout=8)
            res3.encoding = res3.apparent_encoding or 'utf-8'
            consensus_mobile = _parse_consensus_table(res3.text)
            if consensus_mobile["opinion"] or consensus_mobile["target"]:
                consensus = consensus_mobile
            fetch_failed = False
        except Exception as e:
            logger.warning(f"[fetch_company_info_fnguide] {code} 컨센서스(모바일) 조회 실패: {e}")

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
            data["consensus_note"] = "이 종목은 현재 분석을 진행하는 증권사가 없어 매수의견·목표주가 컨센서스가 제공되지 않습니다. 거래량이 적은 중·소형주에서 흔히 나타나는 정상적인 상황이며, 기업 자체 재무 데이터는 아래에서 계속 확인하실 수 있습니다."

    return data

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_fnguide_data(code):
    url = f"https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?pGB=1&gicode=A{normalize_kr_code(code)}"
    df_annual, df_quarter = pd.DataFrame(), pd.DataFrame()
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        res = requests.get(url, headers=headers, timeout=10)
        dfs = pd.read_html(io.StringIO(res.text))
        
        target_df = None
        for df in dfs:
            if df.empty or len(df.columns) < 2: continue
            first_col = df.iloc[:, 0].astype(str)
            if not first_col.str.contains('매출액').any(): continue
            col_str = "".join([str(c) for c in df.columns.tolist()])
            if '202' not in col_str and '201' not in col_str and '203' not in col_str: continue
            if 'KOSPI' in col_str or '코스피' in col_str: continue
            target_df = df.copy()
            break
            
        if target_df is None: return df_annual, df_quarter

        df = target_df
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(-1)

        df.rename(columns={df.columns[0]: "재무항목"}, inplace=True)
        df = df.set_index('재무항목')
        df = df.loc[:, ~df.columns.astype(str).str.contains('전년동기', na=False)]
        df = df.T 

        core_items = ['매출액', '영업이익', '당기순이익', '영업이익률', '순이익률', 'ROE', 'PER', 'PBR', '부채비율']
        available = [item for item in core_items if item in df.columns]
        df = df[available]

        df.index = df.index.astype(str).str.replace(r'\(.*?\)', '', regex=True).str.strip()
        df.index.name = "연도/분기"

        df_a = df.iloc[:4].copy()
        df_q = df.iloc[4:].copy()

        def calc_growth(target_df, is_quarter=False):
            if target_df.empty: return target_df
            label = '성장률(QoQ)' if is_quarter else '성장률(YoY)'
            for col in ['매출액', '영업이익']:
                if col in target_df.columns:
                    temp = pd.to_numeric(target_df[col].astype(str).str.replace(',', ''), errors='coerce')
                    target_df[f'{col} {label}'] = temp.pct_change() * 100

            final_cols = []
            for item in available:
                final_cols.append(item)
                if f'{item} {label}' in target_df.columns:
                    final_cols.append(f'{item} {label}')
            return target_df[final_cols].reset_index()

        df_annual = calc_growth(df_a, is_quarter=False)
        df_quarter = calc_growth(df_q, is_quarter=True)

    except Exception as e:
        logger.warning(f"[fetch_fnguide_data] {code} 재무데이터 파싱 실패: {e}")
    return df_annual, df_quarter

def fetch_page_data(sosok, page, headers, cookies):
    time.sleep(random.uniform(0.1, 0.4))
    url = f"https://finance.naver.com/sise/sise_market_sum.naver?sosok={sosok}&page={page}"
    try:
        res = requests.get(url, headers=headers, cookies=cookies, timeout=10)
        res.encoding = 'euc-kr'
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
    except Exception as e:
        logger.warning(f"[fetch_page_data] sosok={sosok} page={page} 조회 실패: {e}")
        return None

def fetch_screener_data_generator():
    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://finance.naver.com/sise/sise_market_sum.naver",
    }
    yield "보안 세션 접속 및 쿠키 발급 중...", 5
    
    session.get("https://finance.naver.com/sise/sise_market_sum.naver", headers=headers)
    time.sleep(0.5)
    
    field_url = "https://finance.naver.com/sise/field_submit.naver?menu=market_sum&returnUrl=https%3A%2F%2Ffinance.naver.com%2Fsise%2Fsise_market_sum.naver&fieldIds=per&fieldIds=pbr&fieldIds=roe&fieldIds=dividend&fieldIds=property_total&fieldIds=debt_total&fieldIds=high52"
    session.get(field_url, headers=headers)
    cookies = session.cookies.get_dict()
    
    all_data = []
    urls = [(sosok, page) for sosok in [0, 1] for page in range(1, 45)]
    total_pages = len(urls)
    completed = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        future_to_url = {executor.submit(fetch_page_data, s, p, headers, cookies): (s, p) for s, p in urls}
        for future in concurrent.futures.as_completed(future_to_url):
            completed += 1
            progress_pct = 10 + int((completed / total_pages) * 80)
            yield f"⚡ 스텔스 모드 스캔 중... ({completed}/{total_pages} 페이지)", progress_pct
            df = future.result()
            if df is not None and not df.empty:
                all_data.append(df)
            
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

HIGH52_PATH = "saved_high52_data.csv"

def merge_high52(df):
    if not os.path.exists(HIGH52_PATH): return df
    try:
        h = pd.read_csv(HIGH52_PATH, dtype={'종목코드': str})
        h['종목코드'] = h['종목코드'].str.replace('.0','', regex=False).str.zfill(6)
        for c in ['52주고점', '고점대비(%)']:
            if c in df.columns: df = df.drop(columns=[c])
        df = df.merge(h[['종목코드', '52주고점', '고점대비(%)']], on='종목코드', how='left')
    except Exception as e:
        logger.warning(f"[merge_high52] {HIGH52_PATH} 병합 실패: {e}")
    return df

def load_screener_df():
    save_path = "saved_screener_data.csv"
    if 'shared_screener_df' in st.session_state and not st.session_state['shared_screener_df'].empty:
        df = st.session_state['shared_screener_df']
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
            st.session_state['shared_screener_df'] = df
            return df
        except Exception as e:
            logger.warning(f"[load_screener_df] {save_path} 로드 실패: {e}")
            return pd.DataFrame()
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
    except Exception as e:
        logger.warning(f"[load_high52_map] {HIGH52_PATH} 로드 실패: {e}")
        return {}

def check_naver_52w_robust(row_dict):
    code = str(row_dict['종목코드']).replace('.0','').zfill(6)
    mkt = row_dict.get('시장', '코스피')
    price, high = 0.0, 0.0

    high52_map = load_high52_map()
    if high52_map:
        saved_high = high52_map.get(code, 0.0)
        if saved_high > 0:
            high = saved_high
            price = float(str(row_dict.get('현재가', 0)).replace(',', ''))
            if price <= 0:
                try:
                    time.sleep(random.uniform(0.05, 0.15))
                    url = f"https://m.stock.naver.com/api/stock/{code}/basic"
                    res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=3)
                    if res.status_code == 200:
                        price_str = res.json().get('closePrice', '0')
                        price = float(str(price_str).replace(',', ''))
                except Exception as e:
                    logger.debug(f"[check_naver_52w_robust] {code} 현재가 보정 조회 실패: {e}")
            if price <= 0:
                # 현재가를 끝내 구하지 못한 경우 high로 대체해 "고점 대비 0%"로 위장시키지 않고 제외함
                return None

            if price > 0 and high > 0:
                drop_pct = ((price - high) / high) * 100
                if drop_pct <= 0.0:
                    return {
                        "종목명": row_dict['종목명'], "종목코드": code, "시장": mkt,
                        "현재가_num": price, "52주최고": high, "고점 / 하락률": drop_pct,
                        "PER": row_dict['PER'], "PBR": row_dict['PBR'], "ROE": row_dict['ROE'],
                        "부채비율": row_dict['부채비율'], "배당수익률": row_dict.get('배당수익률', 0.0),
                        "데이터출처": "📂 CSV"
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
            price = float(str(price_str).replace(',', ''))
            high = float(str(high_str).replace(',', ''))
    except Exception as e:
        logger.debug(f"[check_naver_52w_robust] {code} 모바일 API 조회 실패: {e}")

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
        except Exception as e:
            logger.debug(f"[check_naver_52w_robust] {code} PC페이지 폴백 조회 실패: {e}")

    # 실시간 조회가 모두 실패하면, 스크리너 시점에 이미 확보된 현재가만 보수적으로 사용.
    # (이전에는 그래도 안되면 50000원으로 임의 고정 → 가짜 "고점 대비 0%" 추천이 섞여 나오는 버그가 있었음)
    if price <= 0:
        price = float(str(row_dict.get('현재가', 0)).replace(',', ''))

    if price <= 0 or high <= 0:
        return None

    if price > 0 and high > 0:
        drop_pct = ((price - high) / high) * 100
        if drop_pct <= 0.0:
            return {
                "종목명": row_dict['종목명'], "종목코드": code, "시장": mkt,
                "현재가_num": price, "52주최고": high, "고점 / 하락률": drop_pct,
                "PER": row_dict['PER'], "PBR": row_dict['PBR'], "ROE": row_dict['ROE'],
                "부채비율": row_dict['부채비율'], "배당수익률": row_dict.get('배당수익률', 0.0),
                "데이터출처": "🌐 실시간"
            }
    return None

def find_col(df: pd.DataFrame, candidates: list) -> Optional[str]:
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

def draw_fnguide_details(code):
    info = fetch_company_info_fnguide(code)
    df_annual, df_quarter = fetch_fnguide_data(code)

    if info['name'] != "알 수 없음" or not df_annual.empty:
        opinion_score_html = f'<span style="color: #94A3B8; font-size: 12px; margin-left: 8px;">({info["opinion_score"]})</span>' if info['opinion_score'] else ''
        analyst_count_html = f'<span style="color: #94A3B8; font-size: 12px; margin-left: 8px;">({info["analyst_count"]})</span>' if info['analyst_count'] else ''
        consensus_note_html = f'<div style="background-color:#FFFBEB; border:1px solid #FDE68A; border-radius:6px; padding:10px 14px; margin-bottom:20px; font-size:12.5px; color:#92400E; line-height:1.6;">ℹ️ {info["consensus_note"]}</div>' if info.get("consensus_note") else ''

        st.markdown(f"""
            <div style="background-color: #F8FAFC; border: 1px solid #E2E8F0; border-radius: 8px; padding: 25px; margin-top: 10px; margin-bottom: 20px;">
                <h3 style="margin-top: 0; color: #0F172A; font-size: 20px;">{info['name']} <span style="font-size: 14px; color: #64748B;">({code})</span></h3>
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

        if not df_annual.empty:
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
            tab1, tab2 = st.tabs(["📅 연간 실적 (YoY 흐름)", "📈 분기 실적 (QoQ 흐름)"])
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
                    df_quarter_display = df_quarter.iloc[::-1].reset_index(drop=True)
                    st.dataframe(format_and_style(df_quarter_display), use_container_width=True, hide_index=True)
                else:
                    st.info("해당 기업의 분기 실적 데이터가 제공되지 않습니다.")

            st.markdown("""
                <div style='background-color: #F9FAFB; padding: 15px; border-radius: 8px; margin-top: 15px; font-size: 13px; color: #6B7280;'>
                    💡 <b>알림:</b> 성장률은 직전 연도/분기 대비 증감률입니다. (🔺초록색: 실적 상승 / 🔻빨간색: 실적 하락 및 적자)<br>
                    ⚠️ <b>잠정 표기된 연도</b>는 결산이 완료되지 않아 일부 기간 데이터만 반영된 수치입니다. 성장률은 신뢰도가 낮아 표시하지 않습니다.
                </div>
            """, unsafe_allow_html=True)
    else:
        st.error("해당 종목의 기업 및 재무 정보를 찾을 수 없습니다.")

# =========================
# 🎨 메인 UI 및 사이드바 설정
# =========================
def main():
    st.markdown("""
        <style>
            .stApp { background-color: #F8FAFC !important; }
            section[data-testid="stMain"] > div.block-container { 
                background-color: #FFFFFF !important; border-radius: 12px; 
                box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05); padding: 40px !important; 
                margin-top: 30px; margin-bottom: 30px; max-width: 1400px; border: 1px solid #E5E7EB; 
            }
            
            [data-testid="stSidebar"] { background-color: #0F141F !important; border-right: none !important; }
            [data-testid="stSidebar"] [data-testid="stSidebarUserContent"] {
                padding-left: 0 !important; padding-right: 0 !important; padding-top: 30px !important; padding-bottom: 0 !important;
            }
            [data-testid="stSidebar"] * { color: #8A93A2 !important; }
            .sidebar-logo-text { color: #FFFFFF !important; font-size: 22px !important; font-weight: 900 !important; padding: 0px 25px 30px 25px !important; letter-spacing: -0.5px !important; display: block;}
            
            section[data-testid="stMain"] h1, section[data-testid="stMain"] h2, section[data-testid="stMain"] p { color: #111827 !important; }
            .stTextInput input, .stNumberInput input, .stSelectbox > div > div { background-color: #FFFFFF !important; color: #111827 !important; border: 1px solid #D1D5DB !important; border-radius: 6px !important; }
            
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

            /* 💡 stRadio 위젯 자체 및 상위 래퍼의 잔여 마진/패딩을 모두 제거하여
               다른 요소(버튼, info-box 등)와 좌측 시작선을 완전히 일치시킴.
               구버전(.element-container)과 신버전(stElementContainer) DOM 둘 다 대응 */
            div[data-testid="stRadio"],
            div[data-testid="stRadio"] > div,
            .element-container:has(div[data-testid="stRadio"]),
            div[data-testid="stElementContainer"]:has(div[data-testid="stRadio"]) {
                width: 100% !important;
                max-width: none !important;
                margin: 0 !important;
                padding: 0 !important;
                left: 0 !important;
                position: relative !important;
            }
            div[data-testid="stRadio"] > label[data-testid="stWidgetLabel"] {
                display: none !important; /* collapsed label이 차지하는 잔여 공간 제거 */
            }

            /* 💡 [궁극의 UI 교정] 라디오 그룹을 flex로 변경하여 똑같은 비율로 균등 분할
               (grid보다 BaseWeb 원본 레이아웃 방식과 호환성이 높아 잔여 오프셋 위험이 적음) */
            div[data-testid="stRadio"] > div[role="radiogroup"] {
                display: flex !important;
                flex-direction: row !important;
                flex-wrap: nowrap !important;
                gap: 10px !important;
                width: 100% !important;
                margin: 0 !important;
                margin-left: 0 !important;
                padding-left: 0 !important;
                background-color: transparent !important;
                border: none !important;
                padding: 0 !important;
                align-items: stretch !important; /* 모든 박스 높이를 동일한 트랙 높이로 정렬 */
                justify-content: flex-start !important;
            }

            /* radiogroup 직속 자식(각 라디오 옵션 wrapper)에 남아있을 수 있는
               BaseWeb 기본 margin/padding을 모두 제거 — 좌측 정렬 오프셋 원인 차단 */
            div[data-testid="stRadio"] > div[role="radiogroup"] > * {
                margin: 0 !important;
                min-width: 0 !important;
                flex: 1 1 0% !important; /* 🔥 동일한 너비로 균등 분할 (grid의 1fr과 동일한 효과) */
            }
            div[data-testid="stRadio"] > div[role="radiogroup"] > *:first-child {
                margin-left: 0 !important;
            }

            /* 라디오 탭 내부 블록 디자인 */
            div[data-testid="stRadio"] label[data-baseweb="radio"] {
                background-color: rgba(248, 250, 252, 0.7) !important;
                backdrop-filter: blur(4px) !important;
                border: none !important; /* 🔥 border는 outline으로 대체 → grid 셀 크기 계산에 영향 없음 */
                outline: 2px solid #CBD5E1 !important;
                outline-offset: -2px !important; /* outline이 박스 안쪽에 그려지도록 보정 */
                border-radius: 8px !important;
                padding: 10px 5px !important; 
                margin: 0 !important;
                cursor: pointer !important;
                transition: background-color 0.2s ease-in-out, outline-color 0.2s ease-in-out, box-shadow 0.2s ease-in-out !important;
                display: flex !important;
                flex-direction: row !important;
                justify-content: center !important;
                align-items: center !important;
                width: 100% !important;
                min-height: 44px !important; /* 🔥 모든 박스 높이를 동일하게 고정 */
                box-sizing: border-box !important;
                box-shadow: none !important; /* 기본 그림자 없음 → 선택 시에만 부여 */
            }

            div[data-testid="stRadio"] label[data-baseweb="radio"]:hover {
                background-color: rgba(226, 232, 240, 0.8) !important;
            }

            /* ✅ 클릭 시 (선택된 상태) 파란색 활성화 효과 — outline 두께는 그대로, 색상만 변경 */
            div[data-testid="stRadio"] label[data-baseweb="radio"]:has(input[type="radio"]:checked) {
                background-color: #EEF2FF !important; /* 연한 파란색 배경 */
                outline-color: #6366F1 !important; /* 파란색 테두리 (두께 변화 없음) */
                box-shadow: 0 4px 10px rgba(99, 102, 241, 0.15) !important; /* 입체적인 파란 그림자 */
            }

            /* 🚫 기존 기본 동그라미 아이콘 완벽 숨김 (텍스트 정렬에 영향 없도록 너비 0 처리) */
            div[data-testid="stRadio"] label[data-baseweb="radio"] > div:first-child {
                display: none !important;
                width: 0 !important;
                margin: 0 !important;
            }

            /* 텍스트 컨테이너 정렬 강제 (남은 영역을 가운데 정렬) */
            div[data-testid="stRadio"] label[data-baseweb="radio"] > div:last-child {
                flex: 1 1 auto !important;
                width: 100% !important;
                text-align: center !important;
                display: flex !important;
                justify-content: center !important;
                align-items: center !important;
            }

            /* 텍스트 컬러 및 사이즈 */
            div[data-testid="stRadio"] label[data-baseweb="radio"] p {
                color: #475569 !important;
                font-size: 14.5px !important;
                font-weight: 600 !important;
                margin: 0 !important;
                line-height: 1.3 !important;
                white-space: nowrap !important; /* 🔥 글씨 무조건 한 줄 고정 */
                overflow: hidden !important;
                text-overflow: ellipsis !important; /* 박스를 넘어가면 ... 처리 */
                text-align: center !important;
            }

            /* 텍스트 컬러 (선택됨) */
            div[data-testid="stRadio"] label[data-baseweb="radio"]:has(input[type="radio"]:checked) p {
                color: #4338CA !important; /* 진한 파란색 텍스트 */
                font-weight: 800 !important;
            }

            /* 시장 필터(전체·코스피·코스닥)도 텍스트 길이와 무관하게 완전히 균등한 너비로 분할.
               좁은 컬럼이라 너무 넓어지지 않도록 그룹 자체의 최대 폭만 제한 */
            .st-key-market_filter_box div[data-testid="stRadio"] > div[role="radiogroup"] {
                justify-content: flex-start !important;
                max-width: 320px !important; /* 라디오 그룹 전체 폭 제한 (검색창과 균형 유지) */
            }
            .st-key-market_filter_box div[data-testid="stRadio"] > div[role="radiogroup"] > * {
                flex: 1 1 0% !important; /* 🔥 텍스트 길이 무관, 3칸 완전 균등 분할 */
                min-width: 0 !important;
                max-width: none !important;
            }
            .st-key-market_filter_box div[data-testid="stRadio"] label[data-baseweb="radio"] {
                min-height: 40px !important;
                padding: 8px 10px !important;
            }

            /* ── 대시보드 카드 스타일 ── */
            .dash-section-title { font-size: 16px; font-weight: 700; color: #0F172A; margin: 18px 0 14px 0; letter-spacing: -0.3px; }
            .section-divider { border: none; border-top: 1px solid #E5E7EB; margin: 28px 0; }

            .index-card { background: #FFFFFF; border: 1px solid #E2E8F0; border-radius: 12px; padding: 20px 22px; min-height: 110px; box-shadow: 0 1px 4px rgba(0,0,0,0.04); transition: box-shadow 0.2s; }
            .index-card:hover { box-shadow: 0 4px 12px rgba(0,0,0,0.08); }
            .index-card-title { font-size: 13px; font-weight: 700; color: #1E293B; margin-bottom: 6px; letter-spacing: -0.2px; }
            .index-card-value { font-size: 26px; font-weight: 800; color: #0F172A; margin-bottom: 6px; letter-spacing: -0.5px; }
            .index-card-up   { font-size: 13px; font-weight: 600; color: #DC2626; margin-bottom: 6px; }
            .index-card-down { font-size: 13px; font-weight: 600; color: #2563EB; margin-bottom: 6px; }
            .index-card-neutral { font-size: 13px; font-weight: 600; color: #64748B; margin-bottom: 6px; }
            .index-card-sub  { font-size: 12px; color: #94A3B8; }
        </style>

        <div class="header-container">
            <button class="btn-template-white">회원가입</button>
            <button class="btn-template-blue">로그인</button>
        </div>
    """, unsafe_allow_html=True)

    with st.sidebar:
        st.markdown('<span class="sidebar-logo-text">Inventory Manager</span>', unsafe_allow_html=True)
        st.markdown('<div style="color: #64748B; font-size: 12px; font-weight: 600; padding: 10px 25px 5px 25px; margin-top: 10px;">MAIN MENU</div>', unsafe_allow_html=True)
        
        selected = option_menu(
            menu_title=None,
            options=["대시보드 홈", "추천 종목", "종목 스크리너", "기업 재무 분석", "실시간 배당 순위"],
            icons=["grid-1x2", "bullseye", "sliders", "bar-chart-line", "cash-coin"],
            default_index=0,
            styles={
                "container": { "padding": "0!important", "background-color": "transparent!important", "margin": "0", "border-radius": "0"},
                "icon": {"font-size": "17px", "margin-right": "12px", "color": "inherit"},
                "nav-link": { "font-size": "15px", "color": "#8A93A2", "padding": "14px 25px", "margin": "0", "border-radius": "0", "text-align": "left", "--hover-color": "#1A202C", "display": "flex", "align-items": "center"},
                "nav-link-selected": { "background-color": "#5A4EE5", "color": "#FFFFFF", "font-weight": "600" }
            }
        )

    if   selected == "대시보드 홈":      render_dashboard()
    elif selected == "추천 종목":        render_recommendations()
    elif selected == "종목 스크리너":    render_screener()
    elif selected == "기업 재무 분석":   render_fnguide()
    elif selected == "실시간 배당 순위": render_dividend()

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

    col_refresh, _ = st.columns([1, 5])
    with col_refresh:
        if st.button("데이터 새로고침"):
            fetch_market_index_table.clear()
            fetch_sector_ranking.clear()
            st.rerun()

    st.markdown("<div class='dash-section-title'>📈 시장 지수</div>", unsafe_allow_html=True)
    indices = run_with_progress("시장 지수를 불러오는 중...", fetch_market_index_table)

    def index_color_class(status):
        if status == "up":      return "index-card-up"
        if status == "down":    return "index-card-down"
        return "index-card-neutral"
    def index_arrow(status):
        if status == "up":   return "▲"
        if status == "down": return "▼"
        return "–"

    c1, c2, c3 = st.columns(3)
    cards1 = [(c1, indices.get("kospi",  {})), (c2, indices.get("kosdaq", {})), (c3, indices.get("nasdaq", {}))]
    for col, idx in cards1:
        label    = idx.get("name", "-")
        subtitle = idx.get("subtitle", "")
        status = idx.get("status", "neutral")
        arrow  = index_arrow(status)
        chg    = idx.get("change", "-")
        chgpct = idx.get("change_pct", "")
        vol    = idx.get("volume", "-")
        color_cls = index_color_class(status)
        with col:
            st.markdown(f"""
            <div class="index-card">
                <div class="index-card-title">{label} <span style="font-size:11px; color:#94A3B8;">({subtitle})</span></div>
                <div class="index-card-value">{idx.get('value', '-')}</div>
                <div class="{color_cls}">{arrow} {chg} &nbsp; {chgpct}</div>
                <div class="index-card-sub">거래량 {vol}</div>
            </div>
            """, unsafe_allow_html=True)
            
    st.markdown("<br>", unsafe_allow_html=True)

    c4, c5, c6 = st.columns(3)
    cards2 = [(c4, indices.get("usdkrw", {})), (c5, indices.get("gold", {})), (c6, indices.get("wti", {}))]
    for col, idx in cards2:
        label    = idx.get("name", "-")
        subtitle = idx.get("subtitle", "")
        status = idx.get("status", "neutral")
        arrow  = index_arrow(status)
        chg    = idx.get("change", "-")
        chgpct = idx.get("change_pct", "")
        color_cls = index_color_class(status)
        with col:
            st.markdown(f"""
            <div class="index-card" style="background:#FFFFFF;">
                <div class="index-card-title">{label} <span style="font-size:11px; color:#94A3B8;">({subtitle})</span></div>
                <div class="index-card-value">{idx.get('value', '-')}</div>
                <div class="{color_cls}">{arrow} {chg} &nbsp; {chgpct}</div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("<hr class='section-divider'>", unsafe_allow_html=True)
    st.markdown("<div class='dash-section-title'>🔥 오늘의 핫 섹터 TOP 10</div>", unsafe_allow_html=True)
    df_sector = run_with_progress("업종 데이터를 분석 중...", fetch_sector_ranking)

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
            시장 지수 및 업종 테마는 <b>네이버 금융</b> 실시간 데이터를 기반으로 합니다. 시장 개장 시간(09:00~15:30) 외에는 전일 종가 기준으로 표시될 수 있습니다.
        </div>
    """, unsafe_allow_html=True)

# =========================
# 🤖 AI 종목 진단 엔진
# =========================
def calc_ai_scores(per, pbr, roe, debt, drop_pct, div):
    """재무 데이터 기반 4개 영역 점수 계산 (0~100점) — 타이트 기준"""

    # 1. 재무 건전성 (부채비율 중심 — 빡빡 기준)
    if debt <= 30:        health = 100
    elif debt <= 60:      health = 85
    elif debt <= 100:     health = 68
    elif debt <= 150:     health = 50
    elif debt <= 200:     health = 33
    elif debt <= 300:     health = 18
    else:                 health = 5

    # PBR 보정 (+/- 8점) — 순자산 대비 고평가 여부 반영
    if pbr <= 0.4:    health = min(100, health + 8)
    elif pbr <= 0.8:  health = min(100, health + 4)
    elif pbr >= 2.5:  health = max(0,   health - 8)
    elif pbr >= 1.5:  health = max(0,   health - 4)

    # 2. 성장성 (ROE 기준 — 15% 이상부터 의미있는 점수)
    if roe >= 25:     growth = 100
    elif roe >= 20:   growth = 88
    elif roe >= 15:   growth = 73
    elif roe >= 10:   growth = 52
    elif roe >= 5:    growth = 32
    elif roe >= 0:    growth = 15
    else:             growth = 3

    # 52주 하락률 타이밍 보정 (+/- 12점)
    if drop_pct <= -40:   growth = min(100, growth + 12)
    elif drop_pct <= -30: growth = min(100, growth + 8)
    elif drop_pct <= -20: growth = min(100, growth + 4)
    elif drop_pct <= -10: growth = min(100, growth + 1)
    elif drop_pct >= 0:   growth = max(0,   growth - 8)

    # 3. 수익성 (PER 중심 — 한국 평균 PER 10~12배 감안해 타이트하게)
    if per <= 4:       profit = 100
    elif per <= 6:     profit = 88
    elif per <= 8:     profit = 73
    elif per <= 10:    profit = 58
    elif per <= 13:    profit = 43
    elif per <= 18:    profit = 28
    elif per <= 25:    profit = 15
    else:              profit = 5

    # ROE 보정 (+/- 10점)
    if roe >= 20:    profit = min(100, profit + 10)
    elif roe >= 15:  profit = min(100, profit + 6)
    elif roe >= 10:  profit = min(100, profit + 2)
    elif roe < 5:    profit = max(0,   profit - 10)

    # 4. 배당 매력 (3% 이상부터 진짜 매력 구간)
    if div >= 6.0:        dividend = 100
    elif div >= 4.0:      dividend = 82
    elif div >= 3.0:      dividend = 65
    elif div >= 2.0:      dividend = 45
    elif div >= 1.0:      dividend = 25
    elif div >= 0.1:      dividend = 10
    else:                 dividend = 0

    # 종합 점수 (가중 평균: 건전성 30%, 수익성 30%, 성장 25%, 배당 15%)
    total = int(health * 0.30 + profit * 0.30 + growth * 0.25 + dividend * 0.15)
    total = max(0, min(100, total))

    return {
        "total": total,
        "health": int(health),
        "growth": int(growth),
        "profit": int(profit),
        "dividend": int(dividend),
    }

def render_ai_diagnosis(name, code, per, pbr, roe, debt, drop_pct, div, grade_label):
    """AI 종합 점수 UI 렌더링 (점수 계산만, API 호출 없음)"""
    scores = calc_ai_scores(per, pbr, roe, debt, drop_pct, div)
    total = scores["total"]

    # 총점 색상
    if total >= 85:   total_color = "#7C3AED"; total_label = "최우량"
    elif total >= 70: total_color = "#2563EB"; total_label = "우량"
    elif total >= 55: total_color = "#16A34A"; total_label = "양호"
    elif total >= 40: total_color = "#D97706"; total_label = "보통"
    else:             total_color = "#DC2626"; total_label = "주의"

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
        '<div style="font-size:14px; font-weight:700; color:#0F172A;">AI 종합 점수</div>'
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

def render_recommendations():
    st.header("추천 종목")
    st.markdown("<hr style='margin: 10px 0 15px 0; border-color: #E5E7EB;'>", unsafe_allow_html=True)

    st.markdown("""
        <div style='background:#F8FAFC; border:1px solid #E2E8F0; border-radius:8px; padding:20px; margin-bottom:20px;'>
            <h4 style='margin-top:0; font-size:16px; color:#0F172A;'>🎯 퀀트 스코어링 추천 엔진 (사용자 맞춤 등급제)</h4>
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
        </div>
    """, unsafe_allow_html=True)

    screener_df = load_screener_df()
    if screener_df.empty:
        st.info("⚠️ '종목 스크리너' 탭에서 [실시간 데이터 ⚡초고속 스캔 실행] 버튼을 눌러 데이터를 먼저 불러와주세요! (최초 1회 필수)")
        return

    high52_map = load_high52_map()
    if high52_map:
        st.success(f"✅ 스크리너 CSV 고점 데이터 사용 중 — {len(high52_map):,}종목 로드됨 (네이버 개별 호출 최소화)")
        scan_label = "🚀 퀀트 스캔 실행 (CSV 고점 데이터 활용)"
        scan_workers = 8   
    else:
        st.info("ℹ️ 스크리너 탭 → [52주 고점 데이터 업데이트]에서 KRX CSV를 업로드하면 더 빠르고 정확해집니다. (현재: 네이버 실시간 API 사용)")
        scan_label = "🚀 퀀트 스캔 실행 (네이버 실시간 API)"
        scan_workers = 5

    btn_scan = st.button(scan_label, use_container_width=True)

    if btn_scan:
        load_high52_map.clear()  
        high52_map = load_high52_map()

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
            st.warning("현재 시장 데이터 기준, 최소 요건(D급)을 통과한 종목조차 없습니다. 스크리너 데이터를 갱신해주세요.")
        else:
            val_df = val_df.sort_values('ROE', ascending=False).head(150)
            
            rows = []
            dict_records = val_df.to_dict('records')
            total = len(dict_records)

            if high52_map:
                progress_text = "⚡ CSV 고점 데이터 매칭 중..."
            else:
                progress_text = "⚡ 네이버 실시간 API 스캔 중..."

            pb = st.progress(0, text=progress_text)
            completed = 0
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=scan_workers) as executor:
                futures = {executor.submit(check_naver_52w_robust, r): r for r in dict_records}
                for future in concurrent.futures.as_completed(futures):
                    completed += 1
                    pb.progress(int((completed/total)*100), text=f"{progress_text} ({completed}/{total})")
                    res = future.result()
                    if res: rows.append(res)
                
            pb.progress(100, text="✨ 추천 종목 발굴 완료!")
            time.sleep(0.5)
            pb.empty()
            
            if rows:
                st.session_state['reco_raw_data'] = pd.DataFrame(rows)
            else:
                st.warning("분석 결과 고점 대비 유의미하게 하락한 종목이 없습니다.")

    if 'reco_raw_data' in st.session_state and not st.session_state['reco_raw_data'].empty:
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

        display_df = st.session_state['reco_raw_data'].copy()
        display_df['등급'] = display_df.apply(lambda row: assign_grade(row, strict_debt), axis=1)
        display_df = display_df.dropna(subset=['등급']) 

        if market_filter != "전체":
            display_df = display_df[display_df['시장'].str.contains(market_filter)]

        if selected_grade != "전체보기":
            grade_key = selected_grade.split()[1] 
            display_df = display_df[display_df['등급'].str.contains(grade_key)]

        display_df = display_df.sort_values('고점 / 하락률', ascending=True).reset_index(drop=True)

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

                entry_2nd = price * 0.85
                entry_3rd = price * 0.70
                
                if "S급" in grade_label: bg_color = "#EEF2FF"
                elif "A급" in grade_label: bg_color = "#F0FDF4"
                elif "B급" in grade_label: bg_color = "#FEFCE8"
                elif "C급" in grade_label: bg_color = "#FFF5F5"
                else: bg_color = "#F8FAFC"

                st.markdown(f"""
                <div style="background:{bg_color}; border:1px solid #E2E8F0; border-radius:8px; padding:16px 20px; margin-bottom:5px; margin-top:12px;">
                    <div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:8px; margin-bottom:12px;">
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
                    <div style="display:flex; gap:18px; font-size:12px; color:#64748B; margin-bottom:12px; flex-wrap:wrap;">
                        <span>PER <b style="color:#1E293B;">{per:.2f}배</b></span>
                        <span>PBR <b style="color:#1E293B;">{pbr:.2f}배</b></span>
                        <span>ROE <b style="color:#1E293B;">{roe:.1f}%</b></span>
                        <span>부채비율 <b style="color:#1E293B;">{debt:.1f}%</b></span>
                        <span>배당수익률 <b style="color:#DC2626;">{div:.1f}%</b></span>
                    </div>
                    <div style="display:flex; gap:10px;">
                        <div style="flex:1; background:#FFFFFF; border:1px solid #E2E8F0; border-radius:6px; padding:8px 4px; text-align:center;">
                            <div style="font-size:11px; color:#94A3B8; margin-bottom:2px;">1차 진입 (비중 25%)</div>
                            <div style="font-size:13px; font-weight:700; color:#5A4EE5;">{int(price):,}</div>
                        </div>
                        <div style="flex:1; background:#FFFFFF; border:1px solid #E2E8F0; border-radius:6px; padding:8px 4px; text-align:center;">
                            <div style="font-size:11px; color:#94A3B8; margin-bottom:2px;">2차 진입 (-15% / 35%)</div>
                            <div style="font-size:13px; font-weight:700; color:#1E293B;">{int(entry_2nd):,}</div>
                        </div>
                        <div style="flex:1; background:#FFFFFF; border:1px solid #E2E8F0; border-radius:6px; padding:8px 4px; text-align:center;">
                            <div style="font-size:11px; color:#94A3B8; margin-bottom:2px;">3차 진입 (-30% / 40%)</div>
                            <div style="font-size:13px; font-weight:700; color:#1E293B;">{int(entry_3rd):,}</div>
                        </div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
                
                with st.expander(f"{name} · AI 진단 · 재무분석"):
                    # ── AI 종목 진단 ──
                    render_ai_diagnosis(name, code, per, pbr, roe, debt, drop_pct, div, grade_label)

                    st.markdown("<hr style='margin:16px 0 12px 0; border-color:#E5E7EB;'>", unsafe_allow_html=True)

                    # ── 기존 FnGuide 재무 분석 ──
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
    st.header("종목 스크리너")
    st.markdown("<hr style='margin: 10px 0 15px 0; border-color: #E5E7EB;'>", unsafe_allow_html=True)

    # 탭 디자인 부분
    preset_names = list(SCREENER_PRESETS.keys())
    selected_preset = st.radio("필터 단계", preset_names, horizontal=True, key="screener_preset", label_visibility="collapsed")
    preset = SCREENER_PRESETS[selected_preset]

    if preset:
        st.markdown(f"<div style='font-size:13px; color:#5A4EE5; margin: 6px 0 14px 0;'>💡 {preset['desc']}</div>", unsafe_allow_html=True)
        max_per = preset['per']; max_pbr = preset['pbr']
        min_div = preset['div']; min_roe = preset['roe']
        max_debt = preset['debt']; use_div = preset['use_div']
    else:
        # 💡 [핵심 UI 수정] 직접 설정일 때 검색창은 분리하고 숫자 5개만 황금비율로 배치!
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
        pb = st.progress(0, text="데이터 검색 준비 중...")
        try:
            temp_df = pd.DataFrame()
            for status_msg, pct in fetch_screener_data_generator():
                if isinstance(status_msg, str): pb.progress(pct, text=f"{status_msg}")
                else: temp_df = status_msg
            pb.progress(100, text="분석 완료!")
            time.sleep(0.5)
            pb.empty()
            
            if not temp_df.empty:
                temp_df.to_csv(save_path, index=False, encoding='utf-8-sig')
                st.session_state['shared_screener_df'] = temp_df
        except Exception as e:
            pb.empty()
            st.error(f"데이터를 가져오는데 실패했습니다: {e}")

    with st.expander("📈 52주 고점 데이터 업데이트"):
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
                    st.error(f"[{market_label}] 종목코드 또는 최고가 컬럼을 찾을 수 없습니다. (감지된 컬럼: {list(h_df.columns)})")
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
                    matched = int(mask_h.sum())
                    markets_done = " + ".join(maps.keys())
                    st.success(f"✅ 52주 고점 매칭 완료! ({markets_done}) {matched}종목 업데이트되었습니다. 추천 종목 탭에서 재스캔 시 새 데이터가 바로 적용됩니다.")

    df = load_screener_df()

    if not df.empty:
        st.markdown("<hr style='margin: 30px 0 20px 0; border-color: #E5E7EB;'>", unsafe_allow_html=True)

        ETF_KEYWORDS = 'TIGER|KODEX|ARIRANG|KBSTAR|HANARO|KOSEF|TREX|ACE|SOL|RISE|ETF|인버스|레버리지|선물|리츠|REIT|인덱스|TR$'
        df = df[~df['종목명'].str.contains(ETF_KEYWORDS, regex=True, case=False, na=False)]

        # 💡 [UI 수정] 검색창을 설정 탭 안이 아닌, 하단 결과표 컨트롤러 영역으로 독립 배치!
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

def render_fnguide():
    st.header("기업 재무 분석")
    st.markdown("<hr style='margin: 10px 0 25px 0; border-color: #E5E7EB;'>", unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 1, 4])
    with col1:
        code = st.text_input("종목코드 6자리 입력", placeholder="예: 005930", label_visibility="collapsed")
    with col2:
        search_btn = st.button("🔍 조회", use_container_width=True)

    if search_btn and code:
        code = normalize_kr_code(code)
        with st.spinner("에프앤가이드(FnGuide) 서버에서 데이터를 분석 중입니다..."):
            draw_fnguide_details(code)

def render_dividend():
    st.header("실시간 배당 순위")
    st.markdown("<hr style='margin: 10px 0 25px 0; border-color: #E5E7EB;'>", unsafe_allow_html=True)

    # ── 1행: 검색창 + 조회 ──
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

    # ── 2행: 새로고침(좌) | 캡션(중) | 정상 종목만 보기(우 끝) ──
    col_refresh, col_caption2, col_toggle = st.columns([1.5, 5, 1.5])
    with col_refresh:
        if st.button("데이터 새로고침"):
            fetch_dividend_ranking.clear()
    with col_toggle:
        st.markdown("<div style='display:flex; justify-content:flex-end; align-items:center; padding-top:4px; width:100%;'>", unsafe_allow_html=True)
        st.toggle(
            "정상만 보기",
            value=True,
            key="dividend_clean_filter",
            help="리츠(REITs) / 배당성향 100% 초과 / 배당수익률 30% 초과 종목을 제외합니다."
        )
        st.markdown("</div>", unsafe_allow_html=True)

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

        # ── 정상 종목 필터 적용 ──
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

        # ── 검색 필터 적용 ──
        if search_text:
            name_col = find_col(df, ["종목명"])
            code_col = find_col(df, ["종목코드", "코드"])
            mask = pd.Series([False] * len(df), index=df.index)
            if name_col:
                mask = mask | df[name_col].astype(str).str.contains(search_text, case=False, na=False)
            if code_col:
                mask = mask | df[code_col].astype(str).str.contains(search_text, case=False, na=False)
            df = df[mask]

        # ── 캡션: 2행 중앙에 표시 ──
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