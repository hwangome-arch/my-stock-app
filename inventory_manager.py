import os
import concurrent.futures
import random
import time
import datetime
import re
import io

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
        except: return key, None

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
        except: pass
        return None
        
    rows = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(get_data, n, c) for n, c in sector_etfs]
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res: rows.append(res)
            
    if not rows: return pd.DataFrame()
    return pd.DataFrame(rows).sort_values("등락률_num", ascending=False).reset_index(drop=True)

@st.cache_data(ttl=180, show_spinner=False)
def fetch_hot_stocks():
    watchlist = [
        ("삼성전자", "005930"), ("SK하이닉스", "000660"), ("현대차", "005380"), ("기아", "000270"),
        ("KB금융", "105560"), ("신한지주", "055550"), ("하나금융지주", "086790"), ("메리츠금융지주", "138040"),
        ("삼성바이오로직스","207940"), ("셀트리온", "068270"), ("NAVER", "035420"), ("카카오", "035720"),
        ("LG에너지솔루션", "373220"), ("삼성SDI", "006400"), ("LG화학", "051910"), ("POSCO홀딩스", "005490"),
        ("두산에너빌리티", "034020"), ("한미반도체", "042700"), ("한화에어로스페이스", "012450"), ("LIG넥스원", "079550"),
        ("에코프로비엠", "247540"), ("에코프로", "086520"), ("알테오젠", "196170"), ("HLB", "028300"),
        ("엔켐", "348370"), ("리가켐바이오", "141080"), ("휴젤", "145020"), ("클래시스", "214150"),
        ("HPSP", "403870"), ("삼천당제약", "000250"), ("리노공업", "058470"), ("셀트리온제약", "068760"),
        ("레인보우로보틱스", "277810"), ("실리콘투", "257720"), ("이오테크닉스", "039030"), ("펄어비스", "263750")
    ]
    def get_data(name, code):
        try:
            time.sleep(random.uniform(0.1, 0.3))
            url = f"https://m.stock.naver.com/api/stock/{code}/basic"
            res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=5)
            data = res.json()
            
            price = float(str(data.get('closePrice', '0')).replace(',', ''))
            pct = float(str(data.get('fluctuationsRatio', '0')).replace(',', ''))
            
            high_val = data.get('high52WeekPrice') or data.get('high52Week') or '0'
            high = float(str(high_val).replace(',', ''))
            
            mkt_info = data.get('stockExchangeType', {}).get('name', '코스피')
            mkt = "코스닥" if "코스닥" in str(mkt_info) else "코스피"

            if price > 0:
                return {
                    "종목명": name, "종목코드": code, "현재가_num": price, "등락률_num": pct,
                    "52주최고": high if high > 0 else price, "시장": mkt
                }
        except: pass
        return None
        
    rows = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(get_data, n, c) for n, c in watchlist]
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res: rows.append(res)
            
    if not rows: return pd.DataFrame()
    return pd.DataFrame(rows).sort_values("등락률_num", ascending=False).reset_index(drop=True)

@st.cache_data(ttl=600, show_spinner=False)
def fetch_dividend_ranking():
    url = "https://finance.naver.com/sise/dividend_list.naver"
    try:
        res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
        res.encoding = 'euc-kr'
        dfs = pd.read_html(io.StringIO(res.text))
        for df in dfs:
            if any('종목명' in str(c) for c in df.columns): return df.dropna().head(30)
        return pd.DataFrame()
    except: return pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_company_info_fnguide(code):
    url = f"https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?pGB=1&gicode=A{normalize_kr_code(code)}"
    data = {"name": "알 수 없음", "summary": "제공된 기업개요가 없습니다.", "opinion": "평가 없음", "target": "- 원"}
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        res = requests.get(url, headers=headers, timeout=5)
        html = res.text
        name_match = re.search(r'id="giName"[^>]*>(.*?)</h1>', html)
        if name_match: data["name"] = re.sub(r'<[^>]+>', '', name_match.group(1)).strip()
        summary_match = re.search(r'id="bizSummaryContent"[^>]*>(.*?)</ul>', html, re.DOTALL)
        if summary_match:
            text = re.sub(r'<.*?>', ' ', summary_match.group(1))
            text = re.sub(r'\s+', ' ', text).strip()
            if text: data["summary"] = text
        consensus_match = re.search(r'투자의견\s*/\s*목표주가.*?<td>(.*?)</td>', html, re.DOTALL)
        if consensus_match:
            con_raw = re.sub(r'<[^>]+>', '', consensus_match.group(1)).strip()
            con_text = re.sub(r'\s+', ' ', con_raw)
            if '/' in con_text:
                parts = con_text.split('/')
                op = parts[0].strip().upper()
                tg = parts[1].strip().replace(',', '').replace(' ', '')
            else:
                op = con_text.strip().upper()
                tg = ''
                tg_match = re.search(r'목표주가.*?<td[^>]*>(.*?)</td>', html, re.DOTALL)
                if tg_match: tg = re.sub(r'<[^>]+>', '', tg_match.group(1)).strip().replace(',', '')
            if op and op not in ('-', ''):
                if 'STRONG BUY' in op: data["opinion"] = "🔥 강력매수"
                elif 'BUY' in op: data["opinion"] = "👍 매수"
                elif 'HOLD' in op or '중립' in op: data["opinion"] = "✋ 중립"
                elif 'SELL' in op or '매도' in op: data["opinion"] = "👎 매도"
                elif '매수' in op: data["opinion"] = "👍 매수"
                else: data["opinion"] = op
            if tg and tg not in ('-', ''):
                tg_num = re.sub(r'[^\d]', '', tg)
                if tg_num: data["target"] = f"{int(tg_num):,} 원"
    except Exception: pass
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

    except Exception: pass
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
    except Exception:
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
    
    field_url = "https://finance.naver.com/sise/field_submit.naver?menu=market_sum&returnUrl=https%3A%2F%2Ffinance.naver.com%2Fsise%2Fsise_market_sum.naver&fieldIds=per&fieldIds=pbr&fieldIds=roe&fieldIds=dividend&fieldIds=property_total&fieldIds=debt_total"
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
        
    price_c = get_col(final_df, ['현재가'])
    div_c   = get_col(final_df, ['주당배당금', '배당금'])
    per_c   = get_col(final_df, ['PER', 'PER(배)'])
    pbr_c   = get_col(final_df, ['PBR', 'PBR(배)'])
    roe_c   = get_col(final_df, ['ROE', 'ROE(%)']) 
    prop_c  = get_col(final_df, ['자산총계']) 
    debt_c  = get_col(final_df, ['부채총계']) 
    mkt_c   = get_col(final_df, ['시장'])
    
    final_df['현재가'] = pd.to_numeric(final_df[price_c].astype(str).str.replace(',', ''), errors='coerce').fillna(0.0) if price_c else 0.0
    final_df['주당배당금'] = pd.to_numeric(final_df[div_c].astype(str).str.replace(',', ''), errors='coerce').fillna(0.0) if div_c else 0.0
    final_df['자산총계'] = pd.to_numeric(final_df[prop_c].astype(str).str.replace(',', ''), errors='coerce').fillna(0.0) if prop_c else 0.0
    final_df['부채총계'] = pd.to_numeric(final_df[debt_c].astype(str).str.replace(',', ''), errors='coerce').fillna(0.0) if debt_c else 0.0
    
    final_df['배당수익률'] = 0.0
    mask_div = (final_df['현재가'] > 0) & (final_df['주당배당금'] > 0)
    final_df.loc[mask_div, '배당수익률'] = (final_df.loc[mask_div, '주당배당금'] / final_df.loc[mask_div, '현재가']) * 100
    
    final_df['부채비율'] = 0.0
    final_df['자본총계'] = final_df['자산총계'] - final_df['부채총계']
    mask_debt = (final_df['자본총계'] > 0) & (final_df['부채총계'] >= 0)
    final_df.loc[mask_debt, '부채비율'] = (final_df.loc[mask_debt, '부채총계'] / final_df.loc[mask_debt, '자본총계']) * 100
    
    final_df['PER'] = pd.to_numeric(final_df[per_c].astype(str).str.replace(',', ''), errors='coerce').fillna(0.0) if per_c else 0.0
    final_df['PBR'] = pd.to_numeric(final_df[pbr_c].astype(str).str.replace(',', ''), errors='coerce').fillna(0.0) if pbr_c else 0.0
    final_df['ROE'] = pd.to_numeric(final_df[roe_c].astype(str).str.replace(',', ''), errors='coerce').fillna(0.0) if roe_c else 0.0
    final_df['시장'] = final_df[mkt_c] if mkt_c else "코스피"
    
    final_df = final_df[['종목코드', '종목명', '시장', '현재가', 'PER', 'PBR', '배당수익률', 'ROE', '부채비율']]
    yield final_df, 100

@st.cache_data(ttl=3600*12, show_spinner=False)
def fetch_and_cache_screener_data():
    final_df = None
    for item, _ in fetch_screener_data_generator():
        if isinstance(item, pd.DataFrame): final_df = item
    return final_df

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
            st.session_state['shared_screener_df'] = df
            return df
        except: return pd.DataFrame()
    return pd.DataFrame()

def check_naver_52w_robust(row_dict):
    code = str(row_dict['종목코드']).replace('.0','').zfill(6)
    mkt = row_dict.get('시장', '코스피')
    price, high = 0.0, 0.0
    
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
    except: pass

    if price <= 0 or high <= 0:
        try:
            import yfinance as yf
            suffix = ".KS" if mkt == "코스피" else ".KQ"
            info = yf.Ticker(f"{code}{suffix}").fast_info
            price = info.last_price
            high = getattr(info, "year_high", None)
        except: pass
        
    if not price or price <= 0:
        price = float(row_dict.get('현재가', 0))
        if price <= 0: price = 50000.0 
        
    if not high or high <= 0:
        high = price * 1.15  
        
    if price > 0 and high > 0:
        drop_pct = ((price - high) / high) * 100
        
        if drop_pct <= 0.0:
            return {
                "종목명": row_dict['종목명'], "종목코드": code, "시장": mkt,
                "현재가_num": price, "52주최고": high, "고점대비하락률": drop_pct,
                "PER": row_dict['PER'], "PBR": row_dict['PBR'], "ROE": row_dict['ROE'], 
                "부채비율": row_dict['부채비율'], "배당수익률": row_dict.get('배당수익률', 0.0)
            }
    return None

def draw_fnguide_details(code):
    info = fetch_company_info_fnguide(code)
    df_annual, df_quarter = fetch_fnguide_data(code)
    
    if info['name'] != "알 수 없음" or not df_annual.empty:
        st.markdown(f"""
            <div style="background-color: #F8FAFC; border: 1px solid #E2E8F0; border-radius: 8px; padding: 25px; margin-top: 10px; margin-bottom: 20px;">
                <h3 style="margin-top: 0; color: #0F172A; font-size: 20px;">{info['name']} <span style="font-size: 14px; color: #64748B;">({code})</span></h3>
                <div style="display: flex; gap: 20px; margin-bottom: 20px;">
                    <div style="background-color: #FFFFFF; padding: 12px 20px; border-radius: 6px; border: 1px solid #E2E8F0; font-weight: 600;">
                        <span style="color: #64748B; font-size: 12px; display: block; margin-bottom: 4px;">투자의견 (FnGuide)</span>
                        <span style="color: #5A4EE5; font-size: 16px;">{info['opinion']}</span>
                    </div>
                    <div style="background-color: #FFFFFF; padding: 12px 20px; border-radius: 6px; border: 1px solid #E2E8F0; font-weight: 600;">
                        <span style="color: #64748B; font-size: 12px; display: block; margin-bottom: 4px;">목표주가 컨센서스</span>
                        <span style="color: #0F172A; font-size: 16px;">{info['target']}</span>
                    </div>
                </div>
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
                    elif col_name in ['영업이익률', '순이익률', 'ROE', '부채비율']: return f"{f_val:.2f}%"
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
                st.dataframe(format_and_style(df_annual_display), width='stretch', hide_index=True)
            with tab2:
                if not df_quarter.empty:
                    df_quarter_display = df_quarter.iloc[::-1].reset_index(drop=True)
                    st.dataframe(format_and_style(df_quarter_display), width='stretch', hide_index=True)
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
            div[data-testid="stFileUploader"] section { background-color: #F9FAFB !important; border: 1px dashed #D1D5DB !important; color: #111827 !important;}
            div[data-testid="stFileUploader"] * { color: #111827 !important; }
            
            .header-container { display: flex; justify-content: flex-end; gap: 10px; margin-bottom: 30px; }
            .btn-template-white { padding: 8px 16px; border-radius: 6px; font-size: 13px; font-weight: 600; cursor: pointer; border: 1px solid #D1D5DB !important; background-color: #FFFFFF !important; color: #111827 !important; }
            .btn-template-blue { padding: 8px 16px; border-radius: 6px; font-size: 13px; font-weight: 600; cursor: pointer; border: none !important; background-color: #5A4EE5 !important; color: #FFFFFF !important; }
            
            div[data-testid="stExpander"] { border: 1px solid #E5E7EB !important; border-radius: 8px !important; background-color: #F9FAFB !important; }
            div[data-testid="stExpander"] summary p { color: #374151 !important; font-weight: 600 !important; }
            
            div[data-testid="stTabs"] button { font-weight: 600 !important; color: #64748B !important; font-size: 16px !important; padding-bottom: 15px !important;}
            div[data-testid="stTabs"] button[aria-selected="true"] { color: #5A4EE5 !important; border-bottom-color: #5A4EE5 !important; }

            .index-card { background: #F8FAFC !important; border: 1px solid #E2E8F0 !important; border-radius: 10px !important; padding: 20px 24px !important; }
            .index-card-title { font-size: 13px; color: #64748B !important; font-weight: 600; margin-bottom: 6px; }
            .index-card-value { font-size: 28px; font-weight: 700; color: #0F172A !important; }
            .index-card-up   { font-size: 14px; color: #DC2626 !important; font-weight: 600; margin-top: 4px; }
            .index-card-down { font-size: 14px; color: #16A34A !important; font-weight: 600; margin-top: 4px; }
            .index-card-neutral { font-size: 14px; color: #64748B !important; font-weight: 600; margin-top: 4px; }
            .index-card-sub  { font-size: 12px; color: #94A3B8 !important; margin-top: 2px; }
            .section-divider { border: none; border-top: 1px solid #E5E7EB !important; margin: 28px 0 22px 0; }
            .dash-section-title { font-size: 15px; font-weight: 700; color: #1E293B !important; margin-bottom: 14px; display: flex; align-items: center; gap: 6px; }

            div[role="radiogroup"] { display: flex; background-color: #F3F4F6 !important; padding: 4px; border-radius: 8px; gap: 4px; width: max-content; }
            div[role="radiogroup"] label { padding: 8px 16px !important; background-color: transparent !important; border-radius: 6px; cursor: pointer; transition: all 0.2s ease; border: none !important; }
            div[role="radiogroup"] label:hover { background-color: #E5E7EB !important; }
            div[role="radiogroup"] label[data-checked="true"] { background-color: #FFFFFF !important; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
            div[role="radiogroup"] label[data-checked="true"] p { color: #5A4EE5 !important; font-weight: 600 !important; }
            div[role="radiogroup"] label div[data-testid="stMarkdownContainer"] { margin-left: 0 !important; }
            div[role="radiogroup"] label span[data-baseweb="radio"] { display: none !important; }
            
            div[data-testid="stCheckbox"] label { color: #374151 !important; font-weight: 500 !important; }
        </style>

        <div class="header-container">
            <button class="btn-template-white">회원가입</button>
            <button class="btn-template-blue">로그인</button>
        </div>
    """, unsafe_allow_html=True)

    with st.sidebar:
        st.markdown('<span class="sidebar-logo-text">Inventory Manager</span>', unsafe_allow_html=True)
        
        # 💡 [UI 개선] 사이드바 메뉴 상단에 그룹 라벨 추가
        st.markdown('<div style="color: #64748B; font-size: 12px; font-weight: 600; padding: 10px 25px 5px 25px; margin-top: 10px;">MAIN MENU</div>', unsafe_allow_html=True)
        
        selected = option_menu(
            menu_title=None,
            options=["대시보드 홈", "추천 종목", "저평가 우량주 발굴", "종목 스크리너", "기업 재무 분석", "실시간 배당 순위"],
            # 💡 [UI 개선] 메뉴별 직관적인 Bootstrap 아이콘 매핑
            icons=["grid-1x2", "bullseye", "gem", "sliders", "bar-chart-line", "cash-coin"],
            default_index=1,
            styles={
                "container": { "padding": "0!important", "background-color": "transparent!important", "margin": "0", "border-radius": "0"},
                "icon": {"font-size": "17px", "margin-right": "12px", "color": "inherit"},
                "nav-link": { "font-size": "15px", "color": "#8A93A2", "padding": "14px 25px", "margin": "0", "border-radius": "0", "text-align": "left", "--hover-color": "#1A202C", "display": "flex", "align-items": "center"},
                "nav-link-selected": { "background-color": "#5A4EE5", "color": "#FFFFFF", "font-weight": "600" }
            }
        )

    if   selected == "대시보드 홈":      render_dashboard()
    elif selected == "추천 종목":        render_recommendations()
    elif selected == "저평가 우량주 발굴": render_undervalued()
    elif selected == "종목 스크리너":    render_screener()
    elif selected == "기업 재무 분석":   render_fnguide()
    elif selected == "실시간 배당 순위": render_dividend()

# =========================
# 🌟 [공통 기능] 표 열 속성 정의
# =========================
def get_table_col_config(include_debt=False):
    config = {
        "종목코드": st.column_config.TextColumn("📋종목코드 (클릭 후 복사)"),
        "PER": st.column_config.NumberColumn("PER", format="%.2f 배"),
        "PBR": st.column_config.NumberColumn("PBR", format="%.2f 배"),
        "배당수익률": st.column_config.NumberColumn("배당수익률", format="%.2f %%"),
        "ROE": st.column_config.NumberColumn("ROE", format="%.2f %%"),
        "시장": st.column_config.TextColumn("시장")
    }
    if include_debt:
        config["부채비율"] = st.column_config.NumberColumn("부채비율", format="%.2f %%")
    return config

# =========================
# 🖥️ 기능 구현 함수
# =========================
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
            fetch_hot_stocks.clear()
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
    col_sector, col_stocks = st.columns([1, 1], gap="large")

    with col_sector:
        st.markdown("<div class='dash-section-title'>🔥 오늘의 핫 섹터 TOP 10</div>", unsafe_allow_html=True)
        df_sector = run_with_progress("업종 데이터를 분석 중...", fetch_sector_ranking)

        if not df_sector.empty:
            max_abs = df_sector["등락률_num"].abs().max() or 1
            for _, row in df_sector.iterrows():
                pct   = row["등락률_num"]
                name  = row["업종명"]
                bar_w = int(abs(pct) / max_abs * 100)
                bar_color = "#DC2626" if pct >= 0 else "#16A34A"
                sign  = "+" if pct >= 0 else ""
                pct_disp = f"{sign}{pct:.2f}%"
                st.markdown(f"""
                <div style="display:flex; align-items:center; gap:10px; margin-bottom:8px;">
                    <span style="font-size:13px; color:#1E293B; min-width:110px; font-weight:500;">{name}</span>
                    <div style="flex:1; background:#F1F5F9; border-radius:4px; height:8px;">
                        <div style="width:{bar_w}%; background:{bar_color}; border-radius:4px; height:8px;"></div>
                    </div>
                    <span style="font-size:13px; font-weight:700; color:{bar_color}; min-width:60px; text-align:right;">{pct_disp}</span>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.info("업종 데이터를 불러올 수 없습니다.")

    with col_stocks:
        st.markdown("<div class='dash-section-title'>관심 종목 모니터링</div>", unsafe_allow_html=True)
        df_hot = run_with_progress("관심 종목을 탐색 중...", fetch_hot_stocks)

        tab_up, tab_down, tab_52w = st.tabs(["🚀 급등 종목", "📉 급락 종목", "🎯 52주 고점 대비"])

        if not df_hot.empty:
            def render_stock_list(df_subset):
                for _, row in df_subset.iterrows():
                    name     = row["종목명"]
                    code     = str(row.get("종목코드", "")).zfill(6) if pd.notna(row.get("종목코드")) else "-"
                    price    = row.get("현재가_num", None)
                    pct      = row.get("등락률_num", 0)
                    price_str = f"{int(price):,}원" if pd.notna(price) and price else "-"
                    sign      = "+" if pct > 0 else ""
                    pct_str   = f"{sign}{pct:.2f}%"
                    bar_color = "#DC2626" if pct >= 0 else "#16A34A"
                    bg_color  = "#FEF2F2" if pct >= 0 else "#F0FDF4"
                    
                    st.markdown(f"""
                    <div style="display:flex; align-items:center; justify-content:space-between; padding:9px 0; border-bottom:1px solid #F1F5F9;">
                        <div>
                            <span style="font-size:14px; font-weight:600; color:#0F172A;">{name}</span>
                            <span style="font-size:11px; color:#94A3B8; margin-left:6px;">{code}</span>
                        </div>
                        <div style="text-align:right;">
                            <span style="font-size:14px; font-weight:600; color:#0F172A;">{price_str}</span>
                            <span style="font-size:13px; font-weight:700; color:{bar_color}; background:{bg_color}; padding:2px 8px; border-radius:12px; margin-left:8px;">{pct_str}</span>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
            
            with tab_up:
                df_up = df_hot[df_hot['등락률_num'] >= 0].head(8)
                if not df_up.empty: render_stock_list(df_up)
                else: st.info("현재 상승 중인 종목이 없습니다.")
                
            with tab_down:
                df_down = df_hot[df_hot['등락률_num'] < 0].sort_values("등락률_num", ascending=True).head(8)
                if not df_down.empty: render_stock_list(df_down)
                else: st.info("현재 하락 중인 종목이 없습니다.")
                
            with tab_52w:
                st.markdown("<p style='font-size:12px; color:#64748B; margin-bottom:5px;'>현재가가 52주 최고가 대비 얼마나 하락했는지 보여줍니다.</p>", unsafe_allow_html=True)
                mkt_filter_52w = st.radio("시장 선택", ["코스피", "코스닥"], horizontal=True, label_visibility="collapsed", key="dash_52w")
                
                df_52w = df_hot.copy()
                if '52주최고' in df_52w.columns and '시장' in df_52w.columns:
                    df_52w = df_52w[df_52w['시장'] == mkt_filter_52w]
                    df_52w['고점대비하락률'] = ((df_52w['현재가_num'] - df_52w['52주최고']) / df_52w['52주최고']) * 100
                    df_52w = df_52w.sort_values("고점대비하락률", ascending=True).head(30)
                    
                    if not df_52w.empty:
                        for _, row in df_52w.iterrows():
                            name = row["종목명"]
                            price = row["현재가_num"]
                            drop_pct = row["고점대비하락률"]
                            st.markdown(f"""
                            <div style="display:flex; align-items:center; justify-content:space-between; padding:9px 0; border-bottom:1px solid #F1F5F9;">
                                <div>
                                    <span style="font-size:14px; font-weight:600; color:#0F172A;">{name}</span>
                                </div>
                                <div style="text-align:right;">
                                    <span style="font-size:14px; font-weight:600; color:#0F172A;">{int(price):,}원</span>
                                    <span style="font-size:13px; font-weight:700; color:#16A34A; background:#F0FDF4; padding:2px 8px; border-radius:12px; margin-left:8px;">고점대비 {drop_pct:.1f}%</span>
                                </div>
                            </div>
                            """, unsafe_allow_html=True)
                    else:
                        st.info(f"선택하신 {mkt_filter_52w} 종목 데이터가 없습니다.")
                else:
                    st.info("52주 데이터를 불러오지 못했습니다.")
        else:
            st.info("서버가 응답하지 않아 관심 종목 데이터를 불러올 수 없습니다. 잠시 후 새로고침 해주세요.")

    st.markdown("<hr class='section-divider'>", unsafe_allow_html=True)
    st.markdown("""
        <div style='background:#F0F9FF; border:1px solid #BAE6FD; border-radius:8px; padding:14px 20px; font-size:13px; color:#374151; line-height:1.7;'>
            💡 <b>데이터 안내</b> &nbsp;|&nbsp;
            시장 지수·업종·급등 종목은 <b>네이버 금융</b> 실시간 데이터를 기반으로 합니다. 시장 개장 시간(09:00~15:30) 외에는 전일 종가 기준으로 표시될 수 있습니다.
        </div>
    """, unsafe_allow_html=True)

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
                    <td style='padding:6px; font-weight:600; color:#F59E0B;'>🥉 C급 성장기대주</td><td>25 이하</td><td>2.5 이하</td><td>5% 이상</td><td>-</td><td>200% 이하</td><td>-5% 이하</td>
                </tr>
                <tr style='border-bottom:1px solid #E2E8F0;'>
                    <td style='padding:6px; font-weight:600; color:#10B981;'>🥈 B급 적정가치주</td><td>15 이하</td><td>1.5 이하</td><td>8% 이상</td><td>-</td><td>150% 이하</td><td>-10% 이하</td>
                </tr>
                <tr style='border-bottom:1px solid #E2E8F0;'>
                    <td style='padding:6px; font-weight:600; color:#3B82F6;'>🥇 A급 우량가치주</td><td>12 이하</td><td>1.2 이하</td><td>10% 이상</td><td>1.5% 이상</td><td>120% 이하</td><td>-15% 이하</td>
                </tr>
                <tr style='background-color:#EEF2FF;'>
                    <td style='padding:6px; font-weight:600; color:#7C3AED;'>💎 S급 초저평가 (고배당)</td><td style='color:#7C3AED;'>8 이하</td><td style='color:#7C3AED;'>0.8 이하</td><td style='color:#7C3AED;'>12% 이상</td><td style='color:#7C3AED;'>3.0% 이상</td><td style='color:#7C3AED;'>100% 이하</td><td style='color:#7C3AED;'>-20% 이하</td>
                </tr>
            </table>
        </div>
    """, unsafe_allow_html=True)

    screener_df = load_screener_df()
    if screener_df.empty:
        st.info("⚠️ '종목 스크리너' 탭에서 [실시간 데이터 ⚡초고속 스캔 실행] 버튼을 눌러 데이터를 먼저 불러와주세요! (최초 1회 필수)")
        return

    btn_scan = st.button("🚀 1단계: 전체 시장 딥 스캔 실행 (최초 1회만 누르세요)", use_container_width=True)

    if btn_scan:
        df = screener_df.copy()
        finance_keywords = '금융|은행|증권|보험|캐피탈|지주|투자|저축'
        
        cond = (
            (df['PER'] > 0) & (df['PER'] <= 30) & 
            (df['PBR'] > 0) & (df['PBR'] <= 3.0) &
            (df['ROE'] >= 5) &
            (df['부채비율'] >= 0) & (df['부채비율'] <= 250) &
            (~df['종목명'].astype(str).str.contains(finance_keywords, regex=True, na=False))
        )
        val_df = df[cond].copy()
        
        if val_df.empty:
            st.error("기본 펀더멘털을 만족하는 종목이 없습니다. 스크리너를 다시 실행해 주세요.")
        else:
            val_df = val_df.sort_values('ROE', ascending=False).head(150) 
            
            rows = []
            dict_records = val_df.to_dict('records')
            total = len(dict_records)
            pb = st.progress(0, text="가치주 52주 고점 데이터 병렬 분석 중...")
            completed = 0
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = {executor.submit(check_naver_52w_robust, r): r for r in dict_records}
                for future in concurrent.futures.as_completed(futures):
                    completed += 1
                    pb.progress(int((completed/total)*100), text=f"⚡ 펀더멘털 통과 종목 초고속 스캔 중... ({completed}/{total})")
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
            strict_debt = st.toggle("☑️ 부채비율 '엄격 기준' 적용 (권장)", value=True, help="해제 시 모든 등급의 부채비율 허들을 200%로 완화하여 더 많은 종목을 탐색합니다.")

        st.markdown("<br>", unsafe_allow_html=True)
        selected_grade = st.radio(
            "등급 필터", 
            ["전체보기", "💎 S급", "🥇 A급", "🥈 B급", "🥉 C급"], 
            horizontal=True, 
            label_visibility="collapsed"
        )

        def assign_grade(row, is_strict):
            per, pbr, roe, debt, drop, div = row['PER'], row['PBR'], row['ROE'], row['부채비율'], row['고점대비하락률'], row['배당수익률']
            
            s_debt = 100 if is_strict else 200
            a_debt = 120 if is_strict else 200
            b_debt = 150 if is_strict else 200
            c_debt = 200
            
            if per <= 8 and pbr <= 0.8 and roe >= 12 and debt <= s_debt and drop <= -20.0 and div >= 3.0:
                return "💎 S급 초저평가 (고배당)"
            elif per <= 12 and pbr <= 1.2 and roe >= 10 and debt <= a_debt and drop <= -15.0 and div >= 1.5:
                return "🥇 A급 우량가치주"
            elif per <= 15 and pbr <= 1.5 and roe >= 8 and debt <= b_debt and drop <= -10.0:
                return "🥈 B급 적정가치주"
            elif per <= 25 and pbr <= 2.5 and roe >= 5 and debt <= c_debt and drop <= -5.0:
                return "🥉 C급 성장기대주"
            return None

        display_df = st.session_state['reco_raw_data'].copy()
        display_df['등급'] = display_df.apply(lambda row: assign_grade(row, strict_debt), axis=1)
        display_df = display_df.dropna(subset=['등급']) 

        if market_filter != "전체":
            display_df = display_df[display_df['시장'].str.contains(market_filter)]

        if selected_grade != "전체보기":
            grade_key = selected_grade.split()[1] 
            display_df = display_df[display_df['등급'].str.contains(grade_key)]

        display_df = display_df.sort_values('고점대비하락률', ascending=True).reset_index(drop=True)

        if display_df.empty:
            st.info(f"현재 설정된 필터({market_filter}, {selected_grade})에 부합하는 종목이 없습니다. 조건을 완화해보세요.")
        else:
            st.markdown(f"<div style='margin-bottom:15px; font-weight:600; color:#0F172A;'>검색 결과: 총 {len(display_df)} 종목</div>", unsafe_allow_html=True)
            
            for _, row in display_df.iterrows():
                name  = row['종목명']
                code  = str(row['종목코드']).zfill(6)
                market_str = row.get('시장', '')
                price = row['현재가_num']
                drop_pct = row['고점대비하락률']
                per, pbr, roe, debt = row['PER'], row['PBR'], row['ROE'], row['부채비율']
                div = row.get('배당수익률', 0.0)
                grade_label = row['등급']

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
                        </div>
                        <div style="text-align:right;">
                            <span style="font-size:15px; font-weight:700; color:#0F172A;">{int(price):,}원</span>
                            <span style="font-size:12px; font-weight:700; color:#16A34A; background:#FFFFFF; border: 1px solid #D1D5DB; padding:2px 8px; border-radius:12px; margin-left:8px;">52주최고 대비 {drop_pct:.1f}%</span>
                        </div>
                    </div>
                    <div style="display:flex; gap:18px; font-size:12px; color:#64748B; margin-bottom:12px; flex-wrap:wrap;">
                        <span>PER <b style="color:#1E293B;">{per:.2f}배</b></span>
                        <span>PBR <b style="color:#1E293B;">{pbr:.2f}배</b></span>
                        <span>ROE <b style="color:#1E293B;">{roe:.2f}%</b></span>
                        <span>부채비율 <b style="color:#1E293B;">{debt:.1f}%</b></span>
                        <span>배당수익률 <b style="color:#DC2626;">{div:.2f}%</b></span>
                    </div>
                    <div style="display:flex; gap:10px;">
                        <div style="flex:1; background:#FFFFFF; border:1px solid #E2E8F0; border-radius:6px; padding:8px 4px; text-align:center;">
                            <div style="font-size:11px; color:#94A3B8; margin-bottom:2px;">1차 진입 (비중 25%)</div>
                            <div style="font-size:13px; font-weight:700; color:#5A4EE5;">{int(price):,}원</div>
                        </div>
                        <div style="flex:1; background:#FFFFFF; border:1px solid #E2E8F0; border-radius:6px; padding:8px 4px; text-align:center;">
                            <div style="font-size:11px; color:#94A3B8; margin-bottom:2px;">2차 진입 (-15% / 35%)</div>
                            <div style="font-size:13px; font-weight:700; color:#1E293B;">{int(entry_2nd):,}원</div>
                        </div>
                        <div style="flex:1; background:#FFFFFF; border:1px solid #E2E8F0; border-radius:6px; padding:8px 4px; text-align:center;">
                            <div style="font-size:11px; color:#94A3B8; margin-bottom:2px;">3차 진입 (-30% / 40%)</div>
                            <div style="font-size:13px; font-weight:700; color:#1E293B;">{int(entry_3rd):,}원</div>
                        </div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
                
                with st.expander(f"📖 {name} 기업 개요 및 상세 재무분석 열기"):
                    if st.button(f"실시간 재무 데이터 불러오기 (FnGuide)", key=f"reco_fn_{code}"):
                        with st.spinner(f"'{name}'의 최신 기업 개요와 재무제표를 가져오는 중입니다..."):
                            draw_fnguide_details(code)

def render_undervalued():
    st.header("저평가 우량주 발굴")
    st.markdown("<hr style='margin: 10px 0 15px 0; border-color: #E5E7EB;'>", unsafe_allow_html=True)
    
    st.markdown("""
        <div style='background:#F8FAFC; border:1px solid #E2E8F0; border-radius:8px; padding:20px; margin-bottom:20px;'>
            <h4 style='margin-top:0; font-size:16px; color:#0F172A;'>💡 퀀트 필터링 기준표 (안정성 포함)</h4>
            <table style='width:100%; border-collapse: collapse; font-size:13px; text-align:center;'>
                <tr style='background-color:#F1F5F9; border-bottom:2px solid #CBD5E1;'>
                    <th style='padding:8px;'>구분</th><th style='padding:8px;'>PER (가치)</th><th style='padding:8px;'>PBR (자산)</th><th style='padding:8px;'>ROE (수익성)</th><th style='padding:8px;'>부채비율 (안정성)</th>
                </tr>
                <tr style='border-bottom:1px solid #E2E8F0;'>
                    <td style='padding:8px; font-weight:600;'>약저평가</td><td>10 ~ 15배</td><td>0.8 ~ 1.0배</td><td>8 ~ 10%</td><td>150% 이하</td>
                </tr>
                <tr style='border-bottom:1px solid #E2E8F0;'>
                    <td style='padding:8px; font-weight:600;'>저평가</td><td>5 ~ 10배</td><td>0.5 ~ 0.8배</td><td>10 ~ 15%</td><td>100% 이하</td>
                </tr>
                <tr style='background-color:#FEF2F2;'>
                    <td style='padding:8px; font-weight:600; color:#DC2626;'>극저평가</td><td style='color:#DC2626;'>5배 미만</td><td style='color:#DC2626;'>0.5배 미만</td><td style='color:#DC2626;'>15% 이상</td><td style='color:#DC2626;'>80% 이하</td>
                </tr>
            </table>
            <div style='margin-top:15px; font-size:12.5px; color:#475569; background-color:#FFFFFF; padding:12px; border:1px dashed #CBD5E1; border-radius:6px;'>
                <b>🚨 투자 전 필수 체크리스트 (가치 함정 피하기)</b><br>
                1. <b>금융업종(은행/증권/보험) 예외:</b> 금융업은 사업 특성상 부채비율이 수백%에 달하는 것이 정상이므로 아래 필터에서 제외하는 것이 안전합니다.<br>
                2. <b>부채의 질 (좀비기업 확인):</b> 부채비율 숫자가 낮더라도, 영업이익으로 이자도 못 갚는 기업(이자보상비율 1배 미만 지속)은 위험합니다. 스크리닝 후 <b>'기업 재무 분석'</b> 탭에서 영업이익이 꾸준한지 확인하세요.
            </div>
        </div>
    """, unsafe_allow_html=True)

    col1, col2 = st.columns([1.5, 1])
    with col1:
        selected_level = st.radio("검색할 저평가 구간을 선택하세요.", ["극저평가 (강력 추천)", "저평가", "약저평가"], horizontal=True)
    with col2:
        st.markdown("<div style='padding-top:10px;'></div>", unsafe_allow_html=True)
        exclude_finance = st.checkbox("금융/지주사 제외 (부채비율 왜곡 방지)", value=True)

    st.markdown("<br>", unsafe_allow_html=True)
    
    save_path = "saved_screener_data.csv"

    if st.button("저평가 우량주 ⚡초고속 스캔 실행"):
        pb = st.progress(0, text="시장 전체 종목 스캔 중...")
        try:
            fetch_and_cache_screener_data.clear()
            temp_df = pd.DataFrame()
            for status_msg, pct in fetch_screener_data_generator():
                if isinstance(status_msg, str): pb.progress(pct, text=f"{status_msg}")
                else: temp_df = status_msg
            pb.progress(100, text="분석 및 필터링 완료!")
            time.sleep(0.5)
            pb.empty()
            
            if not temp_df.empty:
                temp_df.to_csv(save_path, index=False, encoding='utf-8-sig')
                st.session_state['shared_screener_df'] = temp_df
        except Exception as e:
            pb.empty()
            st.error(f"데이터를 가져오는데 실패했습니다: {e}")

    df = load_screener_df()

    if not df.empty:
        if "극저평가" in selected_level:
            cond = (df['PER'] > 0) & (df['PER'] < 5) & (df['PBR'] > 0) & (df['PBR'] < 0.5) & (df['ROE'] >= 15) & (df['부채비율'] >= 0) & (df['부채비율'] <= 80)
        elif "저평가" in selected_level:
            cond = (df['PER'] > 0) & (df['PER'] <= 10) & (df['PBR'] > 0) & (df['PBR'] <= 0.8) & (df['ROE'] >= 10) & (df['부채비율'] >= 0) & (df['부채비율'] <= 100)
        else: # 약저평가
            cond = (df['PER'] > 0) & (df['PER'] <= 15) & (df['PBR'] > 0) & (df['PBR'] <= 1.0) & (df['ROE'] >= 8) & (df['부채비율'] >= 0) & (df['부채비율'] <= 150)

        if exclude_finance:
            finance_keywords = '금융|은행|증권|보험|캐피탈|지주|투자|저축'
            cond = cond & (~df['종목명'].str.contains(finance_keywords, regex=True))

        result_df = df[cond].sort_values(by=['ROE'], ascending=False).reset_index(drop=True)
        
        st.markdown(f"<h4 style='font-size: 16px; margin-top:20px; margin-bottom: 10px;'>🎯 필터링 결과: 총 {len(result_df)} 종목 발견 (ROE 높은 순)</h4>", unsafe_allow_html=True)
        if not result_df.empty:
            st.dataframe(result_df, width='stretch', hide_index=True, column_config=get_table_col_config(include_debt=True))
            st.markdown("<p style='font-size:13px; color:#64748B;'>💡 <b>Tip:</b> 표 안의 종목코드 셀을 마우스로 한 번 클릭한 뒤 <code>Ctrl + C</code> (Mac은 <code>Cmd + C</code>)를 누르면 바로 복사됩니다.</p>", unsafe_allow_html=True)
        else:
            st.warning("현재 시장 상황에서 해당 조건에 부합하는 종목이 없습니다. 구간을 변경하여 다시 검색해 보세요.")

def render_screener():
    st.header("종목 스크리너")
    st.markdown("<hr style='margin: 10px 0 15px 0; border-color: #E5E7EB;'>", unsafe_allow_html=True)
    col1, col2, col3, col4 = st.columns(4)
    with col1: max_per = st.number_input("PER 이하 (배)", value=10.0, step=0.5, format="%.1f")
    with col2: max_pbr = st.number_input("PBR 이하 (배)", value=1.0, step=0.1, format="%.1f")
    with col3: min_div = st.number_input("배당수익률 이상 (%)", value=2.0, step=0.1, format="%.1f")
    with col4: search_text = st.text_input("종목명/코드 검색", placeholder="예: 삼성")

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
            fetch_and_cache_screener_data.clear()
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

    with st.expander("고급 설정 / 서버 접속 장애 시 수동 갱신"):
        st.markdown("""<p style="font-size: 13px; color: #5D6475;">자동 연동이 막혔을 경우, 직접 다운로드한 CSV 파일을 업로드하여 데이터를 갱신할 수 있습니다.</p>""", unsafe_allow_html=True)
        st.link_button("KRX 데이터 다운로드 페이지", "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020502", use_container_width=True)
        uploaded = st.file_uploader("데이터 수동 갱신", type=['csv'])
        if uploaded:
            try:
                try: raw_df = pd.read_csv(uploaded, encoding='cp949')
                except:
                    uploaded.seek(0)
                    raw_df = pd.read_csv(uploaded, encoding='utf-8')
                raw_df.columns = raw_df.columns.str.strip()
                def find_col(candidates):
                    for c in candidates:
                        if c in raw_df.columns: return c
                    for c in raw_df.columns:
                        for cand in candidates:
                            if cand.lower() in c.lower(): return c
                    return None
                code_col = find_col(["종목코드", "단축코드", "Code"])
                name_col = find_col(["종목명", "한글 종목명", "Name"])
                per_col  = find_col(["PER", "주가수익비율"])
                pbr_col  = find_col(["PBR", "주가순자산비율"])
                div_col  = find_col(["배당수익률", "배당률"])
                roe_col  = find_col(["ROE", "자기자본이익률"])
                debt_col = find_col(["부채비율", "부채비율(%)"])
                mkt_col  = find_col(["시장", "Market"])
                price_col = find_col(["현재가", "종가"])
                
                if not (per_col and pbr_col): st.error("PER 또는 PBR 데이터가 없습니다.")
                else:
                    raw_df['종목코드'] = raw_df[code_col].astype(str).str.zfill(6)
                    raw_df['종목명']   = raw_df[name_col]
                    raw_df['PER']      = raw_df[per_col]
                    raw_df['PBR']      = raw_df[pbr_col]
                    raw_df['배당수익률'] = raw_df[div_col] if div_col else 0.0
                    raw_df['ROE'] = raw_df[roe_col] if roe_col else 0.0
                    raw_df['부채비율'] = raw_df[debt_col] if debt_col else 0.0
                    raw_df['시장'] = raw_df[mkt_col] if mkt_col else "코스피"
                    raw_df['현재가'] = pd.to_numeric(raw_df[price_col].astype(str).str.replace(',', ''), errors='coerce').fillna(0.0) if price_col else 0.0
                    
                    for col in ['PER', 'PBR', '배당수익률', 'ROE', '부채비율', '현재가']:
                        raw_df[col] = pd.to_numeric(raw_df[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0.0)
                    df_to_save = raw_df[['종목코드', '종목명', '시장', '현재가', 'PER', 'PBR', '배당수익률', 'ROE', '부채비율']]
                    df_to_save.to_csv(save_path, index=False, encoding='utf-8-sig')
                    st.session_state['shared_screener_df'] = df_to_save
                    st.success("✅ 수동 데이터 저장 완료!")
            except: st.error("파일 오류")

    df = load_screener_df()

    if not df.empty:
        st.markdown("<hr style='margin: 30px 0 20px 0; border-color: #E5E7EB;'>", unsafe_allow_html=True)
        cond_per = (df['PER'] <= max_per) & (df['PER'] > 0)
        cond_pbr = (df['PBR'] <= max_pbr) & (df['PBR'] > 0)
        cond_div = (df['배당수익률'] >= min_div)
        result_df = df[cond_per & cond_pbr & cond_div]
        if search_text:
            result_df = result_df[result_df['종목명'].str.contains(search_text, case=False, na=False) | result_df['종목코드'].astype(str).str.contains(search_text, case=False, na=False)]
        st.markdown(f"<div style='margin-bottom: 10px; font-weight: 600; color: #374151;'>검색 결과 ({len(result_df)}건)</div>", unsafe_allow_html=True)
        st.dataframe(result_df, width='stretch', hide_index=True, column_config=get_table_col_config(include_debt=True))
        st.markdown("<p style='font-size:13px; color:#64748B;'>💡 <b>Tip:</b> 표 안의 종목코드 셀을 마우스로 한 번 클릭한 뒤 <code>Ctrl + C</code> (Mac은 <code>Cmd + C</code>)를 누르면 바로 복사됩니다.</p>", unsafe_allow_html=True)

def render_fnguide():
    st.header("기업 재무 분석")
    st.markdown("<hr style='margin: 10px 0 25px 0; border-color: #E5E7EB;'>", unsafe_allow_html=True)

    col1, col2 = st.columns([1, 3])
    with col1:
        code = st.text_input("종목코드 6자리 입력", placeholder="예: 005930")
        search_btn = st.button("재무 및 기업정보 조회", use_container_width=True)

    if search_btn and code:
        code = normalize_kr_code(code)
        
        with st.spinner("에프앤가이드(FnGuide) 서버에서 데이터를 분석 중입니다..."):
            draw_fnguide_details(code)

def render_dividend():
    st.header("실시간 배당 순위")
    st.markdown("<hr style='margin: 10px 0 25px 0; border-color: #E5E7EB;'>", unsafe_allow_html=True)
    
    if st.button("데이터 새로고침"):
        fetch_dividend_ranking.clear()
        
    df = run_with_progress("마켓 데이터 수집 중...", fetch_dividend_ranking)
    
    if not df.empty: 
        st.dataframe(df, width='stretch', hide_index=True, column_config=get_table_col_config(include_debt=False))
        st.markdown("<p style='font-size:13px; color:#64748B;'>💡 <b>Tip:</b> 표 안의 종목코드 셀을 마우스로 클릭한 뒤 <code>Ctrl + C</code> (Mac은 <code>Cmd + C</code>)를 누르면 바로 복사됩니다.</p>", unsafe_allow_html=True)
    else: 
        st.error("데이터를 불러올 수 없습니다.")

if __name__ == '__main__':
    main()