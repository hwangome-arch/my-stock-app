# -*- coding: utf-8 -*-
"""
코스피/코스닥 거래량이 왜 N/A로 나오는지 확인하는 독립 진단 스크립트.

사용법:
    pip install requests
    python debug_naver_volume.py

앱(Streamlit)과 완전히 분리된 스크립트라서, 이 결과를 그대로 캡처해서
보내주시면 정확한 원인(HTTP 상태 코드, 응답 구조, 필드명 등)을 바로 확인할 수 있습니다.
"""

import json
import requests

HEADERS_BASE = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}

SYMBOLS = ["KOSPI", "KOSDAQ"]


def pretty(obj, limit=1500):
    s = json.dumps(obj, ensure_ascii=False, indent=2)
    if len(s) > limit:
        s = s[:limit] + f"\n... (총 {len(s)}자 중 {limit}자만 표시)"
    return s


def check_basic_endpoint(symbol):
    print(f"\n{'='*70}")
    print(f"[1] 가격/등락률용 엔드포인트 (/basic) — {symbol}")
    print(f"{'='*70}")
    url = f"https://m.stock.naver.com/api/index/{symbol}/basic"
    print(f"URL: {url}")
    try:
        res = requests.get(url, headers=HEADERS_BASE, timeout=8)
        print(f"HTTP 상태 코드: {res.status_code}")
        print(f"Content-Type: {res.headers.get('Content-Type')}")
        try:
            data = res.json()
            print("응답 키 목록:", list(data.keys()) if isinstance(data, dict) else type(data).__name__)
            print("accumulatedTradingVolume 필드 존재 여부:",
                  "accumulatedTradingVolume" in data if isinstance(data, dict) else "N/A")
            print("응답 본문(JSON):")
            print(pretty(data))
        except Exception as je:
            print(f"JSON 파싱 실패: {type(je).__name__}: {je}")
            print("응답 본문(텍스트, 앞부분):")
            print(res.text[:1000])
    except Exception as e:
        print(f"요청 자체 실패: {type(e).__name__}: {e}")


def check_realtime_endpoint(symbol, with_referer):
    label = "Referer 포함" if with_referer else "Referer 없음"
    print(f"\n{'-'*70}")
    print(f"[2] 거래량용 엔드포인트 (polling realtime, {label}) — {symbol}")
    print(f"{'-'*70}")
    url = f"https://polling.finance.naver.com/api/realtime/domestic/index/{symbol}"
    headers = dict(HEADERS_BASE)
    if with_referer:
        headers["Referer"] = "https://m.stock.naver.com/"
    print(f"URL: {url}")
    print(f"Headers: {headers}")
    try:
        res = requests.get(url, headers=headers, timeout=8)
        print(f"HTTP 상태 코드: {res.status_code}")
        print(f"Content-Type: {res.headers.get('Content-Type')}")
        try:
            payload = res.json()
            print("최상위 응답 키:", list(payload.keys()) if isinstance(payload, dict) else type(payload).__name__)
            datas = payload.get("datas") if isinstance(payload, dict) else None
            if datas:
                first = datas[0]
                print("datas[0] 키 목록:", list(first.keys()) if isinstance(first, dict) else type(first).__name__)
                print("accumulatedTradingVolume 필드 존재 여부:",
                      "accumulatedTradingVolume" in first if isinstance(first, dict) else "N/A")
            else:
                print("datas 필드가 비어있거나 없음")
            print("응답 본문(JSON):")
            print(pretty(payload))
        except Exception as je:
            print(f"JSON 파싱 실패: {type(je).__name__}: {je}")
            print("응답 본문(텍스트, 앞부분):")
            print(res.text[:1000])
    except Exception as e:
        print(f"요청 자체 실패: {type(e).__name__}: {e}")


if __name__ == "__main__":
    for sym in SYMBOLS:
        check_basic_endpoint(sym)
        check_realtime_endpoint(sym, with_referer=False)
        check_realtime_endpoint(sym, with_referer=True)

    print(f"\n{'='*70}")
    print("완료. 위 출력 전체를 그대로 복사해서 보내주세요.")
    print(f"{'='*70}")
