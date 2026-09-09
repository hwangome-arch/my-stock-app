# -*- coding: utf-8 -*-
"""
backtest_analysis_v2.py — 2단계 분석: 다중회귀 / 리스크 가중치 조정 / 비선형(포화) 변환

전제: backtest_price_axis.py를 먼저 돌려서 backtest_raw_results.csv 가 이미
생성돼 있어야 한다. 이 스크립트는 그 CSV만 읽어서 분석하므로 yfinance
네트워크 접근이 필요 없다 (backtest_price_axis.py와 같은 폴더에 두고 실행).

사용법:
    python backtest_analysis_v2.py

────────────────────────────────────────────────────────────────────────
이 스크립트가 하는 일 (3단계)
────────────────────────────────────────────────────────────────────────
A) 다중회귀: 5개 서브점수(추세/거래량/모멘텀/패턴/리스크)를 동시에 회귀에 넣어서
   서로 얽혀있는 효과를 통제한 "순수" 기여도를 본다. (단독 상관관계만으로는
   서로 상관된 항목들의 진짜 효과를 분리할 수 없기 때문)

B) 리스크 가중치 조정: 5분위 분석에서 리스크만 유일하게 단조적이고 유의미한
   신호를 보였으므로, 리스크 비중을 2배/4배/단독으로 올려가며 재계산한 점수가
   원본보다 이후 수익률을 더 잘 예측하는지 버킷별 요약 + Spearman 상관으로 비교한다.

C) 비선형(포화) 변환: 5분위 분석에서 추세/거래량/모멘텀/패턴점수가 "역U자형"
   (극단적으로 높으면 오히려 안 좋음) 패턴을 보였으므로, 각 항목에 대해
   "최적 구간에서 멀어질수록 감점"하는 비선형 변환을 적용한 새 점수를 만들어본다.

⚠️⚠️⚠️ 매우 중요한 주의사항 (B, C 공통) ⚠️⚠️⚠️
B)의 가중치, C)의 "최적 구간(optimal)" 값은 전부 지금 이 backtest_raw_results.csv
데이터를 보고 "어디가 제일 좋았는지" 역으로 관찰해서 정한 값이다. 즉 같은 데이터로
공식을 조정하고, 같은 데이터로 그 공식이 잘 맞는지 확인하는 것 — 전형적인
인샘플(in-sample) 최적화라 과적합(overfitting) 위험이 크다. 여기서 상관관계가
좋게 나오는 건 "미래 예측력이 있다"는 증거가 아니라 "이 데이터에 맞춰 끼워 맞췄다"는
뜻일 수 있다. 진짜 검증하려면 반드시 기간을 나눠서(예: 2022~2023로 임계값을 정하고
2024~2025로 검증) out-of-sample 테스트를 해야 한다 — 맨 아래
train_test_split_validation() 함수에 그 틀만 만들어뒀다.
"""

from pathlib import Path

import numpy as np
import pandas as pd

RAW_CSV = Path(__file__).parent / "backtest_raw_results.csv"

FORWARD_HORIZONS = ["20거래일(약1개월)", "60거래일(약3개월)", "120거래일(약6개월)"]

SUBSCORE_COLS = {
    "추세": "sub_추세",
    "거래량": "sub_거래량",
    "모멘텀": "sub_모멘텀",
    "패턴점수": "sub_패턴점수",
    "리스크": "sub_리스크",
}

# 원래 공식의 항목별 이론적 만점 (정규화용). 리스크는 0~-80 범위(0이 최상).
SUBSCORE_MAX = {"추세": 200, "거래량": 100, "모멘텀": 100, "패턴점수": 100, "리스크": 80}

SCORE_BUCKETS = [
    (80, 101, "80~100"), (70, 80, "70~79"), (60, 70, "60~69"),
    (50, 60, "50~59"), (40, 50, "40~49"), (0, 40, "0~39"),
]


def load_raw():
    if not RAW_CSV.exists():
        raise FileNotFoundError(
            f"{RAW_CSV} 가 없습니다. 먼저 backtest_price_axis.py를 실행해서 "
            f"backtest_raw_results.csv를 생성한 뒤 이 스크립트를 같은 폴더에서 실행하세요."
        )
    return pd.read_csv(RAW_CSV)


# ════════════════════════════════════════════════════════════════════════
# A) 다중회귀 — 5개 서브점수를 동시에 넣어 각 항목의 '순수' 기여도를 본다
#    (statsmodels/sklearn 없이 numpy만으로 OLS + t값 계산)
# ════════════════════════════════════════════════════════════════════════

def run_multiple_regression(df, out_path=None):
    print("=" * 70)
    print("A) 다중회귀: 5개 서브점수를 동시에 투입한 순수 기여도")
    print("=" * 70)
    print("서브점수는 z-score로 표준화해서 계수 크기를 서로 비교 가능하게 만들었다.")
    print("(계수가 클수록, 다른 4개 항목을 통제한 상태에서 그 항목만의 영향력이 크다는 뜻)\n")

    sub_cols = list(SUBSCORE_COLS.values())
    all_results = []

    for horizon in FORWARD_HORIZONS:
        valid = df.dropna(subset=sub_cols + [horizon]).copy()
        if len(valid) < 30:
            print(f"[{horizon}] 표본 부족으로 스킵\n")
            continue

        X = valid[sub_cols].values.astype(float)
        y = valid[horizon].values.astype(float)

        X_mean, X_std = X.mean(axis=0), X.std(axis=0)
        X_std[X_std == 0] = 1.0
        Xz = (X - X_mean) / X_std

        Xd = np.column_stack([np.ones(len(Xz)), Xz])
        coef, _, _, _ = np.linalg.lstsq(Xd, y, rcond=None)

        n, k = Xd.shape
        y_pred = Xd @ coef
        resid = y - y_pred
        dof = max(n - k, 1)
        sigma2 = (resid ** 2).sum() / dof
        try:
            xtx_inv = np.linalg.inv(Xd.T @ Xd)
            se = np.sqrt(np.diag(xtx_inv) * sigma2)
            t_vals = coef / se
        except np.linalg.LinAlgError:
            t_vals = np.full_like(coef, np.nan)

        ss_tot = ((y - y.mean()) ** 2).sum()
        ss_res = (resid ** 2).sum()
        r2 = 1 - ss_res / ss_tot if ss_tot > 0 else np.nan

        print(f"[{horizon}]  n={n}, R²={r2:.4f}")
        print(f"  {'항목':<8}{'표준화계수':>12}{'t값':>10}  유의성")
        print(f"  {'절편':<8}{coef[0]:>12.3f}{t_vals[0]:>10.2f}")
        for name, c, t in zip(SUBSCORE_COLS.keys(), coef[1:], t_vals[1:]):
            sig = "**" if abs(t) >= 2.58 else ("*" if abs(t) >= 1.96 else "")
            print(f"  {name:<8}{c:>12.3f}{t:>10.2f}  {sig}")
            all_results.append({
                "기간": horizon, "항목": name,
                "표준화계수": round(float(c), 3), "t값": round(float(t), 2), "유의": sig,
            })
        print()

    print("* : |t| >= 1.96 (약 95% 신뢰수준 근사)   ** : |t| >= 2.58 (약 99% 신뢰수준 근사)")
    print("⚠️ 리밸런스 시점이 20거래일 간격으로 겹치는 표본이 많아 관측치가 완전히")
    print("   독립이 아니다 (같은 종목의 인접 시점끼리 상관) → t값이 과장돼 보일 수 있음.\n")

    out = pd.DataFrame(all_results)
    if out_path is None:
        out_path = Path(__file__).parent / "backtest_regression_results.csv"
    out.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"회귀 결과 저장: {out_path}\n")
    return out


# ════════════════════════════════════════════════════════════════════════
# 공통 유틸 — 버킷 요약 + Spearman 상관 출력
# ════════════════════════════════════════════════════════════════════════

def bucket_and_corr(df, score_col, label):
    def bucket_label(s):
        for lo, hi, lbl in SCORE_BUCKETS:
            if lo <= s < hi:
                return lbl
        return "N/A"

    tmp = df.copy()
    tmp["_bucket"] = tmp[score_col].apply(bucket_label)

    print(f"--- [{label}] 버킷별 요약 ---")
    rows = []
    for lo, hi, lbl in SCORE_BUCKETS:
        sub = tmp[tmp["_bucket"] == lbl]
        if sub.empty:
            continue
        row = {"점수구간": lbl, "표본수": len(sub)}
        for h in FORWARD_HORIZONS:
            row[f"{h}_평균(%)"] = round(sub[h].mean(), 2)
            row[f"{h}_승률(%)"] = round((sub[h] > 0).mean() * 100, 1)
        rows.append(row)
    bucket_df = pd.DataFrame(rows)
    if not bucket_df.empty:
        print(bucket_df.to_string(index=False))
    else:
        print("  (버킷별 표본 없음)")

    print(f"\n--- [{label}] Spearman 상관계수 ---")
    for h in FORWARD_HORIZONS:
        valid = tmp.dropna(subset=[score_col, h])
        if len(valid) >= 10:
            corr = valid[score_col].corr(valid[h], method="spearman")
            print(f"  {h}: {corr:.3f}  (n={len(valid)})")
    print()
    return bucket_df


# ════════════════════════════════════════════════════════════════════════
# B) 리스크 가중치 조정 — 원래 5개 항목 동일비중(그냥 합산) 대신 리스크를 더 크게 반영
# ════════════════════════════════════════════════════════════════════════

def build_weighted_score(df, weights):
    """각 서브점수를 이론적 만점 기준 0~1로 정규화한 뒤 가중합 → 0~100 스케일."""
    norm = pd.DataFrame(index=df.index)
    for name, col in SUBSCORE_COLS.items():
        maxv = SUBSCORE_MAX[name]
        if name == "리스크":
            # 리스크는 -80~0, 0(감점없음)이 최상 → (80+risk)/80 로 0~1 정규화 (1이 최상)
            norm[name] = (maxv + df[col]) / maxv
        else:
            norm[name] = df[col] / maxv
        norm[name] = norm[name].clip(0, 1)

    total_w = sum(weights.values())
    if total_w <= 0:
        raise ValueError("가중치 합이 0 이하입니다.")
    weighted = sum(norm[name] * w for name, w in weights.items()) / total_w * 100
    return weighted.round(1)


def run_reweighted_score(df):
    print("=" * 70)
    print("B) 리스크 가중치 조정 재계산")
    print("=" * 70)
    print("원본과 같은 비중(동일비중) 대비, 리스크 비중을 올린 버전들을 비교한다.\n")

    print("[기존] 동일비중 (원본 공식과 동일한 비율로 재정규화)")
    original_weights = {"추세": 200, "거래량": 100, "모멘텀": 100, "패턴점수": 100, "리스크": 80}
    df["score_equal"] = build_weighted_score(df, original_weights)
    bucket_and_corr(df, "score_equal", "동일비중(원본과 동일 비율)")

    print("[실험 1] 리스크 가중치 2배")
    w2 = {"추세": 200, "거래량": 100, "모멘텀": 100, "패턴점수": 100, "리스크": 160}
    df["score_risk2x"] = build_weighted_score(df, w2)
    bucket_and_corr(df, "score_risk2x", "리스크 2배 가중")

    print("[실험 2] 리스크 가중치 4배 (거의 리스크 중심)")
    w4 = {"추세": 200, "거래량": 100, "모멘텀": 100, "패턴점수": 100, "리스크": 320}
    df["score_risk4x"] = build_weighted_score(df, w4)
    bucket_and_corr(df, "score_risk4x", "리스크 4배 가중")

    print("[실험 3] 리스크 단독 점수 (다른 4개 항목 완전 배제)")
    w_only = {"추세": 0, "거래량": 0, "모멘텀": 0, "패턴점수": 0, "리스크": 80}
    df["score_risk_only"] = build_weighted_score(df, w_only)
    bucket_and_corr(df, "score_risk_only", "리스크 단독")

    out_cols = (["ticker", "date", "score_100", "score_equal", "score_risk2x",
                 "score_risk4x", "score_risk_only"] + FORWARD_HORIZONS)
    out_cols = [c for c in out_cols if c in df.columns]
    out_path = Path(__file__).parent / "backtest_reweighted_scores.csv"
    df[out_cols].to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"가중치 실험별 점수 저장: {out_path}\n")


# ════════════════════════════════════════════════════════════════════════
# C) 비선형(포화) 변환 — 극단적으로 높은 추세/거래량/모멘텀/패턴 점수의 "과도한 가산" 억제
# ════════════════════════════════════════════════════════════════════════

def saturate_inverted_u(x, x_min, x_optimal, x_max, penalty_strength=1.0):
    """x_optimal 부근에서 최대치(1.0)를 주고, 멀어질수록 거리제곱에 비례해 감점.
    직전 5분위 분석에서 관찰된 '중간이 제일 좋다'는 역U자형 패턴을 반영한 실험적 변환."""
    x = np.asarray(x, dtype=float)
    span = max(x_max - x_min, 1e-9)
    dist = (x - x_optimal) / span
    penalty = penalty_strength * (dist ** 2)
    return np.clip(1.0 - penalty, 0.0, 1.0)


def run_nonlinear_transform(df):
    print("=" * 70)
    print("C) 비선형(포화) 변환 — 극단값 과도가산 방지")
    print("=" * 70)
    print("직전 5분위 분석에서 '중간 구간이 제일 좋았다'는 패턴이 나온 4개 항목")
    print("(추세/거래량/모멘텀/패턴점수)에 대해 '최적 구간에서 멀어질수록 감점'하는")
    print("방식으로 바꿔본다. 리스크는 원래부터 단조적(0에 가까울수록 좋음)이라 그대로 둔다.\n")
    print("⚠️ 아래 optimal(최적 구간) 값은 지금 이 데이터의 5분위 결과에서 대략 따온")
    print("   값이라 in-sample이다. 실전 적용 전 다른 기간으로 반드시 재검증할 것.\n")

    # 직전 5분위 분석 결과에서 Q3(또는 가장 좋았던 분위)의 범위 중앙값 근처로 설정한 값
    optimal_ranges = {
        "추세":     {"min": 13.0, "opt": 45.0, "max": 200.0},
        "거래량":   {"min": 1.1,  "opt": 28.0, "max": 100.0},
        "모멘텀":   {"min": 4.0,  "opt": 46.0, "max": 100.0},
        "패턴점수": {"min": 1.2,  "opt": 30.0, "max": 96.7},
    }

    norm = pd.DataFrame(index=df.index)
    for name, col in SUBSCORE_COLS.items():
        if name == "리스크":
            norm[name] = ((80 + df[col]) / 80).clip(0, 1)
        else:
            r = optimal_ranges[name]
            norm[name] = saturate_inverted_u(df[col], r["min"], r["opt"], r["max"])

    weights = {"추세": 200, "거래량": 100, "모멘텀": 100, "패턴점수": 100, "리스크": 80}
    total_w = sum(weights.values())
    df["score_nonlinear"] = (sum(norm[name] * w for name, w in weights.items()) / total_w * 100).round(1)

    bucket_and_corr(df, "score_nonlinear", "비선형(포화) 변환 점수")

    print("[비교] 원본 vs 리스크 단독 vs 비선형 변환 — Spearman 상관계수만 정리")
    compare_cols = {"원본(score_100)": "score_100"}
    if "score_risk_only" in df.columns:
        compare_cols["리스크 단독"] = "score_risk_only"
    compare_cols["비선형 변환"] = "score_nonlinear"

    comp_rows = []
    for label, col in compare_cols.items():
        row = {"방식": label}
        for h in FORWARD_HORIZONS:
            valid = df.dropna(subset=[col, h])
            row[h] = round(valid[col].corr(valid[h], method="spearman"), 3) if len(valid) >= 10 else np.nan
        comp_rows.append(row)
    comp_df = pd.DataFrame(comp_rows)
    print(comp_df.to_string(index=False))

    out_path = Path(__file__).parent / "backtest_nonlinear_score.csv"
    keep = [c for c in ["ticker", "date", "score_100", "score_nonlinear"] + FORWARD_HORIZONS if c in df.columns]
    df[keep].to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"\n비선형 변환 점수 저장: {out_path}\n")

    return comp_df


# ════════════════════════════════════════════════════════════════════════
# (참고용 틀) Out-of-sample 검증 — 기간을 나눠서 진짜 검증하고 싶을 때 확장해서 사용
# ════════════════════════════════════════════════════════════════════════

def train_test_split_validation(df, split_date="2024-01-01"):
    """date 기준으로 train/test를 나눠서, train에서 관찰한 패턴(가중치·optimal 구간)이
    test에서도 유지되는지 확인하기 위한 골격 함수. 지금은 행 개수만 보여준다 —
    실제로 쓰려면 optimal_ranges/weights를 train에서만 재추정하도록 확장해야 한다."""
    tmp = df.copy()
    tmp["date"] = pd.to_datetime(tmp["date"])
    train = tmp[tmp["date"] < split_date]
    test = tmp[tmp["date"] >= split_date]
    print(f"[out-of-sample 틀] split_date={split_date} 기준 train={len(train)}행, test={len(test)}행")
    print("→ 가중치(B)나 optimal 구간(C)을 train으로만 정하고 test로 검증하도록 확장해서 사용할 것.\n")
    return train, test


def main():
    df = load_raw()
    print(f"원본 데이터 로드: {len(df)}행\n")

    run_multiple_regression(df)
    run_reweighted_score(df)
    run_nonlinear_transform(df)

    print("=" * 70)
    print("✅ 전체 분석 완료. 저장된 파일:")
    print("   - backtest_regression_results.csv   (A: 다중회귀 계수)")
    print("   - backtest_reweighted_scores.csv    (B: 가중치 실험별 점수)")
    print("   - backtest_nonlinear_score.csv      (C: 비선형 변환 점수)")
    print("=" * 70)
    print("\n⚠️ B), C)는 이 데이터 안에서 맞춰 조정한 것(in-sample)이므로,")
    print("   실전 적용 전 반드시 train_test_split_validation()을 확장해서")
    print("   다른 기간에서도 패턴이 유지되는지 확인할 것을 강력히 권장한다.")


if __name__ == "__main__":
    main()