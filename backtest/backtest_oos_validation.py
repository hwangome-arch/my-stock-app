# -*- coding: utf-8 -*-
"""
backtest_oos_validation.py — Out-of-sample 검증

지금까지 나온 핵심 결론 "리스크 항목이 유일하게 신뢰할 만한 신호다"가
전체 1440개 표본을 다 써서 나온 것이므로, 기간을 반으로 나눠서(train/test)
train에서 본 패턴이 test(안 본 데이터)에서도 유지되는지 확인한다.

전제:
    1. backtest_price_axis.py 실행 → backtest_raw_results.csv 생성
    2. backtest_analysis_v2.py 가 같은 폴더에 있어야 함 (거기 함수를 재사용)

사용법:
    python backtest_oos_validation.py                 # 기본 분할일 2024-01-01
    python backtest_oos_validation.py 2023-07-01       # 분할일 직접 지정
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

from backtest_analysis_v2 import (
    RAW_CSV,
    FORWARD_HORIZONS,
    build_weighted_score,
    run_multiple_regression,
)

DEFAULT_SPLIT_DATE = "2024-01-01"

OUT_DIR = Path(__file__).parent


def load_raw():
    if not RAW_CSV.exists():
        raise FileNotFoundError(
            f"{RAW_CSV} 가 없습니다. 먼저 backtest_price_axis.py를 실행해서 CSV를 만들어두세요."
        )
    df = pd.read_csv(RAW_CSV)
    df["date"] = pd.to_datetime(df["date"])
    return df


def compare_corr(train_df, test_df, score_col, label):
    """train vs test에서 같은 점수(score_col)의 Spearman 상관을 나란히 비교."""
    print(f"\n>>> [{label}] train vs test 상관계수 비교")
    rows = []
    for name, sub in [("train", train_df), ("test", test_df)]:
        row = {"구간": name, "표본수": len(sub)}
        for h in FORWARD_HORIZONS:
            valid = sub.dropna(subset=[score_col, h])
            if len(valid) >= 10:
                row[h] = round(valid[score_col].corr(valid[h], method="spearman"), 3)
            else:
                row[h] = np.nan
        rows.append(row)
    comp = pd.DataFrame(rows)
    print(comp.to_string(index=False))
    return comp


def bucket_winrate_by_group(train_df, test_df, score_col, label, horizon):
    """리스크 단독 점수 기준으로 5분위 승률이 train/test에서 비슷한 순서를 보이는지 확인.
    (버킷 경계가 아니라 분위(quantile)로 나눠야 train/test 표본수가 비슷하게 맞춰진다.)"""
    print(f"\n>>> [{label}] {horizon} 기준 5분위 승률 — train vs test")
    rows = []
    for name, sub in [("train", train_df), ("test", test_df)]:
        valid = sub.dropna(subset=[score_col, horizon]).copy()
        if len(valid) < 25:
            continue
        try:
            valid["q"] = pd.qcut(valid[score_col], 5, labels=False, duplicates="drop")
        except ValueError:
            continue
        for q in sorted(valid["q"].dropna().unique()):
            qsub = valid[valid["q"] == q]
            rows.append({
                "구간": name, "분위": f"Q{int(q) + 1}", "표본수": len(qsub),
                "승률(%)": round((qsub[horizon] > 0).mean() * 100, 1),
                "평균(%)": round(qsub[horizon].mean(), 2),
            })
    result = pd.DataFrame(rows)
    if not result.empty:
        pivot = result.pivot(index="분위", columns="구간", values="승률(%)")
        pivot = pivot.reindex(columns=[c for c in ["train", "test"] if c in pivot.columns])
        print(pivot.to_string())
    return result


def main():
    split_date = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_SPLIT_DATE
    df = load_raw()

    # 점수는 train/test 구분 없이 같은 고정 공식으로 계산 (가중치를 각 구간 데이터로
    # 새로 맞추는 게 아니라, 이미 정한 두 가지 방식을 그대로 양쪽에 적용해 비교하는 것)
    weights_equal = {"추세": 200, "거래량": 100, "모멘텀": 100, "패턴점수": 100, "리스크": 80}
    weights_risk_only = {"추세": 0, "거래량": 0, "모멘텀": 0, "패턴점수": 0, "리스크": 80}
    df["score_equal"] = build_weighted_score(df, weights_equal)
    df["score_risk_only"] = build_weighted_score(df, weights_risk_only)

    train = df[df["date"] < split_date].copy()
    test = df[df["date"] >= split_date].copy()

    print("=" * 70)
    print(f"Out-of-sample 검증  (split_date = {split_date})")
    print("=" * 70)
    print(f"train: {len(train)}행  ({train['date'].min().date()} ~ {train['date'].max().date()})")
    print(f"test : {len(test)}행  ({test['date'].min().date()} ~ {test['date'].max().date()})")

    if len(train) < 100 or len(test) < 100:
        print("\n⚠️ train 또는 test 표본이 100개 미만입니다. split_date를 조정해서 다시 시도하세요.")
        print("   예: python backtest_oos_validation.py 2023-07-01")
        return

    print("\n" + "=" * 70)
    print("1) 원본 방식(동일비중) — train vs test")
    print("=" * 70)
    compare_corr(train, test, "score_equal", "동일비중(원본)")

    print("\n" + "=" * 70)
    print("2) 리스크 단독 — train vs test  ★ 핵심 검증 대상 ★")
    print("=" * 70)
    compare_corr(train, test, "score_risk_only", "리스크 단독")

    for h in FORWARD_HORIZONS:
        bucket_winrate_by_group(train, test, "score_risk_only", "리스크 단독", h)

    print("\n" + "=" * 70)
    print("3) 다중회귀 계수 — train만 vs test만 따로 돌려서 비교")
    print("=" * 70)
    print("(리스크·패턴점수 계수의 부호와 유의성이 train/test 양쪽에서 비슷하게 유지되는지 확인)\n")

    print("[TRAIN 구간]")
    run_multiple_regression(train, out_path=OUT_DIR / "backtest_regression_train.csv")

    print("[TEST 구간]")
    run_multiple_regression(test, out_path=OUT_DIR / "backtest_regression_test.csv")

    print("\n" + "=" * 70)
    print("📋 해석 가이드")
    print("=" * 70)
    print("- 리스크 단독의 상관계수가 train과 test에서 '같은 방향(+)'이고 크기도 비슷한")
    print("  수준이면 → 우연이 아니라 실제로 존재하는 신호일 가능성이 높다.")
    print("- test에서 부호가 뒤집히거나 0 근처로 확 꺼지면")
    print("  → train에서 본 패턴이 그 기간 특유의 우연/노이즈였을 가능성이 있다.")
    print("- train/test 표본이 각각 수백 개 수준이라 20~30% 정도 차이는 노이즈 범위 안일")
    print("  수 있다 — '완전히 똑같아야 한다'가 아니라 '방향과 대략적인 크기가 유지되는지'를 볼 것.")
    print("- split_date를 바꿔가며(예: 2023-01-01, 2023-07-01, 2024-07-01) 여러 번 돌려보고")
    print("  결과가 일관되면 더 신뢰할 수 있다. 한 번의 분할 결과만으로 단정하지 말 것.")


if __name__ == "__main__":
    main()
