# -*- coding: utf-8 -*-
"""
backtest_oos_multi_split.py — 여러 split_date로 OOS 검증을 반복 실행해서
'리스크 단독' 신호가 특정 분할일 하나에 우연히 걸린 게 아닌지 확인한다.

배경:
    backtest_oos_validation.py 를 split_date=2024-01-01 로 한 번 돌린 결과,
    - 20거래일: train -0.031 -> test +0.105  (부호 뒤집힘, 절대값도 작음 → 신뢰 낮음)
    - 60거래일: train  0.062 -> test +0.168  (방향 유지, 오히려 강해짐)
    - 120거래일: train 0.147 -> test +0.141  (거의 동일 → 가장 신뢰할 만함)
    한 번의 분할만으로는 우연인지 판단할 수 없으므로, split_date를 바꿔가며
    반복하고 방향(부호)과 대략적인 크기가 얼마나 안정적인지를 표로 정리한다.

전제:
    1. backtest_price_axis.py 실행 → backtest_raw_results.csv 생성
    2. backtest_analysis_v2.py 가 같은 폴더에 있어야 함 (거기 함수를 재사용)

사용법:
    python backtest_oos_multi_split.py
    python backtest_oos_multi_split.py 2022-10-01 2023-01-01 2023-07-01 2024-01-01 2024-07-01
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

from backtest_analysis_v2 import RAW_CSV, FORWARD_HORIZONS, build_weighted_score

OUT_DIR = Path(__file__).parent

DEFAULT_SPLIT_DATES = [
    "2023-01-01",
    "2023-07-01",
    "2024-01-01",
    "2024-07-01",
]

MIN_ROWS_PER_SIDE = 100  # train/test 각각 최소 표본수


def load_raw():
    if not RAW_CSV.exists():
        raise FileNotFoundError(
            f"{RAW_CSV} 가 없습니다. 먼저 backtest_price_axis.py를 실행해서 CSV를 만들어두세요."
        )
    df = pd.read_csv(RAW_CSV)
    df["date"] = pd.to_datetime(df["date"])
    return df


def spearman_by_side(sub, score_col, horizon):
    valid = sub.dropna(subset=[score_col, horizon])
    if len(valid) < 10:
        return np.nan
    return round(valid[score_col].corr(valid[horizon], method="spearman"), 3)


def quintile_monotonic(sub, score_col, horizon):
    """5분위 승률이 Q1<=Q2<=...<=Q5 로 (거의) 단조증가하면 True.
    분위 경계에서 승률이 뒤집히는 횟수(inversion)를 세어, 0~1이면 '거의 단조'로 본다."""
    valid = sub.dropna(subset=[score_col, horizon]).copy()
    if len(valid) < 25:
        return np.nan, np.nan
    try:
        valid["q"] = pd.qcut(valid[score_col], 5, labels=False, duplicates="drop")
    except ValueError:
        return np.nan, np.nan
    winrates = []
    for q in sorted(valid["q"].dropna().unique()):
        qsub = valid[valid["q"] == q]
        winrates.append((qsub[horizon] > 0).mean() * 100)
    if len(winrates) < 3:
        return np.nan, np.nan
    inversions = sum(1 for i in range(len(winrates) - 1) if winrates[i] > winrates[i + 1])
    is_monotonic = inversions <= 1  # 분위 5개 중 뒤집힘 최대 1번까지는 '거의 단조'로 허용
    return is_monotonic, inversions


def run_one_split(df, split_date, score_col="score_risk_only"):
    train = df[df["date"] < split_date]
    test = df[df["date"] >= split_date]

    if len(train) < MIN_ROWS_PER_SIDE or len(test) < MIN_ROWS_PER_SIDE:
        return None  # 표본 부족 → 이 split_date는 스킵

    rows = []
    for h in FORWARD_HORIZONS:
        corr_train = spearman_by_side(train, score_col, h)
        corr_test = spearman_by_side(test, score_col, h)
        mono_train, inv_train = quintile_monotonic(train, score_col, h)
        mono_test, inv_test = quintile_monotonic(test, score_col, h)

        same_sign = (
            np.sign(corr_train) == np.sign(corr_test)
            if not (np.isnan(corr_train) or np.isnan(corr_test))
            else np.nan
        )

        rows.append({
            "split_date": split_date,
            "horizon": h,
            "train_n": len(train),
            "test_n": len(test),
            "corr_train": corr_train,
            "corr_test": corr_test,
            "부호일치": same_sign,
            "train_5분위_단조": mono_train,
            "test_5분위_단조": mono_test,
        })
    return rows


def main():
    split_dates = sys.argv[1:] if len(sys.argv) > 1 else DEFAULT_SPLIT_DATES
    df = load_raw()

    # score_risk_only 를 고정 공식으로 미리 계산해둔다 (매 split마다 다시 계산할 필요 없음 —
    # 가중치를 train으로 새로 추정하는 게 아니라 원래 정한 공식 하나를 그대로 적용하는 것이므로)
    weights_risk_only = {"추세": 0, "거래량": 0, "모멘텀": 0, "패턴점수": 0, "리스크": 80}
    df["score_risk_only"] = build_weighted_score(df, weights_risk_only)

    print("=" * 80)
    print(f"다중 split_date OOS 검증  (대상: {len(split_dates)}개 분할일)")
    print(f"전체 데이터: {len(df)}행  ({df['date'].min().date()} ~ {df['date'].max().date()})")
    print("=" * 80)

    all_rows = []
    skipped = []
    for sd in split_dates:
        rows = run_one_split(df, sd)
        if rows is None:
            skipped.append(sd)
            continue
        all_rows.extend(rows)

    if skipped:
        print(f"\n⚠️ 표본 부족(train/test 각 {MIN_ROWS_PER_SIDE}행 미만)으로 스킵된 split_date: {skipped}")

    if not all_rows:
        print("\n분석 가능한 split_date가 없습니다. 날짜 범위를 조정해서 다시 시도하세요.")
        return

    result = pd.DataFrame(all_rows)

    print("\n" + "=" * 80)
    print("리스크 단독(score_risk_only) 신호 — split_date별 train/test 상관계수")
    print("=" * 80)
    for h in FORWARD_HORIZONS:
        sub = result[result["horizon"] == h]
        print(f"\n[{h}]")
        print(sub[["split_date", "train_n", "test_n", "corr_train", "corr_test", "부호일치"]]
              .to_string(index=False))

    print("\n" + "=" * 80)
    print("horizon별 요약 — 부호 일치 비율 / 5분위 단조성 유지 비율")
    print("=" * 80)
    summary_rows = []
    for h in FORWARD_HORIZONS:
        sub = result[result["horizon"] == h]
        n_valid = sub["부호일치"].notna().sum()
        n_same_sign = (sub["부호일치"] == True).sum()
        n_mono_test = (sub["test_5분위_단조"] == True).sum()
        n_mono_valid = sub["test_5분위_단조"].notna().sum()
        summary_rows.append({
            "horizon": h,
            "분할횟수": len(sub),
            "부호일치_비율": f"{n_same_sign}/{n_valid}" if n_valid else "N/A",
            "test_5분위단조_비율": f"{n_mono_test}/{n_mono_valid}" if n_mono_valid else "N/A",
            "corr_test_평균": round(sub["corr_test"].mean(), 3),
            "corr_test_표준편차": round(sub["corr_test"].std(), 3),
        })
    summary = pd.DataFrame(summary_rows)
    print(summary.to_string(index=False))

    out_path = OUT_DIR / "backtest_multi_split_summary.csv"
    result.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"\n상세 결과 저장: {out_path}")

    print("\n" + "=" * 80)
    print("📋 읽는 법")
    print("=" * 80)
    print("- '부호일치' 비율이 높을수록(예: 4/4) → 분할일을 바꿔도 방향이 안 바뀌는 안정적 신호.")
    print("- 'corr_test_표준편차'가 작을수록 → test 구간 상관계수가 분할일에 따라 덜 흔들림.")
    print("- test_5분위단조 비율이 높을수록 → Q1~Q5로 갈수록 승률이 계단식으로 오르는 구조가")
    print("  분할일이 달라져도 유지된다는 뜻 (단순 상관계수보다 더 강한 증거).")
    print("- 20거래일처럼 부호가 자주 뒤집히는 horizon은 실전 반영을 보류하고,")
    print("  60/120거래일처럼 일관된 horizon 위주로 신호를 신뢰하는 것을 권장.")


if __name__ == "__main__":
    main()
