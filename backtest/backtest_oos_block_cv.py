# -*- coding: utf-8 -*-
"""
backtest_oos_block_cv.py — 겹치지 않는(non-overlapping) K개 구간으로 블록 교차검증.

배경 / 왜 필요한가:
    backtest_oos_multi_split.py 는 split_date를 여러 개 썼지만, train이 매번
    "맨 처음부터 split_date까지"인 누적(expanding) 방식이라 서로 다른 split끼리
    train/test가 크게 겹쳤다 (예: 2024-07-01 split의 train 920행 안에는
    2024-01-01 split의 train 680행이 통째로 포함됨). 그래서 "4번 확인했다"고
    해도 실질적인 독립 정보량은 그보다 훨씬 적다.

    이 스크립트는 전체 기간을 날짜 순서대로 겹치지 않는 K개 블록으로 나눈 뒤,
    각 블록을 딱 한 번씩만 test로 사용한다 (나머지 블록 = train). 이렇게 하면
    K개의 test 결과가 서로 완전히 독립적인 표본이 되어, "우연히 한 구간에서만
    잘 나온 것" 여부를 훨씬 엄격하게 판단할 수 있다.

    ⚠️ 주의: score_risk_only는 가중치를 데이터에서 추정하지 않고 고정된 공식
    (리스크 항목만 사용)을 그대로 적용하는 것이므로, train에 test 이후 시점의
    데이터가 섞여 들어가도 "미래 정보로 공식을 튜닝했다"는 문제는 없다.
    (만약 나중에 가중치나 임계값을 데이터로部터 추정하는 방식으로 바꾼다면,
    그때는 train을 test보다 반드시 과거 구간으로만 제한해야 한다.)

전제:
    1. backtest_price_axis.py 실행 → backtest_raw_results.csv 생성
    2. backtest_analysis_v2.py 가 같은 폴더에 있어야 함 (거기 함수를 재사용)

사용법:
    python backtest_oos_block_cv.py            # 기본 4개 블록
    python backtest_oos_block_cv.py 6           # 6개 블록으로 나눠서 검증
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

from backtest_analysis_v2 import RAW_CSV, FORWARD_HORIZONS, build_weighted_score

OUT_DIR = Path(__file__).parent

DEFAULT_N_BLOCKS = 4
MIN_ROWS_PER_BLOCK = 60  # 블록(=test) 표본이 이보다 적으면 그 블록은 신뢰도 낮음 경고


def load_raw():
    if not RAW_CSV.exists():
        raise FileNotFoundError(
            f"{RAW_CSV} 가 없습니다. 먼저 backtest_price_axis.py를 실행해서 CSV를 만들어두세요."
        )
    df = pd.read_csv(RAW_CSV)
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date").reset_index(drop=True)


def make_blocks(df, n_blocks):
    """행 개수 기준으로 날짜순 연속 구간을 n_blocks개로 균등 분할 (겹침 없음)."""
    n = len(df)
    edges = np.linspace(0, n, n_blocks + 1).astype(int)
    blocks = []
    for i in range(n_blocks):
        lo, hi = edges[i], edges[i + 1]
        block_df = df.iloc[lo:hi]
        if block_df.empty:
            continue
        label = f"블록{i+1} ({block_df['date'].min().date()}~{block_df['date'].max().date()})"
        blocks.append((label, block_df.index))
    return blocks


def spearman(sub, score_col, horizon):
    valid = sub.dropna(subset=[score_col, horizon])
    if len(valid) < 10:
        return np.nan
    return round(valid[score_col].corr(valid[horizon], method="spearman"), 3)


def quintile_monotonic(sub, score_col, horizon):
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
    return inversions <= 1, inversions


def main():
    n_blocks = int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_N_BLOCKS
    df = load_raw()

    weights_risk_only = {"추세": 0, "거래량": 0, "모멘텀": 0, "패턴점수": 0, "리스크": 80}
    df["score_risk_only"] = build_weighted_score(df, weights_risk_only)

    blocks = make_blocks(df, n_blocks)

    print("=" * 80)
    print(f"비중첩 블록 교차검증 (K={len(blocks)})")
    print(f"전체 데이터: {len(df)}행  ({df['date'].min().date()} ~ {df['date'].max().date()})")
    print("=" * 80)

    small_blocks = []
    all_rows = []
    for label, idx in blocks:
        test = df.loc[idx]
        train = df.drop(idx)

        if len(test) < MIN_ROWS_PER_BLOCK:
            small_blocks.append(label)

        for h in FORWARD_HORIZONS:
            corr_test = spearman(test, "score_risk_only", h)
            corr_train = spearman(train, "score_risk_only", h)
            mono, inv = quintile_monotonic(test, "score_risk_only", h)
            all_rows.append({
                "블록": label,
                "horizon": h,
                "test_n": len(test),
                "train_n": len(train),
                "corr_test(독립)": corr_test,
                "corr_train(참고용)": corr_train,
                "test_부호": "+" if (not np.isnan(corr_test) and corr_test > 0)
                            else ("-" if not np.isnan(corr_test) else "NA"),
                "test_5분위_단조": mono,
            })

    if small_blocks:
        print(f"\n⚠️ 표본이 {MIN_ROWS_PER_BLOCK}행 미만인 블록 (신뢰도 낮음): {small_blocks}")

    result = pd.DataFrame(all_rows)

    print("\n" + "=" * 80)
    print("블록별 test 상관계수 (블록마다 완전히 독립적인 데이터)")
    print("=" * 80)
    for h in FORWARD_HORIZONS:
        sub = result[result["horizon"] == h]
        print(f"\n[{h}]")
        print(sub[["블록", "test_n", "corr_test(독립)", "test_부호", "test_5분위_단조"]]
              .to_string(index=False))

    print("\n" + "=" * 80)
    print("horizon별 요약 — 독립 블록 K개 중 몇 개가 (+)이고 단조적인가")
    print("=" * 80)
    summary_rows = []
    for h in FORWARD_HORIZONS:
        sub = result[result["horizon"] == h]
        n_valid = (sub["test_부호"] != "NA").sum()
        n_pos = (sub["test_부호"] == "+").sum()
        n_mono_valid = sub["test_5분위_단조"].notna().sum()
        n_mono = (sub["test_5분위_단조"] == True).sum()
        summary_rows.append({
            "horizon": h,
            "양(+) 블록 비율": f"{n_pos}/{n_valid}" if n_valid else "N/A",
            "단조 블록 비율": f"{n_mono}/{n_mono_valid}" if n_mono_valid else "N/A",
            "corr_test 평균": round(sub["corr_test(독립)"].mean(), 3),
            "corr_test 표준편차": round(sub["corr_test(독립)"].std(), 3),
            "corr_test 최소": round(sub["corr_test(독립)"].min(), 3),
            "corr_test 최대": round(sub["corr_test(독립)"].max(), 3),
        })
    summary = pd.DataFrame(summary_rows)
    print(summary.to_string(index=False))

    out_path = OUT_DIR / "backtest_block_cv_summary.csv"
    result.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"\n상세 결과 저장: {out_path}")

    print("\n" + "=" * 80)
    print("📋 읽는 법")
    print("=" * 80)
    print(f"- 이번엔 {len(blocks)}개 블록이 서로 완전히 겹치지 않으므로, '양(+) 블록 비율'이")
    print("  K개 중 K개(예: 4/4)에 가까울수록 우연이 아니라는 증거로서 훨씬 신뢰할 만하다.")
    print("- corr_test 최소값이 여전히 양수(+)라면 → 최악의 블록에서도 신호 방향이 안 깨진 것.")
    print("- corr_test 표준편차가 corr_test 평균보다 훨씬 작으면 → 블록 간 흔들림이 적은 안정적 신호.")
    print("- 블록 개수(K)를 늘리면(예: 6, 8) 블록당 표본이 줄어드는 대신 더 세밀하게 볼 수 있다.")
    print("  단, 블록당 표본이 너무 작아지면(< 60행) 상관계수 자체의 신뢰도가 떨어지니 주의.")


if __name__ == "__main__":
    main()
