#!/usr/bin/env python3
"""
plot_results.py - Generate all required CS 223 plots from experiment CSV data.

Produces:
  Per workload:
  1. Throughput vs Threads(OCC vs 2PL)
  2. Throughput vs Contention(OCC vs 2PL)
  3. Avg Response Time vs Threads
  4. Avg Response Time vs Contention
  5. Retry rate vs Contention(aborts/retries analysis)
  6. Response time distribution(p50/p95/p99)
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
import numpy as np
import os
import sys

matplotlib.rcParams.update({'font.size': 12, 'figure.dpi': 150})
os.makedirs("plots", exist_ok=True)

FIXED_THREADS = 8
FIXED_HOT_PROB = 0.5

def load_data(filename):
    if not os.path.exists(filename):
        print(f"Warning:{filename} not found, skipping.")
        return None
    df = pd.read_csv(filename)
    return df

def plot_throughput_vs_threads(df, wl_name):
    fig, ax = plt.subplots(figsize=(7, 5))
    sub = df[df['hot_prob'] == FIXED_HOT_PROB]
    for mode, color, marker in [('OCC', 'royalblue', 'o'),('2PL', 'tomato', 's')]:
        d = sub[sub['mode'] == mode].groupby('threads')['throughput_tps'].mean().reset_index()
        ax.plot(d['threads'], d['throughput_tps'], label=mode, color=color,
                marker=marker, linewidth=2, markersize=7)
    ax.set_xlabel('Number of Threads')
    ax.set_ylabel('Throughput(txns/sec)')
    ax.set_title(f'{wl_name}: Throughput vs Threads\n(contention p={FIXED_HOT_PROB})')
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    path = f"plots/{wl_name}_throughput_vs_threads.png"
    fig.savefig(path)
    plt.close(fig)
    print(f"Saved:{path}")

def plot_throughput_vs_contention(df, wl_name):
    fig, ax = plt.subplots(figsize=(7, 5))
    sub = df[df['threads'] == FIXED_THREADS]
    for mode, color, marker in [('OCC', 'royalblue', 'o'),('2PL', 'tomato', 's')]:
        d = sub[sub['mode'] == mode].groupby('hot_prob')['throughput_tps'].mean().reset_index()
        ax.plot(d['hot_prob'], d['throughput_tps'], label=mode, color=color,
                marker=marker, linewidth=2, markersize=7)
    ax.set_xlabel('Contention Probability(p)')
    ax.set_ylabel('Throughput(txns/sec)')
    ax.set_title(f'{wl_name}: Throughput vs Contention\n({FIXED_THREADS} threads)')
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    path = f"plots/{wl_name}_throughput_vs_contention.png"
    fig.savefig(path)
    plt.close(fig)
    print(f"Saved:{path}")

def plot_response_vs_threads(df, wl_name):
    fig, ax = plt.subplots(figsize=(7, 5))
    sub = df[df['hot_prob'] == FIXED_HOT_PROB]
    for mode, color, marker in [('OCC', 'royalblue', 'o'),('2PL', 'tomato', 's')]:
        d = sub[sub['mode'] == mode].groupby('threads')['avg_response_ms'].mean().reset_index()
        ax.plot(d['threads'], d['avg_response_ms'], label=mode, color=color,
                marker=marker, linewidth=2, markersize=7)
    ax.set_xlabel('Number of Threads')
    ax.set_ylabel('Avg Response Time(ms)')
    ax.set_title(f'{wl_name}: Response Time vs Threads\n(contention p={FIXED_HOT_PROB})')
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    path = f"plots/{wl_name}_response_vs_threads.png"
    fig.savefig(path)
    plt.close(fig)
    print(f"Saved:{path}")

def plot_response_vs_contention(df, wl_name):
    fig, ax = plt.subplots(figsize=(7, 5))
    sub = df[df['threads'] == FIXED_THREADS]
    for mode, color, marker in [('OCC', 'royalblue', 'o'),('2PL', 'tomato', 's')]:
        d = sub[sub['mode'] == mode].groupby('hot_prob')['avg_response_ms'].mean().reset_index()
        ax.plot(d['hot_prob'], d['avg_response_ms'], label=mode, color=color,
                marker=marker, linewidth=2, markersize=7)
    ax.set_xlabel('Contention Probability(p)')
    ax.set_ylabel('Avg Response Time(ms)')
    ax.set_title(f'{wl_name}: Response Time vs Contention\n({FIXED_THREADS} threads)')
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    path = f"plots/{wl_name}_response_vs_contention.png"
    fig.savefig(path)
    plt.close(fig)
    print(f"Saved:{path}")

def plot_retry_vs_contention(df, wl_name):
    fig, ax = plt.subplots(figsize=(7, 5))
    sub = df[df['threads'] == FIXED_THREADS]
    for mode, color, marker in [('OCC', 'royalblue', 'o'),('2PL', 'tomato', 's')]:
        d = sub[sub['mode'] == mode].groupby('hot_prob')['retry_pct'].mean().reset_index()
        ax.plot(d['hot_prob'], d['retry_pct'], label=mode, color=color,
                marker=marker, linewidth=2, markersize=7)
    ax.set_xlabel('Contention Probability(p)')
    ax.set_ylabel('Retry/Abort Rate(%)')
    ax.set_title(f'{wl_name}: Retry Rate vs Contention\n({FIXED_THREADS} threads)')
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    path = f"plots/{wl_name}_retries_vs_contention.png"
    fig.savefig(path)
    plt.close(fig)
    print(f"Saved:{path}")

def plot_response_distribution(df, wl_name):
    """Plot p50/p95/p99 response time distribution for a representative config."""
    fig, ax = plt.subplots(figsize=(8, 5))
    sub = df[(df['threads'] == FIXED_THREADS) &(df['hot_prob'] == FIXED_HOT_PROB)]

    modes = ['OCC', '2PL']
    percentiles = ['p50_ms', 'p95_ms', 'p99_ms']
    labels = ['p50', 'p95', 'p99']
    x = np.arange(len(labels))
    width = 0.35
    colors = ['royalblue', 'tomato']

    for i, mode in enumerate(modes):
        d = sub[sub['mode'] == mode][percentiles].mean()
        vals = [d[p] for p in percentiles]
        ax.bar(x + i * width, vals, width, label=mode, color=colors[i], alpha=0.8)

    ax.set_xlabel('Percentile')
    ax.set_ylabel('Response Time(ms)')
    ax.set_title(f'{wl_name}: Response Time Distribution\n(threads={FIXED_THREADS}, p={FIXED_HOT_PROB})')
    ax.set_xticks(x + width / 2)
    ax.set_xticklabels(labels)
    ax.legend()
    ax.grid(True, alpha=0.3, axis='y')
    fig.tight_layout()
    path = f"plots/{wl_name}_response_distribution.png"
    fig.savefig(path)
    plt.close(fig)
    print(f"Saved:{path}")

def main():
    for wl_name in ['workload1', 'workload2']:
        csv_path = f"results/{wl_name}_results.csv"
        df = load_data(csv_path)
        if df is None:
            continue
        print(f"\n==={wl_name} ===")
        print(f"  Rows:{len(df)}, Modes:{df['mode'].unique()}")

        plot_throughput_vs_threads(df, wl_name)
        plot_throughput_vs_contention(df, wl_name)
        plot_response_vs_threads(df, wl_name)
        plot_response_vs_contention(df, wl_name)
        plot_retry_vs_contention(df, wl_name)
        plot_response_distribution(df, wl_name)

    print("\nAll plots saved to plots/")

if __name__ == '__main__':
    main()
