#!/bin/bash
# run_experiments.sh
# Sweeps threads and contention for both OCC and 2PL, for both workloads

set -e

BINARY="./build/txn_processor"
WORKLOAD1_INPUT="workload1/input1.txt"
WORKLOAD1_WORKLOAD="workload1/workload1.txt"
WORKLOAD2_INPUT="workload2/input2.txt"
WORKLOAD2_WORKLOAD="workload2/workload2.txt"

TXNS_PER_THREAD=300     # Reduce if experiments take too long
FIXED_THREADS=8         # For contention sweep
FIXED_HOT_PROB=0.5      # For thread sweep
HOT_SIZE=10

THREADS=(1 2 4 8 16)
HOT_PROBS=(0.0 0.2 0.4 0.6 0.8 1.0)
MODES=("occ" "2pl")

mkdir -p results

echo "=========================================="
echo "CS 223 Transaction Processing Experiments"
echo "=========================================="

run_experiment(){
    local wl_name=$1
    local input=$2
    local workload=$3
    local mode=$4
    local threads=$5
    local hot_prob=$6
    local db_path="./rocksdb_${wl_name}_${mode}_t${threads}_p${hot_prob}"
    local output="results/${wl_name}_results.csv"

    echo "  [${wl_name}] mode=${mode} threads=${threads} hot_prob=${hot_prob}"
    rm -rf "$db_path"
    $BINARY \
        --input "$input" \
        --workload "$workload" \
        --mode "$mode" \
        --threads "$threads" \
        --txns "$TXNS_PER_THREAD" \
        --hot-prob "$hot_prob" \
        --hot-size "$HOT_SIZE" \
        --db-path "$db_path" \
        --output "$output"
    rm -rf "$db_path"
}

echo ""
echo "--- Experiment 1: Throughput vs Threads(fixed contention p=${FIXED_HOT_PROB}) ---"
for wl_name in "workload1" "workload2"; do
    input="workload${wl_name: -1}/input${wl_name: -1}.txt"
    workload="workload${wl_name: -1}/${wl_name}.txt"
    for mode in "${MODES[@]}"; do
        for threads in "${THREADS[@]}"; do
            run_experiment "$wl_name" "$input" "$workload" "$mode" "$threads" "$FIXED_HOT_PROB"
        done
    done
done

echo ""
echo "--- Experiment 2: Throughput vs Contention(fixed threads=${FIXED_THREADS}) ---"
for wl_name in "workload1" "workload2"; do
    input="workload${wl_name: -1}/input${wl_name: -1}.txt"
    workload="workload${wl_name: -1}/${wl_name}.txt"
    for mode in "${MODES[@]}"; do
        for hot_prob in "${HOT_PROBS[@]}"; do
            run_experiment "$wl_name" "$input" "$workload" "$mode" "$FIXED_THREADS" "$hot_prob"
        done
    done
done

echo ""
echo "=========================================="
echo "All experiments complete. Results in results/"
echo "Run: python3 plot_results.py"
echo "=========================================="
