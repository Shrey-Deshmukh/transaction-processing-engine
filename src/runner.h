#pragma once
#include <vector>
#include <thread>
#include <atomic>
#include <random>
#include <functional>
#include <chrono>
#include <mutex>
#include <algorithm>
#include "ccmode.h"
#include "parser.h"
#include "storage.h"
#include "occ.h"
#include "twopl.h"

struct RunStats{
    long long committed = 0;
    long long total_retries = 0;
    long long total_response_us = 0;
    double throughput_tps = 0;
    double avg_response_ms = 0;
    std::vector<long long> response_times_us;
    mutable std::mutex rt_mutex;

    
    RunStats() = default;
    RunStats(const RunStats&) = delete;
    RunStats& operator=(const RunStats&) = delete;

    
    struct TxnTypeStats{
        long long committed = 0;
        long long total_response_us = 0;
        std::vector<long long> response_times_us;
    };
    std::vector<TxnTypeStats> per_type;
};

struct RunConfig{
    int num_threads = 4;
    int txns_per_thread = 500;
    double hot_probability = 0.5;
    int hot_set_size = 10;
    CCMode mode = CCMode::OCC;
};

class KeySelector{
public:
    KeySelector(const std::vector<std::string>& all_keys, int hot_size, double hot_prob, unsigned seed)
        : all_keys_(all_keys), hot_prob_(hot_prob), rng_(seed){
        hot_size = std::min(hot_size,(int)all_keys.size());
        
        hot_keys_.assign(all_keys.begin(), all_keys.begin() + hot_size);
    }

    std::string selectKey(const std::string& prefix_filter = ""){
        std::uniform_real_distribution<double> prob_dist(0.0, 1.0);
        bool use_hot =(prob_dist(rng_) < hot_prob_) && !hot_keys_.empty();

        const std::vector<std::string>& pool = use_hot ? hot_keys_ : all_keys_;

        
        if(!prefix_filter.empty()){
            std::vector<std::string> filtered;
            for(auto& k : pool){
                if(k.rfind(prefix_filter, 0) == 0) filtered.push_back(k);
            }
            if(!filtered.empty()){
                std::uniform_int_distribution<int> idx(0, filtered.size() - 1);
                return filtered[idx(rng_)];
            }
        }

        std::uniform_int_distribution<int> idx(0, pool.size() - 1);
        return pool[idx(rng_)];
    }

    
    std::string selectKeyWithPrefix(const std::string& prefix){
        std::vector<std::string> matching;
        for(auto& k : all_keys_){
            if(k.rfind(prefix, 0) == 0) matching.push_back(k);
        }
        if(matching.empty()) return all_keys_[0];
        std::uniform_int_distribution<int> idx(0, matching.size() - 1);
        return matching[idx(rng_)];
    }

private:
    const std::vector<std::string>& all_keys_;
    std::vector<std::string> hot_keys_;
    double hot_prob_;
    std::mt19937 rng_;
};

inline std::unordered_map<std::string, std::string> buildParams(
    const TransactionTemplate& tmpl,
    KeySelector& selector,
    const std::vector<std::string>& all_keys){

    std::unordered_map<std::string, std::string> params;

    
    
    
    auto inferPrefix = [](const std::string& param) -> std::string{
        // 1. Handle Workload 1(Banking)
        // Maps FROM_KEY and TO_KEY to Account records(e.g., "A_15")
        if(param == "FROM_KEY" || param == "TO_KEY"){
            return "A_";
        }
        
        // 2. Handle Workload 2(TPC-C) dynamically
        // Finds "_KEY" and uses the letters before it.
        // Example: "W_KEY" -> "W_", "S_KEY_1" -> "S_", "D_KEY" -> "D_"
        size_t key_pos = param.find("_KEY");
        if(key_pos != std::string::npos && key_pos > 0){
            return param.substr(0, key_pos) + "_"; 
        }
        
        // Fallback: if we don't know, just return empty(picks any random key)
        return ""; 
    };
    
    std::unordered_set<std::string> used;

    for(auto& pname : tmpl.input_params){
        std::string prefix = inferPrefix(pname);
        std::string key;
        int attempts = 0;
        do{
            if(prefix.empty()){
                key = selector.selectKey();
            } else{
                key = selector.selectKeyWithPrefix(prefix);
            }
            attempts++;
        } while(used.count(key) && attempts < 20);
        used.insert(key);
        params[pname] = key;
    }

    return params;
}

template<typename Manager>
void workerThread(
    int thread_id,
    const Workload& workload,
    const std::vector<std::string>& all_keys,
    const RunConfig& config,
    Manager& mgr,
    RunStats& stats){

    KeySelector selector(all_keys, config.hot_set_size, config.hot_probability,
                         std::random_device{}() + thread_id);

    std::mt19937 txn_rng(std::random_device{}() + thread_id * 31337);
    int num_txn_types = workload.transactions.size();

    long long local_committed = 0;
    long long local_retries = 0;
    std::vector<long long> local_rt;
    local_rt.reserve(config.txns_per_thread);

    
    std::vector<long long> type_committed(num_txn_types, 0);
    std::vector<long long> type_response_us(num_txn_types, 0);
    std::vector<std::vector<long long>> type_rt(num_txn_types);

    for(int i = 0; i < config.txns_per_thread; i++){
        
        int txn_type = txn_rng() % num_txn_types;
        const TransactionTemplate& tmpl = workload.transactions[txn_type];

        auto params = buildParams(tmpl, selector, all_keys);

        long long resp_us = 0;
        int retries = mgr.runTransaction(tmpl, params, resp_us);

        local_committed++;
        local_retries += retries;
        local_rt.push_back(resp_us);

        type_committed[txn_type]++;
        type_response_us[txn_type] += resp_us;
        type_rt[txn_type].push_back(resp_us);
    }

    
   {
        std::lock_guard<std::mutex> lg(stats.rt_mutex);
        stats.committed += local_committed;
        stats.total_retries += local_retries;
        for(auto rt : local_rt){
            stats.total_response_us += rt;
            stats.response_times_us.push_back(rt);
        }
        for(int t = 0; t < num_txn_types; t++){
            stats.per_type[t].committed += type_committed[t];
            stats.per_type[t].total_response_us += type_response_us[t];
            for(auto rt : type_rt[t]) stats.per_type[t].response_times_us.push_back(rt);
        }
    }
}

inline std::unique_ptr<RunStats> runWorkload(
    const Workload& workload,
    StorageLayer& storage,
    const RunConfig& config){

    auto stats = std::make_unique<RunStats>();
    stats->per_type.resize(workload.transactions.size());

    std::vector<std::string> all_keys = storage.getAllKeys();
    if(all_keys.empty()) throw std::runtime_error("Database is empty. Load data first.");

    
    std::shuffle(all_keys.begin(), all_keys.end(), std::mt19937(42));

    auto wall_start = std::chrono::steady_clock::now();

    if(config.mode == CCMode::OCC){
        OCCManager mgr(storage);
        std::vector<std::thread> threads;
        for(int t = 0; t < config.num_threads; t++){
            threads.emplace_back(workerThread<OCCManager>,
                t, std::cref(workload), std::cref(all_keys),
                std::cref(config), std::ref(mgr), std::ref(*stats));
        }
        for(auto& th : threads) th.join();
    } else{
        TwoPLManager mgr(storage);
        std::vector<std::thread> threads;
        for(int t = 0; t < config.num_threads; t++){
            threads.emplace_back(workerThread<TwoPLManager>,
                t, std::cref(workload), std::cref(all_keys),
                std::cref(config), std::ref(mgr), std::ref(*stats));
        }
        for(auto& th : threads) th.join();
    }

    auto wall_end = std::chrono::steady_clock::now();
    double elapsed_s = std::chrono::duration<double>(wall_end - wall_start).count();

    stats->throughput_tps = stats->committed / elapsed_s;
    stats->avg_response_ms =(stats->committed > 0)
        ?(stats->total_response_us / 1000.0) / stats->committed
        : 0;

    return stats;
}
