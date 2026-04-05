#include <iostream>
#include <string>
#include <stdexcept>
#include <filesystem>
#include <iomanip>
#include <algorithm>
#include <numeric>
#include <fstream>
#include "src/ccmode.h"
#include "src/record.h"
#include "src/storage.h"
#include "src/parser.h"
#include "src/executor.h"
#include "src/occ.h"
#include "src/twopl.h"
#include "src/runner.h"

void printUsage(const char* prog){
    std::cout << "Usage: " << prog << " [OPTIONS]\n"
              << "\nRequired:\n"
              << "  --input <file>      Path to INSERT data file\n"
              << "  --workload <file>   Path to WORKLOAD file\n"
              << "  --mode <occ|2pl>    Concurrency control mode\n"
              << "\nOptional:\n"
              << "  --threads <N>       Number of worker threads(default: 4)\n"
              << "  --txns <N>          Transactions per thread(default: 500)\n"
              << "  --hot-prob <0-1>    Hotset selection probability(default: 0.5)\n"
              << "  --hot-size <N>      Number of hot keys(default: 10)\n"
              << "  --db-path <dir>     RocksDB directory(default: ./rocksdb_data)\n"
              << "  --output <file>     Write CSV stats to file\n"
              << "\nExample:\n"
              << "  " << prog << " --input workload1/input1.txt --workload workload1/workload1.txt"
              << " --mode occ --threads 8 --hot-prob 0.7\n";
}

void printStats(const RunStats& stats, const RunConfig& cfg, const Workload& wl){
    std::cout << "\n=== Results ===\n";
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "Mode:              " <<(cfg.mode == CCMode::OCC ? "OCC" : "Conservative 2PL") << "\n";
    std::cout << "Threads:           " << cfg.num_threads << "\n";
    std::cout << "Txns/thread:       " << cfg.txns_per_thread << "\n";
    std::cout << "Hot probability:   " << cfg.hot_probability << "\n";
    std::cout << "Hot set size:      " << cfg.hot_set_size << "\n";
    std::cout << "Committed:         " << stats.committed << "\n";
    std::cout << "Retries:           " << stats.total_retries << "\n";
    double retry_pct =(stats.committed + stats.total_retries > 0)
        ? 100.0 * stats.total_retries /(stats.committed + stats.total_retries) : 0;
    std::cout << "Retry rate:        " << retry_pct << "%\n";
    std::cout << "Throughput:        " << stats.throughput_tps << " txns/sec\n";
    std::cout << "Avg response time: " << stats.avg_response_ms << " ms\n";

    
    if(!stats.response_times_us.empty()){
        std::vector<long long> sorted_rt = stats.response_times_us;
        std::sort(sorted_rt.begin(), sorted_rt.end());
        size_t n = sorted_rt.size();
        std::cout << "Response time p50: " << sorted_rt[n * 50 / 100] / 1000.0 << " ms\n";
        std::cout << "Response time p95: " << sorted_rt[n * 95 / 100] / 1000.0 << " ms\n";
        std::cout << "Response time p99: " << sorted_rt[n * 99 / 100] / 1000.0 << " ms\n";
    }

    
    if(wl.transactions.size() > 1){
        std::cout << "\n--- Per Transaction Type ---\n";
        for(size_t i = 0; i < wl.transactions.size(); i++){
            auto& ts = stats.per_type[i];
            double avg_ms = ts.committed > 0 ?(ts.total_response_us / 1000.0) / ts.committed : 0;
            std::cout << "  Txn type " << i << ": committed=" << ts.committed
                      << ", avg_response=" << avg_ms << " ms\n";
        }
    }
}

void writeCSV(const std::string& path, const RunStats& stats, const RunConfig& cfg){
    bool new_file = !std::filesystem::exists(path);
    std::ofstream f(path, std::ios::app);
    if(new_file){
        f << "mode,threads,hot_prob,hot_size,txns_per_thread,committed,retries,retry_pct,"
          << "throughput_tps,avg_response_ms,p50_ms,p95_ms,p99_ms\n";
    }
    double retry_pct =(stats.committed + stats.total_retries > 0)
        ? 100.0 * stats.total_retries /(stats.committed + stats.total_retries) : 0;

    double p50 = 0, p95 = 0, p99 = 0;
    if(!stats.response_times_us.empty()){
        std::vector<long long> sorted_rt = stats.response_times_us;
        std::sort(sorted_rt.begin(), sorted_rt.end());
        size_t n = sorted_rt.size();
        p50 = sorted_rt[n * 50 / 100] / 1000.0;
        p95 = sorted_rt[n * 95 / 100] / 1000.0;
        p99 = sorted_rt[n * 99 / 100] / 1000.0;
    }

    f << std::fixed << std::setprecision(4)
      <<(cfg.mode == CCMode::OCC ? "OCC" : "2PL") << ","
      << cfg.num_threads << ","
      << cfg.hot_probability << ","
      << cfg.hot_set_size << ","
      << cfg.txns_per_thread << ","
      << stats.committed << ","
      << stats.total_retries << ","
      << retry_pct << ","
      << stats.throughput_tps << ","
      << stats.avg_response_ms << ","
      << p50 << "," << p95 << "," << p99 << "\n";

    std::cout << "Stats appended to: " << path << "\n";
}

int main(int argc, char* argv[]){
    if(argc < 2){ printUsage(argv[0]); return 1; }

    std::string input_file, workload_file, db_path = "./rocksdb_data", output_csv;
    RunConfig config;
    std::string mode_str = "occ";

    for(int i = 1; i < argc; i++){
        std::string arg = argv[i];
        if(arg == "--help" || arg == "-h"){ printUsage(argv[0]); return 0; }
        else if(arg == "--input"    && i+1 < argc) input_file    = argv[++i];
        else if(arg == "--workload" && i+1 < argc) workload_file = argv[++i];
        else if(arg == "--mode"     && i+1 < argc) mode_str      = argv[++i];
        else if(arg == "--threads"  && i+1 < argc) config.num_threads   = std::stoi(argv[++i]);
        else if(arg == "--txns"     && i+1 < argc) config.txns_per_thread = std::stoi(argv[++i]);
        else if(arg == "--hot-prob" && i+1 < argc) config.hot_probability = std::stod(argv[++i]);
        else if(arg == "--hot-size" && i+1 < argc) config.hot_set_size   = std::stoi(argv[++i]);
        else if(arg == "--db-path"  && i+1 < argc) db_path  = argv[++i];
        else if(arg == "--output"   && i+1 < argc) output_csv = argv[++i];
        else{ std::cerr << "Unknown argument: " << arg << "\n"; return 1; }
    }

    if(input_file.empty() || workload_file.empty()){
        std::cerr << "Error: --input and --workload are required.\n";
        printUsage(argv[0]); return 1;
    }

    if(mode_str == "occ")       config.mode = CCMode::OCC;
    else if(mode_str == "2pl")  config.mode = CCMode::TWO_PL;
    else{ std::cerr << "Unknown mode: " << mode_str << ". Use occ or 2pl.\n"; return 1; }

    try{
        
        std::cout << "Opening database at: " << db_path << "\n";
        StorageLayer storage(db_path);

        
        std::cout << "Loading data from: " << input_file << "\n";
        loadInsertFile(input_file, storage);

        auto all_keys = storage.getAllKeys();
        std::cout << "Loaded " << all_keys.size() << " keys.\n";

        
        std::cout << "Parsing workload: " << workload_file << "\n";
        Workload wl = loadWorkloadFile(workload_file);
        std::cout << "Found " << wl.transactions.size() << " transaction template(s).\n";

        
        std::cout << "\nRunning workload(" << config.num_threads << " threads, "
                  << config.txns_per_thread << " txns/thread, "
                  << "hot_prob=" << config.hot_probability
                  << ", hot_size=" << config.hot_set_size << ")...\n";

        auto stats = runWorkload(wl, storage, config);

        printStats(*stats, config, wl);

        if(!output_csv.empty()){
            writeCSV(output_csv, *stats, config);
        }

    } catch(const std::exception& e){
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }

    return 0;
}
