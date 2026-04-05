#pragma once
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>
#include <algorithm>
#include <stdexcept>
#include "storage.h"
#include "executor.h"

struct LockEntry{
    std::mutex mtx;
    bool held = false;
    std::thread::id holder;
};

class TwoPLManager{
public:
    explicit TwoPLManager(StorageLayer& storage) : storage_(storage){}

    
    
    int runTransaction(const TransactionTemplate& tmpl,
                       const std::unordered_map<std::string, std::string>& params,
                       long long& response_us_out){

        auto start = std::chrono::steady_clock::now();

        
        std::vector<std::string> keys = getTransactionKeys(tmpl, params);

        
        std::vector<std::string> sorted_keys = keys;
        std::sort(sorted_keys.begin(), sorted_keys.end());

        int retries = 0;

        
        
        uint64_t my_ts = next_timestamp_.fetch_add(1);

        while(true){
            std::vector<std::string> held_keys;
            bool acquired_all = true;

            for(auto& key : sorted_keys){
                LockEntry& entry = getLockEntry(key);

                
                bool got_lock = false;

                
                bool lock_available = entry.mtx.try_lock();
                if(lock_available){
                    entry.held = true;
                    entry.holder = std::this_thread::get_id();
                    held_keys.push_back(key);
                    got_lock = true;
                }

                if(!got_lock){
                    
                    for(auto& hk : held_keys){
                        LockEntry& he = getLockEntry(hk);
                        he.held = false;
                        he.mtx.unlock();
                    }
                    held_keys.clear();
                    acquired_all = false;
                    break;
                }
            }

            if(acquired_all){
                
                TxnContext ctx;
                ctx.params = params;

                bool ok = executeTransactionOps(tmpl, ctx,
                    [&](const std::string& key, Record& out) -> bool{
                        return storage_.get(key, out);
                    });

                if(ok){
                    
                    for(auto& [key, new_val] : ctx.write_set){
                        storage_.put(key, new_val);
                    }
                }

                
                for(auto& hk : sorted_keys){
                    LockEntry& he = getLockEntry(hk);
                    he.held = false;
                    he.mtx.unlock();
                }

                auto end = std::chrono::steady_clock::now();
                response_us_out = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
                return retries;
            }

            
            retries++;

            
            
            
            
            int base_us = 200;
            
            
            int wait_us = base_us +(int)(my_ts % 500);
            
            wait_us +=(rand() % 100);

            
            
            if(retries > 20){
                wait_us = 50;
            }

            std::this_thread::sleep_for(std::chrono::microseconds(wait_us));
        }
    }

private:
    StorageLayer& storage_;
    std::atomic<uint64_t> next_timestamp_{0};

    
    
    std::mutex table_mutex_;
    std::unordered_map<std::string, std::unique_ptr<LockEntry>> lock_table_;

    LockEntry& getLockEntry(const std::string& key){
        std::lock_guard<std::mutex> tg(table_mutex_);
        auto it = lock_table_.find(key);
        if(it == lock_table_.end()){
            lock_table_[key] = std::make_unique<LockEntry>();
            return *lock_table_[key];
        }
        return *it->second;
    }
};
