#pragma once
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <atomic>
#include <chrono>
#include <thread>
#include "storage.h"
#include "executor.h"

class OCCManager{
public:
    explicit OCCManager(StorageLayer& storage) : storage_(storage){}

    
    
    int runTransaction(const TransactionTemplate& tmpl,
                       const std::unordered_map<std::string, std::string>& params,
                       long long& response_us_out){

        int retries = 0;
        while(true){
            auto start = std::chrono::steady_clock::now();

            TxnContext ctx;
            ctx.params = params;
            
            bool ok = executeTransactionOps(tmpl, ctx,
                [&](const std::string& key, Record& out) -> bool{
                    return storage_.get(key, out);
                });

            if(!ok){
                
                retries++;
                std::this_thread::sleep_for(std::chrono::microseconds(100 + retries * 50));
                continue;
            }

            
            
          {
                std::unique_lock<std::mutex> val_lock(validation_mutex_);

                bool valid = true;
                for(auto& [key, read_val] : ctx.read_set){
                    Record current;
                    if(!storage_.get(key, current)){ valid = false; break; }
                    
                    if(current != read_val){ valid = false; break; }
                }

                if(valid){
                    
                    for(auto& [key, new_val] : ctx.write_set){
                        storage_.put(key, new_val);
                    }
                    
                    auto end = std::chrono::steady_clock::now();
                    response_us_out = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
                    return retries;
                }
                
            }

            
            retries++;
            
            int backoff_us = 100 *(1 << std::min(retries, 8));
            std::this_thread::sleep_for(std::chrono::microseconds(backoff_us));
        }
    }

private:
    StorageLayer& storage_;
    std::mutex validation_mutex_; 
};
