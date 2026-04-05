#pragma once
#include <string>
#include <unordered_map>
#include <shared_mutex>
#include <stdexcept>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include "record.h"

class StorageLayer{
public:
    StorageLayer(const std::string& db_path){
        rocksdb::Options options;
        options.create_if_missing = true;
        options.error_if_exists = false;

        rocksdb::Status s = rocksdb::DB::Open(options, db_path, &db_);
        if(!s.ok()){
            throw std::runtime_error("Failed to open RocksDB: " + s.ToString());
        }
    }

    ~StorageLayer(){
        delete db_;
    }

    
    void put(const std::string& key, const Record& record){
        std::string serialized = serialize(record);
        rocksdb::WriteOptions wo;
        auto s = db_->Put(wo, key, serialized);
        if(!s.ok()) throw std::runtime_error("RocksDB Put failed: " + s.ToString());
    }

    
    bool get(const std::string& key, Record& out) const{
        std::string value;
        rocksdb::ReadOptions ro;
        auto s = db_->Get(ro, key, &value);
        if(s.IsNotFound()) return false;
        if(!s.ok()) throw std::runtime_error("RocksDB Get failed: " + s.ToString());
        out = deserialize(value);
        return true;
    }

    
    std::vector<std::string> getAllKeys() const{
        std::vector<std::string> keys;
        rocksdb::ReadOptions ro;
        auto* iter = db_->NewIterator(ro);
        for(iter->SeekToFirst(); iter->Valid(); iter->Next()){
            std::string k = iter->key().ToString();
            keys.push_back(k);
        }
        delete iter;
        return keys;
    }

private:
    rocksdb::DB* db_ = nullptr;

    
    static std::string serialize(const Record& r){
        std::string out;
        for(auto& [k, v] : r){
            out += k + "=" + v + "\n";
        }
        return out;
    }

    static Record deserialize(const std::string& s){
        Record r;
        size_t pos = 0;
        while(pos < s.size()){
            size_t nl = s.find('\n', pos);
            if(nl == std::string::npos) nl = s.size();
            std::string line = s.substr(pos, nl - pos);
            size_t eq = line.find('=');
            if(eq != std::string::npos){
                r[line.substr(0, eq)] = line.substr(eq + 1);
            }
            pos = nl + 1;
        }
        return r;
    }
};
