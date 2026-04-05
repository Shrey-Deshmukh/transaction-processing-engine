#pragma once
#include <string>
#include <unordered_map>
#include <stdexcept>
#include <sstream>
#include <regex>

using Record = std::unordered_map<std::string, std::string>;

inline Record parseRecordLiteral(const std::string& s){
    Record r;
    
    size_t start = s.find('{');
    size_t end = s.rfind('}');
    if(start == std::string::npos || end == std::string::npos) return r;
    std::string inner = s.substr(start + 1, end - start - 1);

    
    std::vector<std::string> pairs;
    int depth = 0;
    bool inQuote = false;
    std::string cur;
    for(char c : inner){
        if(c == '"') inQuote = !inQuote;
        if(!inQuote && c == ','){
            pairs.push_back(cur);
            cur.clear();
        } else{
            cur += c;
        }
    }
    if(!cur.empty()) pairs.push_back(cur);

    for(auto& pair : pairs){
        size_t colon = pair.find(':');
        if(colon == std::string::npos) continue;
        std::string key = pair.substr(0, colon);
        std::string val = pair.substr(colon + 1);
        
        auto trim = [](std::string& t){
            size_t a = t.find_first_not_of(" \t\r\n");
            size_t b = t.find_last_not_of(" \t\r\n");
            t =(a == std::string::npos) ? "" : t.substr(a, b - a + 1);
        };
        trim(key); trim(val);
        
        if(val.size() >= 2 && val.front() == '"' && val.back() == '"'){
            val = val.substr(1, val.size() - 2);
        }
        r[key] = val;
    }
    return r;
}

inline double getNumeric(const Record& r, const std::string& field){
    auto it = r.find(field);
    if(it == r.end()) throw std::runtime_error("Field not found: " + field);
    return std::stod(it->second);
}

inline void setNumeric(Record& r, const std::string& field, double val){
    
    if(val ==(long long)val){
        r[field] = std::to_string((long long)val);
    } else{
        r[field] = std::to_string(val);
    }
}
