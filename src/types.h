#pragma once
#include <string>
#include <unordered_map>
#include <vector>
#include <chrono>

using Record = std::unordered_map<std::string, std::string>;

using DB = std::unordered_map<std::string, Record>;

enum class CCMode{ OCC, TWO_PL };

struct Stats{
    std::atomic<long long> committed{0};
    std::atomic<long long> aborted{0};       
    std::atomic<long long> lock_retries{0};  
    std::atomic<long long> total_response_us{0};

};

struct TransactionTemplate{
    std::string name;
    std::vector<std::string> input_param_names;
    struct Op{
        enum class Type{ READ, WRITE, ASSIGN, FIELD_SET, COMMIT } type;
        std::string lhs;        
        std::string rhs;        
        std::string field;      
        std::string field_val;  
    };
    std::vector<Op> ops;
};
