#pragma once
#include <string>
#include <vector>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <regex>
#include "record.h"
#include "storage.h"

struct TxnOp{
    enum class Type{
        READ,        
        WRITE,       
        FIELD_READ,  
        FIELD_WRITE, 
    } type;

    std::string var;        
    std::string key_param;  
    std::string src_var;    
    std::string field;      
    std::string expr;       
};

struct TransactionTemplate{
    std::vector<std::string> input_params; 
    std::vector<TxnOp> ops;
};

struct Workload{
    std::vector<TransactionTemplate> transactions;
};

static std::string trim(const std::string& s){
    size_t a = s.find_first_not_of(" \t\r\n");
    if(a == std::string::npos) return "";
    size_t b = s.find_last_not_of(" \t\r\n");
    return s.substr(a, b - a + 1);
}

inline void loadInsertFile(const std::string& filepath, StorageLayer& storage){
    std::ifstream f(filepath);
    if(!f) throw std::runtime_error("Cannot open: " + filepath);

    std::string line;
    bool inInsert = false;
    while(std::getline(f, line)){
        line = trim(line);
        if(line == "INSERT"){ inInsert = true; continue; }
        if(line == "END") break;
        if(!inInsert || line.empty()) continue;

        
        std::regex re(R"(KEY:\s*(\S+),\s*VALUE:\s*(\{.*\}))");
        std::smatch m;
        if(std::regex_search(line, m, re)){
            std::string key = m[1].str();
            
            if(!key.empty() && key.back() == ',') key.pop_back();
            Record rec = parseRecordLiteral(m[2].str());
            storage.put(key, rec);
        }
    }
}

struct FieldExpr{
    std::string var;
    std::string field;
    char op = 0;         
    double rhs_num = 0;
    std::string rhs_var; 
    bool rhs_is_var = false;
    bool rhs_is_fieldref = false;
    std::string rhs_field;
};

inline FieldExpr parseFieldExpr(const std::string& expr){
    FieldExpr fe;
    
    std::regex full(R"((\w+)\[\"(\w+)\"\]\s*([+\-])\s*(\d+(\.\d+)?))");
    std::regex varref(R"((\w+)\[\"(\w+)\"\]\s*([+\-])\s*(\w+)\[\"(\w+)\"\])");
    std::regex simple(R"((\w+)\[\"(\w+)\"\])");

    std::smatch m;
    if(std::regex_search(expr, m, varref)){
        fe.var = m[1]; fe.field = m[2];
        fe.op = m[3].str()[0];
        fe.rhs_is_var = true; fe.rhs_is_fieldref = true;
        fe.rhs_var = m[4]; fe.rhs_field = m[5];
    } else if(std::regex_search(expr, m, full)){
        fe.var = m[1]; fe.field = m[2];
        fe.op = m[3].str()[0];
        fe.rhs_num = std::stod(m[4]);
    } else if(std::regex_search(expr, m, simple)){
        fe.var = m[1]; fe.field = m[2];
        
    }
    return fe;
}

inline Workload loadWorkloadFile(const std::string& filepath){
    Workload wl;
    std::ifstream f(filepath);
    if(!f) throw std::runtime_error("Cannot open: " + filepath);

    std::string line;
    bool inWorkload = false;
    bool inTransaction = false;
    TransactionTemplate cur;

    auto pushTxn = [&](){
        if(!cur.input_params.empty()){
            wl.transactions.push_back(cur);
            cur = TransactionTemplate{};
        }
    };

    while(std::getline(f, line)){
        line = trim(line);
        if(line.empty() || line[0] == '#') continue;

        if(line == "WORKLOAD"){ inWorkload = true; continue; }
        if(!inWorkload) continue;
        if(line == "END"){ pushTxn(); break; }

        
        if(line.rfind("TRANSACTION", 0) == 0){
            pushTxn();
            inTransaction = true;
            
            std::regex re(R"(INPUTS:\s*([\w,\s]+)\))");
            std::smatch m;
            if(std::regex_search(line, m, re)){
                std::stringstream ss(m[1].str());
                std::string tok;
                while(std::getline(ss, tok, ',')){
                    tok = trim(tok);
                    if(!tok.empty()) cur.input_params.push_back(tok);
                }
            }
            continue;
        }

        if(!inTransaction) continue;
        if(line == "BEGIN" || line == "COMMIT") continue;

        
       {
            std::regex re(R"((\w+)\s*=\s*READ\((\w+)\))");
            std::smatch m;
            if(std::regex_match(line, m, re)){
                TxnOp op;
                op.type = TxnOp::Type::READ;
                op.var = m[1];
                op.key_param = m[2];
                cur.ops.push_back(op);
                continue;
            }
        }

        
       {
            std::regex re(R"(WRITE\((\w+),\s*(\w+)\))");
            std::smatch m;
            if(std::regex_match(line, m, re)){
                TxnOp op;
                op.type = TxnOp::Type::WRITE;
                op.key_param = m[1];
                op.src_var = m[2];
                cur.ops.push_back(op);
                continue;
            }
        }

        
       {
            std::regex re(R"((\w+)\[\"(\w+)\"\]\s*=\s*(.+))");
            std::smatch m;
            if(std::regex_match(line, m, re)){
                TxnOp op;
                op.type = TxnOp::Type::FIELD_WRITE;
                op.var = m[1];
                op.field = m[2];
                op.expr = trim(m[3]);
                cur.ops.push_back(op);
                continue;
            }
        }

        
       {
            std::regex re(R"((\w+)\s*=\s*(\w+)\[\"(\w+)\"\])");
            std::smatch m;
            if(std::regex_match(line, m, re)){
                TxnOp op;
                op.type = TxnOp::Type::FIELD_READ;
                op.var = m[1];
                op.src_var = m[2];
                op.field = m[3];
                cur.ops.push_back(op);
                continue;
            }
        }
    }

    return wl;
}
