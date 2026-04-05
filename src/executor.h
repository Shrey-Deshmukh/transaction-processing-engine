#pragma once
#include <string>
#include <unordered_map>
#include <stdexcept>
#include <regex>
#include <cmath>
#include "record.h"
#include "parser.h"

struct ExprResult{
    double value = 0;
};

inline ExprResult evalExpr(const std::string& expr,
                            const std::unordered_map<std::string, Record>& vars,
                            const std::unordered_map<std::string, double>& scalars){
    ExprResult res;
  {
        std::regex re(R"((\w+)\[\"(\w+)\"\]\s*([+\-\*])\s*(\d+(\.\d+)?))");
        std::smatch m;
        if(std::regex_match(expr, m, re)){
            auto it = vars.find(m[1]);
            if(it == vars.end()) throw std::runtime_error("Unknown var: " + m[1].str());
            double base = getNumeric(it->second, m[2]);
            double rhs = std::stod(m[4]);
            char op = m[3].str()[0];
            res.value =(op == '+') ? base + rhs :(op == '-') ? base - rhs : base * rhs;
            return res;
        }
    }
  {
        std::regex re(R"((\w+)\[\"(\w+)\"\]\s*([+\-\*])\s*(\w+)\[\"(\w+)\"\])");
        std::smatch m;
        if(std::regex_match(expr, m, re)){
            auto it1 = vars.find(m[1]); auto it2 = vars.find(m[4]);
            if(it1 == vars.end()) throw std::runtime_error("Unknown var: " + m[1].str());
            if(it2 == vars.end()) throw std::runtime_error("Unknown var: " + m[4].str());
            double base = getNumeric(it1->second, m[2]);
            double rhs  = getNumeric(it2->second, m[5]);
            char op = m[3].str()[0];
            res.value =(op == '+') ? base + rhs :(op == '-') ? base - rhs : base * rhs;
            return res;
        }
    }
  {
        std::regex re(R"((\w+)\[\"(\w+)\"\])");
        std::smatch m;
        if(std::regex_match(expr, m, re)){
            auto it = vars.find(m[1]);
            if(it == vars.end()) throw std::runtime_error("Unknown var: " + m[1].str());
            res.value = getNumeric(it->second, m[2]);
            return res;
        }
    }
  {
        std::regex re(R"((\w+)\s*([+\-\*])\s*(\d+(\.\d+)?))");
        std::smatch m;
        if(std::regex_match(expr, m, re)){
            auto it = scalars.find(m[1]);
            if(it != scalars.end()){
                double base = it->second;
                double rhs = std::stod(m[3]);
                char op = m[2].str()[0];
                res.value =(op == '+') ? base + rhs :(op == '-') ? base - rhs : base * rhs;
                return res;
            }
        }
    }
  {
        std::regex re(R"((\w+)\s*([+\-\*])\s*(\w+))");
        std::smatch m;
        if(std::regex_match(expr, m, re)){
            auto it1 = scalars.find(m[1]);
            auto it2 = scalars.find(m[3]);
            if(it1 != scalars.end() && it2 != scalars.end()){
                char op = m[2].str()[0];
                res.value =(op == '+') ? it1->second + it2->second
                          :(op == '-') ? it1->second - it2->second
                          : it1->second * it2->second;
                return res;
            }
        }
    }
  {
        auto it = scalars.find(expr);
        if(it != scalars.end()){ res.value = it->second; return res; }
    }
    try{ res.value = std::stod(expr); return res; }
    catch(...){

    }

    throw std::runtime_error("Cannot evaluate expr: " + expr);
}

struct TxnContext{
    std::unordered_map<std::string, std::string> params;

    
    std::unordered_map<std::string, Record> vars;
    std::unordered_map<std::string, double> scalars;
    
    std::unordered_map<std::string, Record> read_set;    
    std::unordered_map<std::string, Record> write_set;

    
    std::vector<std::string> all_keys;
};

inline std::vector<std::string> getTransactionKeys(
    const TransactionTemplate& tmpl,
    const std::unordered_map<std::string, std::string>& params){
    std::vector<std::string> keys;
    for(auto& op : tmpl.ops){
        if(op.type == TxnOp::Type::READ || op.type == TxnOp::Type::WRITE){
            auto it = params.find(op.key_param);
            if(it != params.end()){                
                bool found = false;
                for(auto& k : keys) if(k == it->second){ 
                    found = true; 
                    break; 
                }
                if(!found) keys.push_back(it->second);
            }
        }
    }
    return keys;
}

inline bool executeTransactionOps(
    const TransactionTemplate& tmpl,
    TxnContext& ctx,
    std::function<bool(const std::string&, Record&)> fetch_fn){

    ctx.vars.clear();
    ctx.scalars.clear();
    ctx.read_set.clear();
    ctx.write_set.clear();

    for(auto& op : tmpl.ops){
        switch(op.type){
        case TxnOp::Type::READ:{
            std::string key = ctx.params.at(op.key_param);
            Record rec;
            if(!fetch_fn(key, rec)) return false;
            ctx.vars[op.var] = rec;
            ctx.read_set[key] = rec;
            break;
        }
        case TxnOp::Type::WRITE:{
            std::string key = ctx.params.at(op.key_param);
            ctx.write_set[key] = ctx.vars.at(op.src_var);
            break;
        }
        case TxnOp::Type::FIELD_READ:{
            auto& rec = ctx.vars.at(op.src_var);
            ctx.scalars[op.var] = getNumeric(rec, op.field);
            break;
        }
        case TxnOp::Type::FIELD_WRITE:{
            auto& rec = ctx.vars.at(op.var);
            ExprResult er = evalExpr(op.expr, ctx.vars, ctx.scalars);
            setNumeric(rec, op.field, er.value);
            break;
        }
        }
    }
    return true;
}
