//
// Created by xusheng on 4/11/19.
//

#ifndef SERVING_REQCACHE_HPP
#define SERVING_REQCACHE_HPP


#include <prediction.pb.h>
#include "model.grpc.pb.h"
#include <list>


class DownStreamConn;
class CacheEntry;

class ReqCache {
public:
    ReqCache(uint32_t cache_capacity_mb): 
        cache_start(1), capacity(cache_capacity_mb), cache_size(0){}
    ~ReqCache() = default;

    void set(const std::unique_ptr<prediction::request>& req, const std::shared_ptr<modeltest::output>& reply, bool need_insert);

    std::shared_ptr<modeltest::output> get(const std::unique_ptr<prediction::request>&);

    uint flush(uint start_seq, const std::shared_ptr<DownStreamConn> &d);

    inline void set_cache_start(uint s){
        cache_start = s;
    }
private:
    std::vector<std::shared_ptr<CacheEntry>> replies;

    //save the iterator in the map so that it is easy to handle the cache_; . 

    class lru_cache_entry {
    public:
        lru_cache_entry(const std::string &in, std::shared_ptr<modeltest::output> o)
            : input_(in), output_(std::move(o)){}

        std::string input_; 
        std::shared_ptr<modeltest::output> output_; 
    };



    std::unordered_map<std::string, std::list<std::shared_ptr<lru_cache_entry>>::iterator> mappings;

    std::list <std::shared_ptr<lru_cache_entry>> lru_cache_; 

    std::mutex mu;

    uint cache_start;

    uint64_t capacity; 

    uint64_t cache_size; 



};

class CacheEntry{
public:
    CacheEntry(uint32_t id, std::shared_ptr<modeltest::output> o): req_id(id), output_(std::move(o)) {}

    uint32_t req_id;

    std::shared_ptr<modeltest::output> output_;

};




#endif //SERVING_REQCACHE_HPP
