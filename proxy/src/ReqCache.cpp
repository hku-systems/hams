//
// Created by xusheng on 4/11/19.
//

#include "ReqCache.hpp"
#include <glog/logging.h>
#include "DownStreamConn.hpp"

void ReqCache::set(const std::unique_ptr<prediction::request> &req, const std::shared_ptr<modeltest::output>& reply, bool need_insert) {
    std::lock_guard<std::mutex> l(mu);

    auto e = std::make_shared<CacheEntry> (req->req_id(), reply);
    replies.push_back(e);

    if (need_insert){

        if (mappings.count(req->input_().inputstream()) == 0) {
            //check size.

            if (cache_size >= capacity){
                DLOG(INFO) << "Cache full, going to evict using LRU, capacity: " << capacity << "size: " << cache_size;  
                auto back = lru_cache_.back();
                cache_size--; 
                mappings.erase(back->input_);
                lru_cache_.pop_back();
            }
            auto entry = std::make_shared<lru_cache_entry>(req->input_().inputstream(), reply);
            lru_cache_.push_front(entry);
            mappings[req->input_().inputstream()] = lru_cache_.begin();
            cache_size++; 
        }
    }
}

std::shared_ptr<modeltest::output> ReqCache::get(const std::unique_ptr<prediction::request> &req) {
    std::lock_guard<std::mutex> l(mu);


    if (mappings.count(req->input_().inputstream()) == 0){
        return nullptr;
    }else{
        //reorder the list by moving the this value to the front. 
        auto itr  = mappings[req->input_().inputstream()]; 
        if (itr != lru_cache_.begin()){
            lru_cache_.splice(lru_cache_.begin(), lru_cache_, itr, std::next(itr)); 
        }
        return (*mappings[req->input_().inputstream()])->output_; 
    }
}


uint ReqCache::flush(uint start_seq, const std::shared_ptr<DownStreamConn> &d) {
    std::lock_guard<std::mutex> l(mu);


    if (start_seq - cache_start < 0){
        LOG(FATAL) << "FLUSH: start seq smaller than cache start seq, start seq = " << start_seq << " reply_cache_start_seq" << cache_start;
    }
    else if(start_seq - cache_start >= replies.size()){
        LOG(WARNING) << "FLUSH: nothing to flush,  start seq = " << start_seq << " reply_cache_start_seq" << cache_start << " cache size" << replies.size();
    }


    auto r = replies.begin()+ start_seq - cache_start;

    DLOG(INFO) << "starting to flushing model output to recovered downstream, start seq = " << start_seq << " reply_cache_start_seq" << cache_start;

    d->recover(start_seq);


    for (; r!= replies.end(); r++) {

        prediction::request downstream_req;
        downstream_req.mutable_input_()->set_inputtype(r->operator->()->output_->outputtype());
        downstream_req.mutable_input_()->set_inputstream(r->operator->()->output_->outputstream());

        downstream_req.set_req_id(r->operator->()->req_id);

        d->pass_async(downstream_req);
    }
    return uint(replies.size()) + cache_start - 1;
}