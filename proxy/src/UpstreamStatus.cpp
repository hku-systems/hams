//
// Created by xusheng on 2/28/19.
//



#include "UpstreamStatus.hpp"
#include "Proxy.hpp"
#include <glog/logging.h>
#include "common.hpp"
#include "Reducer.hpp"


#define DEBUG




void UpstreamStatus::receive(const prediction::request *req) {
    std::lock_guard<std::mutex> lock_g(mu);

    if (stopped){
        DLOG(INFO) << "Upstream has already been reported as failed, dropping reqs, upstream = " <<this->uri << ", req order = " << req->seq();
        return;
    }

    //call the constructor to make a deep copy.
    std::unique_ptr<prediction::request> r = std::make_unique<prediction::request>(*req);

    if (r->seq() == max_in_order + 1){
        DLOG(INFO) << "received in-order request, seq = " << r->seq() << std::endl;

        reducer->pass(std::move(r), shared_from_this());

        max_in_order++;


        int count = 0;

        while(!buffer.empty()){
            if (buffer.begin()->second->seq() == max_in_order + 1){
                reducer->pass(std::move(buffer.begin()->second), shared_from_this());

                max_in_order++;
                buffer.erase(max_in_order);
                count++;
            }
            else{
                break;
            }

        }

#ifdef DEBUG

        if (count != 0) {
            if (!buffer.empty()) {
                DLOG(INFO) << "Flushed " << count << "request in queue" << "max_in_order = " << max_in_order
                           << "buffer begin" << buffer.begin()->second->seq();
            } else {
                DLOG(INFO) << "Flushed " << count << "request in queue" << "max_in_order = " << max_in_order
                           << "buffer empty";

            }
        }
#endif

    }
    else if (r->seq() <= max_in_order){
        DLOG(WARNING)<< "received duplicated upstream request, seq = " << req->seq();
        return;
    }
    else{
        DLOG(WARNING)<< "received out of order request, seq = " << req->seq() << ", max recv = " << max_in_order << ", uri = " << this->uri;

        //Not in order, save it.
        buffer[req->seq()] = std::move(r);
    }
}

uint UpstreamStatus::handle_failure() {


    std::lock_guard<std::mutex> lock_g(mu);
    stopped = true;
    DLOG(INFO) << "upstream [" <<uri << "] finding max_in_order: " << max_in_order;

    return max_in_order;
}

uint UpstreamStatus::getMaxInOrder() {
    std::lock_guard<std::mutex> lock_g(mu);
    return max_in_order;

}
