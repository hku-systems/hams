//
// Created by xusheng on 2/28/19.
//

#ifndef SERVING_UPSTREAMSTATUS_HPP
#define SERVING_UPSTREAMSTATUS_HPP

#include <atomic>
#include "prediction.grpc.pb.h"
#include <glog/logging.h>


class Proxy;
class Reducer;

class UpstreamStatus : public std::enable_shared_from_this<UpstreamStatus> {
public:
    explicit UpstreamStatus(std::shared_ptr<Reducer> reducer, std::string uri, uint model_id)
        : reducer(std::move(reducer)), max_in_order (0), stopped(false), uri(std::move(uri)), model_id_(model_id) {}
    ~UpstreamStatus()= default;


    void receive(const prediction::request *req);


    uint handle_failure();

    void recover(uint max){
        DLOG(INFO) <<  "Upstream: " << uri << "'s max_in_order set to" << max;
        this->max_in_order = max;
    }


    uint getMaxInOrder();

    std::shared_ptr<Reducer> get_reducer(){
        return reducer;
    }

    std::string to_string(){
        return uri;
    }

    uint get_max_in_order(){
        return max_in_order; 
    }

    uint model_id(){
        return model_id_;
    }

private:

    std::shared_ptr<Reducer> reducer;

    std::mutex mu;

    uint max_in_order;

    //for out order request
    std::map<uint, std::unique_ptr<prediction::request> > buffer ;

    bool stopped;

    std::string uri;


    uint model_id_; 

};


#endif //SERVING_UPSTREAMSTATUS_HPP
