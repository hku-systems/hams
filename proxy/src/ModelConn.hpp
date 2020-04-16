//
// Created by xusheng on 2/20/19.
//

#ifndef SERVING_MODELCONN_HPP
#define SERVING_MODELCONN_HPP


#include <grpc++/channel.h>
#include "prediction.grpc.pb.h"
#include "model.grpc.pb.h"
#include "ClientConn.hpp"
#include <thread>
#include <queue>
#include "common.hpp"
#include <atomic>
#include "ReqCache.hpp"
#include <glog/logging.h>
#include "Config.hpp"
#include <mutex>

class DownStreamConn;
class Proxy;
class ReqCache;
class Config;


class ModelConn : public ClientConn {
    //TODO: there are more common fields that can be ported to super class.
private:
    std::unique_ptr<modeltest::PredictService::Stub> stub;

    //for request propagation and state retrival loop.
    std::thread t1, t2;

    std::shared_ptr<Proxy> proxy;

    std::queue<std::unique_ptr<prediction::request>> pending_reqs;
    std::mutex pending_queue_lock;
    std::condition_variable pending_queue_cond;

    std::mutex state_retrival_lock;
    std::condition_variable state_retrival_cond;
    bool state_retrival_flag; 

    ReqCache cache;
    std::shared_ptr<Config> config;
    
    uint cache_hit_count; 
    uint cache_miss_count;
    uint n_request;  

    grpc::Status predict_sync(const prediction::request &req, modeltest::output *reply);
    grpc::Status batch_predict_sync(const std::vector<std::unique_ptr<prediction::request>>& requests, modeltest::BatchOuput *outputs);

    std::vector<std::vector<std::shared_ptr<modeltest::output>>> output_buffer;

    std::string get_state(); 

public:
    explicit ModelConn(const std::string &model_ip, int port, std::shared_ptr<Proxy> proxy, std::shared_ptr<Config> c)
        :ClientConn(model_ip, port), proxy(std::move(proxy)), cache(c->CACHE_SIZE_MB), config(std::move(c)),
        cache_hit_count(0), cache_miss_count(0) {
        stub = modeltest::PredictService::NewStub(channel);
        t1 = std::thread(&ModelConn::predict_loop, this);
        t1.detach();

        t2 = std::thread(&ModelConn::state_retrival_loop, this); 
        t2.detach();

        DLOG(INFO) << "created ModelConn object, ip = " << model_ip << " port = " << port;
    };
    ~ModelConn()= default;


    grpc::Status apply_state(std::string); 


    bool enqueue_request (std::unique_ptr<prediction::request>);


    grpc_connectivity_state  get_channel_state(){
        return channel->GetState(true);
    }

//    void handle_predict_reply()= delete;

    void predict_loop();
    void state_retrival_loop();

    uint flush_cached_model_reply(uint start_seq, const std::shared_ptr<DownStreamConn> &d);



    bool  ping_model(int retry_ms, int retry_times);



    void recover(uint start_seq);

    float cache_hit_rate(){
        return float(cache_hit_count) / float(cache_hit_count + cache_miss_count); 
    }

    uint get_n_request(){
        return n_request; 
    }
};


#endif //SERVING_MODELCONN_HPP
