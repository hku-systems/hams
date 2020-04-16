//
// Created by xusheng on 2/20/19.
//

#ifndef SERVING_DOWNSTREAMCONN_HPP
#define SERVING_DOWNSTREAMCONN_HPP

#include <thread>
#include <atomic>
#include "ClientConn.hpp"
#include "prediction.grpc.pb.h"
#include "DownstreamGroup.hpp"


class Proxy;
class DownstreamGroup;
class DownStreamCall;
class Config;
/*
 * This class serve as client side for calling downstream DAG proxy.
 * Holds the status (e.g., request number) for the connection.
 */

class DownStreamConn : public ClientConn, public std::enable_shared_from_this<DownStreamConn>  {
    //This class maintains a connection to *one* downstream proxy.
public:
    DownStreamConn(const std::string& peer_ip, uint port, uint model_id, std::string peer_cont_name, std::string peer_model_name, std::shared_ptr<Proxy> proxy, const std::string& my_uri
    , std::shared_ptr<Config> conf)
        :ClientConn(peer_ip, port),  config(std::move(conf)), MaxPassed(0), proxy(std::move(proxy)),
        my_uri(my_uri), failed(false), model_id_(model_id), peer_container_name_(peer_cont_name), peer_model_container_name_(peer_model_name)
        {
        stub = prediction::ProxyServer::NewStub(channel);

        t = std::thread(&DownStreamConn::handleAsyncACK, this);
        t.detach();

        std::cout << "created a connection to " << peer_ip << ":" <<port <<std::endl;
    };
    ~DownStreamConn()= default;


    void pass_async(const prediction::request& req);

    void handleAsyncACK();

    void handle_recovery(const prediction::recovery_req& req);

    bool report_failure();

    inline void set_group(std::shared_ptr<DownstreamGroup> group){
        this->my_group = std::move(group);
    }

    inline std::shared_ptr<DownstreamGroup> get_group(){

        return my_group;
    }

    inline std::string to_string(){
        return "peer_id" + std::to_string(model_id_) + "peer_container = " + peer_container_name_ + "addr" +peer_addr_ + ":" + std::to_string(peer_port_);
    }


    inline void recover(uint start_seq){
        DLOG(INFO) << "Recover downstream conn "<< peer_container_name_ << "'s MaxPassed to " << start_seq -1;
        MaxPassed = start_seq -1;
    }

    std::unique_ptr<prediction::ping_reply> ping_downstream(int retry_ms, int retry_times);


    // bool set_primary(const std::string &ip, int port);


    bool retry(const DownStreamCall* call);


    std::shared_ptr<Config> config;

    uint model_id(){
        return model_id_;
    }

    std::string peer_container_name(){
        return peer_container_name_;
    }

    std::string peer_model_cont_name(){
        return peer_model_container_name_;
    }



private:
    std::unique_ptr<prediction::ProxyServer::Stub> stub;

    grpc::CompletionQueue cq;

    std::atomic<std::uint32_t> MaxPassed;



    std::shared_ptr<Proxy> proxy;
    std::shared_ptr<DownstreamGroup> my_group;

    std::thread t;




    std::string my_uri;

    std::atomic<bool> failed;

    uint model_id_;

    std::string peer_container_name_;

    //used for recovery.
    std::string peer_model_container_name_;

};



class DownStreamCall {
public:
    DownStreamCall()= default;
    ~DownStreamCall()= default;

//        std::shared_ptr<prediction::request> req;
    prediction::request req;
    modeltest::response ack;

    grpc::ClientContext ctx;
    grpc::Status status;

    std::unique_ptr<grpc::ClientAsyncResponseReader<modeltest::response>> resp_reader;
};

#endif //SERVING_DOWNSTREAMCONN_HPP
