#ifndef SERVING_FRONTENDCONN_HPP
#define SERVING_FRONTENDCONN_HPP

#include "ClientConn.hpp"
#include "glog/logging.h"
#include <grpc++/grpc++.h>
#include "prediction.grpc.pb.h"
#include <thread>

class FrontEndConn : public ClientConn, public std::enable_shared_from_this<FrontEndConn> {

public:
    FrontEndConn(const std::string& peer_ip, uint port, const std::string& my_uri):
        ClientConn(peer_ip, port), MaxPassed(0), my_uri(my_uri)
    {
        stub = prediction::ProxyServer::NewStub(channel);


        async_handler = std::thread(&FrontEndConn::handle_async_resp, this); 
        async_handler.detach(); 

        DLOG(INFO) << "Created a connection to front end" << peer_ip << port; 

    }

    void pass_async(const prediction::request& req);

    void handle_async_resp(); 

private: 

    std::unique_ptr<prediction::ProxyServer::Stub> stub;

    grpc::CompletionQueue cq;

    std::thread async_handler;

    uint MaxPassed; 

    std::string my_uri;

};


class FrontEndCall{
public:
    FrontEndCall()=default; 
    ~FrontEndCall()=default; 

    prediction::request req;
    modeltest::response ack;

    grpc::ClientContext ctx;
    grpc::Status status;

    std::unique_ptr<grpc::ClientAsyncResponseReader<modeltest::response>> resp_reader;
};


#endif //SERVING_FRONTENDCONN_HPP