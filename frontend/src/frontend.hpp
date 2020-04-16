#ifndef FRONTEND_H
#define FRONTEND_H


#include "prediction.grpc.pb.h"
#include <glog/logging.h>
#include "grpc++/grpc++.h"
#include <atomic>

struct Output; 


class Frontend : public std::enable_shared_from_this<Frontend>{


public:
    Frontend(std::string entry_name, std::string entry_port, int n_workers)
        :entry_name(entry_name),  port_str(entry_port), n_workers(n_workers), MaxSeq(0){
            channel = grpc::CreateChannel(entry_name+":"+entry_port, grpc::InsecureChannelCredentials());
            stub = prediction::ProxyServer::NewStub(channel);
        }
    
    
    ~Frontend(){
        server_->Shutdown();
        cq_client_ ->Shutdown(); 
        cq_result_ ->Shutdown();
    }

    void run (); 

    void handle_client(); 

    void handle_result(); 

    std::map<uint, std::shared_ptr<Output>> outputs; 
    std::mutex mu; 


    std::unique_ptr<grpc::ServerCompletionQueue> cq_client_;
    std::unique_ptr<grpc::ServerCompletionQueue> cq_result_; 

    prediction::ProxyServer::AsyncService service_; 
    std::unique_ptr<grpc::Server> server_;

    std::string entry_name;
    std::string port_str; 


    std::shared_ptr<grpc::Channel> channel;
    std::unique_ptr<prediction::ProxyServer::Stub> stub;


    int n_workers; 
    std::atomic_uint MaxSeq;
};


class ClientReq{
public:
    ClientReq(prediction::ProxyServer::AsyncService* service, grpc::ServerCompletionQueue* cq, std::shared_ptr<Frontend> f)
        : service_(service), cq_(cq), responder_(&ctx_), status_(CREATE), frontend_(std::move(f)) {
        Proceed();
        }

    void Proceed();
     

private:
    prediction::ProxyServer::AsyncService* service_;
    // The producer-consumer queue where for asynchronous server notifications.
    grpc::ServerCompletionQueue* cq_;
    grpc::ServerContext ctx_;


    prediction::request request_; 

    modeltest::response response; 


    grpc::ServerAsyncResponseWriter<modeltest::response> responder_;

    enum CallStatus { CREATE, PROCESS, FINISH };

    CallStatus status_;  // The current serving state.

    std::shared_ptr<Frontend> frontend_; 
};

class Result{
    public:
    Result(prediction::ProxyServer::AsyncService* service, grpc::ServerCompletionQueue* cq, std::shared_ptr<Frontend> f)
        : service_(service), cq_(cq), responder_(&ctx_), status_(CREATE), frontend_(std::move(f)) {
        Proceed();
        }

    void Proceed();
     

private:
    prediction::ProxyServer::AsyncService* service_;

    grpc::ServerCompletionQueue* cq_;
    grpc::ServerContext ctx_;


    prediction::request request_; 

    modeltest::response response; 

    grpc::ServerAsyncResponseWriter<modeltest::response> responder_;

    enum CallStatus { CREATE, PROCESS, FINISH };

    CallStatus status_;  // The current serving state.

    std::shared_ptr<Frontend> frontend_; 

};


struct Output{
    std::condition_variable cond; 
    std::string output_; 
    bool finished; 
};

#endif //FRONTEND_H