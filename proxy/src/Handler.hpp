//
// Created by xusheng on 2/21/19.
//

#ifndef SERVING_HANDLER_HPP
#define SERVING_HANDLER_HPP

#include <grpc++/grpc++.h>
#include <prediction.pb.h>
#include "prediction.grpc.pb.h"
#include "Proxy.hpp"
#include "UpstreamStatus.hpp"

class Proxy;

class Handler: public prediction::ProxyServer::Service{
public:
    Handler(int port, std::shared_ptr<Proxy> proxy)
        : port(port), proxy(std::move(proxy)){};
    ~Handler()= default;

    void wait();
    void serve();

    void add_upstream(const std::string & uri, std::shared_ptr<UpstreamStatus>);


    grpc::Status downstream(grpc::ServerContext *context,
                            const prediction::request *request, modeltest::response *ack) override; 


    grpc::Status Durable (grpc::ServerContext *context, 
                            const prediction::modelstate *state, modeltest::response *ack) override;

    grpc::Status BackNotifyDurable (grpc::ServerContext *context, 
                            const prediction::backnotifyreq *req, modeltest::response *ack) override; 
    

    grpc::Status prepare_recover_successor (grpc::ServerContext* context,
                                  const prediction::successor_prepare_req* req, prediction::successor_prepare_reply* maxseq) override;

    grpc::Status prepare_recover_predecessor (grpc::ServerContext* context,
                                              const prediction::recovery_req* req, modeltest::response* ack) override;

    grpc::Status ping (grpc::ServerContext* context,
                                              const prediction::ping_req* check, prediction::ping_reply* reply) override;

    grpc::Status commit_recover_successor (grpc::ServerContext* context,
                                              const prediction::recovery_req* req, modeltest::response* ack) override;

    grpc::Status commit_recover_predecessor (grpc::ServerContext* context,
                                           const prediction::recovery_req* req, modeltest::response* ack) override;

    grpc::Status recover_new_proxy (grpc::ServerContext* context,
                                    const prediction::new_proxy_recover_req* req, modeltest::response* ack) override;


    // grpc::Status set_primary (grpc::ServerContext* context,
    //                           const prediction::proxy_info* req, modeltest::response* ack) override;

    grpc::Status promote_primary (grpc::ServerContext* context,
                              const prediction::new_proxy_recover_req *req, modeltest::response* ack) override;

    grpc::Status SetModel(grpc::ServerContext* context,
                          const prediction::modelinfo *info, modeltest::response* ack) override;

    grpc::Status SetDAG(grpc::ServerContext* context,
    const prediction::dag *dag, modeltest::response* ack) override;

    grpc::Status GetRuntimeInfo(grpc::ServerContext* context,
        const prediction::runtimequery *req, prediction::runtimeinfo *reply) override; 

    grpc::Status AdjustBatchSize(grpc::ServerContext* context,
        const prediction::batch_adj_request *req,  modeltest::response *ack) override; 


    grpc::Status SetModelReplica(grpc::ServerContext* context,
            const prediction::modelinfolist *list, modeltest::response *ack) override;
private:

    int port;

    std::unique_ptr<grpc::Server> server;
    std::shared_ptr<Proxy> proxy;

    std::unique_ptr<grpc::ServerBuilder> builder;


    std::mutex upstreams_mu;
    std::map<std::string, std::shared_ptr<UpstreamStatus>> upstreams;

    std::mutex recover_global_lock;

};


#endif //SERVING_HANDLER_HPP
