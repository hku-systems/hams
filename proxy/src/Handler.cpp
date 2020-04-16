//
// Created by xusheng on 2/21/19.
//

#include "Handler.hpp"
#include <glog/logging.h>
#include "Config.hpp"
#include "Reducer.hpp"
#include "Dag.hpp"
//#define REDUCER_TEST

/*
 * The server side implementation of downstream
 * A proxy's method is called when it received a request from upstream
 */

void Handler::serve() {
    std::string listen_address("[::]:" + std::to_string(port));

    builder = std::make_unique<grpc::ServerBuilder>();

    builder->AddListeningPort(listen_address, grpc::InsecureServerCredentials());
    builder->RegisterService(this);

    server = builder->BuildAndStart();

    LOG(INFO) << "Proxy RPC server listening on " << listen_address ;

}


grpc::Status Handler::downstream(grpc::ServerContext *context, const prediction::request *request,
                                 modeltest::response *ack) {


                        
                                    
    auto proxy_status = proxy->status();

    DLOG(INFO) << "Downstream called, received request, src = " << request->src_uri() << " proxy status " << proxy_status << "seq = " << request->seq(); 
    while (proxy_status != PROXY_READY){
        //DLOG(WARNING) << "proxy not ready, status = " << proxy_status ;
        usleep(100 * 1000); //100ms TODO: Change to cond.
        proxy_status = proxy->status();
    }

    if (proxy->getModelId() == 1){
        //I am the first one.
        DLOG(INFO) << "received request " << request->seq(); 
    }


    int retry_count = 0;

    auto it = upstreams.end();

    while(true){
        upstreams_mu.lock();
        it = upstreams.find(request->src_uri());
        if (it != upstreams.end()){
            DLOG(INFO) << "Found upstream status for " << request->src_uri(); 
            break;
        }
        upstreams_mu.unlock();
        if (++retry_count >= 100){
            LOG(FATAL) << "Received request from unexpected upstream:" << request->src_uri();
        }
        DLOG(INFO) << "upstream not ready, wait for 100ms" << request->src_uri(); 
        usleep(100 * 1000);
    }

    //pass to the upstreamStatus hanlder
    it->second->receive(request);

    upstreams_mu.unlock();

    ack->set_status(std::to_string(request->seq()));

    return grpc::Status::OK;


}

//Caller:: primary of a stateful model
//Callee:: backup of a stateful model
grpc::Status Handler::Durable (grpc::ServerContext *context, 
                               const prediction::modelstate *state, 
                               modeltest::response *ack){


    //Step 1: Apply the state (tensors) to the model, save buffered outputs,  
          



    return grpc::Status::OK; 
}

//Caller:: downstream backup
//Callee:: upstream backup
grpc::Status Handler::BackNotifyDurable(grpc::ServerContext *context, 
                                        const prediction::backnotifyreq *notification, 
                                        modeltest::response *ack){
    DLOG(INFO) << "received backup notification from" << notification->my_uri() << "up to index" << notification->max_id() << std::endl; 
    proxy->receive_back_notification(notification);
    return grpc::Status::OK;
}

/*
 * This RPC function is called if the Proxy is one of the downstreams of a failed proxy.
 */
grpc::Status Handler::prepare_recover_successor (grpc::ServerContext *context, const prediction::successor_prepare_req *req,
                                       prediction::successor_prepare_reply *max_seq) {
    std::lock_guard<std::mutex> l(recover_global_lock);


    std::string failed_uri = req->failed_proxy().container_name();

    DLOG(INFO) << "finding max recv for " << failed_uri;

    std::lock_guard<std::mutex> lock_(upstreams_mu);


    auto u = upstreams.find(failed_uri);
    if (u != upstreams.end()){
        //
        proxy->set_status(PROXY_UPSTREAM_FAILED);

        //let the upstream status to clear state.
        uint max_received = u->second->handle_failure();
        max_seq->set_seq(max_received);



        //The upstreams status is waited until GC to be garbage collected.

        return grpc::Status::OK;
    }
    else{
        //TODO: does not handle special case: upstream fails before sending out any data.
        LOG(WARNING) << "I am not a successor of" << failed_uri << "check the logic";
        return grpc::Status(grpc::StatusCode::ABORTED, "not a successor");
    }

}

grpc::Status Handler::prepare_recover_predecessor(grpc::ServerContext *context, const prediction::recovery_req *req,
                                                  modeltest::response *ack) {
    DLOG(INFO) << "Handle Prepare-Pred request for " << req->failed_proxy().container_name() << ":"<< req->failed_proxy().ip();

    std::lock_guard<std::mutex> lock_(recover_global_lock);


    std::shared_ptr<DownStreamConn> failed_conn = proxy->find_downstream_conn(req->failed_proxy().container_name());

    if (failed_conn != nullptr){

        if (req->is_stateful()){
            if (req->is_primary()){
                //TODO: handle primary failure.
                failed_conn->handle_recovery(*req);
            }
            else{
                DLOG(INFO) << "Faild proxy is a backup, only need to stop sending req to it";
                failed_conn->handle_recovery(*req);
            }
        }
        else{
            if (proxy->status() != PROXY_FAILURE_MANAGER){
                proxy->set_status(PROXY_DOWNSTREAM_FAILED);
            }

            DLOG(INFO) << "Found connection object to the failed proxy";

            failed_conn->handle_recovery(*req);
            if (nullptr == failed_conn->get_group()->find_conn(req->new_proxy().container_name())){

                //Create a new connection.
                std::shared_ptr<DownStreamConn> new_conn = std::make_shared<DownStreamConn>(req->new_proxy().ip(), req->new_proxy().port(), req->new_proxy().model_id(), req->new_proxy().container_name(), req->new_proxy().model_container_name(), proxy, proxy->my_uri, proxy->config);
                failed_conn->get_group()->add(new_conn);
            }
            else{
                LOG(WARNING) << "connection to new proxy already exists: failed_container = "  << req->failed_proxy().container_name() << "new_container" << req->new_proxy().container_name();
            }
        }
        return grpc::Status::OK;
    }else{
        return grpc::Status(grpc::StatusCode::ABORTED, "not a predecessor");
    }
}


grpc::Status Handler::ping(grpc::ServerContext *context, const prediction::ping_req *req,
                           prediction::ping_reply *reply) {

    reply->set_status(proxy->status());
    reply->set_stateful(proxy->is_stateful());
    reply->set_is_primary(proxy->is_primary());
    return grpc::Status::OK;
}

grpc::Status Handler::commit_recover_successor(grpc::ServerContext *context, const prediction::recovery_req *req,
                                               modeltest::response *ack) {
    ack->set_status("OK");

    //create an upstream status using the new_proxy;
    std::string new_uri = req->new_proxy().container_name();


    std::lock_guard<std::mutex> l(recover_global_lock);
    std::lock_guard<std::mutex> lock_(upstreams_mu);


    std::string failed_uri = req->failed_proxy().container_name();
    auto u = upstreams.find(failed_uri);

    if (u == upstreams.end()){
        LOG(FATAL) << "Failed to find upstreams status of the failed proxy" ;
    }
    //using the same counter to dedup.
    //Inherit the counter.
    auto max_received = u->second->getMaxInOrder();


    if (upstreams.count(new_uri) == 0) {
        auto new_upstream = std::make_shared<UpstreamStatus>(u->second->get_reducer(), new_uri, req->new_proxy().model_id());
        u->second->get_reducer()->register_upstream(new_upstream);
        u->second->get_reducer()->remove_upstream(u->second);

        new_upstream->recover(max_received);

        this->add_upstream(new_uri, new_upstream);

        DLOG(INFO) << "Created an UpstreamStatus object for recovered upstream: " << new_uri;
    }
    else{
        upstreams[new_uri]->recover(max_received);
        DLOG(INFO) << "Recovered sequence for already recovered upstream: " << new_uri;
    }


    proxy->set_status(PROXY_READY);

    return grpc::Status::OK;
}


grpc::Status Handler::commit_recover_predecessor(grpc::ServerContext *context, const prediction::recovery_req *req,
                                               modeltest::response *ack) {

    std::lock_guard<std::mutex> lock_(recover_global_lock);


    if (proxy->is_stateful() && !proxy->is_primary()){
        DLOG(INFO) << "Committing recover as predecessor, no need to do anything source:" << context->peer();
    }
    else{
        DLOG(INFO) << "Committing recover as predecessor, source:" << context->peer();

        ack->set_status("OK");

        std::shared_ptr<DownStreamConn> new_conn = proxy->find_downstream_conn(req->new_proxy().container_name());

        // 2019/05/28
        // A stateful's predecessor does not need to flush
        if (req->failed_proxy().stateful()){
            DLOG(INFO) << "No need to flush model reply as a predecessor for a stateful proxy";
        }
        else{
            if (new_conn != nullptr){
                //This function includes recover the downstream sequence number.
                proxy->flush_cached_model_reply(req->start_seq(), new_conn);
            }else{
                DLOG(WARNING) << "connection not created";
                return grpc::Status(grpc::StatusCode::INTERNAL, "connection not created");
            }
        }
    }


    proxy->set_status(PROXY_READY);

    return grpc::Status::OK;
}


grpc::Status Handler::recover_new_proxy(grpc::ServerContext *context, const prediction::new_proxy_recover_req *req,
                                        modeltest::response *ack) {
    std::lock_guard<std::mutex> l(recover_global_lock);


    if (proxy->status() != PROXY_RECOVERING){
        return grpc::Status(grpc::StatusCode::INTERNAL, "Not in the recovery mode");
    }


    proxy->setModelId(req->model_id());
    proxy->setMyUri(req->my_uri());

    int batch_size = req->batch_size();
    if (batch_size < 0){
        proxy->set_batch_size(0-batch_size);
        proxy->set_dynamic_batch(true);
    }else{
        proxy->set_batch_size(batch_size);
        proxy->set_dynamic_batch(false);
    }
    proxy->set_dag_name(req->dag_name());

    proxy->register_model(req->model_ip(), 22222);

    LOG(INFO) << "registered model: ip = " << req->model_ip() << "model_id = " << req->model_id();



    std::lock_guard<std::mutex> lock_(upstreams_mu);

    upstreams.clear();

    for (auto &g: req->pred_groups()){
        if (g.proxies_size() >1){
            auto reducer = std::make_shared<Reducer>(proxy, up_reduce);
            for (auto &p: g.proxies()){
                std::string new_uri = p.container_name();
                auto new_upstream = std::make_shared<UpstreamStatus>(reducer, new_uri, p.model_id());
                reducer->register_upstream(new_upstream);
                new_upstream->recover(p.seq()-1);
                this->add_upstream(new_uri, new_upstream);
                DLOG(INFO) << "Recovery: Created an UpstreamStatus object for the recovered new proxy: " << new_uri;
            }
            DLOG(INFO) << "Recovery: Created a group of reducer upstreams, size = " << g.proxies_size();
        }
        else {
            auto reducer = std::make_shared<Reducer>(proxy, up_direct);
            auto p = g.proxies(0);
            std::string new_uri = p.container_name();
            auto new_upstream = std::make_shared<UpstreamStatus>(reducer, new_uri, p.model_id());
            reducer->register_upstream(new_upstream);
            new_upstream->recover(p.seq()-1);
            this->add_upstream(new_uri, new_upstream);
            DLOG(INFO) << "Recovery: Created an UpstreamStatus object for the recovered new proxy: " << new_uri;
        }
    }



    proxy->clear_down_groups();
    for (auto &g: req->down_groups()){
        auto group = std::make_shared<DownstreamGroup>(proxy);
        for (auto &c: g.proxies()){
            auto conn = std::make_shared<DownStreamConn>(c.ip(), c.port(), c.model_id(), c.container_name(), c.model_container_name(), proxy, proxy->my_uri, proxy->config);
            group->add(conn);
            DLOG(INFO) << "Recovery: Created an downstream connection for the recovered new proxy: " << conn->to_string();
        }
        proxy->add_downstream_group(group);
    }

    proxy->recover_all_downstreams(req->start_seq()-1);

    proxy->recover_model_conn(req->start_seq()-1);

    proxy->set_status(PROXY_READY);

    return grpc::Status::OK;
}


// grpc::Status Handler::set_primary(grpc::ServerContext *context, const prediction::proxy_info *req,
//                                   modeltest::response *ack) {

//     std::lock_guard<std::mutex> lock_(recover_global_lock);

//     if (req->ip() + ":" + std::to_string(req->port()) == proxy->my_uri) {
//         //I am the primary
//         proxy->set_primary(true);
//         LOG(INFO) << "I am set as the primary";
//     }else{
//         proxy->set_primary(false);
//         LOG(INFO) << "I am set as a backup";
//     }
//     return  grpc::Status::OK;
// }


grpc::Status Handler::promote_primary(grpc::ServerContext *context, const prediction::new_proxy_recover_req *req,
                                      modeltest::response *ack) {

    std::lock_guard<std::mutex> lock_(recover_global_lock);

    if (proxy->is_primary()){
        LOG(WARNING) << "Already promoted as primary";
        return grpc::Status(grpc::StatusCode::INTERNAL, "Already promoted as primary");
    }


    //This logic is somehow 'recursive'
    //Just for all downstream of me.

    DLOG(INFO) << "Promoted as primary";

    std::lock_guard<std::mutex> l(upstreams_mu);

    for (auto &g : req->pred_groups()){
        if (g.proxies_size() > 1){
            //Check whether this group exists..
            //TODO: 2019/05/30:
            //This may cause danling groups, but seems ok. 
            //Find a way to clear them.
            bool group_exist = false; 
            std::shared_ptr<Reducer> reducer;
            for (auto &p: g.proxies()){
                std::string uri = p.container_name(); 
                auto u = upstreams.find(uri);
                if (u != upstreams.end()){
                    reducer = u->second->get_reducer();
                    group_exist = true; 
                    break; 
                }
            }
            if (group_exist){
                std::vector<std::string> new_group_uris; 
                for (auto &p: g.proxies()){
                    std::string uri = p.container_name(); 
                    new_group_uris.push_back(uri); 
                    if (upstreams.count(uri) == 0){
                        //This is a new pred that does not exists before.
                        auto new_upstream = std::make_shared<UpstreamStatus>(reducer, uri, p.model_id());
                        reducer->register_upstream(new_upstream);
                        
                        bool found = false; 
                        for (auto &u : upstreams){
                            if (u.second->model_id() == p.model_id()){
                                new_upstream->recover(u.second->get_max_in_order());
                                DLOG(WARNING) << "found previous max recv for: " << p.model_container_name() << ", set to" << u.second->get_max_in_order() << "instead of p.seq()-1";
                                found = true; 
                                break; 
                            }
                        }
                        if (!found) {
                            new_upstream->recover(p.seq()-1);
                            DLOG(WARNING) << "Didn't found previous max recv for: " << p.model_container_name() << " set to" << p.seq()-1;
                        }
                        
                        
                        this->add_upstream(uri, new_upstream);
                        DLOG(INFO) << "Promote Primary: Created an UpstreamStatus object for the predecessor of the new primar, pred_uri = " << uri;
                    }
                }
                reducer->truncate(new_group_uris);
            }
            else{
                auto reducer = std::make_shared<Reducer>(proxy, up_reduce);
                for (auto &p: g.proxies()){
                    std::string new_uri = p.container_name();
                    auto new_upstream = std::make_shared<UpstreamStatus>(reducer, new_uri, p.model_id());
                    reducer->register_upstream(new_upstream);

                    bool found = false; 
                    for (auto &u : upstreams){
                        if (u.second->model_id() == p.model_id()){
                            new_upstream->recover(u.second->get_max_in_order());
                            DLOG(WARNING) << "found previous max recv for: " << p.model_container_name() << ", set to" << u.second->get_max_in_order() << "instead of p.seq()-1";
                            found = true; 
                            break; 
                        }
                    }
                    if (!found) {
                        new_upstream->recover(p.seq()-1);
                        DLOG(WARNING) << "Didn't found previous max recv for: " << p.model_container_name() << " set to" << p.seq()-1;
                    }

                    this->add_upstream(new_uri, new_upstream);
                    DLOG(INFO) << "Recovery: Created an UpstreamStatus object for the recovered new proxy: " << new_uri;
                }
                DLOG(INFO) << "Recovery: Created a group of reducer upstreams, size = " << g.proxies_size();
            }
        }
        else {
            //A direct upstreams, just check and add.
            auto p = g.proxies(0);
            std::string uri = p.container_name();

            if (upstreams.count(uri) == 0){

                auto reducer = std::make_shared<Reducer> (proxy, up_direct);

                auto new_upstream = std::make_shared<UpstreamStatus>(reducer, uri, p.model_id());
                reducer->register_upstream(new_upstream);
                
                bool found = false; 
                for (auto &u : upstreams){
                    if (u.second->model_id() == p.model_id()){
                        new_upstream->recover(u.second->get_max_in_order());
                        DLOG(WARNING) << "found previous max recv for: " << p.model_container_name() << ", set to" << u.second->get_max_in_order() << "instead of p.seq()-1";
                        found = true; 
                        break; 
                    }
                }
                if (!found) {
                    new_upstream->recover(p.seq()-1);
                    DLOG(WARNING) << "Didn't found previous max recv for: " << p.model_container_name() << " set to" << p.seq()-1;
                }

                this->add_upstream(uri, new_upstream);

                DLOG(INFO) << "Promote Primary: Created an UpstreamStatus object for the predecessor of the new primar, pred_uri = " << uri;
            }
        }
    }



    proxy->clear_down_groups();
    for (auto &g: req->down_groups()){
        auto group = std::make_shared<DownstreamGroup>(proxy);
        for (auto &c: g.proxies()){
            auto conn = std::make_shared<DownStreamConn>(c.ip(), c.port(), c.model_id(), c.container_name(), c.model_container_name(), proxy, proxy->my_uri, proxy->config);
            group->add(conn);
            DLOG(INFO) << "Recovery: Created an downstream connection for the recovered new proxy: " << conn->to_string();
        }
        proxy->add_downstream_group(group);
    }

    proxy->recover_all_downstreams(req->start_seq()-1);

    proxy->set_primary(true);


    proxy->flush_cached_model_reply(req->start_seq()-1);

    proxy->set_status(PROXY_READY); 

    DLOG(INFO) << "Promoted primary success"; 


    return grpc::Status::OK;
}


grpc::Status Handler::SetModel(grpc::ServerContext *context, const prediction::modelinfo *info,
                               modeltest::response *ack) {
    if (proxy->status() != PROXY_MODEL_UNSET){
        LOG(WARNING) << "Model Already Set, status = " << proxy->status();
        return grpc::Status(grpc::StatusCode::INTERNAL, "Model Already Set");
    }


    proxy->setModelId(info->modelid());

    proxy->setModelName(info->modelname());

    proxy->register_model(info->modelip(), info->modelport());

    LOG(INFO) << "registered model: ip = " << info->modelip() << "port = " << info->modelport();

    proxy->set_status(PROXY_MODEL_SET);
    return  grpc::Status::OK;


}



grpc::Status Handler::SetModelReplica(grpc::ServerContext* context,
                             const prediction::modelinfolist *modellist, modeltest::response *ack){
    if (proxy->status() != PROXY_MODEL_UNSET){
        LOG(WARNING) << "Model Already Set, status = " << proxy->status();
        return grpc::Status(grpc::StatusCode::INTERNAL, "Model Already Set");
    }

    for (auto &info : modellist->list()){
        proxy->setModelId(info.modelid());

        proxy->setModelName(info.modelname());

        proxy->register_model(info.modelip(), info.modelport());

        LOG(INFO) << "registered model: ip = " << info.modelip() << "port = " << info.modelport();

    }

    proxy->set_status(PROXY_MODEL_SET);
    return  grpc::Status::OK;

};


grpc::Status Handler::SetDAG(grpc::ServerContext *context, const prediction::dag *dag, modeltest::response *ack) {
    if (proxy->status() != PROXY_MODEL_SET){
        return grpc::Status(grpc::StatusCode::INTERNAL, "Model Not Set");
    }


    DLOG(INFO) << "DAG:" << std::endl << dag->dag_(); 

    DAG d; 
    d.parse(dag->dag_());

    auto my_uri = proxy->config->PROXY_NAME;

    proxy->setMyUri(my_uri); 
    proxy->set_dag_name(d.dag_name); 

    //set stateful
    if (d.proxies[proxy->getModelId()-1].stateful){
        proxy->set_stateful(true);
        DLOG(INFO) << "I am stateful"; 
    }else{
        proxy->set_stateful(false);
        DLOG(INFO) << "I am stateless";  
    }
    //set is primary. 

    if (proxy->is_stateful()){
        if(d.proxies[proxy->getModelId()-1].container_name == proxy->config->PROXY_NAME){
            DLOG(INFO) << "I am primary"; 
            proxy->set_primary(true); 
        }
        else{
            DLOG(INFO) << "I am backup"; 
            proxy->set_primary(false); 
        }
    }else{
        proxy->set_primary(false); 
    }


    proxy->set_batch_size(d.proxies[proxy->getModelId()-1].batch_size); 
    proxy->set_dynamic_batch(d.proxies[proxy->getModelId()-1].dynamic_batch);

    int suceessor_count = 0; 

    for (auto &e: d.dist_eges){
        DLOG(INFO) << "pasing dist edge" << e.to_string(); 
        if (e.from == proxy->getModelId()){
            DLOG(INFO) << "--- from me, need to create downstreams, count: " << e.tos.size(); 
            for (auto &to: e.tos){
                std::vector<std::shared_ptr<DownStreamConn>> conns;
                auto group = std::make_shared<DownstreamGroup>(proxy);
                
                if (d.proxies[to-1].stateful){
                    DLOG(INFO) << "------ from me (" << e.from << "), creating downstream to " << to << "statful"; 


                    auto conn = std::make_shared<DownStreamConn>(d.proxies[to-1].ip, proxy->config->PROXY_PORT, uint(to),  d.proxies[to-1].container_name,  d.proxies[to-1].model_container_name, proxy, my_uri, proxy->config);
                    conns.push_back(conn);
                    DLOG(INFO) << "------ Set DAG: created a downstream Conn to primary " << conn->to_string();

                    auto conn_backup = std::make_shared<DownStreamConn>(d.proxies[to-1].backup_ip, proxy->config->PROXY_PORT, uint(to),  d.proxies[to-1].backup_container_name, d.proxies[to-1].backup_model_container_name,  proxy, my_uri, proxy->config);
                    conns.push_back(conn_backup);
                    DLOG(INFO) << "------ Set DAG: created a downstream Conn to bakcup " << conn_backup->to_string();

                    group->set_primary_conn(conn); 
                }
                else{
                    DLOG(INFO) << "------ from me (" << e.from << "), creating downstream to " << to << "stateless"; 

                    auto conn = std::make_shared<DownStreamConn>(d.proxies[to-1].ip, proxy->config->PROXY_PORT, uint(to),  d.proxies[to-1].container_name,  d.proxies[to-1].model_container_name, proxy, my_uri, proxy->config);
                    conns.push_back(conn);
                    DLOG(INFO) << "Set DAG: created a downstream Conn to stateless " << conn->to_string();
                }

                group->add(conns);
                proxy->add_downstream_group(group);
                suceessor_count++; 
            }
        }
        else{
            for (auto &to: e.tos){
                if (to == proxy->getModelId()){
                    DLOG(INFO) << "--- to me, need to create upstream status for " << e.from; 

                    //only need to set upstream for the primary
                    upstreams_mu.lock();
                    auto reducer =   std::make_shared<Reducer>(proxy, up_direct);
                    auto u = std::make_shared<UpstreamStatus>(reducer, d.proxies[e.from-1].container_name, e.from);
                    reducer->register_upstream(u);
                    this->add_upstream(d.proxies[e.from-1].container_name, u);
                    DLOG(INFO) << "added upstream status, type = direct, from = " << e.from << "cont" << d.proxies[e.from-1].container_name;
                    upstreams_mu.unlock();
                }
            }
        }
    }
    for (auto &e: d.reduce_edges){
        DLOG(INFO) << "pasing reduce edge" << e.to_string(); 
        if (e.to == proxy->getModelId()){
            //A reduce edge to me.
            //Only need to add the primary because only the primary is sending request to me. 
            DLOG(INFO) << "--- to me, need to create upstream statuses, sizes  " << e.froms.size(); 

            
            upstreams_mu.lock();
            std::shared_ptr<Reducer> reducer;
            reducer = std::make_shared<Reducer>(proxy, up_reduce);

            for (auto &from: e.froms){
                DLOG(INFO) << "------ to me, need to create upstream status for " << from; 
                auto u = std::make_shared<UpstreamStatus>(reducer, d.proxies[from-1].container_name, from);
                reducer->register_upstream(u);
                this->add_upstream(d.proxies[from-1].container_name, u);
                DLOG(INFO) << "------ SET DAG: added upstream status, type = reduce, from = " << from << " cont " << d.proxies[from-1].container_name;
            }
            upstreams_mu.unlock();
        }
        else{
            for (auto &from: e.froms){

                if (from == proxy->getModelId()){
                    //I need to send to a model. 
                    DLOG(INFO) << "--- from me(" << from <<") need to create downstream connection to  " << e.to; 

                    std::vector<std::shared_ptr<DownStreamConn>> conns;
                    auto group = std::make_shared<DownstreamGroup>(proxy);

                    if (d.proxies[e.to-1].stateful){

                        DLOG(INFO) << "------ from me(" << from <<") need to create downstream connection to  " << e.to << "statful"; 

                        //create downstream to both. 
                        auto conn = std::make_shared<DownStreamConn>(d.proxies[e.to-1].ip, proxy->config->PROXY_PORT, uint(e.to),  d.proxies[e.to-1].container_name, d.proxies[e.to-1].model_container_name, proxy, my_uri, proxy->config);
                        conns.push_back(conn);
                        DLOG(INFO) << "------ Set DAG: created a downstream Conn to primary " << conn->to_string();


                        auto conn_backup = std::make_shared<DownStreamConn>(d.proxies[e.to-1].backup_ip, proxy->config->PROXY_PORT, uint(e.to),  d.proxies[e.to-1].backup_container_name,  d.proxies[e.to-1].backup_model_container_name, proxy, my_uri, proxy->config);
                        conns.push_back(conn_backup);
                        DLOG(INFO) << "------ Set DAG: created a downstream Conn to bakcup " << conn_backup->to_string();

                        group->set_primary_conn(conn); 
                    }
                    else{
                        DLOG(INFO) << "------ from me(" << from <<") need to create downstream connection to  " << e.to << "stateless"; 

                        auto conn = std::make_shared<DownStreamConn>(d.proxies[e.to-1].ip, proxy->config->PROXY_PORT, uint(e.to),  d.proxies[e.to-1].container_name,  d.proxies[e.to-1].model_container_name, proxy, my_uri, proxy->config);
                        conns.push_back(conn);
                        DLOG(INFO) << "------ Set DAG: created a downstream Conn to stateless " << conn->to_string();
                    }
                    group->add(conns);
                    proxy->add_downstream_group(group);
                    suceessor_count++; 
                }
            }
        }
    }
    
    //create PFMs for backup, and NFM connections
    if (proxy->is_stateful() && !proxy->is_primary()){
        std::vector<int> PFMs = d.get_PFM(proxy->getModelId());
        for (auto &p: PFMs){
            std::string uri = d.proxies[p-1].container_name; 
            proxy->backup_notificatoins[uri] = -1; 
        }


        std::vector<int> NFMs = d.get_NFM(proxy->getModelId());
        for (auto &p: NFMs){
            std::string ip = d.proxies[p-1].backup_ip;
            std::string port = std::to_string(proxy->config->PROXY_PORT); 

            auto channel = grpc::CreateChannel(ip+ ":" + port, grpc::InsecureChannelCredentials()); 
            auto stub = prediction::ProxyServer::NewStub(channel);
            proxy->next_stateful_models.push_back(std::move(stub));
        }

    }



    if (proxy->getModelId() == 1){
        auto reducer = std::make_shared<Reducer>(proxy, up_direct);

        auto u = std::make_shared<UpstreamStatus>(reducer, "front-end", 0);
        reducer->register_upstream(u);

        upstreams_mu.lock();
        this->add_upstream("front-end", u);
        DLOG(INFO) << "I am the source proxy, creating an upstream status for front-end"; 
        upstreams_mu.unlock();
    }

    if (suceessor_count == 0){
        //I am a sink node. 
        DLOG(INFO) << "I am the sink proxy, will pass output to frontEnd"; 

        proxy->set_sink(d.front_end_addr); 
    }

    std::thread t(&Proxy::init, proxy);
    t.detach();


    return  grpc::Status::OK;
}





void Handler::wait(){
    server->Wait();
}

//IMPORTANT: This function must be called when the lock is held.
void Handler::add_upstream(const std::string & uri, std::shared_ptr<UpstreamStatus> u) {
//    std::lock_guard<std::mutex> l(upstreams_mu);

    if (upstreams.count(uri) != 0){
        LOG(FATAL) << "adding an upstream already existed:" << uri;
    }

    upstreams[uri] = std::move(u);
    LOG(INFO) << "Added an upstream status for " << uri;

}


grpc::Status Handler::GetRuntimeInfo(grpc::ServerContext* context, const prediction::runtimequery *req, prediction::runtimeinfo *reply){
    reply->set_cache_hit_rate(proxy->cache_hit_rate()); 

    reply->set_n_requests(proxy->get_n_requests()); 



    return grpc::Status::OK;
} 

grpc::Status Handler::AdjustBatchSize(grpc::ServerContext* context, const prediction::batch_adj_request *req,  modeltest::response *ack) {
    return grpc::Status::OK;
}; 