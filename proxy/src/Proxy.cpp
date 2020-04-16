//
// Created by xusheng on 2/16/19.
//

#include "Proxy.hpp"
#include <glog/logging.h>
#include "Config.hpp"
#include "FrontendConn.hpp"

bool Proxy::enqueue_request(std::unique_ptr<prediction::request> request) {
    int model_index = request->seq() % model_count; 
    return model_conn_list[model_index]->enqueue_request(std::move(request));
}


void Proxy::register_model(const std::string &model_ip, int model_port) {
    //Currently does not support adding model while running...
    auto model_conn = std::make_unique<ModelConn>(model_ip, model_port, shared_from_this(), this->config);
    model_conn_list.push_back(std::move(model_conn)); 
    model_count++; 
} 

void Proxy::set_stateful(bool stateful){
    this->is_stateful_ = stateful;
}
void Proxy::start_handler(){
    handler = std::make_unique<Handler>(proxy_port, shared_from_this());
    handler->serve();
}

void Proxy::wait(){
    handler->wait();
}

void Proxy::add_downstream_group(const std::shared_ptr<DownstreamGroup> &group) {
    DLOG(INFO) << "added downstream group" << *group;

    auto i = std::find(downstream_groups.begin(), downstream_groups.end(), group);
    if (i == downstream_groups.end()){
        downstream_groups.push_back(group);
    }
}


/*
 * This function should be called in the order of the request sequence.
 */
void Proxy::pass_downstream(std::unique_ptr<prediction::request>  request) {
    if (is_stateful_ && !is_primary_){
        //I am a backup, no need to send to downstream
        return;
    }
    else{
        std::unique_lock<std::mutex> lck(downstream_reqs_lock);

        uint seq = request->seq();
        downstream_reqs[seq] = std::move(request);

        downstream_reqs_cond.notify_all();

        lck.unlock();
    }
}

/*
 * This function should be called in sequential order 
 */
void Proxy::make_durable(const prediction::modelstate &state){
    grpc::ClientContext context; 
    modeltest::response resp; 

    backup_proxy->Durable(&context, state, &resp);
}


std::shared_ptr<DownStreamConn> Proxy::find_downstream_conn(const std::string &container_name){
    for (auto &g : downstream_groups){
        auto found = g->find_conn(container_name);
        if (found != nullptr){
            return found;
        }
    }
    return nullptr;
}



uint Proxy::flush_cached_model_reply(uint start_seq, const std::shared_ptr<DownStreamConn> &d){
    return model_conn_list[0]->flush_cached_model_reply(start_seq, d);
}


uint Proxy::flush_cached_model_reply (uint start_seq){
    DLOG(INFO) << "Going to flush cached model reply, start seq: " << start_seq << "ds groups size: " << downstream_groups.size(); 
    for (auto &dg : downstream_groups){
        dg->flush_cached_to_all(start_seq);
    }

    return 0;
}

void Proxy::recover_all_downstreams(uint start_seq) {
    for (auto &g : downstream_groups) {
        g->recover_all(start_seq);
    }
}

void Proxy::init(){
    //Step 1: Ping the model.
    DLOG(INFO) << "initializing proxy";


    for (auto& model_conn: model_conn_list){
        bool model_ok = model_conn->ping_model(config->TIMEOUT_MS, config->INIT_PING_RETRY_TIMES);

        if (!model_ok){
            LOG(WARNING) << "Ping model failed";
            proxy_status.store(PROXY_MODEL_FAILED);
            return;
        }
    }

    DLOG(INFO) << "Model ping succeeded";


    this->downstream_reqs_handler = std::thread(&Proxy::handle_downstream_reqs, this);

    //step 2: ping downstream
    for (auto &g : downstream_groups){
        bool downstream_ok = g->wait_downstreams_ready(config->TIMEOUT_MS, config->INIT_PING_RETRY_TIMES);

        if (!downstream_ok){
            //TODO: currently doing nothing
            LOG(WARNING) << "ping downstream failed" ;

            proxy_status.store(PROXY_DOWNSTREAM_FAILED);
            return;
        }
    }
    DLOG(INFO) << "Proxy intilization done";

    proxy_status.store(PROXY_READY);
}



void Proxy::receive_back_notification(const prediction::backnotifyreq *notification){

   std::unique_lock<std::mutex> lk(back_noti_lock); 

   std::string uri = notification->my_uri();  //avoiding too long lines
   if (notification->max_id() > backup_notificatoins[uri]){
       backup_notificatoins[uri] = notification->max_id();
       back_noti_cond.notify_all(); 
   }
   else{
      //No need to FATAL, 
      //Maybe caused by a retried RPC (e.g., reply is lost);

      //LOG(FATAL) << "Receive wrong backup notification"; 
   }
}


void Proxy::receive_state_from_primary(const prediction::modelstate *state){
    //Step 1: check whether all upstream are ready
    auto ms = std::make_shared<ModelState>();

    ms->tensors = state->tensors();
    for (int i = 0; i < state->ids_size(); i++){
        ms->ids.push_back(state->ids(i));
    }

    for (int i = 0; i < state->outputs_size(); i++){
        ms->outputs.push_back(state->outputs(i));
    }

    this->model_states.push_back(ms);

    bool need_wait = false; 
    for (auto &kv: backup_notificatoins){
        if (kv.second < state->ids(state->ids_size()-1)){
            need_wait = true; 
        }
    }
    
    if (!need_wait){
        apply_state(ms);
    }
    else{
        //Fixit: 
        //This implementation causes code redundancy. 
        //wait and apply should be very rare for correctness. 
        auto t = std::thread(&Proxy::wait_and_apply_state, this, ms);
        t.detach();
    }
}

void Proxy::apply_state(std::shared_ptr<ModelState> ms){
   //Step 1: apply to model
    this->model_conn_list[0]->apply_state(ms->tensors);


    if (model_states.front() == ms){
        model_states.erase(model_states.begin());

    } else{
        LOG(FATAL) << "Not applying states in order"  << std::endl; 
    }
   //Step 2: tell downstreams 
    prediction::backnotifyreq req; 
    req.set_my_uri(my_uri);
    req.set_max_id(ms->ids.back()); 

    for (auto &d: next_stateful_models){
        grpc::ClientContext context; 
        modeltest::response ack; 
        d->BackNotifyDurable(&context, req, &ack);
    }
}

void Proxy::wait_and_apply_state(std::shared_ptr<ModelState> ms){
    while(true){
        std::unique_lock<std::mutex> lk(back_noti_lock); 
        back_noti_cond.wait(lk); 


        bool need_wait = false; 
        for (auto &kv: backup_notificatoins){
            if (kv.second < ms->ids.back()){
                lk.unlock();
                continue;
            }
        }

        if (ms != model_states.front()){
            lk.unlock();
            continue;
        }

        return apply_state(ms);
    }
}


void Proxy::recover_model_conn(uint start_seq) {
        model_conn_list[0]->recover(start_seq);
}

int Proxy::getModelId() const {
    return model_id_;
}

void Proxy::setModelId(int modelId) {
    model_id_ = modelId;
}

void Proxy::setModelName(const std::string &modelName) {
    model_name_ = modelName;
}

const std::string &Proxy::getModelName() const {
    return model_name_;
}

void Proxy::setMyUri(const std::string &myUri) {
    my_uri = myUri;
}


void Proxy::set_sink(const std::string &front_end_addr){
    is_sink_ = true; 
    this->front_end_addr_ = front_end_addr;
    front_end_conn = std::make_unique<FrontEndConn>(front_end_addr, 22223, my_uri); 
}

void Proxy::handle_downstream_reqs() {
    uint max_passed = 0;
    while(true){

        std::unique_lock<std::mutex> lck(downstream_reqs_lock);

        downstream_reqs_cond.wait(lck, [this, max_passed]{return !downstream_reqs.empty() && downstream_reqs.begin()->first == max_passed + 1;});

        auto next_req = std::move(downstream_reqs.begin()->second);

        downstream_reqs.erase(downstream_reqs.begin());

        lck.unlock();

        max_passed++;

        DLOG(INFO) << "proxy passed to downstream(s), req_seq" << next_req->seq();


        if (is_sink_){
            //passing result to frontEnd
            if (front_end_conn != nullptr){
                front_end_conn->pass_async(*next_req);
            }
            else{
                LOG(FATAL) << "front end conn is nullptr";
            }
            return;
        }
        else{
            //passing to downstreams
            switch (ds_intergroup_policy){
                case Downstream_Intergroup_policy_t::all :
                    for (auto const &d: downstream_groups){
                        d->pass(*next_req);
                    }
                    return;
                case Downstream_Intergroup_policy_t::RR  :
                    return;
            }
        }


    }


}
