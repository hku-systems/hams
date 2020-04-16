//
// Created by xusheng on 2/16/19.
//

#ifndef AI_SERVING_CPP_PROXY_HPP
#define AI_SERVING_CPP_PROXY_HPP

#include <string>
#include "ModelConn.hpp"
#include "Handler.hpp"
#include "DownStreamConn.hpp"
#include "DownstreamGroup.hpp"
#include "common.hpp"
#include "Config.hpp"
#include "FrontendConn.hpp"
#include "prediction.grpc.pb.h"
#include "ModelState.hpp"



class Handler;
class ModelConn;
class DownstreamGroup;
class FrontEndConn; 
class ModelState;




class Proxy :  public std::enable_shared_from_this<Proxy> {
private:

    int proxy_port;

    std::string model_name_;

    int model_id_;



    //stateful
    bool is_stateful_;
    std::atomic<bool> is_primary_;



    //batch settings
    int batch_size_;
    int batch_wait_ms;

    // std::unique_ptr<ModelConn> model_conn;

    std::vector<std::unique_ptr<ModelConn>> model_conn_list; 


    std::unique_ptr<Handler> handler;

    std::vector<std::shared_ptr<DownstreamGroup>> downstream_groups;

    Downstream_Intergroup_policy_t ds_intergroup_policy;

    std::atomic<proxy_status_t> proxy_status;

    std::string dag_name_;

    bool is_sink_; 

    std::string front_end_addr_; 


    std::unique_ptr<FrontEndConn> front_end_conn; 

    bool dynamic_batch_;

    int model_count;



    std::map<uint, std::unique_ptr<prediction::request>> downstream_reqs;
    std::thread downstream_reqs_handler;
    std::mutex downstream_reqs_lock;
    std::condition_variable downstream_reqs_cond;

    std::vector<std::shared_ptr<ModelState>> model_states; 

public:
    //Fixit: write setter and getter, and change to private

    //key: Next Stateful Model URI, value: max sequnece number for whom ACK is received
    std::map<std::string, uint32_t> backup_notificatoins;  
    std::mutex back_noti_lock; 
    std::condition_variable back_noti_cond; 


    std::vector<std::unique_ptr<prediction::ProxyServer::Stub>> next_stateful_models; 

    std::unique_ptr<prediction::ProxyServer::Stub> backup_proxy; 

public:

    Proxy(int port, std::shared_ptr<Config> config)
        : proxy_port(port),
        ds_intergroup_policy(all),
        is_sink_(false),
        front_end_conn(nullptr), 
        model_count(0),
        config(std::move(config))
        {
        if (this->config->RECOVERY){
            proxy_status = PROXY_RECOVERING;
        }
        else{
            proxy_status = PROXY_MODEL_UNSET;
        }
    } ;

    //Set the setup..
    //This should be a RPC function called by the manager.

    void init();

    void start_handler();

    //this state
    void register_model(const std::string &model_ip, int model_port);

    virtual bool enqueue_request(std::unique_ptr<prediction::request>);

    void pass_downstream(std::unique_ptr<prediction::request>  request);

    void make_durable(const prediction::modelstate &state);

    void receive_state_from_primary(const prediction::modelstate *state);
    
    void receive_back_notification(const prediction::backnotifyreq *notification); 

    void apply_state(std::shared_ptr<ModelState>);
    
    void wait_and_apply_state(std::shared_ptr<ModelState>);

    void wait();

    void add_downstream_group(const std::shared_ptr<DownstreamGroup> &group);

    std::string my_uri;

    void setMyUri(const std::string &myUri);


    //flush to a single downstream
    uint flush_cached_model_reply(uint start_seq, const std::shared_ptr<DownStreamConn> &d);

    //flush to all downstream
    uint flush_cached_model_reply (uint start_seq);

    void recover_all_downstreams(uint start_seq);

    std::shared_ptr<DownStreamConn> find_downstream_conn(const std::string &container_name);

    inline void clear_down_groups(){
        downstream_groups.clear();
    };


    inline proxy_status_t status(){
        return proxy_status.load();
    }

    inline void set_status(proxy_status_t status){
        proxy_status.store(status);
    }

    inline bool compare_and_swap_status (proxy_status_t expected, proxy_status_t desired){
        return proxy_status.compare_exchange_strong(expected, desired);
    }



    inline bool is_stateful(){
        return is_stateful_;
    }

    inline bool is_primary(){
        return is_primary_.load();
    }

    inline void set_primary(bool is_primary){
        is_primary_.store(is_primary);
    }

    void recover_model_conn (uint start_seq);

    void setModelId(int modelId);

    int getModelId() const;

    const std::string &getModelName() const;

    void setModelName(const std::string &modelName);

    std::shared_ptr<Config> config;

    void set_dag_name(const std::string &name){
        dag_name_ = name;
    }

    std::string dag_name(){
        return dag_name_;
    }

    void set_sink(const std::string &front_end_addr);

    void set_stateful(bool stateful); 

    float cache_hit_rate(){
        return 0.0;
//        return model_conn->cache_hit_rate();
    }

    uint get_n_requests(){
        return 0;
//        return model_conn->get_n_request();
    }

    void set_batch_size(int batch_size){
        this-> batch_size_ = batch_size; 
    }

    int batch_size(){
        return batch_size_; 
    }

    void set_dynamic_batch(bool d){
        dynamic_batch_ = d;
    }

    bool dynamic_batch(){
        return dynamic_batch_;
    }

    void handle_downstream_reqs();




};


#endif //AI_SERVING_CPP_PROXY_HPP
