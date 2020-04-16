//
// Created by xusheng on 3/24/19.
//

#ifndef SERVING_FAILUREHANDLER_HPP
#define SERVING_FAILUREHANDLER_HPP


#include "DownstreamGroup.hpp"
#include "management.grpc.pb.h"
#include "Dag.hpp"
#include <climits>

class DownstreamGroup;
class p_info;
class recovery_info;
//class DAG;


enum recover_status{
    RECOVER_OK = 0,
    RECOVER_HANDLED = 2,
    RECOVER_MORE_FAILURE = 3,
    RECOVER_UNKNOWN = 4,
};


class FailureHandler {
public:
    explicit FailureHandler(std::shared_ptr<DownstreamGroup> group, std::shared_ptr<DownStreamConn> conn, std::shared_ptr<Proxy> proxy)
        : initiator(std::move(group)), failed_conn(std::move(conn)),
          proxy(std::move(proxy)), start_seq(UINT_MAX) {};
    ~FailureHandler()= default;

    recover_status Handle();

    [[deprecated]]
    void fake_report_failure(const p_info& failed, prediction::failure_reply &reply);


    void report_failure(const p_info &failed, management::FailureResponse* resp);



    recover_status recover();

    recover_status notify_failure(const p_info& failed_info, p_info &new_proxy);


    recover_status prepare_successors(const p_info &failed, std::unique_ptr<recovery_info> &f);

    recover_status prepare_predecessors(const p_info& failed, std::unique_ptr<recovery_info>& f);

    recover_status commit_successors(const p_info& failed, std::unique_ptr<recovery_info>& f);

    recover_status commit_predeccsors(const p_info& failed, std::unique_ptr<recovery_info>& f);

    recover_status wait_new_proxy_up(std::unique_ptr<recovery_info>& f);

    recover_status update_start_seq();

    bool recursive_update_start_seq();
    /*
     * 1. success
     * 0. already handled by others
     * -1: failed/
     */
    recover_status recover_new_proxy_state(std::unique_ptr<recovery_info>& f);

    recover_status promote_primary(std::unique_ptr<recovery_info>& f);

private:
    std::shared_ptr<DownstreamGroup> initiator;
    std::shared_ptr<DownStreamConn> failed_conn;
    std::shared_ptr<Proxy> proxy;

//    uint start_seq;

//    uint division_factor;

    /*
     * New proxies and their infos.
     *
     */
    std::map<p_info, std::unique_ptr<recovery_info>> failures;

    std::unique_ptr<DAG> current_dag;

    std::vector<int> failed_stateful_model_ids;

    uint start_seq; 
};


class p_info {
public:
    explicit p_info(const prediction::proxy_info &p)
            :ip(p.ip()), port(p.port()), stateful(p.stateful()), is_primary(p.is_primary()),
             model_id(p.model_id()), cotainer_name(p.container_name()),
             model_container_name(p.model_container_name()){}
    p_info(const p_info &other) = default;

    p_info(std::string ip, uint port, uint id, std::string container_name, std::string model_container_name, bool stateful, bool primary)
            :ip(std::move(ip)), port(port), stateful(stateful), is_primary(primary),
             model_id(id), cotainer_name(std::move(container_name)), model_container_name(model_container_name){}
    p_info()= default;

    ~p_info()= default;



    inline bool operator==(const p_info &other) const {
        return cotainer_name == other.cotainer_name;
//
//        return (model_id == other.model_id) && (is_primary == other.is_primary);
    }

    inline bool operator< (const p_info &other) const{
        if (model_id < other.model_id) {
            return true;
        }
        else if(model_id > other.model_id){
            return false;
        }
        else {
            return (is_primary < other.is_primary);
        }
    }


    inline prediction::proxy_info serialize() const{
        prediction::proxy_info p;
        p.set_ip(ip);
        p.set_port(port);
        p.set_stateful(stateful);
        p.set_is_primary(is_primary);
        p.set_model_id(model_id);
        p.set_container_name(cotainer_name);
        p.set_model_container_name(model_container_name);
        return p;
    }


    std::string ip;
    uint port;
    bool stateful;
    bool is_primary;
    uint model_id;


    // uint p_start_seq; //as pred;
    // uint s_max_received; //as successor;

    std::string cotainer_name;

    std::string model_container_name;


};





class recovery_info {
public:
    explicit recovery_info(const prediction::proxy_info& newly)
        :  new_(newly){};
    explicit recovery_info(const p_info& newly)
        : new_(newly){};
    ~recovery_info()= default;



    p_info new_;

    std::vector<p_info> predecessors;
    std::vector<p_info> new_predecessors;
    std::vector<p_info> backup_predecessors; //Those predecessors work as backup (not sending to it).


    std::vector<p_info> successors;
    std::vector<p_info> new_successors;

    // uint ds_max_seq; 
    // uint division_factor;

    std::string model_ip;
    uint model_id;


};


#endif //SERVING_FAILUREHANDLER_HPP
