//
// Created by xusheng on 3/6/19.
//

#include "DownstreamGroup.hpp"
#include <glog/logging.h>
#include <limits>
#include <prediction.pb.h>
#include "FailureHandler.hpp"
#include <sys/time.h>


bool DownstreamGroup::add(std::shared_ptr<DownStreamConn> conn) {
    for (auto &d: downstreams){
        if (d -> peer_container_name() == conn->peer_container_name()){
            LOG(WARNING) << "connection already exist" << conn->to_string();
            return false;
        }
    }

    conn->set_group(shared_from_this());

    DLOG(INFO) << "Added new connection" << conn->to_string() << "# connection" << downstreams.size()+1;

    downstreams.push_back(std::move(conn));




    return true;
}

void DownstreamGroup::add(std::vector<std::shared_ptr<DownStreamConn>> conns) {
    if (downstreams.empty()){
        downstreams = std::move(conns);
        for (auto &d: downstreams){
           d->set_group(shared_from_this());
        }
    }else{
        for (auto &c: conns){
            add(c);
        }
    }
}

void DownstreamGroup::pass(const prediction::request &req) {
    for (auto &c : downstreams){
        c->pass_async(req);
    }
}


bool DownstreamGroup::handleFailure(const std::shared_ptr<DownStreamConn> &conn) {


    struct timeval tv1;

    gettimeofday(&tv1, NULL);

    LOG(WARNING) << "Downstream failed: " << conn->to_string() << "stateful" << is_stateful_;



    bool set_to_manager = proxy->compare_and_swap_status(PROXY_READY, PROXY_FAILURE_MANAGER); 
    if (!set_to_manager){
        DLOG(INFO) << "Cannot set to failure manager, current statust: " << proxy->status(); 
        return false; 
    }

    FailureHandler f(shared_from_this(), conn, proxy);

    recover_status s = f.Handle();
    if (s == RECOVER_OK){
        proxy->set_status(PROXY_READY);
    }else{
        //If it is already ready, no need to set back to failed.
        LOG(INFO) << "Other manager has already handled the (correlated) failure of " << conn->to_string() << "number of downstreams" << downstreams.size();
        proxy->compare_and_swap_status(PROXY_FAILURE_MANAGER, PROXY_DOWNSTREAM_FAILED);
    }





    LOG(INFO) << "Finished recovering for (correlated) failure of " << conn->to_string() << "number of downstreams" << downstreams.size();


    struct timeval tv2;

    gettimeofday(&tv2, NULL);


    LOG(WARNING) << "[Recovery time]: total = " << tv2.tv_sec - tv1.tv_sec << "s" << tv2.tv_usec - tv1.tv_usec << "us";


    return true;
}

std::shared_ptr<DownStreamConn> DownstreamGroup::find_conn(const std::string &container_name){
    for (auto &d: downstreams){
        if (d->peer_container_name()== container_name){
            return d;
        }
    }
    return nullptr;
}


void DownstreamGroup::recover_all(uint start_seq){
    for (auto &d: downstreams){
        d->recover(start_seq);
    }
}


//Call during initialization.
bool DownstreamGroup::wait_downstreams_ready(int retry_ms, int retry_times) {

    DLOG(INFO) << "DownstreamGroup: wait downstreams ready" << *this;

    for (auto &d: downstreams){
        std::unique_ptr<prediction::ping_reply> reply;

        reply = d->ping_downstream(retry_ms,retry_times);
        if (reply == nullptr){
            return false;
        }

        if (reply->stateful()){
            this->is_stateful_ = true;
            DLOG(INFO) << "Downstream Group is stateful ";
//            std::cout<< this->is_stateful_ << std::endl;
        }else{
            this->is_stateful_ = false;
            DLOG(INFO) << "Downstream Group is stateless ";
//            std::cout<< this->is_stateful_ << std::endl;

        }
    }

    // if (this->is_stateful_){
    //     //Need some heuristic (e.g, the one first replies to the ping request)
    //     //currently use the first one.
    //     std::string p_ip = downstreams[0]->peer_addr();
    //     int p_port = downstreams[0]->peer_port();

    //     for (auto &d: downstreams) {
    //         d->set_primary(p_ip, p_port);
    //     };
    // }

    // primary_conn = downstreams[0];


    return true;
}

void DownstreamGroup::flush_cached_to_all(uint max) {
    for (auto &d: downstreams){
        this->proxy->flush_cached_model_reply(max, d);
    }
}

std::ostream &operator<<(std::ostream &out, const DownstreamGroup &g) {
    for (auto & d : g.downstreams){
        out << d->to_string() << ", ";
    }


    return out;
}


