//
// Created by xusheng on 3/24/19.
//

#include "FailureHandler.hpp"
#include <glog/logging.h>
#include <utility>
#include "Config.hpp"
#include "Dag.hpp"
#include <sys/time.h>


std::ostream &operator<<(std::ostream &out, p_info const &p) {
    out << " [model id = " << p.model_id << ", is_primary = " << p.is_primary <<", addr = "<< p.ip << ":" << p.port << ", container name = " <<p.cotainer_name << ", stateful:" << p.stateful << "] ";
    return out;
}



recover_status FailureHandler::Handle() {




    p_info failed_info = p_info(failed_conn->peer_addr(), failed_conn->peer_port(), failed_conn->model_id(), failed_conn->peer_container_name(), failed_conn->peer_model_cont_name(), initiator->is_stateful(), initiator->is_primary(failed_conn));

    /*
     * Step 1: Report to the manager
     * Get form the manager: successors, predecessors of the failed proxy + info of the new proxy.
     */

    p_info new_proxy;
    auto recover_status = notify_failure(failed_info, new_proxy);
    if (recover_status != RECOVER_OK){
        return recover_status;
    }



    return recover();
}

recover_status FailureHandler::recover(){

    /*
     * Step 2:
     * Stateless: Send prepare message to all predecessors
     * Let the predecessors stop sending requests to the failed proxy.
     * Get form the successors: ACK.
     *
     * Stateful: no need for this process.
     */
    for (auto& f: failures){
        if (f.first.stateful) {
            recover_status status = this->prepare_predecessors(f.first, f.second);
            if (status == RECOVER_HANDLED || status == RECOVER_UNKNOWN) {
                return status;
            }
            else if (status == RECOVER_MORE_FAILURE){
                return recover();
            }
        }
        else{
            //stateful
            recover_status status = this->prepare_predecessors(f.first, f.second);
            if (status == RECOVER_HANDLED || status == RECOVER_UNKNOWN) {
                return status;
            }
            else if (status == RECOVER_MORE_FAILURE){
                return recover();
            }
        }
    }
    /*
     * Step 3: Send prepare message to all successors
     * Get form the successors: Max received seq.
     */
    for (auto& f: failures){
        if (f.first.stateful){
            if (f.first.is_primary){
                recover_status status = this->prepare_successors(f.first, f.second);
                if (status == RECOVER_HANDLED || status == RECOVER_UNKNOWN) {
                    return status;
                }
                else if (status == RECOVER_MORE_FAILURE){
                    return recover();
                }
            }
            else {
                DLOG(INFO) << "No need to prepare-successor for a backup" << f.first;
            }
        }
        else{
            recover_status status = this->prepare_successors(f.first, f.second);
            if (status == RECOVER_HANDLED || status == RECOVER_UNKNOWN) {
                return status;
            }
            else if (status == RECOVER_MORE_FAILURE){
                return recover();
            }
        }
    }


    // update_start_seq();


    /*
     * Step 4
     * Stateless: Wait for the new proxy to be online
     *
     * Stateful: no need to do anything
     */

    for (auto &f: failures){
        if (f.first.stateful){
            DLOG(INFO) << "No need to wait for new proxy for stateful failure" << f.first;
        }
        else {
            recover_status status = this->wait_new_proxy_up(f.second);
            if (status != RECOVER_OK){
                LOG(FATAL) << "Not implemented 3";
            }
        }
    }

    /* 
     * Step 5: 
     * Commit the recovery at all successors. 
     * 
     */
	

    for (auto& f: failures){
        if (f.first.stateful){
            if (f.first.is_primary) {
                recover_status status = this->commit_successors(f.first, f.second);
                if (status == RECOVER_HANDLED || status == RECOVER_UNKNOWN) {
                    return status;
                }
                else if (status == RECOVER_MORE_FAILURE){
                    return recover();
                }
            }
            else{
                DLOG(INFO) << "No need to commit successor as backup" << f.first;
            }
        }
        else{
            recover_status status = this->commit_successors(f.first, f.second);
            if (status == RECOVER_HANDLED || status == RECOVER_UNKNOWN) {
                return status;
            }
            else if (status == RECOVER_MORE_FAILURE){
                return recover();
            }
        }
    }


    /*
     * Step 6:
     * Stateless: recover the state of the newly deployed proxy. 
     * 
     * Stateful: prmote a backup to primary, the newly prmoted primary will flushed all 
     * missing states. 
    */ 

    for (auto& f: failures){

        if (f.first.stateful){
            if (f.first.is_primary){
                recover_status status= this->promote_primary(f.second);
                if (status == RECOVER_UNKNOWN){
                    LOG(FATAL) << "Not implemented 5";
                }
                else if (status == RECOVER_HANDLED){
                    return status;
                }
            }
            else{
                DLOG(INFO) << "No need to recover state for backup" << f.first;
            }
        }
        else{
            recover_status status = this->recover_new_proxy_state(f.second);
            if (status == RECOVER_UNKNOWN){
                LOG(FATAL) << "Not implemented 4";
            }
            else if (status == RECOVER_HANDLED){
                return status;
            }
        }

    }

    /*
     * Step 7:  
     * Starteless: let the predecessor start to flush from the start seq. 
     *
     * Stateful: do nothing. 
     */


    for (auto& f: failures){

        if (f.first.stateful){
            if (f.first.is_primary){
                //(wrong) 2010/05/28: seems no need for this
                // This is needed to pull a pred-from a downstream-failed status.  
                // DLOG(INFO) << "No need to commit predecessor for primary [testing]" << f.first;
                recover_status status= this->commit_predeccsors(f.first, f.second);
                if (status == RECOVER_HANDLED || status == RECOVER_UNKNOWN) {
                    return status;
                }
                else if (status == RECOVER_MORE_FAILURE){
                    return recover();
                }
            }
            else{
                DLOG(INFO) << "No need to commit predecessor for backup" << f.first;
            }
        }
        else{
            recover_status status = this->commit_predeccsors(f.first, f.second);
            if (status == RECOVER_HANDLED || status == RECOVER_UNKNOWN) {
                return status;
            }
            else if (status == RECOVER_MORE_FAILURE){
                return recover();
            }
        }
    }

    return RECOVER_OK;
}



recover_status FailureHandler::notify_failure(const p_info& failed_info, p_info &new_proxy){


    management::FailureResponse response;

    struct timeval tv1;

    gettimeofday(&tv1, NULL);


    report_failure(failed_info, &response);

    if (response.status() != "") {
        DLOG(INFO) << "failure already handled";
        return RECOVER_HANDLED;
    }

    struct timeval tv2;

    gettimeofday(&tv2, NULL);

    LOG(WARNING) << "[Recovery time]: report failure = " << tv2.tv_sec - tv1.tv_sec << "s" << tv2.tv_usec - tv1.tv_usec << "us";

//    DAG d;

    auto d = std::make_unique<DAG>();

    d->parse(response.newruntimedag());
    std::string new_model_ip;
    std::string new_proxy_ip;
    std::string new_container_name;
    std::string new_model_name;

    uint failed_id = failed_info.model_id;



    bool is_stateful = d->proxies[failed_id-1].stateful;
    bool is_primary = false;

    if (is_stateful){
        //TODO: check primary.

        //If the primary fails, the new proxy is the new primary
        //Else, is the current primary;

        //remember this id;
        failed_stateful_model_ids.push_back(failed_id);

        is_primary = response.isprimary();
        new_model_ip = d->proxies[failed_id-1].model_ip;
        new_proxy_ip = d->proxies[failed_id-1].ip;
        new_container_name = d->proxies[failed_id-1].container_name;
        new_model_name = d->proxies[failed_id-1].model_container_name;
    }else{
        new_model_ip = d->proxies[failed_id-1].model_ip;
        new_proxy_ip = d->proxies[failed_id-1].ip;
        new_container_name = d->proxies[failed_id-1].container_name;
        new_model_name = d->proxies[failed_id-1].model_container_name;
    }

    p_info p(new_proxy_ip, 22223, failed_id, new_container_name, new_model_name, is_stateful, is_primary);

    new_proxy = p;
    DLOG(INFO) << "Report failure done, failed: " <<failed_info << ", new proxy: "<<new_proxy;

               //Reminder: already set stateful
    auto recovery = std::make_unique<recovery_info>(new_proxy);
    recovery->model_id = failed_id;
    recovery->model_ip = new_model_ip;


    std::vector<p_info> predecessors;
    std::vector<p_info> successors;


    int div_factor = 0;

    for (const auto &e: d->dist_eges){
        if (e.from == int(failed_id)){
            //all dist ends are successors.
            for (const auto &to: e.tos){
                if (d->proxies[to-1].stateful){
                    p_info succ_primary(d->proxies[to-1].ip, 22223, to, d->proxies[to-1].container_name, d->proxies[to-1].model_container_name, true, true);
                    successors.push_back(succ_primary);
                    DLOG(INFO) << "Pushed successor" << succ_primary;


                    p_info succ_backup(d->proxies[to-1].backup_ip, 22223, to, d->proxies[to-1].backup_container_name, d->proxies[to-1].backup_model_container_name, true, false);
                    successors.push_back(succ_backup);
                    DLOG(INFO) << "Pushed successor" << succ_backup;

                }else{
                    p_info succ(d->proxies[to-1].ip, 22223, to, d->proxies[to-1].container_name, d->proxies[to-1].model_container_name, false, false);
                    successors.push_back(succ);
                    DLOG(INFO) << "Pushed successor" << succ;

                }
            }
        }
        else{
            //check if it is one end of the primary.
            for (const auto &to: e.tos){
                if (to == int(failed_id)){
                    //There is a dist edge to me, "from" is my pred->
                    if (d->proxies[e.from-1].stateful){
                        p_info pred_primary(d->proxies[e.from-1].ip, 22223, e.from, d->proxies[e.from-1].container_name, d->proxies[e.from-1].model_container_name, true, true);
                        predecessors.push_back(pred_primary);
                        DLOG(INFO) << "Pushed predecessor" << pred_primary;

                        p_info pred_backup(d->proxies[e.from-1].backup_ip, 22223, e.from, d->proxies[e.from-1].backup_container_name, d->proxies[e.from-1].backup_model_container_name, true, false);
                        predecessors.push_back(pred_backup);
                        DLOG(INFO) << "Pushed predecessor" << pred_backup;

                    }
                    else{
                        p_info pred(d->proxies[e.from-1].ip, 22223, e.from, d->proxies[e.from-1].container_name, d->proxies[e.from-1].model_container_name,  false, false);
                        predecessors.push_back(pred);
                        DLOG(INFO) << "Pushed predecessor" << pred;

                    }
                    div_factor++;
                }
            }
        }
    }

    for (const auto& e:d->reduce_edges){
        if (e.to == int(failed_id)){
            //A reduce edge to me, all are my predecessors.
            for (const auto &from: e.froms){
                if (d->proxies[from-1].stateful){
                    p_info pred_primary(d->proxies[from-1].ip, 22223, from, d->proxies[from-1].container_name, d->proxies[from-1].model_container_name, true, true);
                    predecessors.push_back(pred_primary);
                    DLOG(INFO) << "Pushed predecessor" << pred_primary;



                    p_info pred_backup(d->proxies[from-1].backup_ip, 22223, from, d->proxies[from-1].backup_container_name, d->proxies[from-1].backup_model_container_name, true, false);
                    predecessors.push_back(pred_backup);
                    DLOG(INFO) << "Pushed predecessor" << pred_backup;

                }
                else{
                    p_info pred(d->proxies[from-1].ip, 22223, from, d->proxies[from-1].container_name, d->proxies[from-1].model_container_name, false, false);
                    predecessors.push_back(pred);
                    DLOG(INFO) << "Pushed predecessor" << pred;

                }
            }
            div_factor++;
        }
        else{
            for (const auto &from: e.froms){
                if (from == int(failed_id)){
                    //I am one of the "from" side of a dist edge. "to" is my successors.
                    if (d->proxies[e.to-1].stateful){
                        p_info succ_primary(d->proxies[e.to-1].ip, 22223, e.to, d->proxies[e.to-1].container_name, d->proxies[e.to-1].model_container_name, true, true);
                        successors.push_back(succ_primary);
                        DLOG(INFO) << "Pushed successor" << succ_primary;


                        p_info succ_backup(d->proxies[e.to-1].backup_ip, 22223, e.to, d->proxies[e.to-1].backup_container_name, d->proxies[e.to-1].backup_model_container_name, true, false);
                        successors.push_back(succ_backup);
                        DLOG(INFO) << "Pushed successor" << succ_backup;

                    }else{
                        p_info succ(d->proxies[e.to-1].ip, 22223, e.to, d->proxies[e.to-1].container_name, d->proxies[e.to-1].model_container_name, false, false);
                        successors.push_back(succ);
                        DLOG(INFO) << "Pushed successor" << succ;
                    }
                }
            }

        }
    }

    //TODO: Can be merged together, but trying to keep the old code.



//    int failed_primary = 0;
//
    for (auto &info: predecessors){
        //check failure.
//        auto info = p_info(s);

        if (!info.stateful){
            if (failures.count(info) == 0){
                recovery->predecessors.push_back(info);
                LOG(WARNING) << "pushing predecessos,  failed proxy: " << failed_info
                             << " predecessor :" << info.cotainer_name;
            }else{
                //I already know it has failed.
                //Replace it with the one I know.
                LOG(WARNING) << "One predecessor has already failed, failed: " << failed_info
                             << " predecessor :" << info.model_container_name << " new pred" << failures[info]->new_;
                recovery->new_predecessors.emplace_back(p_info(failures[info]->new_));
            }
        }
        else{
            //stateful predecessor
            if (info.is_primary){


                if (failures.count(info) == 0){
                    recovery->predecessors.push_back(info);
                    LOG(INFO) << "pushing predecessos,  failed proxy: " << failed_info
                                 << " predecessor :" << info.port;
                }
                else{
                    /*
                     * TODO: 2019/05/29 Fixit later.
                     * The predecessor priamry has failed.
                     * Currently the new backup is ignored.
                     * So, don't send request to the new backup.
                     * Otherwise, errors will be reported.
                     */
                    p_info new_primary = failures[info]->new_;
                    new_primary.is_primary = true;
                    new_primary.stateful = true;
                    recovery->new_predecessors.push_back(new_primary);

                    auto as_backup = std::find(recovery->backup_predecessors.begin(), recovery->backup_predecessors.end(), new_primary);

                    if (as_backup != recovery->backup_predecessors.end()){
                        recovery->backup_predecessors.erase(as_backup);
                        LOG(WARNING) << "predecessor " << info << "of" << failed_info
                                     << "is a failed stateful replica, the new primary is " << new_primary
                                     << "pushing it to new-predecessors[], and removed out from backup predecessors[]";
                    }else{
                        LOG(WARNING) << "predecessor " << info << "of" << failed_info
                                     << "is a failed stateful replica, the new primary " << new_primary
                                     << "is pushed to predecessors[]";
                    }

                }
            }
            else{
                //backup predecessors
                if (failures.count(info) == 0){
                    bool promoted = false;

                    for (auto &f: failures){
                        if (f.second->new_ == info){
                            LOG(INFO) << "predecessor" << info <<" is a new primary, no need to push into backup-preds[]";
                            promoted = true;
                            break;
                        }
                    }

                    if (!promoted){
                        recovery->backup_predecessors.push_back(info);
                    }
                }
                else{
                    DLOG(INFO) << "no need to worry about failed backup predecessor";
                }
            }
        }





    }

    //TODO: currently assume even distribution.





//            uint(recovery->predecessors.size() + recovery->new_predecessors.size()+failed_primary);





    for (auto &info: successors){
        if (failures.count(info) == 0){
            recovery->successors.push_back(info);
            DLOG(INFO) << "pushing successor, failed proxy" << failed_info << ", successor: " << info;
        }else{
            recovery->new_successors.emplace_back(p_info(failures[info]->new_));
            DLOG(INFO) << "pushing successor, successor already failed, failed proxy: " << failed_info << ", successor: " << info;
        }
    }


    failures[failed_info] = (std::move(recovery));
    this->current_dag = std::move(d);

    return RECOVER_OK;
}





recover_status FailureHandler::prepare_successors(const p_info &failed, std::unique_ptr<recovery_info> &f) {
    for (auto s = f->successors.begin(); s != f->successors.end(); ++s) {
        DLOG(INFO) << "[Succ-Prepare]: asking successor to prepare for recovery, failed = "<< failed << ", succ =" << *s;

        if (s->stateful && !s->is_primary){
            //A backup successor.
            if (std::count(failed_stateful_model_ids.begin(), failed_stateful_model_ids.end(), s->model_id) != 0){
                
                //Check whether the backup has become primary. 
                bool promoted = false; 
                for (auto &rec : failures){
                    if (rec.second->new_.cotainer_name == s->cotainer_name){
                        DLOG(INFO) << "This successor is promoted as primary" << *s; 
                        promoted = true; 
                        break; 
                    }
                }
                
                if (!promoted){
                    DLOG(INFO) << "Successor is a newly started backup successor, no need to ask for prepare for now: " << *s;
                    continue; 
                }
            }
        }



        prediction::successor_prepare_reply successor_reply;
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() +
                             std::chrono::milliseconds(500));

        auto channel = grpc::CreateChannel(s->ip + ":" + std::to_string(s->port), grpc::InsecureChannelCredentials());

        auto stub = prediction::ProxyServer::NewStub(channel);


        prediction::successor_prepare_req req;

        auto failed_serialized = failed.serialize();
        auto new_serialized = f->new_.serialize();

        req.mutable_new_proxy()->CopyFrom(new_serialized);
        req.mutable_failed_proxy()->CopyFrom(failed_serialized);

        grpc::Status status = stub->prepare_recover_successor(&context, req, &successor_reply);

        if (!status.ok()) {
            LOG(WARNING) << "successor prepare: Successor failed !!" << status.error_code();
            /* One Successor of the failed proxy has failed.
             *
             * Step a: get the successors of the failed successor.
             * Get form the successors: Max received seq.
            */
            if (!s->stateful){
                p_info new_succ;
                auto recover_status = this->notify_failure(*s, new_succ);
                if (recover_status != RECOVER_OK){
                    return recover_status;
                }

                //FIXIT: several copies.
                f->successors.erase(s);
                f->new_successors.push_back(new_succ);
            }
            else{
                if (s->is_primary){
                    p_info new_succ;
                    auto recover_status = this->notify_failure(*s, new_succ);
                    if (recover_status != RECOVER_OK){
                        return recover_status;
                    }
                    LOG(WARNING) << "one stateful primary successor of " << failed << "failed" << *s;

                }
                //TODO: 2019/05/30 Ignore correlated failure for backup.
                LOG(WARNING) << "[IGNORE for now] one stateful backup successor of " << failed << "failed" << *s;

                f->successors.erase(s);


            }
            return RECOVER_MORE_FAILURE;
        }
        else{
            DLOG(INFO) << "got successor's reply" << *s << "max " << successor_reply.seq();

            if (successor_reply.seq() < this->start_seq){
                this->start_seq = successor_reply.seq();
                DLOG(INFO) << "Start seq set to " << start_seq; 
            }
        }

    }

    return RECOVER_OK;
}


// bool FailureHandler::recursive_update_start_seq() {
//     bool need_recursion = false;

//     for (auto &f: failures){
//         if (f.second->ds_max_seq == 0){
//             bool has_unknown = false;
//             uint min = std::numeric_limits<unsigned int>::max();

//             for (auto &s: f.second->new_successors){
//                 std::map<p_info, std::unique_ptr<recovery_info>>::iterator itr; 
//                 for (itr = failures.begin(); itr != failures.end(); ++itr){
//                     if (itr->second->new_.model_id == s.model_id){
//                         break;
//                     }
//                 }
//                 if (itr == failures.end()) {
//                     LOG(FATAL) << "new successor not in the failed list " << s;
//                 }
//                 else{
//                     if (itr->second->ds_max_seq == 0){
//                         has_unknown = true;
//                         need_recursion = true;
//                     }
//                     else{

//                         auto me = std::find(itr->second->new_predecessors.begin(), itr->second->new_predecessors.end(), f.second->new_);
//                         if (me == itr->second->new_predecessors.end()){
//                             LOG(FATAL) << "I am not in the new pred list " <<  f.second->new_;
//                         }
//                         if (me->p_start_seq -1 < min){
//                             min = me->p_start_seq - 1;
//                         }
//                     }
//                 }
//             }
//             if (has_unknown){
//                 continue;
//             }

//             for (auto &s: f.second->successors){
//                 if (s.s_max_received < min){
//                     min = s.s_max_received;
//                 }
//             }
//             LOG(INFO) << "collected downstream max sequence " << f.first << " = " << min << "division factor  = " << f.second->division_factor;

//             //for not divisible,
//             if (min % f.second->division_factor != 0){
//                 auto tmp = min;
//                 min = min / f.second->division_factor * f.second->division_factor;
//                 LOG(INFO) << "Max seq not Not divisible, fall back from "  << tmp << "to" << min;
//             }


//             f.second->ds_max_seq = min;

//             for (auto &p: f.second->predecessors){
//                 p.p_start_seq = min / f.second->division_factor + 1;
//                 DLOG(INFO) << "set " << f.first << "'s predecessor start seq" << p <<" = " << p.p_start_seq;

//             }

//             for (auto &p: f.second->new_predecessors){
//                 p.p_start_seq = min / f.second->division_factor + 1;
//                 DLOG(INFO) << "set " << f.first << "'s (new) predecessor start seq" << p <<" = " << p.p_start_seq;
//             }
//         }



//     }
//     return need_recursion;

// }


// recover_status FailureHandler::update_start_seq(){

//     //reset all to initial
//     for (auto &f: failures){
//         f.second->ds_max_seq = 0;
//         for (auto &p: f.second->predecessors){
//             p.p_start_seq = 0;
//         }
//     }


//     while(recursive_update_start_seq()){}
//     return RECOVER_OK;
// }


recover_status FailureHandler::prepare_predecessors(const p_info& failed, std::unique_ptr<recovery_info>& f) {

    prediction::recovery_req rec;

    auto new_serialized = f->new_.serialize();
    auto failed_serialized = failed.serialize();

    rec.mutable_new_proxy()->CopyFrom(new_serialized);
    rec.mutable_failed_proxy()->CopyFrom(failed_serialized);
    rec.set_is_primary(failed.is_primary);
    rec.set_is_stateful(failed.stateful);


    for (auto p = f->predecessors.begin(); p != f->predecessors.end(); p++) {
        DLOG(INFO) << "asking predeceoosr to prepare for recover" << *p;

        modeltest::response ack;
        grpc::ClientContext context;

        auto channel = grpc::CreateChannel(p->ip + ":" + std::to_string(p->port), grpc::InsecureChannelCredentials());

        auto stub = prediction::ProxyServer::NewStub(channel);

        grpc::Status status = stub->prepare_recover_predecessor(&context, rec, &ack);

        if (!status.ok()) {
            LOG(WARNING) << "predecessor prepare: predecessor failed !!" << status.error_code();
            if (!p->stateful) {
                p_info new_pred;
                auto recover_status = this->notify_failure(*p, new_pred);
                if (recover_status != RECOVER_OK){
                    return recover_status;
                }

                f->predecessors.erase(p);
                f->new_predecessors.push_back(new_pred);
            } else {

                p_info new_pred;
                auto recover_status = this->notify_failure(*p, new_pred);
                if (recover_status != RECOVER_OK){
                    return recover_status;
                }
                f->predecessors.erase(p);
                f->new_predecessors.push_back(new_pred);
                LOG(WARNING) << "Not heavily tested 2";
            }
            return RECOVER_MORE_FAILURE;
        }

        LOG(INFO) << "predcessor prepared to recover : " << *p;
    }

    for (auto p = f->backup_predecessors.begin(); p != f->backup_predecessors.end(); p++) {
        DLOG(INFO) << "asking backup predecessor to prepare for recover" << *p;
        DLOG(WARNING) << "Currently not handling backup predecessor, will implement in the future";
        //TODO: 2019/05/29  All commented, not handling for now.
//        modeltest::response ack;
//        grpc::ClientContext context;
//
//        auto channel = grpc::CreateChannel(p->ip + ":" + std::to_string(p->port), grpc::InsecureChannelCredentials());
//
//        auto stub = prediction::ProxyServer::NewStub(channel);
//
//        grpc::Status status = stub->prepare_recover_predecessor(&context, rec, &ack);
//
//        if (!status.ok()) {
//            LOG(WARNING) << "backup predecessor prepare: predecessor failed !!" << *p;
//            this->notify_failure(*p);
//            f->backup_predecessors.erase(p);
//            return false;
//        }

        LOG(INFO) << "backup predcessor prepared to recover : " << *p;
    }


    return RECOVER_OK;
}


recover_status FailureHandler::wait_new_proxy_up(std::unique_ptr<recovery_info>& f) {
    DLOG(INFO) << "pinging proxy" << f->new_;

    prediction::ping_req req;
    prediction::ping_reply reply;
    grpc::ClientContext context;

    auto channel = grpc::CreateChannel(f->new_.ip+":"+std::to_string(f->new_.port), grpc::InsecureChannelCredentials());

    auto stub = prediction::ProxyServer::NewStub(channel);

    grpc::Status status = stub->ping(&context, req, &reply);


    if (!status.ok()){
        if (status.error_code() == grpc::DEADLINE_EXCEEDED || status.error_code() == grpc::UNAVAILABLE){
            LOG(WARNING) << "Retrying pinging!!" ;
            usleep(200 * 1000);
            wait_new_proxy_up(f);
        }
        else{
            LOG(FATAL) << "ping: Simultaneous failure NOT implemented !!" << status.error_code() ;
        }
    }
    return RECOVER_OK;
}


recover_status FailureHandler::recover_new_proxy_state(std::unique_ptr<recovery_info>& f) {
    DLOG(INFO) << "asking new proxy to recover state" << f->new_;

    prediction::new_proxy_recover_req req;
    modeltest::response ack;
    grpc::ClientContext context;


    auto channel = grpc::CreateChannel(f->new_.ip+":"+std::to_string(f->new_.port), grpc::InsecureChannelCredentials());

    auto stub = prediction::ProxyServer::NewStub(channel);

    std::vector<std::string> reduce_pred_names;

    for (auto e: current_dag->reduce_edges){
        if (e.to == int(f->model_id)){
            auto pred_group = req.add_pred_groups();
            for (auto from : e.froms){
                bool found = false; 
                for (auto &p : f->predecessors){
                    if (int(p.model_id) == from){
                        found = true; 
                        auto pred = pred_group->add_proxies();
                        pred->CopyFrom(p.serialize());
                        pred->set_seq(this->start_seq);
                        reduce_pred_names.push_back(p.cotainer_name);
                        DLOG(INFO) << "recover new: has reduce edge to me, added" << p;
                    }
                }
                for (auto &p : f->new_predecessors){
                    if (int(p.model_id) == from){
                        found = true; 
                        auto pred = pred_group->add_proxies();
                        pred->CopyFrom(p.serialize());
                        pred->set_seq(this->start_seq);
                        reduce_pred_names.push_back(p.cotainer_name);
                        DLOG(INFO) << "recover new: has reduce edge to me, added" << p;
                    }
                }
                if (!found){
                    LOG(FATAL) << "an reducer pred not in predecessors list" << from;
                }
            }
        }
    }



    for (auto &p : f->predecessors){
        if (std::count(reduce_pred_names.begin(), reduce_pred_names.end(), p.cotainer_name) == 0){
            auto pred_group = req.add_pred_groups();
            auto pred = pred_group->add_proxies();

            pred->CopyFrom(p.serialize());
            pred->set_seq(this->start_seq);
            DLOG(INFO) << "recover new, added non reduce pred: " <<p; 
        }
    }

    for (auto &p : f->new_predecessors){
        if (std::count(reduce_pred_names.begin(), reduce_pred_names.end(), p.cotainer_name) == 0){
            auto pred_group = req.add_pred_groups();
            auto pred = pred_group->add_proxies();

            pred->CopyFrom(p.serialize());
            pred->set_seq(this->start_seq);
            DLOG(INFO) << "recover new, added non reduce pred: " <<p; 
        }
    }




    for (auto &s: f->successors){
        auto down_group  = req.add_down_groups();
        auto downstream = down_group->add_proxies();
        downstream->CopyFrom(s.serialize());
    }

    req.set_start_seq(this->start_seq+1);


    for (auto &s: f->new_successors){
        auto down_group  = req.add_down_groups();
        auto downstream = down_group->add_proxies();
        downstream->CopyFrom(s.serialize());
    }


    req.set_model_id(f->model_id);
    req.set_model_ip(f->model_ip);
    req.set_my_uri(f->new_.cotainer_name);

    grpc::Status status = stub->recover_new_proxy(&context, req, &ack);

    if (!status.ok()){
        if (status.error_code() == grpc::StatusCode::INTERNAL){
            if (status.error_message() == "Not in the recovery mode"){
                LOG(WARNING) <<"failure handled by other proxies";
                return RECOVER_HANDLED; //2019/05/29/ Seems not gona happen in current implementation.
            }
        }else{;
            return RECOVER_UNKNOWN;
        }
    }
    return RECOVER_OK;

}


recover_status FailureHandler::commit_successors(const p_info& failed, std::unique_ptr<recovery_info>& f) {
    prediction::recovery_req rec;
    rec.set_start_seq(this->start_seq+1);

    auto new_serialized = f->new_.serialize();
    auto failed_serialized = failed.serialize();

    rec.mutable_new_proxy()->CopyFrom(new_serialized);
    rec.mutable_failed_proxy()->CopyFrom(failed_serialized);

    for (auto s = f->successors.begin(); s != f->successors.end(); s++) {
        DLOG(INFO) << "asking successor to commit for recovery" << *s;

        if (s->stateful && !s->is_primary){
            //A backup successor.
            if (std::count(failed_stateful_model_ids.begin(), failed_stateful_model_ids.end(), s->model_id) != 0){
                DLOG(INFO) << "Successor is a newly started backup successor, no need to ask for prepare for now: " << *s;
                continue;
            }
        }


        modeltest::response ack;
        grpc::ClientContext context;
        auto channel = grpc::CreateChannel(s->ip + ":" + std::to_string(s->port), grpc::InsecureChannelCredentials());

        auto stub = prediction::ProxyServer::NewStub(channel);



        grpc::Status status = stub->commit_recover_successor(&context, rec, &ack);

        if (!status.ok()) {
            LOG(WARNING) << "successor commit: Successor failed !!" << status.error_code();
            /* One Successor of the failed proxy has failed.
             *
             * Step a: get the successors of the failed successor.
             * Get form the successors: Max received seq.
            */
            if (!s->stateful){
                p_info new_succ;
                auto recover_status = this->notify_failure(*s, new_succ);

                if (recover_status != RECOVER_OK){
                    return recover_status;
                }

                f->successors.emplace_back(new_succ);
                f->successors.erase(s);
            }
            else{
                LOG(FATAL) << "Not implemented 5";
            }
            return RECOVER_MORE_FAILURE;
        }
    }

    DLOG(INFO) << "All successors commit to recovery, failed proxy = " << failed;
    return RECOVER_OK;

}



recover_status FailureHandler::commit_predeccsors(const p_info& failed, std::unique_ptr<recovery_info>& f) {
    prediction::recovery_req rec;


    auto new_serialized = f->new_.serialize();
    auto failed_serialized = failed.serialize();

    rec.mutable_new_proxy()->CopyFrom(new_serialized);
    rec.mutable_failed_proxy()->CopyFrom(failed_serialized);


    for (auto p = f->predecessors.begin(); p != f->predecessors.end(); p++) {
        DLOG(INFO) << "asking predecessor to commit for recovery" << *p;

        modeltest::response ack;
        grpc::ClientContext context;
        auto channel = grpc::CreateChannel(p->ip + ":" + std::to_string(p->port), grpc::InsecureChannelCredentials());

        auto stub = prediction::ProxyServer::NewStub(channel);
        rec.set_start_seq(this->start_seq);


        grpc::Status status = stub->commit_recover_predecessor(&context, rec, &ack);

        if (!status.ok()) {
            LOG(WARNING) << "predecessors commit: predecessors failed !!" << status.error_code();
            /* One Successor of the failed proxy has failed.
             *
             * Step a: get the successors of the failed successor.
             * Get form the successors: Max received seq.
            */
            if (!p->stateful){
                p_info new_pred;
                auto recover_status = this->notify_failure(*p, new_pred);
                if (recover_status != RECOVER_OK){
                    return recover_status;
                }


                f->predecessors.erase(p);
                f->new_predecessors.push_back(new_pred);
            }
            else{
                LOG(FATAL) << "Not implemented 6";
            }
            return RECOVER_MORE_FAILURE;
        }
    }


    for (auto p = f->backup_predecessors.begin(); p != f->backup_predecessors.end(); p++) {
        DLOG(INFO) << "asking backup predecessor to commit for recovery" << *p;
        DLOG(WARNING) << "Currently not handling backup predecessor, will implement in the future";
        //TODO: 2019/05/29  All commented, not handling for now.
//        modeltest::response ack;
//        grpc::ClientContext context;
//        auto channel = grpc::CreateChannel(p->ip + ":" + std::to_string(p->port), grpc::InsecureChannelCredentials());
//
//        auto stub = prediction::ProxyServer::NewStub(channel);
//        rec.set_start_seq(p->p_start_seq);
//
//
//        grpc::Status status = stub->commit_recover_predecessor(&context, rec, &ack);
//
//        if (!status.ok()) {
//            LOG(WARNING) << "predecessors commit: backup predecessors failed !!" << status.error_code();
//            /* One Successor of the failed proxy has failed.
//             *
//             * Step a: get the successors of the failed successor.
//             * Get form the successors: Max received seq.
//            */
//
//            LOG(FATAL) << "Not implemented 6";
//
//            return false;
//        }
    }



    DLOG(INFO) << "All predecessors commit to recovery, failed = " << failed;
    return RECOVER_OK;

}


recover_status FailureHandler::promote_primary(std::unique_ptr<recovery_info> &f) {
    DLOG(INFO) << "Promoting as primary for " << f->new_;

    prediction::new_proxy_recover_req req;


     std::vector<std::string> reduce_pred_names;

    for (auto e: current_dag->reduce_edges){
        if (e.to == int(f->model_id)){
            auto pred_group = req.add_pred_groups();
            for (auto from : e.froms){
                bool found = false; 
                for (auto &p : f->predecessors){
                    if (int(p.model_id) == from){
                        found = true; 
                        auto pred = pred_group->add_proxies();
                        pred->CopyFrom(p.serialize());
                        pred->set_seq(this->start_seq);
                        reduce_pred_names.push_back(p.cotainer_name);
                        DLOG(INFO) << "promote primary: has reduce edge to me, added" << p;
                    }
                }
                for (auto &p : f->new_predecessors){
                    if (int(p.model_id) == from){
                        found = true; 
                        auto pred = pred_group->add_proxies();
                        pred->CopyFrom(p.serialize());
                        pred->set_seq(this->start_seq);
                        reduce_pred_names.push_back(p.cotainer_name);
                        DLOG(INFO) << "promote primary: has reduce edge to me, added" << p;
                    }
                }
                if (!found){
                    LOG(FATAL) << "an reducer pred not in predecessors list" << from;
                }
            }
        }
    }



    for (auto &p : f->predecessors){
        if (std::count(reduce_pred_names.begin(), reduce_pred_names.end(), p.cotainer_name) == 0){
            auto pred_group = req.add_pred_groups();
            auto pred = pred_group->add_proxies();

            pred->CopyFrom(p.serialize());
            pred->set_seq(this->start_seq);
            DLOG(INFO) << "promote primary, added non reduce pred: " <<p; 
        }
    }

    for (auto &p : f->new_predecessors){
        if (std::count(reduce_pred_names.begin(), reduce_pred_names.end(), p.cotainer_name) == 0){
            auto pred_group = req.add_pred_groups();
            auto pred = pred_group->add_proxies();

            pred->CopyFrom(p.serialize());
            pred->set_seq(this->start_seq);
            DLOG(INFO) << "promote primary, added non reduce pred: " <<p; 
        }
    }


    for (auto &s: f->successors){
        auto down_group  = req.add_down_groups();
        auto downstream = down_group->add_proxies();
        downstream->CopyFrom(s.serialize());
    }

    req.set_start_seq(this->start_seq+1);


    for (auto &s: f->new_successors){
        auto down_group  = req.add_down_groups();
        auto downstream = down_group->add_proxies();
        downstream->CopyFrom(s.serialize());
    }





    modeltest::response ack;
    grpc::ClientContext context;

    auto channel = grpc::CreateChannel(f->new_.ip + ":" + std::to_string(f->new_.port), grpc::InsecureChannelCredentials());

    auto stub = prediction::ProxyServer::NewStub(channel);

    auto status = stub->promote_primary(&context, req, &ack);

    if (!status.ok()){
        if (status.error_code() == grpc::StatusCode::INTERNAL){
            if (status.error_message() == "Already promoted as primary"){
                LOG(WARNING) <<"failure handled by other proxies";
                return RECOVER_HANDLED;
            }
        }

        return RECOVER_UNKNOWN;
    }
    else{
        return RECOVER_OK;
    }
}





[[deprecated]]
void FailureHandler::fake_report_failure(const p_info& info, prediction::failure_reply &reply) {
    prediction::proxy_info failed = info.serialize();

    LOG(INFO) << "Server notified the failure of " << failed.ip() << ":" << failed.port() << "stateful:" << failed.stateful();

    if (failed.stateful() && failed.port() == 1232){
        reply.mutable_new_proxy()->set_ip("127.0.0.1");
        reply.mutable_new_proxy()->set_port(1233);
        reply.mutable_new_proxy()->set_stateful(true);
        reply.set_is_primary(true);


        auto sucessor = reply.add_successors();
        sucessor->set_stateful(false);
        sucessor->set_ip("127.0.0.1");
        sucessor->set_port(1231);


        auto predecessor = reply.add_predecessors();
        predecessor->set_stateful(false);
        predecessor->set_ip("127.0.0.1");
        predecessor->set_port(1234);

    }


    else if (failed.port() == 4230 || failed.port() == 4231 || failed.port() == 4232){

        reply.mutable_new_proxy()->set_ip("127.0.0.1");
        reply.mutable_new_proxy()->set_port(4231);
        reply.mutable_new_proxy()->set_stateful(true);

        if (failed.port() == 4232){
            reply.set_is_primary(true);
        }else{
            reply.set_is_primary(false);
        }



        auto sucessor = reply.add_successors();
        sucessor->set_stateful(false);
        sucessor->set_ip("127.0.0.1");
        sucessor->set_port(4229);


        auto predecessor = reply.add_predecessors();
        predecessor->set_stateful(false);
        predecessor->set_ip("127.0.0.1");
        predecessor->set_port(4233);

    }


    else if (failed.port() == 5231 || failed.port() == 5230 || failed.port() == 5229){

        reply.mutable_new_proxy()->set_ip("127.0.0.1");
        reply.mutable_new_proxy()->set_port(5230);
        reply.mutable_new_proxy()->set_stateful(true);

        if (failed.port() == 5231){
            reply.set_is_primary(true);
        }else{
            reply.set_is_primary(false);
        }



        auto sucessor = reply.add_successors();
        sucessor->set_stateful(false);
        sucessor->set_ip("127.0.0.1");
        sucessor->set_port(5228);

        sucessor = reply.add_successors();
        sucessor->set_stateful(false);
        sucessor->set_ip("127.0.0.1");
        sucessor->set_port(5227);


        auto predecessor = reply.add_predecessors();
        predecessor->set_stateful(false);
        predecessor->set_ip("127.0.0.1");
        predecessor->set_port(5233);

        predecessor = reply.add_predecessors();
        predecessor->set_stateful(false);
        predecessor->set_ip("127.0.0.1");
        predecessor->set_port(5232);

    }


    else if (failed.port() == 5233 || failed.port() == 5232){
        reply.mutable_new_proxy()->set_ip("127.0.0.1");
        reply.mutable_new_proxy()->set_port(failed.port() + 10);
        reply.mutable_new_proxy()->set_stateful(false);


        auto predecessor = reply.add_predecessors();
        predecessor->set_stateful(false);
        predecessor->set_ip("127.0.0.1");
        predecessor->set_port(5234);

        auto sucessor = reply.add_successors();
        sucessor->set_stateful(true);
        sucessor->set_ip("127.0.0.1");
        sucessor->set_port(5231);
        sucessor->set_is_primary(true);

        sucessor = reply.add_successors();
        sucessor->set_stateful(true);
        sucessor->set_ip("127.0.0.1");
        sucessor->set_port(5230);
        sucessor->set_is_primary(false);


        sucessor = reply.add_successors();
        sucessor->set_stateful(true);
        sucessor->set_ip("127.0.0.1");
        sucessor->set_port(5229);
        sucessor->set_is_primary(false);



    }
    else if (failed.port() == 5228 || failed.port() == 5227){
        reply.mutable_new_proxy()->set_ip("127.0.0.1");
        reply.mutable_new_proxy()->set_port(failed.port() + 10);
        reply.mutable_new_proxy()->set_stateful(false);


        auto predecessor = reply.add_predecessors();
        predecessor->set_ip("127.0.0.1");
        predecessor->set_port(5231);
        predecessor->set_stateful(true);
        predecessor->set_is_primary(true);

        predecessor = reply.add_predecessors();
        predecessor->set_ip("127.0.0.1");
        predecessor->set_port(5230);
        predecessor->set_stateful(true);
        predecessor->set_is_primary(false);

        predecessor = reply.add_predecessors();
        predecessor->set_ip("127.0.0.1");
        predecessor->set_port(5229);
        predecessor->set_stateful(true);
        predecessor->set_is_primary(false);



        auto sucessor = reply.add_successors();
        sucessor->set_stateful(false);
        sucessor->set_ip("127.0.0.1");
        sucessor->set_port(5226);

    }


    else if (!failed.stateful()){
        reply.mutable_new_proxy()->set_ip("127.0.0.1");
        reply.mutable_new_proxy()->set_port(failed.port() + 10);
        reply.mutable_new_proxy()->set_stateful(false);



        if (failed.port() == 1231 || failed.port() == 1232 || failed.port() == 1233){
            auto sucessor = reply.add_successors();
            sucessor->set_stateful(false);
            sucessor->set_ip("127.0.0.1");
            sucessor->set_port(failed.port() - 1);


            auto predecessor = reply.add_predecessors();
            predecessor->set_stateful(false);
            predecessor->set_ip("127.0.0.1");
            predecessor->set_port(failed.port() + 1);

        }






        else if (failed.port() == 2231){
            auto sucessor = reply.add_successors();
            sucessor->set_stateful(false);
            sucessor->set_ip("127.0.0.1");
            sucessor->set_port(2230);


            auto predecessor = reply.add_predecessors();
            predecessor->set_stateful(false);
            predecessor->set_ip("127.0.0.1");
            predecessor->set_port(2233);

            predecessor = reply.add_predecessors();
            predecessor->set_stateful(false);
            predecessor->set_ip("127.0.0.1");
            predecessor->set_port(2232);


        }
        else if (failed.port() == 2232){
            auto sucessor = reply.add_successors();
            sucessor->set_stateful(false);
            sucessor->set_ip("127.0.0.1");
            sucessor->set_port(2231);


            auto predecessor = reply.add_predecessors();
            predecessor->set_stateful(false);
            predecessor->set_ip("127.0.0.1");
            predecessor->set_port(2234);

        }

        else if (failed.port() == 2233){
            auto sucessor = reply.add_successors();
            sucessor->set_stateful(false);
            sucessor->set_ip("127.0.0.1");
            sucessor->set_port(2231);


            auto predecessor = reply.add_predecessors();
            predecessor->set_stateful(false);
            predecessor->set_ip("127.0.0.1");
            predecessor->set_port(2234);

        }

        else if (failed.port() == 3233){
            auto sucessor = reply.add_successors();
            sucessor->set_stateful(false);
            sucessor->set_ip("127.0.0.1");
            sucessor->set_port(3232);

            sucessor = reply.add_successors();
            sucessor->set_stateful(false);
            sucessor->set_ip("127.0.0.1");
            sucessor->set_port(3231);

            sucessor = reply.add_successors();
            sucessor->set_stateful(false);
            sucessor->set_ip("127.0.0.1");
            sucessor->set_port(3230);


            auto predecessor = reply.add_predecessors();
            predecessor->set_stateful(false);
            predecessor->set_ip("127.0.0.1");
            predecessor->set_port(3234);

        }


        else if (failed.port() == 3232 || failed.port() == 3231 || failed.port() == 3230){
            auto sucessor = reply.add_successors();
            sucessor->set_stateful(false);
            sucessor->set_ip("127.0.0.1");
            sucessor->set_port(3229);


            auto predecessor = reply.add_predecessors();
            predecessor->set_stateful(false);
            predecessor->set_ip("127.0.0.1");
            predecessor->set_port(3233);

        }

        else if (failed.port() == 3229){
            auto sucessor = reply.add_successors();
            sucessor->set_stateful(false);
            sucessor->set_ip("127.0.0.1");
            sucessor->set_port(3228);


            auto predecessor = reply.add_predecessors();
            predecessor->set_stateful(false);
            predecessor->set_ip("127.0.0.1");
            predecessor->set_port(3232);

            predecessor = reply.add_predecessors();
            predecessor->set_stateful(false);
            predecessor->set_ip("127.0.0.1");
            predecessor->set_port(3231);

            predecessor = reply.add_predecessors();
            predecessor->set_stateful(false);
            predecessor->set_ip("127.0.0.1");
            predecessor->set_port(3230);

        }

        else if (failed.port() == 3243){
            auto sucessor = reply.add_successors();
            sucessor->set_stateful(false);
            sucessor->set_ip("127.0.0.1");
            sucessor->set_port(3242);

            sucessor = reply.add_successors();
            sucessor->set_stateful(false);
            sucessor->set_ip("127.0.0.1");
            sucessor->set_port(3241);

            sucessor = reply.add_successors();
            sucessor->set_stateful(false);
            sucessor->set_ip("127.0.0.1");
            sucessor->set_port(3240);


            auto predecessor = reply.add_predecessors();
            predecessor->set_stateful(false);
            predecessor->set_ip("127.0.0.1");
            predecessor->set_port(3234);


        }


        else if (failed.port() == 3242 || failed.port() == 3241 || failed.port() == 3240){
            auto sucessor = reply.add_successors();
            sucessor->set_stateful(false);
            sucessor->set_ip("127.0.0.1");
            sucessor->set_port(3239);


            auto predecessor = reply.add_predecessors();
            predecessor->set_stateful(false);
            predecessor->set_ip("127.0.0.1");
            predecessor->set_port(3243);

        }

        else if (failed.port() == 4229){
            auto sucessor = reply.add_successors();
            sucessor->set_stateful(false);
            sucessor->set_ip("127.0.0.1");
            sucessor->set_port(4228);


            auto predecessor = reply.add_predecessors();
            predecessor->set_stateful(true);
            predecessor->set_ip("127.0.0.1");
            predecessor->set_port(4230);
            predecessor->set_is_primary(false);

            predecessor = reply.add_predecessors();
            predecessor->set_stateful(true);
            predecessor->set_ip("127.0.0.1");
            predecessor->set_port(4231);
            predecessor->set_is_primary(false);


            predecessor = reply.add_predecessors();
            predecessor->set_stateful(true);
            predecessor->set_ip("127.0.0.1");
            predecessor->set_port(4232);
            predecessor->set_is_primary(true);


        }

        else if (failed.port() == 4233){
            auto sucessor = reply.add_successors();
            sucessor->set_stateful(true);
            sucessor->set_is_primary(true);
            sucessor->set_ip("127.0.0.1");
            sucessor->set_port(4232);

            sucessor = reply.add_successors();
            sucessor->set_stateful(true);
            sucessor->set_is_primary(false);
            sucessor->set_ip("127.0.0.1");
            sucessor->set_port(4230);

            sucessor = reply.add_successors();
            sucessor->set_stateful(true);
            sucessor->set_is_primary(false);
            sucessor->set_ip("127.0.0.1");
            sucessor->set_port(4231);


            auto predecessor = reply.add_predecessors();
            predecessor->set_stateful(false);
            predecessor->set_ip("127.0.0.1");
            predecessor->set_port(4234);

        }



        else{
            //general case
	    LOG(FATAL) << "test case not implemented" ;

        }





    }
    else{
        LOG(FATAL) << "Server Not implemented, not the testing case";
    }
}



void FailureHandler::report_failure(const p_info &failed, management::FailureResponse* resp) {

    auto channel = grpc::CreateChannel(proxy->config->ADMIN_IP+":55555", grpc::InsecureChannelCredentials());

    auto stub = management::ManagementServer::NewStub(channel);


    grpc::ClientContext context;

    management::FailureInfo info;

    info.set_hostip(failed.ip);
    info.set_modelid(std::to_string(failed.model_id));
    info.set_appid(proxy->dag_name());
    info.set_modelname(failed.model_container_name);

    LOG(WARNING) << "Reporting failure of model " << failed;

    auto status =  stub->ReportContainerFailure(&context, info, resp);



    if (!status.ok()){
        LOG(FATAL) << "error for report failure not handled, code" << status.error_code() ;
    }

    DLOG(INFO) <<"report failure done, status [" << resp->status() <<"]EOF";
}
