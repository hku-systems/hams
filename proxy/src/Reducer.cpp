//
// Created by xusheng on 5/2/19.
//

#include "Reducer.hpp"
#include "glog/logging.h"
#include "Proxy.hpp"
#include "prediction.grpc.pb.h"
#include "prediction.pb.h"

void Reducer::register_upstream(const std::shared_ptr<UpstreamStatus> & up){
    std::lock_guard<std::mutex> l(upstreams_mu);


//    for (auto &u: upstreams){
//        if (u == up) {
//            LOG(FATAL) << "Registering upstream: already in the vector: " << up->to_string();
//        }
//    }

    if (buffers.count(up) != 0){
        LOG(FATAL) << "Registering upstream: already in the vector: " << up->to_string();
    }


    std::vector<std::unique_ptr<prediction::request> > b;

    buffers[up] = std::move(b);


    LOG(INFO) << "Registered upstream to reducer" <<this << ": " << up->to_string();

}

void Reducer::remove_upstream(const std::shared_ptr<UpstreamStatus> &up) {
    std::lock_guard<std::mutex> l(upstreams_mu);
    if (buffers.count(up) == 0){
        LOG(WARNING) << "\n\n+++Removing upstream status not in this reducer+++\n\n\n\n" << up->to_string();
    }
    else{
        size_t removed = buffers.erase(up);
        LOG(INFO) << "Removed " << removed << "upstreams [" << up->to_string() << "], current count = " << buffers.size();
    }
}
void Reducer::truncate(const std::vector<std::string> & remain_list){
    std::lock_guard<std::mutex> l(upstreams_mu);
    for (auto it = buffers.cbegin(); it != buffers.cend(); ){
        auto up = std::find(remain_list.begin(), remain_list.end(), it->first->to_string());
        if (up == remain_list.end()){
            DLOG(INFO) << "[Truncate], removing upstream status from reducer: "<< it->first->to_string();
            buffers.erase(it++); //An interesting logic here. :-)
        }else{
            ++it;
        }
    }
    DLOG(INFO) << "Truncate complete, the remaing number of upstreams = " << buffers.size();
}

void Reducer::pass(std::unique_ptr<prediction::request> r, const std::shared_ptr<UpstreamStatus>& source){
    std::lock_guard<std::mutex> l(upstreams_mu);

    switch (policy){
        case up_RR:
            LOG(FATAL) << "RR policy not implemented";
            break;
        case up_reduce:
        {
            DLOG(INFO) << "Passed request to reducer, " << source->to_string() << "seq" << r->seq();

            if (buffers.count(source) != 0){
                buffers[source].push_back(std::move(r));
            }
            else{
                DLOG(WARNING) << "Reducer received request not for this reducer, drop it, source " << source->to_string() << "seq" << r->seq();
                return;
            }
           
            //Fixit: this may be slow for ultra-small models

            bool not_complete = false;
            for (auto &it : buffers) {
                if (it.second.empty()) {
                    DLOG(INFO) << "reducer empty, " << it.first->to_string();
                    not_complete = true;
                    break;
                }
            }
            if (not_complete){
                DLOG(INFO) << "Some upstream has no request, break, count" << ",total " << buffers.size() ;
                break;
            }

            DLOG(INFO) << "All upstream has request, trying to combine";

            std::string s;
            uint seq = 0;

            auto combined_req = std::make_unique<prediction::request>();

            uint n_buffer = 0;
            for (auto &it: buffers) {
                std::unique_ptr<prediction::request> req = std::move(it.second.front());
                if (seq == 0) {
                    seq = req->seq();
                    combined_req->set_seq(seq);
                    combined_req->set_req_id(req->req_id());
                } else if (seq != req->seq()) {
                    LOG(FATAL) << "Trying to combining input, sequence mismatch, :" << seq << " and " << req->seq();
                }

                s += req->input_().inputstream();
                n_buffer++;

                //consolidate lineage info
                //Each info is a tuple of <name, seq>
                for (int i = 0; i < req->lineage_info_size(); i++){
                    auto li = combined_req->add_lineage_info();
                    li->CopyFrom(req->lineage_info(i));
                }

                if (n_buffer != buffers.size()){
                    s += "|";
                }

                it.second.erase(it.second.begin());
            }

            combined_req->mutable_input_()->set_inputstream(s);
            combined_req->mutable_input_()->set_inputtype("String");
            proxy->enqueue_request(std::move(combined_req));
        }
        break;

        case up_direct:
            proxy->enqueue_request(std::move(r));
            break;
    }
}