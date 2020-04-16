//
// Created by xusheng on 2/20/19.
//

#include "ModelConn.hpp"
#include "prediction.grpc.pb.h"
#include <grpc++/grpc++.h>
#include <glog/logging.h>
#include "Proxy.hpp"
#include "Config.hpp"

grpc::Status ModelConn::predict_sync(const prediction::request &req, modeltest::output *reply){
    grpc::ClientContext context;

    DLOG(INFO) << "calling model, seq = " <<req.seq() << " input = " << req.input_().inputstream() << " id = " << req.req_id()  << std::endl;



    grpc::Status status = stub->Predict(&context, req.input_(), reply);
    return status;
}

grpc::Status ModelConn::batch_predict_sync(const std::vector<std::unique_ptr<prediction::request>> &requests,
                                           modeltest::BatchOuput *outputs) {
    modeltest::BatchInput inputs;

    for (const auto &r : requests){
        auto i = inputs.add_inputs();
        i->CopyFrom(r->input_());
    }

    DLOG(INFO) << "calling model, batch size = " << inputs.inputs_size();
    if (!requests.empty()){
        DLOG(INFO) << "----- first sequence: " << requests.front()->seq();
    }

    grpc::ClientContext context;
    grpc::Status status = stub->BatchPredict(&context, inputs, outputs);

    return status;





}


bool ModelConn::enqueue_request(std::unique_ptr<prediction::request> req){

    DLOG(INFO) << "enquing request input:" << req->input_().inputstream();
    n_request++;

    std::unique_lock<std::mutex> lck(pending_queue_lock);

    pending_reqs.push(std::move(req));
    pending_queue_cond.notify_all();

    lck.unlock();

    return true;
}

void ModelConn::state_retrival_loop(){
    LOG(INFO) << "created a thread for state retrivial loop" << std::endl; 

    while (true)
    {
        std::unique_lock<std::mutex> lck(state_retrival_lock); 
        state_retrival_cond.wait(lck, [this]{
            return state_retrival_flag;
            }
        );

        prediction::modelstate ms; 
        std::string tensors = get_state(); 
        ms.set_tensors(tensors);

        std::vector<std::shared_ptr<modeltest::output>> outputs = output_buffer.front();
        for (auto &o : outputs){
          ms.add_outputs(o->outputstream());
        }


        proxy->make_durable(ms);
        state_retrival_flag = false; 
        lck.unlock();
    }
    

}

std::string ModelConn::get_state(){
    modeltest::TensorState ts; 
    modeltest::response emptyReq; 
    grpc::ClientContext context; 

    grpc::Status status = stub->GetState(&context, emptyReq, &ts);

    return ts.tensors();
}

grpc::Status ModelConn::apply_state(std::string state){
    grpc::ClientContext context; 
    modeltest::TensorState ts; 
    ts.set_tensors(state); 
    modeltest::response resp; 

    grpc::Status status = stub->ApplyState(&context, ts, &resp);

    return status; 
}


void ModelConn::predict_loop() {

    LOG(INFO) << "created a thread for handling prediction" <<std::endl;
    while (true) {

        std::unique_lock<std::mutex> lck(pending_queue_lock);

        pending_queue_cond.wait(lck, [this]{return !pending_reqs.empty(); });

        if (proxy->batch_size() == 0){
            // No batch needed. 
            std::unique_ptr<prediction::request> req = std::move(pending_reqs.front());
            pending_reqs.pop();
            lck.unlock();

            std::shared_ptr<modeltest::output> reply = nullptr;

            // Stateful model cannot leverage caching
            // Stateless model can leverage caching, even in the presence of non-determinsim. 
            if (!proxy->is_stateful() && config->CACHING_ENABLED){
                reply = cache.get(req);
            }

            if (reply == nullptr){
                //cache miss or stateful
                DLOG(INFO) << "Cache miss! id = " << req->req_id() << " input = " << req->input_().inputstream();
                cache_miss_count++; 
                reply = std::make_shared<modeltest::output>();
                this->predict_sync(*req, reply.get());


                if (!proxy->is_stateful() && config->CACHING_ENABLED){
                    cache.set(req, reply, true);
                }else{
                    //no need to insert into the input->output mapping
                    cache.set(req, reply, false);
                }

                if (proxy->is_stateful()){
                    //notify the state loop to asynchronosly update
                    std::unique_lock<std::mutex> slck(state_retrival_lock);

                    std::vector<std::shared_ptr<modeltest::output>> vec; 
                    vec.push_back(reply);
                    output_buffer.push_back(vec);

                    state_retrival_flag = true; 
                    state_retrival_cond.notify_all();

                    slck.unlock();
                }

            }else{
                cache.set(req, reply, false);
                cache_hit_count++; 
                DLOG(INFO) << "Cache hit! id = " << req->req_id() << " input = " << req->input_().inputstream();
            }

            auto downstream_req = std::make_unique<prediction::request>();
            downstream_req->mutable_input_()->set_inputstream(reply->outputstream());
            downstream_req->mutable_input_()->set_inputtype(reply->outputtype());
            downstream_req->set_req_id(req->req_id());
            downstream_req->mutable_timestamp()->CopyFrom(req->timestamp());
            downstream_req->set_seq(req->seq());
            //pass the req's lineage information downwards
            for (int i = 0; i<req->lineage_info_size(); i++){
                auto li = downstream_req->add_lineage_info();
                li->CopyFrom(req->lineage_info(i));
            } 


            proxy->pass_downstream(std::move(downstream_req));
        }
        else{
            //handle batching.. 
            int count = 0;

            std::vector<std::unique_ptr<prediction::request>> downstream_reqs; 
            std::vector<std::unique_ptr<prediction::request>> reqs_to_predcit;
            std::vector<std::unique_ptr<prediction::request>> cache_hit_reqs;
            std::vector<std::shared_ptr<modeltest::output>> cache_hit_outputs;
            std::vector<bool> cache_hits;

            uint dr_start_seq; 
            dr_start_seq = pending_reqs.front()->seq();

            DLOG(INFO) << "batch prediction, batch size" << proxy->batch_size() << ", this batch's start seq:" << dr_start_seq;


            size_t min_length = SIZE_MAX;
            int min_len_index = -1;


            while (count < proxy->batch_size() && !pending_reqs.empty()) {
                //get a request from front. 
                std::unique_ptr<prediction::request> req = std::move(pending_reqs.front());
                pending_reqs.pop();
                //make a downstream request to maintain the order. 
                downstream_reqs.emplace_back(std::make_unique<prediction::request>());
//                req->set_seq(++req_seq);

                std::shared_ptr<modeltest::output> reply = nullptr;

                if (!proxy->is_stateful() && config->CACHING_ENABLED) {
                    reply = cache.get(req);
                }

                if (reply == nullptr) {
                    cache_miss_count++;
                    DLOG(INFO) << "Cache miss! id = " << req->req_id() << " seq = " << req->seq();
                    if (req->input_().inputstream().size() < min_length){
                        min_length = req->input_().inputstream().size();
                        min_len_index = count;
                    }
                    reqs_to_predcit.push_back(std::move(req));
                    cache_hits.push_back(false);
                    count++;
                } else {
                    cache_hit_count++;
                    DLOG(INFO) << "Cache hit! id = " << req->req_id() << " seq = " << req->seq();
                    cache_hit_reqs.push_back(std::move(req));
                    cache_hits.push_back(true);
                    cache_hit_outputs.push_back(reply);
                }
            }
            lck.unlock();


            DLOG(INFO) << "Finished packing batch, total (downstream_reqs) size = " << downstream_reqs.size()
                << ", total (cache_hits) size = " << cache_hits.size()
                << ", reqs_to_predcit size = " << reqs_to_predcit.size()
                << ", cache_hit_reqs size = " << cache_hit_reqs.size()
                << ", cache_hit_outputs size = " << cache_hit_outputs.size();



            if (!proxy->dynamic_batch() && !reqs_to_predcit.empty()){
                //Static batch, not empty.

                int pad_size = proxy->batch_size() - reqs_to_predcit.size();

                DLOG(INFO) << "Batch size = " << proxy->batch_size() << ", going to pad "
                    << pad_size << " requests, with the min length input, input index: " << min_len_index;

                for (int i = 0; i < pad_size; i++){
                    //padding with the shortest request.
                    auto req = std::make_unique<prediction::request>();
                    req->CopyFrom(*reqs_to_predcit[min_len_index]);
                    reqs_to_predcit.push_back(std::move(req));
                }
            }


            modeltest::BatchOuput predict_outputs;

            if (!reqs_to_predcit.empty()){
                this->batch_predict_sync(reqs_to_predcit, &predict_outputs);
            }
            else{
                DLOG(INFO) << "all cache hit, no need to call the model";
            }
            size_t hit_cur = 0;
            size_t miss_cur = 0;

            for (size_t i = 0; i < cache_hits.size(); i++){
                if (cache_hits[i] == true){
                    downstream_reqs[i]->mutable_input_()->set_inputstream(cache_hit_outputs[hit_cur]->outputstream());
                    downstream_reqs[i]->mutable_input_()->set_inputtype(cache_hit_outputs[hit_cur]->outputtype());
                    downstream_reqs[i]->set_req_id(cache_hit_reqs[hit_cur]->req_id());
                    downstream_reqs[i]->mutable_timestamp()->CopyFrom(cache_hit_reqs[hit_cur]->timestamp());
                    downstream_reqs[i]->set_seq(cache_hit_reqs[hit_cur]->seq());
                    //stash the lineage information to downstream
                    for (int j = 0; j<cache_hit_reqs[hit_cur]->lineage_info_size(); j++){
                        auto li = downstream_reqs[i]->add_lineage_info();
                        li->CopyFrom(cache_hit_reqs[hit_cur]->lineage_info(j));
                    }

                    cache.set(cache_hit_reqs[hit_cur], cache_hit_outputs[hit_cur], false);
                    hit_cur++;
                }
                else{
                    downstream_reqs[i]->mutable_input_()->set_inputstream(predict_outputs.outputs(miss_cur).outputstream());
                    downstream_reqs[i]->mutable_input_()->set_inputtype(predict_outputs.outputs(miss_cur).outputtype());
                    downstream_reqs[i]->set_req_id(reqs_to_predcit[miss_cur]->req_id());
                    downstream_reqs[i]->mutable_timestamp()->CopyFrom(reqs_to_predcit[miss_cur]->timestamp());
                    downstream_reqs[i]->set_seq(reqs_to_predcit[miss_cur]->seq());
                    //stash the lineage information to downstream
                    for (int j = 0; j<reqs_to_predcit[miss_cur]->lineage_info_size(); j++){
                        auto li = downstream_reqs[i]->add_lineage_info();
                        li->CopyFrom(reqs_to_predcit[miss_cur]->lineage_info(j));
                    }
                    auto reply = std::make_shared<modeltest::output>();
                    reply->CopyFrom(predict_outputs.outputs(miss_cur));
                    cache.set(reqs_to_predcit[miss_cur], reply, true);
                    miss_cur++;
                }
            }

            if (proxy->is_stateful()){
                //notify the state loop to asynchronosly update
                std::unique_lock<std::mutex> slck(state_retrival_lock);

                std::vector<std::shared_ptr<modeltest::output>> vec; 
                
                for (int i = 0; i<predict_outputs.outputs_size(); i++){
                    auto s_output = std::make_shared<modeltest::output> (predict_outputs.outputs(i)); 
                    vec.push_back(s_output);  
                }

                state_retrival_flag = true; 
                state_retrival_cond.notify_all();

                slck.unlock();
            }


            for (auto &r: downstream_reqs){
                proxy->pass_downstream(std::move(r));
            }
        }
    }
}

uint ModelConn::flush_cached_model_reply(uint start_seq, const std::shared_ptr<DownStreamConn> &d) {
    return cache.flush(start_seq, d);
}

bool  ModelConn::ping_model(int retry_ms, int retry_times){

    if (retry_times < 0){
        return false;
    }


    DLOG(INFO) << "PINGing model on " << peer_addr_ << ":" << peer_port_ << ", remaining retry times" << retry_times;


//    auto stub = prediction::ModelServer::NewStub(channel);

    modeltest::hi h;
    modeltest::response reply;
    grpc::ClientContext context;

    context.set_deadline(std::chrono::system_clock::now() +
                           std::chrono::milliseconds(retry_ms));


    grpc::Status status = stub->Ping(&context, h, &reply);

    if (status.ok()){
        return true;
    }
    else {
        DLOG(INFO) << "error pinging model, code = " <<status.error_code();
        if (status.error_code() == grpc::DEADLINE_EXCEEDED ) {
            return ping_model(retry_ms, retry_times-1);
        }
        else if (status.error_code() == grpc::UNAVAILABLE){
            usleep(uint(retry_ms) * 1000);
            return ping_model(retry_ms, retry_times-1);
        }
        else{
            return false;
        }
    }




}

void ModelConn::recover(uint start_seq) {
    cache.set_cache_start(start_seq);
}
