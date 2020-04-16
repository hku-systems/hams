//
// Created by xusheng on 2/20/19.
//

#include "DownStreamConn.hpp"
#include <glog/logging.h>
#include <prediction.pb.h>
#include "common.hpp"
#include "Config.hpp"




void DownStreamConn::pass_async(const prediction::request &req) {

    if (this->failed){
        return;
    }


    DLOG(INFO) << "Passing to downstream" << this->to_string() << "req id" << req.req_id();

    auto *call = new DownStreamCall;


    call->ctx.set_deadline(std::chrono::system_clock::now() +
                           std::chrono::milliseconds(config->TIMEOUT_MS));

    //TODO:: this may incur overhead if request is large
    call->req.CopyFrom(req);
    call->req.set_seq(++this->MaxPassed);
    call->req.set_src_uri(my_uri);

    call->resp_reader = stub->PrepareAsyncdownstream(&call->ctx, call->req, &cq);
    call->resp_reader->StartCall();
    //Register call back
    call->resp_reader->Finish(&call->ack, &call->status, (void*)call);

}

void DownStreamConn::handleAsyncACK() {
    void *got_tag;
    bool ok = false;
    while (cq.Next(&got_tag, &ok)){
        if (!ok){
            //TODO: handling here
            LOG(FATAL) <<"something wrong with the Completion Queue handling";
        }

        auto *call = static_cast<DownStreamCall*>(got_tag);


        if (call->status.ok()){
            DLOG(INFO) <<"Received ACK from downstream : "<< this->peer_container_name_ <<", seq = " <<call->ack.status();
        }
        else if (call->status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED) {
            //Two
            DLOG(INFO) << "Passing to downstream failed, deadline exceeded" << this->peer_container_name_; 	

            auto state = channel->GetState(true);
            if (state != GRPC_CHANNEL_READY){
                if (!channel->WaitForConnected(std::chrono::system_clock::now() +
                            std::chrono::milliseconds(config->TIMEOUT_MS))){

                    LOG(WARNING) << "Downstream Connection failure, call error: " << call->status.error_code() << " channel state: " << state;
                    if (report_failure()){
                        delete call;
                        return;
                    }
                    else{
                        LOG(WARNING) << "Recovery already in progress, retry";
                        retry(call);
                    }
                }
            }
            else{
                LOG(WARNING) << "Retrying passing to downstream after a timeout, seq = " << call->req.seq() << "downstream: " << this->peer_container_name_;
                //retry-the-timeout.
                retry(call);

            }
        }
        else if (call->status.error_code() == grpc::StatusCode::UNAVAILABLE){
            LOG(WARNING) << "Downstream Connection failure" << this->to_string() << "error code " << call->status.error_code() <<"req id" <<call->req.req_id();
            if (report_failure()){
                delete call;
                return;
            }
            else{
                LOG(WARNING) << "Recovery already in progress, sleep and retry";
                usleep(config->TIMEOUT_MS * 1000);
                retry(call);
            }
            //return;
        }
        else{
            LOG(FATAL) << "RPC Pass to downstream Failure not handled, seq = " << call->req.seq()
                << "error code" << call->status.error_code()
                << "msg" << call->status.error_message();
            return;
        }
        delete call;
    }
}

bool DownStreamConn::retry(const DownStreamCall* call){
    auto *call_retry = new DownStreamCall;
    call_retry->ctx.set_deadline(std::chrono::system_clock::now() +
                                 std::chrono::milliseconds(config->TIMEOUT_MS));

    //TODO:: this may incur overhead if request is large
    call_retry->req.CopyFrom(call->req);
    call_retry->req.set_seq(call->req.seq());
    call_retry->resp_reader = stub->PrepareAsyncdownstream(&call_retry->ctx, call_retry->req, &cq);
    call_retry->resp_reader->StartCall();
    //Register call back
    call_retry->resp_reader->Finish(&call_retry->ack, &call_retry->status, (void*)call_retry);

    return true;
}


bool DownStreamConn::report_failure() {
    if (this->failed == true){
        return true;
    }

    return my_group->handleFailure(shared_from_this());
}

void DownStreamConn::handle_recovery(const prediction::recovery_req& req) {

    this->failed = true;

}

std::unique_ptr<prediction::ping_reply> DownStreamConn::ping_downstream(int retry_ms, int retry_times) {
    if (retry_times < 0){
        return nullptr;
    }


    DLOG(INFO) << "PINGing downstream proxy " <<peer_addr() <<":" << peer_port_ << " remaining retry times" << retry_times;


//    auto stub = prediction::ProxyServer::NewStub(channel);

    prediction::ping_req req;
    grpc::ClientContext context;

    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::milliseconds(retry_ms));

    auto reply = std::make_unique<prediction::ping_reply>();

    grpc::Status status = stub->ping(&context, req, reply.get());

    DLOG(INFO) << "Downstream ping ack rpc status = " << status.error_code() << " downstream status = " <<reply->status();


    if (status.ok()) {
        auto downstream_status = proxy_status_t(reply->status());

        switch (downstream_status) {
            case PROXY_MODEL_UNSET:
            case PROXY_MODEL_SET:
            case PROXY_RECOVERING:
                usleep(uint(retry_ms) * 1000);
                return ping_downstream(retry_ms, retry_times - 1);
            case PROXY_READY:
                return reply;
            case PROXY_DOWNSTREAM_FAILED:
            case PROXY_MODEL_FAILED:
                return nullptr;
            default:
                return nullptr;
        }
    }
    else if (status.error_code() == grpc::DEADLINE_EXCEEDED){
        return ping_downstream(retry_ms, retry_times - 1);
    }
    else if (status.error_code() == grpc::UNAVAILABLE){
        usleep(uint(retry_ms) * 1000);
        return ping_downstream(retry_ms, retry_times-1);
    }
    else{
        return nullptr;
    }

}

// bool DownStreamConn::set_primary(const std::string &ip, int port) {

//     prediction::proxy_info info;
//     info.set_stateful(true);
//     info.set_ip(ip);
//     info.set_port(port);


//     modeltest::response resp;
//     grpc::ClientContext context;

//     auto status = stub->set_primary(&context, info, &resp);

//     return status.ok();
// }
