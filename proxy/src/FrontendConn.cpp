

#include "FrontendConn.hpp"
#include "prediction.grpc.pb.h"

void FrontEndConn::pass_async(const prediction::request& req){

    auto *call = new FrontEndCall; 

    call->req.CopyFrom(req); 
    call->req.set_seq(++this->MaxPassed);
    call->req.set_src_uri(my_uri);

    DLOG(INFO) << "Passing output back to frontEnd" << call->req.seq(); 


    call->resp_reader = stub->PrepareAsyncoutputstream(&call->ctx, call->req, &cq);
    call->resp_reader->StartCall(); 
    call->resp_reader->Finish(&call->ack, &call->status, (void*)call);
}

void FrontEndConn::handle_async_resp(){
    void *got_tag;
    bool ok = false;

    while (cq.Next(&got_tag, &ok)){
        if (!ok){
            //TODO: handling here
            LOG(FATAL) <<"something wrong with the Completion Queue handling";
        }

        auto *call = static_cast<FrontEndCall*>(got_tag);


        if (call->status.ok()){
            DLOG(INFO) <<"Received ACK from frontEnd, seq = " <<call->req.seq();
        }
        else {
            LOG(WARNING) << "Sending output to front-end: error not handled, status: " << call->status.error_code() << call->status.error_message(); 
        }
    }
}