//
// Created by xusheng on 2/22/19.
//
#include <grpc++/grpc++.h>
#include "prediction.grpc.pb.h"
#include <unistd.h>
#include <thread>
#include <glog/logging.h>
#include <prediction.pb.h>
#include "DownStreamConn.hpp"

#include <sys/time.h>
#include "Config.hpp"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using grpc::CompletionQueue;

class FakeSMR{
public:
    FakeSMR(std::shared_ptr<Channel>  channel) : stub(prediction::ProxyServer::NewStub(channel)){}

   void pass(const std::string &input, uint64_t seq){

        auto *call = new ClientCall;
        call->req.mutable_input_()->set_inputstream(input);
        call->req.set_seq(seq);
        call->req.set_req_id(seq); 

        struct timeval tv;
        gettimeofday(&tv, NULL);
        call->req.mutable_timestamp()->set_seconds(tv.tv_sec);
        call->req.mutable_timestamp()->set_nanos(tv.tv_usec * 1000);

        call->req.set_src_uri("127.0.0.1:1234");


        call->resp_reader = stub->PrepareAsyncdownstream(&call->context, call->req, &cq);
        call->resp_reader->StartCall();
        call->resp_reader->Finish(&call->ack, &call->status, (void*)call);
    }

    void asyncComplete(){
        void *got_tag;
        bool ok = false;
        while (cq.Next(&got_tag, &ok)){
            auto *call = static_cast<ClientCall*>(got_tag);
            if (!ok){
                LOG(ERROR) <<"something wrong with the Completion Queue handling";
                return;
            }

            if (call->status.ok()){
                LOG(INFO) <<"Async Call Succeeded, seq = " <<call->ack.status();
            }else{
                LOG(ERROR) << "++ Async Call Failed, seq = " <<call->ack.status();
            }
            delete call;
        }
    }

private:
    std::unique_ptr<prediction::ProxyServer::Stub> stub;

    CompletionQueue cq;


    struct ClientCall {
        prediction::request req;
        modeltest::response ack;


        ClientContext context;
        Status status;
        std::unique_ptr<grpc::ClientAsyncResponseReader<modeltest::response>> resp_reader;

    };
};

int main(int argc, char** argv) {
    // Instantiate the client. It requires a channel, out of which the actual RPCs
    // are created. This channel models a connection to an endpoint (in this case,
    // localhost at port 50051). We indicate that the channel isn't authenticated
    // (use of InsecureChannelCredentials()).

    google::InitGoogleLogging(argv[0]);

    if (argc != 3){
        LOG(FATAL) << "usage: fakeSMR port sleep_time_ms" ;
    }


    auto config = std::make_shared<Config>();

    DownStreamConn d("127.0.0.1", std::stoi(argv[1]), 1,  "front-end", nullptr,  "127.0.0.1:1234", config);


    if (d.ping_downstream(100, 100) == nullptr){
        LOG(FATAL) << "first proxy not ready" ;
    }


    FakeSMR client(grpc::CreateChannel(
            "localhost:" + std::string(argv[1]), grpc::InsecureChannelCredentials()));

    auto t = std::thread(&FakeSMR::asyncComplete, &client);


    int sleep_ms = std::stoi(argv[2]);

    int seq = 1;
    while (true){
        LOG(INFO) << "sending request: " << seq << std::endl;
        std::string tmp = std::to_string((seq+1)/2);
        client.pass(tmp, uint64_t(seq));
        usleep(sleep_ms * 1000);
        seq++;
    }

    t.join();



    return 0;
}