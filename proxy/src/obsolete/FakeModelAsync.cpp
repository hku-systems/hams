//
// Created by xusheng on 2/22/19.
//
#include <grpc++/grpc++.h>
#include <iostream>
#include "prediction.grpc.pb.h"
#include <glog/logging.h>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

class FakeModelAsync{
public:
    FakeModelAsync()= default;
    ~FakeModelAsync() {
        server->Shutdown();
        // Always shutdown the completion queue after the server.
        cq->Shutdown();
    }

    void run(std::string server_address){
        ServerBuilder builder;
        builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
        builder.RegisterService(&service);

        cq = builder.AddCompletionQueue();

        server = builder.BuildAndStart();
        LOG(INFO) << "Server listening on " << server_address;

        handleReqs();
    }

private:
    class CallData{
    public:
        CallData(prediction::ModelServer::AsyncService* service, grpc::ServerCompletionQueue* cq)
            : service(service),
              cq(cq),
              responder(&ctx),
              status(CREATE){
            Proceed();
        };
        ~CallData()= default;


        void Proceed(){
            switch (status){
                case CREATE:
                    status = PROCESS;
                    service->Requestpredict(&ctx, &req, &responder, cq, cq, this);
                    break;
                case PROCESS:
                    new CallData(service, cq);

                    reply.set_seq(req.seq());
                    reply.set_output(req.input());
                    LOG(INFO) <<"received prediction request, seq = " <<req.seq() <<"req" <<req.input();

                    status = FINISH;
                    responder.Finish(reply, Status::OK, this);
                    break;
                case FINISH:
                    delete(this);
                    break;
            }
        }
    private:

        prediction::ModelServer::AsyncService* service;

        grpc::ServerCompletionQueue* cq;

        ServerContext ctx;

        prediction::request req;

        prediction::reply reply;

        grpc::ServerAsyncResponseWriter<prediction::reply> responder;

        enum CallStatus{
            CREATE,
            PROCESS,
            FINISH,
        };

        CallStatus  status;
    };

    std::unique_ptr<grpc::ServerCompletionQueue> cq;
    prediction::ModelServer::AsyncService service;
    std::unique_ptr<grpc::Server> server;

    void handleReqs(){
        new CallData(&service, cq.get());

        void *tag;
        bool ok;
        while (true){

            bool ret = cq->Next(&tag, &ok);
            if (!ok || !ret){
                LOG(ERROR) <<"hander not ok, shut down";
                return;
            }
            static_cast<CallData*>(tag)->Proceed();
        }


    };

};

int main(int argc, char** argv) {
    google::InitGoogleLogging(argv[0]);

    std::string server_address("127.0.0.1:"+std::string(argv[1]));

    FakeModelAsync server;

    server.run(server_address);

}
