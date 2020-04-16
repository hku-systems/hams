#include <iostream>
#include <memory>
#include <string>
#include <unistd.h>
#include <grpc++/grpc++.h>


#include "prediction.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using prediction::ProxyServer;
using prediction::request; 
using modeltest::response; 

// Logic and data behind the server's behavior.
class GreeterServiceImpl final : public ProxyServer::Service {
public:
    GreeterServiceImpl(){
        channel = CreateChannel("localhost:22224", grpc::InsecureChannelCredentials());
        stub_ = ProxyServer::NewStub(channel); 
    }

  Status downstream(ServerContext* context, const request* req,
                  response* reply) override {
    
    grpc::ClientContext c; 
    prediction::request out_req; 
    out_req.CopyFrom(*req);
    modeltest::response resp;  
    std::cout << "received request" << req->seq(); 
        
    usleep(3 * 1000); 
    auto status = stub_->outputstream(&c, out_req, &resp); 
    if (!status.ok()){
        std::cout << "code" << status.error_code() << std::endl; 
    }
    
    reply->set_status("ok");
    return Status::OK;
  }

  std::shared_ptr<grpc::Channel> channel;

  std::unique_ptr<ProxyServer::Stub> stub_;

};

void RunServer() {
  std::string server_address("0.0.0.0:22223");
  GreeterServiceImpl service;

  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

int main(int argc, char** argv) {
  RunServer();

  return 0;
}
