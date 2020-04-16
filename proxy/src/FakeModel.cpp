//
// Created by xusheng on 2/25/19.
//

#include <grpc++/grpc++.h>
#include <iostream>
#include "prediction.grpc.pb.h"
#include <glog/logging.h>
#include <model.grpc.pb.h>
#include <unistd.h>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;



class FakeModelImpl final : public modeltest::PredictService::Service {
    Status Predict(ServerContext *context, const modeltest::input *input, modeltest::output *reply) override {
        LOG(INFO)<<"service called, input" << input->inputstream();
        reply->set_outputstream(input->inputstream());
        reply->set_outputtype(input->inputtype());
        usleep(11* 1000);
        return Status::OK;
    }
    Status Ping (ServerContext *context, const modeltest::hi *req, modeltest::response *reply) override {
        return Status::OK;
    }

    Status BatchPredict (ServerContext *context, const modeltest::BatchInput *inputs, modeltest::BatchOuput *outputs) override{

        LOG(INFO)<<"batch predict called, count" << inputs->inputs_size();
        for (const auto &i : inputs->inputs()){
            LOG(INFO) << "----- input: " << i.inputstream();
            auto o = outputs->add_outputs();
            o->set_outputtype(i.inputtype());
            o->set_outputstream(i.inputstream());
        }
        usleep((10 + inputs->inputs_size()) * 1000);

        return Status::OK;
    }
};

int main(int argc, char** argv) {
    std::string server_address("[::]:"+std::string(argv[1]));
    FakeModelImpl service;

    ServerBuilder builder;

    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());

    builder.RegisterService(&service);

    std::unique_ptr<Server> server(builder.BuildAndStart());

    LOG(INFO) << "Server listening on " << server_address;

    server->Wait();
}