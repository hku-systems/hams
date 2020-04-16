//
// Created by xusheng on 3/4/19.
//

#include "OutputChecker.hpp"
#include <glog/logging.h>
#include <sys/time.h>
#include "Config.hpp"

bool OutputChecker::predict_async(std::unique_ptr<prediction::request> request) {

    struct timeval tv;

    gettimeofday(&tv, NULL);



    LOG(INFO) << "seq = " << request->seq() <<" id: " << request->req_id() <<" : " << request->input_().inputstream() <<
        " passed " << tv.tv_sec-request->timestamp().seconds() << "s" << tv.tv_usec - request->timestamp().nanos()/1000 << "us";

    return true;
};


int main(int argc, char* argv[]){
    //TODO: this arg parsing is very bad

    google::InitGoogleLogging(argv[0]);

    if (argc != 2){
        LOG(FATAL) <<"usage: outputChecker port";
        return -1;
    }


    auto config = std::make_shared<Config>();

    std::shared_ptr<OutputChecker> checker = std::make_shared<OutputChecker>(std::stoi(argv[1]), config);

    checker->init();
    checker->start_handler();


    checker->wait();
}

