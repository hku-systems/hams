#include <iostream>
#include <cstdlib>
#include <stdlib.h>     /* strtol */
#include "frontend.hpp"

int main(int argc, char** argv){
    google::InitGoogleLogging(argv[0]);
    
    LOG(INFO) << "n_workers" << std::getenv("MAX_WORKERS") << std::endl; 
    LOG(INFO) << "entry_proxy_name" << std::getenv("ENTRY_PROXY_NAME") << std::endl; 
    LOG(INFO) << "entry_proxy_port" << std::getenv("ENTRY_PROXY_PORT") << std::endl; 

    std::string entry_proxy_name = std::getenv("ENTRY_PROXY_NAME");
    std::string entry_proxy_port = std::getenv("ENTRY_PROXY_PORT");




    int n_workers = strtol(std::getenv("MAX_WORKERS"), nullptr, 10); 


    auto f = std::make_shared<Frontend>(entry_proxy_name, entry_proxy_port, n_workers); 

    f->run(); 
}