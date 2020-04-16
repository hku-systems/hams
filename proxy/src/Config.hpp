//
// Created by xusheng on 5/1/19.
//

#ifndef SERVING_CONFIG_HPP
#define SERVING_CONFIG_HPP


#include <cstdlib>
#include "glog/logging.h"

typedef unsigned int uint;


class Config {
public:
    explicit Config() {
        if (const char *timeout = std::getenv("TIMEOUT_MS")){
            TIMEOUT_MS = uint(strtol(timeout, nullptr, 10));
            if (TIMEOUT_MS == 0){
                LOG(FATAL) << "Failed to convert passed TIMEOUT_MS to uint";
            }
        }
        else{
            TIMEOUT_MS = 500;
        }


        if (const char *retry = std::getenv("INIT_RETRY_TIMES")){
            INIT_PING_RETRY_TIMES = uint(strtol(retry, nullptr, 10));
            if (INIT_PING_RETRY_TIMES == 0){
                LOG(FATAL) << "Failed to convert passed INIT_RETRY_TIMES to uint";
            }
        }
        else{
            INIT_PING_RETRY_TIMES = 1000;
        }


        if (const char *port = std::getenv("PROXY_PORT")){
            PROXY_PORT = uint(strtol(port, nullptr, 10));
            if (PROXY_PORT == 0){
                LOG(FATAL) << "Failed to convert passed PROXY_PORT to uint";
            }
        }
        else{
            PROXY_PORT = 22223;
        }

      if (const char *cache_size = std::getenv("CACHE_SIZE_MB")){
            CACHE_SIZE_MB = uint(strtol(cache_size, nullptr, 10));
            if (CACHE_SIZE_MB == 0){
                LOG(FATAL) << "Failed to convert passed CACHE_SIZE_MB to uint";
            }
        }
        else{
            CACHE_SIZE_MB = 1024;
        }


        if (const char *cli = std::getenv("CLI_TEST")){
            if (strcmp(cli, "true") == 0 || strcmp(cli, "True") == 0 || strcmp(cli, "TRUE") == 0 || strcmp(cli, "T") == 0){
                CLI_TEST = true;
            }
            else{
                CLI_TEST = false;
            }
        }else{
            CLI_TEST = false;
        }


        if (const char *cli = std::getenv("CACHING")){
            if (strcmp(cli, "true") == 0 || strcmp(cli, "True") == 0 || strcmp(cli, "TRUE") == 0 || strcmp(cli, "T") == 0){
                CACHING_ENABLED = true;
            }
            else{
                CACHING_ENABLED = false;
            }
        }else{
            CACHING_ENABLED = true;
        }



        if (const char *cli = std::getenv("RECOVERY")){
            if (strcmp(cli, "true") == 0 || strcmp(cli, "True") == 0 || strcmp(cli, "TRUE") == 0 || strcmp(cli, "T") == 0){
                RECOVERY = true;
            }
            else{
                RECOVERY = false;
            }
        }else{
            RECOVERY = false;
        }




        if (const char *cli = std::getenv("ADMIN_IP")){
            ADMIN_IP = cli;
        }else{
            ADMIN_IP = "localhost";
        }

        if (const char *cli = std::getenv("PROXY_NAME")){
            PROXY_NAME = cli;
        }else{
            PROXY_NAME = "!E!R!R!O!R!";
        }

        LOG(WARNING) << "Read Config from Env" << std::endl
            << "++++++++ PROXY_PORT: " << PROXY_PORT << std::endl
            << "++++++++ TIMEOUT_MS: " << TIMEOUT_MS << std::endl
            << "++++++++ INIT_PING_RETRY_TIMES: " << INIT_PING_RETRY_TIMES << std::endl
            << "++++++++ CLI_TEST: " << CLI_TEST << std::endl
            << "++++++++ CACHING: " << CACHING_ENABLED << std::endl
            << "++++++++ ADMIN_IP:" << ADMIN_IP << std::endl
            << "++++++++ RECOVERY: " << RECOVERY << std::endl
            << "++++++++ PROXY_NAME: " << PROXY_NAME << std::endl
            << "++++++++ CACHE_SIZE_MB:" << CACHE_SIZE_MB <<std::endl;
    }

    uint PROXY_PORT;  //default 22223

    uint TIMEOUT_MS;  //default 5000

    uint INIT_PING_RETRY_TIMES;  //default 1000

    std::string ADMIN_IP;

    bool CLI_TEST; // default false

    bool CACHING_ENABLED;

    bool RECOVERY;

    std::string PROXY_NAME;

    uint CACHE_SIZE_MB; 
};



#endif //SERVING_CONFIG_HPP
