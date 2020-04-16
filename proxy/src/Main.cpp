//
// Created by xusheng on 3/4/19.
//
#include "Proxy.hpp"
#include <glog/logging.h>
#include "common.hpp"
#include "Config.hpp"


int main(int argc, char* argv[]){
    //TODO: this arg parsing is very bad

    google::InitGoogleLogging(argv[0]);



    std::shared_ptr<Config> config = std::make_shared<Config>();


    std::shared_ptr<Proxy> proxy;

    if (config->CLI_TEST == false){
        proxy = std::make_shared<Proxy>(config->PROXY_PORT, config);

        proxy->start_handler();
    }
    else {

        if (argc < 4) {
            LOG(FATAL)
                    << "usage: proxy proxy_port model_port is_stateful(true/false) {group_count [downstream_ip downstream_port]}"
                    << std::endl;
            return -1;
        }

        std::string my_uri = std::string("127.0.0.1") + ":" + argv[1];

        proxy = std::make_shared<Proxy>(std::stoi(argv[1]), config);
        proxy->setMyUri(my_uri);
        proxy->start_handler();


        if (strcmp(argv[3], "true") == 0) {
            proxy->register_model("127.0.0.1", std::stoi(argv[2]));

            DLOG(INFO) << "Stateful from parsing the cmd";
        } else {
            proxy->register_model("127.0.0.1", std::stoi(argv[2]));
            DLOG(INFO) << "Stateless from parsing the cmd";
        }


        if (strcmp(argv[4], "recovery") == 0) {
            proxy->set_status(PROXY_RECOVERING);
        } else {
            int parsed = 4;
            while (argc > parsed) {
                int group_count = std::stoi(argv[parsed++]);

                std::vector<std::shared_ptr<DownStreamConn>> conns;
                for (int i = 0; i < group_count; i++) {
                    std::string ip = argv[parsed];
                    auto conn = std::make_shared<DownStreamConn>(ip, uint(std::stoi(argv[parsed + 1])), 1, ip, ip, proxy,
                                                                 my_uri, config);
                    parsed += 2;
                    conns.push_back(conn);
                }
                auto group = std::make_shared<DownstreamGroup>(proxy);
                group->add(conns);

                proxy->add_downstream_group(group);

            }
            proxy->init();
        }
    }


    proxy->wait();
}

