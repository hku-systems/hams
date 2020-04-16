//
// Created by xusheng on 3/4/19.
//

#ifndef SERVING_OUTPUTCHECKER_HPP
#define SERVING_OUTPUTCHECKER_HPP


#include "Proxy.hpp"



class OutputChecker : public Proxy {
public:
    explicit OutputChecker(int port, std::shared_ptr<Config> config)
        :Proxy(port, config){};
    ~OutputChecker()= default;

    bool predict_async(std::unique_ptr<prediction::request> request) override;
};


#endif //SERVING_OUTPUTCHECKER_HPP
