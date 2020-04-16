//
// Created by xusheng on 5/2/19.
//

#ifndef SERVING_REDUCER_HPP
#define SERVING_REDUCER_HPP


#include <memory>
#include <vector>
#include "common.hpp"
#include <memory>
#include <map>
#include <mutex>

class Proxy;
class UpstreamStatus;
namespace prediction{
    class request;
}



class Reducer : public std::enable_shared_from_this<Reducer> {
public:
    explicit Reducer (std::shared_ptr<Proxy> proxy, reducer_policy_t policy):
        proxy(std::move(proxy)), policy(policy)
        {};

    void register_upstream(const std::shared_ptr<UpstreamStatus>&);

    void remove_upstream(const std::shared_ptr<UpstreamStatus> &);

    void pass(std::unique_ptr<prediction::request>, const std::shared_ptr<UpstreamStatus>&);

    void truncate(const std::vector<std::string> & remain_list);


private:
//    std::vector<std::shared_ptr<UpstreamStatus>> upstreams;
//    std::vector<std::vector<std::unique_ptr<prediction::request>>> buffers;

    std::map<std::shared_ptr<UpstreamStatus>, std::vector<std::unique_ptr<prediction::request>>> buffers;


    std::mutex upstreams_mu;


    std::shared_ptr<Proxy> proxy;

    reducer_policy_t policy;

};


#endif //SERVING_REDUCER_HPP
