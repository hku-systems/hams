//
// Created by xusheng on 3/4/19.
//

#ifndef SERVING_DOWNSTREAMGROUP_HPP
#define SERVING_DOWNSTREAMGROUP_HPP

#include "prediction.grpc.pb.h"
#include "Proxy.hpp"

class DownStreamConn;
class Proxy;

class DownstreamGroup : public std::enable_shared_from_this<DownstreamGroup> {
public:
    explicit DownstreamGroup(std::shared_ptr<Proxy> proxy)
        : is_stateful_(false), proxy(std::move(proxy)) {};
    ~DownstreamGroup()= default;


    virtual void pass(const prediction::request &req);
    virtual bool add(std::shared_ptr<DownStreamConn> conn);
    virtual void add(std::vector<std::shared_ptr<DownStreamConn>> conns);

    virtual bool handleFailure(const std::shared_ptr<DownStreamConn> &conn);

    std::shared_ptr<DownStreamConn> find_conn(const std::string&  container_name);

    void recover_all(uint start_seq);

    bool wait_downstreams_ready(int retry_ms, int retry_times);

    void flush_cached_to_all(uint max);


    inline bool is_stateful(){
        return is_stateful_;
    }

    inline bool is_primary(std::shared_ptr<DownStreamConn>& conn){
        return conn == primary_conn;
    }

    void set_primary_conn(std::shared_ptr<DownStreamConn> p){
        primary_conn = std::move(p); 
    }

    friend std::ostream &operator<<(std::ostream &out, const DownstreamGroup &g);
private:
    std::vector<std::shared_ptr<DownStreamConn>> downstreams;

    bool is_stateful_;

    std::shared_ptr<DownStreamConn> primary_conn;

    std::shared_ptr<Proxy> proxy;



};



#endif //SERVING_DOWNSTREAMGROUP_HPP
