//
// Created by xusheng on 3/4/19.
//

#ifndef SERVING_REPLICATEDDOWNSTREAM_HPP
#define SERVING_REPLICATEDDOWNSTREAM_HPP

#include "../DownStreamConn.hpp"
#include "../DownstreamGroup.hpp"

class ReplicatedDownstream : DownstreamGroup{
public:
    ReplicatedDownstream()= default;
    ~ReplicatedDownstream()= default;

    int addReplica(std::shared_ptr<DownStreamConn> conn){
        return this->add(std::move(conn));
    };

    bool assignLeader(std::shared_ptr<DownStreamConn> conn);


private:
    //use shared for making pointers point to leader.
    std::vector<std::shared_ptr<DownStreamConn>> replicas;

    std::shared_ptr<DownStreamConn> leader;

};

#endif //SERVING_REPLICATEDDOWNSTREAM_HPP
