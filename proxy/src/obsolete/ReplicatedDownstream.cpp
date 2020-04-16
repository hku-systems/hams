//
// Created by xusheng on 3/4/19.
//

#include "ReplicatedDownstream.hpp"



bool ReplicatedDownstream::assignLeader(std::shared_ptr<DownStreamConn> conn) {
    auto it = std::find(replicas.begin(), replicas.end(), conn);
    if (it == replicas.end()){
        //not found;
        return false;
    }else{
        leader = conn;
        return true;
    }

}