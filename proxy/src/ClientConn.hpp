//
// Created by xusheng on 2/20/19.
//

#ifndef SERVING_CLIENTCONN_HPP
#define SERVING_CLIENTCONN_HPP

#include <string>
#include <grpc++/grpc++.h>


class ClientConn {
public:
    ClientConn(const std::string &ip_str, uint port)
        : peer_addr_(ip_str),
          peer_port_(port),
          channel(grpc::CreateChannel(ip_str+":"+std::to_string(port), grpc::InsecureChannelCredentials())){}
    ~ClientConn()= default;

    bool connect(){
        bool connected = channel->WaitForConnected(gpr_time_add(
                gpr_now(GPR_CLOCK_REALTIME),
                gpr_time_from_seconds(60, GPR_TIMESPAN)));

        std::cout << "Connecting to RPC server, channel state: "<< channel->GetState(false) << std::endl;

        return connected;


    };

    inline std::string peer_addr(){
        return peer_addr_;
    }

    inline uint peer_port(){
        return peer_port_;
    }



protected:
    std::string peer_addr_;
    uint peer_port_;
    std::shared_ptr<grpc::Channel> channel;
};


#endif //SERVING_CLIENTCONN_HPP
