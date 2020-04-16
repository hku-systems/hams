//
// Created by xusheng on 3/8/19.
//

#ifndef SERVING_ENUMS_H
#define SERVING_ENUMS_H

//#define CLI_TEST

//#define PROXY_PORT 22223
//
//#define REQ_TIMEOUT_MS 100
//#define CHANNEL_CONNECT_TIMEOUT_MS 1000
//
//#define INIT_PING_TIMEOUT_MS 200
//#define INIT_PING_RETRY_TIMES 1000

enum Downstream_Intergroup_policy_t {

    //Result of my model is passed to *all
            all = 0,

    //Result of my model is passed to
            RR = 1,

    //More comming...
};

enum proxy_status_t {
    PROXY_READY = 0,
    PROXY_MODEL_UNSET = 1,
    PROXY_MODEL_SET = 2,

    PROXY_RECOVERING = 3,
    PROXY_MODEL_FAILED = 4,
    PROXY_DOWNSTREAM_FAILED = 5,
    PROXY_FAILURE_MANAGER = 6,
    PROXY_UPSTREAM_FAILED = 7
};

enum model_status_t {

};

enum reducer_policy_t{
    up_direct = 0,
    up_reduce = 1,
    up_RR = 2,
};





#endif //SERVING_ENUMS_H
