
cmake_minimum_required(VERSION 2.6)

set(CMAKE_CXX_FLAGS "-g -Wall -Werror -std=c++14")


project(frontend)
set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_HOME_DIRECTORY}/bin")



list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}")
find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)
find_package(glog REQUIRED)


set(PROTOS
        "${CMAKE_CURRENT_LIST_DIR}/../protobufs/model.proto"
        "${CMAKE_CURRENT_LIST_DIR}/../protobufs/prediction.proto"
        "${CMAKE_CURRENT_LIST_DIR}/../protobufs/management.proto"
        )


set(PROTO_SRC_DIR ${CMAKE_CURRENT_LIST_DIR}/src/proto-src)
file(MAKE_DIRECTORY ${PROTO_SRC_DIR})
include_directories(${PROTO_SRC_DIR})

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_SRC_DIR} ${PROTOS})
grpc_generate_cpp(GRPC_SRCS GRPC_HDRS ${PROTO_SRC_DIR} ${PROTOS})

set(LINK_LIBS
        gRPC::grpc++_reflection
        protobuf::libprotobuf
        glog::glog
)


set(src_files 
        src/frontend.cpp
        src/frontend.hpp
        src/main.cpp
        ${PROTO_SRCS}
        ${GRPC_SRCS}
)
      
add_executable(frontend
        ${src_files}
        )


target_link_libraries(frontend
        ${LINK_LIBS}
        )

add_executable(test_proxy
        src/test.cpp
        ${PROTO_SRCS}
        ${GRPC_SRCS}
        )

target_link_libraries(test_proxy
        ${LINK_LIBS}
        )


