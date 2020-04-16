
cmake_minimum_required(VERSION 2.6)

#file(MAKE_DIRECTORY ${RUNTIME_OUTPUT_DIRECTORY})
#
# GRPC and Protobuf
#
# Note that a third-party Cmake module is used.
# https://github.com/IvanSafonov/grpc-cmake-example.
#



cmake_minimum_required(VERSION 2.6)

set(CMAKE_CXX_FLAGS "-g -Wall -Werror -std=c++14")


project(serving)
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




set(proxy_source_files
        src/Dag.hpp
        src/Dag.cpp
        src/FrontendConn.hpp
        src/FrontendConn.cpp
        src/Proxy.cpp
        src/Proxy.hpp
        src/ModelConn.cpp
        src/ModelConn.hpp
        ${PROTO_SRCS}
        ${GRPC_SRCS}
        src/DownStreamConn.cpp
        src/DownStreamConn.hpp
        src/ClientConn.cpp
        src/ClientConn.hpp
        src/Handler.cpp
        src/Handler.hpp
        src/UpstreamStatus.cpp
        src/UpstreamStatus.hpp
        src/DownstreamGroup.hpp
        src/DownstreamGroup.cpp
        src/common.hpp
        src/FailureHandler.hpp
        src/FailureHandler.cpp
        src/ReqCache.cpp
        src/ReqCache.hpp
        src/Config.hpp
        src/Reducer.cpp src/Reducer.hpp)

add_library(lib_proxy STATIC
        ${proxy_source_files}
        )

add_executable(proxy
        src/Main.cpp
        )


target_link_libraries(proxy
        lib_proxy
        ${LINK_LIBS}
        )


#add_executable(fakeSMR
#        src/FakeSMR.cpp
#        )
#
#target_link_libraries(fakeSMR
#        lib_proxy
#        ${LINK_LIBS}
#
#        )
#
add_executable(fakeModel
        src/FakeModel.cpp
        )


target_link_libraries(fakeModel
        lib_proxy
        ${LINK_LIBS}
        )

#add_executable(outputChecker
#        src/OutputChecker.cpp
#        src/OutputChecker.hpp
#        )
#
#target_link_libraries(outputChecker
#        lib_proxy
#        ${LINK_LIBS}
#        )
