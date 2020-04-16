FROM ubuntu

RUN apt-get update &&\
      apt-get install -y build-essential autoconf git pkg-config automake libtool curl make g++ unzip cmake wget &&\
      apt-get clean

ENV GRPC_RELEASE_TAG=v1.19.0

RUN git clone -b ${GRPC_RELEASE_TAG} https://github.com/grpc/grpc /var/local/git/grpc &&\
   cd /var/local/git/grpc &&\
   git submodule update --init &&\
   echo "--- installing protobuf ---" &&\
   cd third_party/protobuf &&\
   ./autogen.sh &&\
   ./configure --enable-shared &&\
   make -j$(nproc) &&\
   make install &&\
   make clean &&\
   ldconfig &&\
   echo "--- installing grpc ---" &&\
   cd /var/local/git/grpc &&\
   make -j$(nproc) &&\
   make install &&\
   make clean &&\
   ldconfig &&\
   rm -rf /var/local/git

RUN cd / &&\
    wget https://github.com/google/glog/archive/v0.3.5.zip &&\
    unzip v0.3.5.zip &&\
    cd glog-0.3.5 &&\
    cmake . &&\
    cmake --build . --target install &&\
    rm -rf /glog-0.3.5 &&\
    rm -rf v0.3.5.zip


COPY ./proxy /ai-serving
COPY ./protobufs /protobufs
WORKDIR /ai-serving
RUN cmake . && make 

EXPOSE 22223

ENV GLOG_logtostderr=1

CMD ["/ai-serving/bin/proxy"]
