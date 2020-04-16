# Proxy for Hams 

This page shows how to build Proxy for Hams.

## Build on local Machine 

### 1. Install dependencies.

``` 
$ apt-get update
$ apt-get install -y build-essential \
                     autoconf\
                     pkg-config\
                     automake\
                     libtool\
                     curl\ 
                     make\
                     g++\
                     unzip\
                     cmake\ 
                     wget
$ apt-get clean
```

### 2. Install grpc and protobuf via CMake. 

For more information, follow this grpc's official [link](https://github.com/grpc/grpc/blob/master/BUILDING.md#building-with-cmake)

We tested our code with GRPC rlease v1.19.

```
$ export GRPC_RELEASE_TAG=v1.19.x 
$ git clone -b ${GRPC_RELEASE_TAG} https://github.com/grpc/grpc /var/local/git/grpc
$ cd /var/local/git/grpc
$ git submodule update --init
$ cd third_party/protobuf
$ ./autogen.sh
$ ./configure --enable-shared
$ make
$ make install
$ make clean
$ ldconfig
$ cd /var/local/git/grpc
$ make
$ make install
$ make clean
$ ldconfig
```

### 3. Install Glog
For more information, follow this glog's official [link](https://github.com/google/glog/blob/master/cmake/INSTALL.md) 

We tested our code with Glog release v0.3.5.
```
$ cd /var/local
$ wget https://github.com/google/glog/archive/v0.3.5.zip 
$ unzip v0.3.5.zip
$ cd glog-0.3.5
$ cmake
$ cmake --build . --target install
$ cd /var/local
$ rm -rf glog-0.3.5
$ rm -rf v0.3.5.zip
```

### 4. Build the proxy 
```
$ cd Hams_dir/proxy
$ cmake .
$ make -j
```

## Build with docker 

Tested with Docker version 19.03.8, build `afacb8b7f0`.

```
$ cd Hams_dir
$ docker build -f proxy/Dockerfile -t ai-proxy .
```