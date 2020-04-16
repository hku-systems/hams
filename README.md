## Brief
HAMS is a research project. HAMS means high availability for ML service graphs.

The project follows Apache 2.0. 

The code is still under code cleaning, document preparation. Current HAMS only support key functionalities that deploy a directed graph of ML services and provide fault tolerance to the service. 

More functionalities like fine grained control of the services and more flexibilities are still under development.

Authors:

- Shixiong Zhao (sxzhao@cs.hku.hk)
- Xusheng Chen (xschen@cs.hku.hk)


## Architecture

```admin```: Deployment interfaces of services. Management daemon. The admin interfaces is developed from Clipper (ucbrise lab, https://github.com/ucbrise/clipper), and finally only remains some of the code structures.

```applications```: Definition and dockerfiles for the applications.

```frontend```: A simple frontend developed based on gRPC that receives and buffers requests.

```proxy```: Proxy code. Each container has a proxy. All proxies together build up the runtime and fault tolerance logics.

```grpcclient```: Contains client code.

```dockerfiles```: Contains dockerfiles to build up development dockers, proxy dockers, and frontend dockers.


## Quick Start

### Step 1: Run Development Docker 
```sh
docker run -it --network=host -v [YOUR_CODE_PATH_TO_HAMS]:/hams -v /var/run/docker.sock:/var/run/docker.sock -v /tmp:/tmp [Py35_Development_Docker]
```

### Step 2: Go to hams_admin dir

```sh
cd /hams/hams_admin
```
#### Step 3: Start DAG deployment

#### Test case 1: simple dag (No Prediction)
```sh
python simple_dag.py
```

#### Test case 2: predict stock price
```sh
python stock.py
```
### Step 4: See the dockers/logs
```sh
docker container ls 
docker container [CONTAINER_ID]
```
### Step 5: Stock DAG input / request
```sh
docker run -it --network hams_network zsxhku/grpcclient [IP_OF_THE_ENTRY_PROXY] 22223
```
You can see the /grpcclient/app/grpc_client.py and see the implementations and implement you own grpcclient docker

But remember you should run the grpcclient docker under hams_network


### Step 5: Stop containers
```sh
python stop_all.py
```

## Build your own application DAG and deployment 






