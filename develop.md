## Windows Subsystem Linux

```sh
docker run -it --network=host -v /c/code/hams-develop:/hams -v /var/run/docker.sock:/var/run/docker.sock -v /tmp:/tmp zsxhku/hamspy35dev
```
```sh
docker run -it --network hams_network -e PROXY_NAME=proxytest -e PROXY_PORT=22223 proxytest
```
```sh
docker run -it --network hams_network -e MODEL_NAME =grpctest -e MODEL_PORT=22222 grpctest
```
### Show all docker logs 
```sh
docker ps -q | xargs -L 1 docker logs
```

### kill all docker 
```sh
docker kill $(docker ps -a -q)
```

### Translation test

```sh
python ../applications/translation/client.py 172.18.0.3 22223
```

## MacOS

```sh
docker run -it --network=host -v /Users:/Users -v /var/run/docker.sock:/var/run/docker.sock -v /tmp:/tmp zsxhku/hams_test:version1
```

## Linux server

```sh
docker run -it --network=host -v /home/hkucs/hams:/hams -v /var/run/docker.sock:/var/run/docker.sock -v /tmp:/tmp zsxhku/hams_test:version1
```

## Kill process by name

```sh
sudo kill -9 $(ps aux | grep 'dockerd' | awk '{print $2}')
```

docker ps -a | grep python | awk '{print $1}' | xargs --no-run-if-empty docker kill

## Run grpcclient 

```sh
docker run -it --network hams_network zsxhku/grpcclient --stock 10.0.0.3 22223
```

## Show grpcclient help

```sh
docker run -it --network hams_network zsxhku/grpcclient --help
```

## zsh close git status 

```sh
git config --add oh-my-zsh.hide-status 1
git config --add oh-my-zsh.hide-dirty 1
```

## start redis test container 

```sh
docker run --name redis-test -p 33333:33333 -d redis:alpine redis-server --port 33333
```

## redis cli

```sh
docker run -it --network host --rm redis redis-cli -p 33333 
docker run -it --network hams_network --rm redis redis-cli -p 33333 -h
```
## local test managment_grpc_server

```sh
cd debug
./src/grpcmanagement/management_grpc_server localhost 33333
./src/grpcmanagement/client
```

## Recovery test 

Get runtime dag

```sh
docker run -it --network hams_network --rm zsxhku/grpcclient --getruntimedag 172.18.0.3 55555 simpledag test 1 
```

Recover container 

```sh
docker run -it --network hams_network --rm zsxhku/grpcclient --recover 172.18.0.3 55555 simpledag-test 1
```
 



## GPU Docker 

Firstly install a Docker-ce (Communitiy Edition)

https://docs.docker.com/install/linux/docker-ce/ubuntu/#install-using-the-convenience-script

Then install nvidia docker runtime

https://github.com/NVIDIA/nvidia-docker

## Docker image build frontend/admindaemon

docker image build -t zsxhku/frontend -f ../dockerfiles/FrontendDockerFile .

docker image build -t zsxhku/admindocker -f ../dockerfiles/AdminDockerFile .




