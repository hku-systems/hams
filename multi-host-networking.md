# Mulitiple host networking with *Swarm*

### Basic requirements: 
 1. At least **2** hosts shoule be available;
 1. all the host are located in the same network, and each host can access the others by specifying their IP address;
 1. port 2377 of each host should be exposed; and,
 1. all the host support the docker service. 

## Step 1 Initialize a swarm cluster and construct an overlay network on one host

```sh
docker swarm leave -f 
docker swarm init --listen-addr 0.0.0.0:2377 --advertise-addr [[HOST_IP]]:2377
docker create -d overlay --attachable hams_network
```
One may check whether the above commands is conducted successfully by

```sh
docker swarm ca
docker network ls
```
Normally there should exist a bridge network named `docker_gwbridge`, an overlay network named `ingress` and an overlay network named `cluster_network`, all of which belong to the swarm scope. 

## Step 2 Add the other host(s) to the swarm cluster:
 

```sh
# On the host where the swarm cluster is initialized
docker swarm join-token manager
# Copy the command together with the token returned by the above command
```

```sh
# On the other host(s)
docker swarm join --token [[TOKEN]]
# Exactly the command and the token which are copied just now
```

One may confirm that the host is added to the swarm cluster and gains the access to the overlay network successfully by
```sh
docker network ls
```

## Step 3 Set up the IP and port info

On every host: 

```sh
cd hams-develop/hams_admin
```

**Modify the file named host_list. **

The first line contains the number of hosts in the network.
The rest are the IP and port of **all** the hosts, including the current working one, in form of: 

```
[[IP]]:[[PORT]]
```

By defauly, [[PORT]] is 2377 here. 

**Then, run the auto_set_ip.py. **

This will modified each file whose name starts with "cluster" so that when running the file, 
connections will be created between the existing hosts declared in the host_list. 

## Step 4 Deploy the applications

The following should be executed with all necessary docker images built and updated. 

On any host:

```sh
# For deploying:
python3 cluster_[[APP_NAME]].py
# For stopping: 
python3 cluster_stop_all.py
```

e.g.

```sh
# For deploying:
python3 cluster_simple_dag.py
# For stopping: 
python3 cluster_stop_all.py
```
