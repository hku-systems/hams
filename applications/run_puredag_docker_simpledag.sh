#!/usr/bin/env bash
#/bin/sh

docker run -d --network=hams_network -it --name test1 pure-simpledag:container1 python3 /container/dag.py --forward test2
docker run -d --network=hams_network -it --name test2 pure-simpledag:container2 python3 /container/dag.py --forward test3 test4 
docker run -d --network=hams_network -it --name test3 pure-simpledag:container3 python3 /container/dag.py --forward test5
docker run -d --network=hams_network -it --name test4 pure-simpledag:container4 python3 /container/dag.py --forward test5
docker run -d --network=hams_network -it --name test5 pure-simpledag:container5 python3 /container/dag.py --reduce 2



