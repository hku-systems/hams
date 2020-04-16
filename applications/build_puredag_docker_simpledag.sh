#/bin/bash

docker build -f ./simpledag/simpledagPureDagDockerfile --build-arg APPNAME="simpledag" --build-arg CONTAINER=1 -t pure-simpledag:container1 .
docker build -f ./simpledag/simpledagPureDagDockerfile --build-arg APPNAME="simpledag" --build-arg CONTAINER=2 -t pure-simpledag:container2 .
docker build -f ./simpledag/simpledagPureDagDockerfile --build-arg APPNAME="simpledag" --build-arg CONTAINER=3 -t pure-simpledag:container3 .
docker build -f ./simpledag/simpledagPureDagDockerfile --build-arg APPNAME="simpledag" --build-arg CONTAINER=4 -t pure-simpledag:container4 .
docker build -f ./simpledag/simpledagPureDagDockerfile --build-arg APPNAME="simpledag" --build-arg CONTAINER=5 -t pure-simpledag:container5 .

