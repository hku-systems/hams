FROM ubuntu:bionic
RUN apt-get update
RUN apt-get install python3 -y
RUN apt-get install python3-pip -y
RUN pip3 install grpcio
RUN pip3 install protobuf


#ADD app /container/
COPY simpledag/container1/app /container/
COPY grpc/app/ /container/

#COPY app /container/
CMD ["/container/container_entry.sh", "tensorflow-container", "/container/server.py"]

EXPOSE 22222