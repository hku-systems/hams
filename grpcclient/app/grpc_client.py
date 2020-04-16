
#!/usr/bin/env python
import argparse

import grpc
from google.protobuf.timestamp_pb2 import Timestamp

import management_pb2
import management_pb2_grpc
import model_pb2
import model_pb2_grpc
import prediction_pb2
import prediction_pb2_grpc
import proxy_pb2
import proxy_pb2_grpc

###############################################################################
##             Communicate with Management
###############################################################################



def AddModel(modelinfo, mg_ip, mg_port):

    # modelinfo = management_pb2.ModelInfo().ParseFromString(modelinfo)

    channel = grpc.insecure_channel('{mg_ip}:{mg_port}'.format(
        mg_ip = mg_ip,
        mg_port = mg_port
    ))
    stub = management_pb2_grpc.ManagementServerStub(channel)
    response = stub.AddModel(modelinfo)

    print('SetModel call OK with response{res}'.format(res=response.status))
 
def AddModelContainer(modelcontainerinfo, mg_ip, mg_port):


    # modelcontainerinfo = management_pb2.ModelContainerInfo().ParseFromString(modelcontainerinfo)

    channel = grpc.insecure_channel('{mg_ip}:{mg_port}'.format(
        mg_ip = mg_ip,
        mg_port = mg_port
    ))
    stub = management_pb2_grpc.ManagementServerStub(channel)
    response = stub.AddModel(modelcontainerinfo)

    print('SetModel call OK with response{res}'.format(res=response.status))
 
def AddProxyContainer(proxycontainerinfo, mg_ip, mg_port):


    # proxycontainerinfo = management_pb2.ProxyContainerInfo().ParseFromString(proxycontainerinfo)

    channel = grpc.insecure_channel('{mg_ip}:{mg_port}'.format(
        mg_ip = mg_ip,
        mg_port = mg_port
    ))
    stub = management_pb2_grpc.ManagementServerStub(channel)
    response = stub.AddModel(proxycontainerinfo)

    print('SetModel call OK with response{res}'.format(res=response.status))


def AddRuntimeDAG(runtimedaginfo, mg_ip, mg_port):

    channel = grpc.insecure_channel('{mg_ip}:{mg_port}'.format(
        mg_ip = mg_ip,
        mg_port = mg_port
    ))
    stub = management_pb2_grpc.ManagementServerStub(channel)
    response = stub.AddRuntimeDAG(runtimedaginfo)

    print('AddRuntimeDAG call OK with response{res}'.format(res=response.status))



###############################################################################
##             Communicate with Proxy 
###############################################################################

def setModel(proxy_ip, proxy_port, container_name, container_count, container_ip, container_port):
    channel_proxy = grpc.insecure_channel('{proxy_ip}:{proxy_port}'.format(
        proxy_ip=proxy_ip,
        proxy_port="22223"
    ))
    stub_proxy = prediction_pb2_grpc.ProxyServerStub(channel_proxy)
    response = stub_proxy.SetModel(prediction_pb2.modelinfo(
        modelName=container_name,
        modelId=int(container_count),
        modelPort=22222,
        modelIp=container_ip
    ))

    print('SetModel call OK with response{res}'.format(res=response.status))

def setProxy(container_ip, container_port, proxy_name, proxy_port):
    #tells the model container its proxy's info
    channel_container = grpc.insecure_channel('{container_ip}:{container_port}'.format(
        container_ip=container_ip,
        container_port="22222"
    ))
    stub_container = model_pb2_grpc.PredictServiceStub(
        channel_container)
    response = stub_container.SetProxy(model_pb2.proxyinfo(
        proxyName=proxy_name,
        proxyPort="22223"
    ))

    print('SetProxy call OK with response{res}'.format(res=response.status))


def setDAG(proxy_ip, proxy_port, expanded_dag):
    channel_proxy = grpc.insecure_channel('{proxy_ip}:{proxy_port}'.format(
        proxy_ip=proxy_ip,
        proxy_port="22223"
    ))
    stub_proxy = prediction_pb2_grpc.ProxyServerStub(channel_proxy)
    response = stub_proxy.SetDAG(prediction_pb2.dag(dag_=expanded_dag))

    print('SetDAG call OK with response{res}'.format(res=response.status))


###############################################################################
##             Communicate with Application
###############################################################################

def stockPredict(ip, port):

    channel = grpc.insecure_channel('%s:%s'%(ip, port))
    stub = prediction_pb2_grpc.ProxyServerStub(channel)
    stock_name = "AAPL"
    response = stub.downstream(prediction_pb2.request(input_ = model_pb2.input(inputType = 'string', inputStream = stock_name),src_uri = "localhost", seq = 1, req_id =1, timestamp = Timestamp().GetCurrentTime()))
    print('Response\n{res}'.format(res=response.status))



###############################################################################
##             Tests
###############################################################################


def recover(ip,port, appid, modelid, container_name):

    channel = grpc.insecure_channel('%s:%s'%(ip, port))
    stub = management_pb2_grpc.ManagementServerStub(channel)
    response = stub.ReportContainerFailure(management_pb2.FailureInfo(appid = appid, modelid=modelid, modelname=container_name))

    print('Response\n {res}'.format(res=response.newruntimedag))


def GetRuntimeDAG(ip,port, name, version, dagid):
    channel = grpc.insecure_channel('%s:%s'%(ip, port))
    stub = management_pb2_grpc.ManagementServerStub(channel)
    response = stub.GetRuntimeDAG(management_pb2.RuntimeDAGInfo(name=name, version=version, id=dagid))
    print('Response\n {res}'.format(res=response.file))

def puredag(address):
    channel = grpc.insecure_channel(address)
    stub = model_pb2_grpc.PredictServiceStub(channel)
    response = stub.Predict(model_pb2.input(inputStream="Hello", inputType="String"))



def main():

    parser = argparse.ArgumentParser(description='Grpc client')

    parser.add_argument('--setmodel', nargs=6, type=str)
    parser.add_argument('--setproxy', nargs=4, type=str)
    parser.add_argument('--setdag', nargs="+", type=str)
    parser.add_argument('--stock', nargs=2, type=str)

    parser.add_argument('--recover', nargs=5, type=str)
    
                       
    parser.add_argument('--addruntimedag', nargs="+", type=str)

    parser.add_argument('--getruntimedag', nargs=5, type=str)

    parser.add_argument('--purepredict', nargs=1, type=str)

                       
    args = parser.parse_args()


    if args.purepredict is not None:
        puredag(args.purepredict[0])

    if args.addruntimedag is not None:
        #print("=============received arguments============")
        #print(args.addruntimedag)
        #dic = eval(args.addruntimedag[0])
        expanded_dag = ""
        for line in args.addruntimedag[6:]:
            expanded_dag = expanded_dag + line + "\n"
        proto = management_pb2.RuntimeDAGInfo(file=expanded_dag, id=args.addruntimedag[0], name=args.addruntimedag[1], version=args.addruntimedag[2], format=args.addruntimedag[3])
        AddRuntimeDAG(proto, args.addruntimedag[4], args.addruntimedag[5])

    if args.recover is not None:
        recover(args.recover[0], args.recover[1], args.recover[2], args.recover[3],args.recover[4])

    if args.getruntimedag is not None:
        GetRuntimeDAG(args.getruntimedag[0], args.getruntimedag[1], args.getruntimedag[2],args.getruntimedag[3],args.getruntimedag[4])

    if args.stock is not None:
        #print(args.stock)
        stockPredict(args.stock[0], args.stock[1])

    if args.setmodel is not None:
        #print(args.setmodel)
        setModel(args.setmodel[0],args.setmodel[1],args.setmodel[2],args.setmodel[3], args.setmodel[4], args.setmodel[5])


    if args.setproxy is not None:
        #print(args.setproxy)
        setProxy(args.setproxy[0],args.setproxy[1],args.setproxy[2],args.setproxy[3])

    if args.setdag is not None:
        #print(args.setdag) 
        expanded_dag = ""
        for line in args.setdag[2:]:
            expanded_dag = expanded_dag + line + "\n"

        setDAG(args.setdag[0],args.setdag[1],expanded_dag)

        

if __name__ == '__main__':
    main()
