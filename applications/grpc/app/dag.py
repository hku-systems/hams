from concurrent import futures
import base64
import time 
import os

import model_pb2
import model_pb2_grpc


import predict as predict_fn

import grpc

import argparse

class PredictService(model_pb2_grpc.PredictServiceServicer):
    
    # def GetEncode(self, request, context):
    #     return test_pb2.encodetext(enctransactionID = encoding(request.pttransactionID),
    #                                         encproperties = encoding(request.ptproperties),
    #                                         encsenderID = request.ptsenderID)

    def __init__(self, forward_list, reduce):
        self.forward_list=forward_list
        self.reduce=reduce
        self.reduce_count = 0
        self.reduce_list = ["","","","","","","",""]
        self.reduce_list_backup = ["","","","","","","",""]
    
    def call(self, address, input_type, output):

        print('[Call]Set up channel to %s'%(address))

        channel = grpc.insecure_channel("%s:22222"%(address))

        stub = model_pb2_grpc.PredictServiceStub(channel)

        
        if (len(self.forward_list) == 1):
            print('[Call] self.forward_list == 1, keep the reduce index')
            t = input_type ## IF only one forward, keep the reduce index - which is input_type
        else:
            print('[Call] generate reduce index')
            t = str(self.forward_list.index(address)) ## [TODO] If seperate forward, generate reduce index

        
        print('[Call]%s Forward Index'%(t))

        response = stub.Predict(model_pb2.input(
            inputType=t,
            inputStream=output
        ))
        print('[Call]Sent to {address}'.format(
            address=address
        ))

        return response.outputStream

    def Predict(self, request, context):
        print("[Predict]Input:{request}\n".format(request=request))
        input_type = request.inputType
        input_stream = request.inputStream
        
        if(self.reduce > 0):
            if self.reduce_count < self.reduce-1:

                self.reduce_list[int(input_type)] = input_stream
                self.reduce_count = self.reduce_count+1
                print("[Predict] Wait for Reduce (current %d:total %d)"%(self.reduce_count, self.reduce))
                return model_pb2.output(outputType = "string", outputStream = "Reduce")

            else: 

                self.reduce_list[int(input_type)] = input_stream
                print("[DEBUG] input_stream: ", input_stream)
                print("[DEBUG] reduce_count: ", self.reduce_count)
                print("[DEBUG] reduce_list: ", self.reduce_list)
                input_stream = '|'.join(self.reduce_list[0:self.reduce_count+1])
                print("[Predict] Received all Reduce (current %d:total %d)"%(self.reduce_count+1, self.reduce))
                self.reduce_list = self.reduce_list_backup.copy()
                self.reduce_count = 0


        output = predict_fn.predict(input_stream)

#        print("goes here")

        print("[Predict]Output:" + output)


        data_list=list()


        if self.forward_list is not None:
            with futures.ThreadPoolExecutor(max_workers=16) as executor:
                future_to_excute = {executor.submit(
                    self.call, address, input_type, output): address for address in self.forward_list}
                for future in futures.as_completed(future_to_excute):
                    address = future_to_excute[future]
                    try:
                        data = future.result()
                        data_list.append(data)
                    except Exception as exc:
                        print('%s generated an exception: %s' % (address, exc))
                    else:
                        print('[Predict]Received Output:\n%s' % (data))

        ret = output

        print('[Predict]data_list size%d'%(len(data_list)))

        if len(data_list) > 0:
            if len(data_list) > 1:
                for d in data_list:
                    if d is not "Reduce":
                        ret = d
            else:
                ret = data_list[0]

        print('[Predict]Finally return:'+ret)

        return model_pb2.output(outputType = input_type, outputStream = ret)

    def Ping(self, request, context):

        print("received request:{request}\n".format(request=request))
        hi_msg = request.msg


        if (self.proxy_name == None or self.proxy_port == None):
            return model_pb2.response(status = "ProxyNotSet")

        r = "This is %s \n"%(self.model_name)


        return model_pb2.response(status = r)
   

def serve():


    
    parser = argparse.ArgumentParser(description='DAG Server: python dag.py --forward ip:port')

    parser.add_argument('--forward', nargs='*', type=str)

    parser.add_argument('--reduce', nargs='?', type=int, default=0)

    args = parser.parse_args()


    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    service = PredictService(args.forward, args.reduce)
    model_pb2_grpc.add_PredictServiceServicer_to_server(service,server)
#    server.add_insecure_port('[::]:22222')

    server.add_insecure_port('[::]:22222')
    server.start()
    print("[Serve]Model Server Started")

    try:
        while True:
            time.sleep(60*60*24)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == '__main__':
    serve()
