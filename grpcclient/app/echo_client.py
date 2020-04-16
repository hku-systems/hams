import model_pb2_grpc
import model_pb2
import proxy_pb2_grpc
import proxy_pb2
import prediction_pb2_grpc
import prediction_pb2
import sys
import grpc
from google.protobuf.timestamp_pb2 import Timestamp
from time import sleep



def main():
    ip = sys.argv[1]
    channel = grpc.insecure_channel('%s:%s'%(ip, 22223))
    stub = prediction_pb2_grpc.ProxyServerStub(channel)
    
    counter = 0


    while(1):
        timestamp = Timestamp()
        timestamp.GetCurrentTime()
        print("sending request to proxy on", ip)
        req_content = str(counter) 
        response = stub.downstream(prediction_pb2.request(input_ = model_pb2.input(inputType = 'string', inputStream = req_content),src_uri = "front-end", seq = counter, req_id = counter, timestamp = timestamp))
        print('Response\n{res}'.format(res=response.status))
        sleep(0.1)
        counter = counter + 1


        

if __name__ == '__main__':
    main()
