import base64
import os
import time
from concurrent import futures
import threading

import argparse
import sys
import datetime

import requests, json, numpy as np


from multiprocessing import Process, Queue, Lock

from google.protobuf.timestamp_pb2 import Timestamp

import grpc

#from hams_admin.grpcclient import grpc_client
from hams_admin.rpc import (management_pb2, management_pb2_grpc, model_pb2,
                               model_pb2_grpc, prediction_pb2,
                               prediction_pb2_grpc)

import logging

logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-9s) %(message)s',)




def oursystem(ip, port, inputt):


    channel = grpc.insecure_channel('%s:%s'%(ip, port))
    stub = prediction_pb2_grpc.ProxyServerStub(channel)
    response = stub.downstream(prediction_pb2.request(input_ = model_pb2.input(inputType = 'string', inputStream = inputt)))
    return response.status


def withoutproxy(ip, port, inputt):

    channel = grpc.insecure_channel('%s:%s'%(ip, port))
    stub = model_pb2_grpc.PredictServiceStub(channel)
    response = stub.Predict(model_pb2.input(inputStream=inputt, inputType="String"))

    return response.outputStream

def hams(ip, port, inputt):


    headers = {"Content-type": "application/json"}
    requests.post("http://{}:{}/hello-world/predict".format(ip,port), headers=headers, data=json.dumps({"input": [inputt]})).json()

    return "Raft OK"

def bigball(ip, port, inputt):

    # Better to use hams to process bigball container.
    return "bigball"
 
 
# Producer function that places data on the Queue
def producer(queue, lock, ip, port, inputt_list, func):
    # Synchronize access to the console
    with lock:
        print('Starting worker => {}'.format(os.getpid()))
         
    # Query and return output on the Queue
    for inputt in inputt_list:
        #time.sleep(random.randint(0, 10))
        output = func(ip, port, inputt)
        #queue.put(output)

        with lock:
            print("Input {} returns Output: {}".format(inputt, output))
 
    # Synchronize access to the console
    with lock:
        print('Worker {} exiting...'.format(os.getpid()))
 
# Currently no need
# The consumer function takes data off of the Queue
def consumer(queue, lock):
    # Synchronize access to the console
    with lock:
        print('Starting consumer => {}'.format(os.getpid()))
     
    # Run indefinitely
    while True:
        time.sleep(random.randint(0, 2))
         
        # If the queue is empty, queue.get() will block until the queue has data
        output = queue.get()
 
        # Synchronize access to the console
        with lock:
            print('{} got {}'.format(os.getpid(), output))
 
 
def main():


    parser = argparse.ArgumentParser(description='concurrent client')

    parser.add_argument('--worker', nargs=1, type=int, help="Worker num")
    parser.add_argument('--ip', nargs=1, type=str, help="Ip address of your query frontend")
    parser.add_argument('--port', nargs=1, type=str, help="Port of your query frontend, for Hams, put an arbitrary INT")
    parser.add_argument('--system', nargs=1, type=str, help="System name: oursystem/withoutproxy/hams")
                       
    args = parser.parse_args()
     
    # Generate your inputt list here

    l1 = ["AAPL"]

    l10 = ['MET', 'FAST', 'MET', 'M', 'EBAY', 'LH', 'GS', 'IBM', 'FL', 'MMC']

    l50 = ['SEE', 'M', 'COO', 'CVS', 'MMC', 'GE', 'MAC', 'GS', 'WAT', 'CMG', 'TSS', 'TRIP', 'ANTM', 'PNC', 'TDG', 'COG', 'HES', 'OKE', 'BAC', 'CNP', 'WAT', 'MU', 'KIM', 'RF', 'NEM', 'KR', 'MA', 'COST', 'CI', 'PH', 'CI', 'IT', 'CAT', 'MAR', 'DXC', 'ADP', 'CMG', 'HES', 'UA', 'PNC', 'KIM', 'IT', 'VZ', 'HPE', 'IT', 'LEG', 'MA', 'CF', 'COST', 'ACN']

    l98 = ['HAS', 'IRM', 'TEL', 'EL', 'ESS', 'COP', 'KEY', 'FE', 'CBS', 'IFF', 'NOV', 'IRM', 'FL', 'BBY', 'MS', 'FAST', 'CRM', 'NUE', 'MSCI', 'MMC', 'AIG', 'WELL', 'STT', 'CMA', 'RMD', 'FB', 'FB', 'IFF', 'WU', 'USB', 'NI', 'EA', 'TRIP', 'EBAY', 'AON', 'MS', 'TXN', 'USB', 'IRM', 'CE', 'BK', 'ROL', 'ANTM', 'NVDA', 'SEE', 'CNC', 'DXC', 'APA', 'APA', 'UPS', 'DOW', 'CAT', 'MET', 'HIG', 'LOW', 'CAT', 'VZ', 'MSCI', 'MA', 'BEN', 'RMD', 'BEN', 'HPE', 'PGR', 'CNC', 'PH', 'PGR', 'MAC', 'NOV', 'BEN', 'ICE', 'TAP', 'ABC', 'MMC', 'ESS', 'COST', 'HD', 'CVS', 'KIM', 'CAG', 'CNC', 'UPS', 'MO', 'BEN', 'FL', 'GS', 'EL', 'CMA', 'FE', 'IP', 'KIM', 'LOW', 'CF', 'NUE', 'FL', 'USB', 'CBS', 'CMA']

    l100 = ['HAS', 'IRM', 'TEL', 'EL', 'ESS', 'COP', 'KEY', 'FE', 'CBS', 'IFF', 'NOV', 'IRM', 'FL', 'BBY', 'MS', 'FAST', 'CRM', 'NUE', 'MSCI', 'MMC', 'AIG', 'WELL', 'STT', 'CMA', 'RMD', 'FB', 'FB', 'IFF', 'WU', 'USB', 'NI', 'EA', 'TRIP', 'HAL', 'EBAY', 'AON', 'MS', 'TXN', 'USB', 'IRM', 'CE', 'BK', 'ROL', 'ANTM', 'NVDA', 'SEE', 'CNC', 'DXC', 'APA', 'APA', 'UPS', 'DOW', 'CAT', 'MET', 'HIG', 'LOW', 'CAT', 'VZ', 'MSCI', 'MA', 'BEN', 'RMD', 'BEN', 'HPE', 'PGR', 'CNC', 'PH', 'PGR', 'MAC', 'NOV', 'BEN', 'ICE', 'TAP', 'ABC', 'MMC', 'ESS', 'COST', 'HD', 'CVS', 'KIM', 'CAG', 'CNC', 'UPS', 'MO', 'BEN', 'FL', 'GS', 'EL', 'CMA', 'FE', 'IP', 'KIM', 'LOW', 'CF', 'NUE', 'FL', 'USB', 'CBS', 'RF', 'CMA']

    l500 = ['FE', 'EL', 'CI', 'ED', 'CF', 'FL', 'HD', 'ARE', 'CAT', 'ABC', 'HPE', 'VAR', 'MSCI', 'WELL', 'CMG', 'CPB', 'PG', 'SRE', 'MAR', 'NSC', 'MMC', 'KO', 'FB', 'ALL', 'IRM', 'M', 'TXT', 'AMAT', 'NI', 'PEP', 'MO', 'FL', 'DIS', 'ARE', 'INFO', 'JPM', 'BK', 'FB', 'ED', 'COG', 'NFLX', 'EBAY', 'COG', 'EA', 'ARE', 'BLK', 'ED', 'NEM', 'HAL', 'HP', 'MRO', 'CMS', 'ALL', 'DRE', 'ICE', 'COST', 'KO', 'WU', 'IT', 'USB', 'SEE', 'SEE', 'EBAY', 'DRE', 'PFE', 'ABC', 'CI', 'FE', 'ADP', 'FE', 'TXT', 'M', 'ED', 'HAL', 'PG', 'JPM', 'IT', 'AMD', 'MAS', 'NOV', 'KHC', 'CL', 'FL', 'CMA', 'PRU', 'KHC', 'MET', 'AMAT', 'PEG', 'SEE', 'APA', 'ICE', 'MO', 'FB', 'CMA', 'HAL', 'PNC', 'NI', 'CF', 'MO', 'HII', 'LEN', 'PG', 'FL', 'BAC', 'FOX', 'IBM', 'TRIP', 'FAST', 'NI', 'NI', 'CPB', 'MA', 'TRIP', 'COST', 'FL', 'UPS', 'EW', 'FB', 'HAL', 'ICE', 'PEG', 'CRM', 'ICE', 'SO', 'BK', 'ESS', 'BA', 'KEYS', 'EW', 'HUM', 'BLK', 'MU', 'TRIP', 'WAT', 'ANTM', 'EL', 'COST', 'HD', 'COO', 'DRE', 'KEY', 'FAST', 'MRO', 'MO', 'CE', 'SEE', 'PEP', 'MAS', 'ABT', 'FE', 'IP', 'HD', 'KEY', 'ED', 'HUM', 'KR', 'GPS', 'COO', 'IT', 'MAS', 'FAST', 'MSCI', 'WELL', 'IR', 'TSS', 'NEM', 'RMD', 'JPM', 'AIG', 'M', 'WELL', 'MAT', 'HD', 'IR', 'EMR', 'CMA', 'LEG', 'IR', 'ATO', 'CAT', 'HAS', 'CB', 'VAR', 'AIG', 'RMD', 'WYNN', 'CPB', 'MET', 'MO', 'VAR', 'EBAY', 'KEY', 'FB', 'AIG', 'TRIP', 'SEE', 'BR', 'ARE', 'TMO', 'EA', 'CME', 'BA', 'IFF', 'MO', 'PNC', 'IRM', 'CB', 'ALL', 'NVDA', 'CI', 'KHC', 'HAS', 'HAL', 'BA', 'HAS', 'TRIP', 'CI', 'TJX', 'MAS', 'MO', 'MSI', 'CRM', 'M', 'DISH', 'MAT', 'XOM', 'CRM', 'CVS', 'USB', 'KO', 'EBAY', 'USB', 'MA', 'UPS', 'CE', 'COO', 'TXT', 'HUM', 'WELL', 'MAR', 'MSCI', 'CTL', 'TAP', 'FE', 'ACN', 'MA', 'COF', 'APA', 'DRE', 'ADM', 'ANTM', 'HFC', 'GE', 'IBM', 'PH', 'WU', 'PM', 'EBAY', 'HAS', 'EBAY', 'WAT', 'ES', 'ANTM', 'DISH', 'VAR', 'HD', 'ABC', 'KEY', 'EBAY', 'TAP', 'FE', 'ADP', 'CRM', 'KEY', 'JPM', 'EA', 'STI', 'VAR', 'LIN', 'KEY', 'MA', 'FE', 'CAT', 'EBAY', 'HIG', 'TSS', 'COO', 'NI', 'LOW', 'CI', 'MU', 'FOX', 'USB', 'DRI', 'ARE', 'CF', 'NOV', 'MSCI', 'MS', 'LH', 'VZ', 'COO', 'VAR', 'COST', 'BK', 'HAS', 'KIM', 'KHC', 'MO', 'TSS', 'EW', 'VZ', 'MAR', 'HPE', 'BAC', 'PKI', 'NI', 'CL', 'DIS', 'ADM', 'AIG', 'CAT', 'STT', 'CI', 'KEY', 'CL', 'PEP', 'IT', 'TRIP', 'GE', 'CAT', 'VZ', 'NI', 'TDG', 'MSCI', 'GE', 'WYNN', 'ARE', 'MAS', 'TMK', 'IT', 'CAT', 'CMS', 'AMP', 'COP', 'CF', 'ED', 'ESS', 'CL', 'HD', 'FB', 'CAT', 'ESS', 'HPE', 'KEY', 'ACN', 'HD', 'AMAT', 'IRM', 'ANTM', 'JPM', 'CVS', 'ATO', 'DFS', 'TRIP', 'USB', 'CRM', 'ALL', 'MSI', 'IFF', 'SO', 'KEY', 'FB', 'COO', 'FB', 'FB', 'TMK', 'CME', 'WELL', 'IR', 'SRE', 'ROL', 'CBRE', 'APA', 'PM', 'DISH', 'GS', 'LOW', 'AMAT', 'TRIP', 'FIS', 'TMK', 'PH', 'IR', 'PNC', 'PH', 'CMG', 'CME', 'USB', 'M', 'EBAY', 'MS', 'CRM', 'KEY', 'CPB', 'PNR', 'LH', 'IT', 'BR', 'TEL', 'CMA', 'M', 'DOW', 'CBRE', 'COST', 'IT', 'PEG', 'BEN', 'PG', 'CVS', 'KHC', 'M', 'ALL', 'IBM', 'EL', 'ALL', 'ANTM', 'ADI', 'ARE', 'LEG', 'HAS', 'ADI', 'SRE', 'LOW', 'USB', 'ED', 'EBAY', 'ATO', 'EBAY', 'COST', 'TAP', 'BA', 'ALL', 'CVS', 'DFS', 'MMC', 'ED', 'HPQ', 'MAS', 'SPG', 'NOC', 'PSA', 'BK', 'M', 'TAP', 'CAG', 'CF', 'HAS', 'CMS', 'FB', 'ALL', 'KIM', 'GS', 'USB', 'FAST', 'HPQ', 'FAST', 'USB', 'FOX', 'HES', 'NI', 'TEL', 'FB', 'KR', 'HAS', 'WAT', 'IRM', 'EBAY', 'IRM', 'M', 'COP', 'M', 'PRU', 'PKI', 'MAS', 'M', 'CF', 'XOM', 'SO', 'MET', 'MO', 'BR', 'CAT', 'ESS', 'AMP', 'MS', 'HAL', 'LB', 'CMA']

    l1000 = ['TAP', 'COST', 'UPS', 'CMA', 'DISH', 'MOS', 'FIS', 'ABC', 'CE', 'DRE', 'SO', 'CNC', 'CAT', 'COP', 'NI', 'BAC', 'PH', 'VAR', 'ADM', 'MET', 'MO', 'MO', 'MET', 'EBAY', 'AMD', 'MAR', 'DISH', 'FB', 'HP', 'MA', 'GPS', 'MAR', 'AMAT', 'MMC', 'COST', 'FE', 'WELL', 'FB', 'LH', 'MU', 'CE', 'HD', 'CHD', 'AMAT', 'FOX', 'GS', 'TRIP', 'IT', 'MMC', 'MO', 'CVS', 'FE', 'SEE', 'ED', 'SO', 'VZ', 'EBAY', 'CB', 'SO', 'UPS', 'PFE', 'MU', 'HON', 'LEN', 'ESS', 'RL', 'MSCI', 'PH', 'APA', 'EA', 'INFO', 'PNR', 'ABC', 'WAT', 'STT', 'BK', 'APA', 'MET', 'IBM', 'HAS', 'MSCI', 'BEN', 'ALL', 'ALL', 'HAL', 'MU', 'DVA', 'CNC', 'ARE', 'TJX', 'FL', 'HP', 'AES', 'IP', 'CRM', 'BBY', 'AMD', 'AMAT', 'ADM', 'ROL', 'MS', 'SO', 'KR', 'KIM', 'SRE', 'AIG', 'MA', 'USB', 'COP', 'MSCI', 'MSCI', 'APA', 'MU', 'KO', 'COST', 'EL', 'BAC', 'CAG', 'CL', 'IP', 'DISH', 'NFLX', 'KIM', 'NEM', 'NOV', 'SEE', 'FB', 'M', 'FE', 'CE', 'HUM', 'IT', 'EW', 'DFS', 'SO', 'UPS', 'CI', 'PKI', 'IRM', 'ES', 'WAT', 'CNP', 'TWTR', 'NSC', 'USB', 'MAS', 'ABT', 'HUM', 'PH', 'RMD', 'KIM', 'SEE', 'CMA', 'CF', 'LH', 'HP', 'KO', 'APA', 'HAS', 'NFLX', 'DRE', 'KEY', 'ADI', 'HUM', 'WAT', 'BA', 'KEY', 'KIM', 'MA', 'CI', 'IR', 'FE', 'SO', 'ALL', 'MO', 'KEY', 'AMP', 'M', 'PNR', 'VAR', 'BA', 'FE', 'COO', 'NVDA', 'NVDA', 'ANTM', 'LB', 'CHD', 'BR', 'CBRE', 'MS', 'DXC', 'ROL', 'TIF', 'IT', 'WU', 'RMD', 'RMD', 'PG', 'IRM', 'LEN', 'ARE', 'MO', 'ALL', 'IR', 'FL', 'TSS', 'SRE', 'EW', 'LOW', 'EA', 'VAR', 'FAST', 'FE', 'IR', 'AES', 'FOX', 'DRE', 'HAS', 'KO', 'AMD', 'M', 'MAC', 'MAT', 'VZ', 'VAR', 'IR', 'ANTM', 'KEYS', 'JPM', 'JPM', 'EL', 'CF', 'TRIP', 'ANTM', 'MA', 'IRM', 'IT', 'HP', 'FOX', 'EBAY', 'SEE', 'PNR', 'HAS', 'HAL', 'COST', 'IT', 'TMK', 'PG', 'FB', 'BBT', 'EBAY', 'MS', 'CRM', 'KEY', 'HAS', 'COST', 'ANTM', 'GS', 'SO', 'KEY', 'KEY', 'CMA', 'TMO', 'MAS', 'PNR', 'CAG', 'ANTM', 'MET', 'CMA', 'NI', 'FE', 'MU', 'PNC', 'CL', 'HAS', 'MSCI', 'EL', 'TRIP', 'EBAY', 'CNC', 'CI', 'AMT', 'ABC', 'CAT', 'GPS', 'COP', 'MAT', 'MAT', 'WM', 'EMR', 'COG', 'HAS', 'ES', 'FL', 'TMO', 'MAS', 'ROL', 'LB', 'IFF', 'IT', 'CVS', 'IRM', 'IT', 'CI', 'CAT', 'HPE', 'BA', 'MAR', 'MET', 'ESS', 'MO', 'HPE', 'M', 'ADP', 'TRIP', 'CAT', 'CF', 'USB', 'PRU', 'CME', 'APC', 'ARE', 'HAS', 'STT', 'KR', 'CVS', 'WELL', 'TXT', 'IR', 'PH', 'ABT', 'EL', 'DVA', 'MMC', 'LH', 'QCOM', 'MET', 'APA', 'APA', 'UA', 'COO', 'MAT', 'MS', 'MMM', 'IT', 'TXT', 'BEN', 'MAC', 'PH', 'ED', 'SO', 'HAS', 'CMG', 'PSA', 'RMD', 'M', 'BA', 'ED', 'NSC', 'EA', 'TRIP', 'CNP', 'NVDA', 'IT', 'FL', 'MO', 'KEYS', 'NVDA', 'CAT', 'DRE', 'CME', 'KR', 'MU', 'ED', 'NSC', 'DFS', 'USB', 'HES', 'AIG', 'APA', 'HES', 'COST', 'ANTM', 'PRU', 'COP', 'EBAY', 'COST', 'AON', 'PEG', 'CNC', 'EL', 'HAS', 'PEG', 'CI', 'WELL', 'CF', 'SPG', 'CPB', 'PKG', 'KEY', 'BEN', 'KR', 'LB', 'EBAY', 'PKI', 'TMK', 'FOX', 'JPM', 'PSA', 'ALL', 'MU', 'MS', 'TRIP', 'USB', 'PFE', 'PM', 'IR', 'HD', 'COP', 'COST', 'UPS', 'MA', 'PEP', 'PM', 'ANTM', 'BR', 'EBAY', 'ESS', 'CBS', 'TAP', 'KR', 'TRIP', 'MOS', 'NOC', 'CMG', 'KEY', 'DISH', 'IRM', 'COST', 'TRIP', 'TRIP', 'IT', 'WM', 'FL', 'WAT', 'STI', 'AMD', 'CL', 'NI', 'STT', 'EBAY', 'USB', 'PEG', 'KO', 'TXN', 'CNC', 'AMP', 'KO', 'SO', 'ROK', 'HAS', 'HPE', 'PSA', 'ARE', 'MMM', 'CB', 'RF', 'JPM', 'DRE', 'RMD', 'EW', 'BEN', 'SO', 'TAP', 'LB', 'CAT', 'IT', 'CMS', 'KR', 'ATO', 'MAS', 'DFS', 'ED', 'TRIP', 'INFO', 'ALL', 'FB', 'SO', 'UA', 'HD', 'HAS', 'VZ', 'CAG', 'FIS', 'MSCI', 'ARE', 'FE', 'KEY', 'M', 'BAC', 'APC', 'TWTR', 'RMD', 'CF', 'IFF', 'SEE', 'WELL', 'AES', 'DXC', 'EL', 'BA', 'WAT', 'KIM', 'HD', 'FOX', 'FE', 'M', 'AON', 'IRM', 'BBT', 'CB', 'MAR', 'MPC', 'RMD', 'FIS', 'HAL', 'FOX', 'BR', 'MRO', 'XOM', 'CE', 'BLK', 'TXT', 'MMM', 'ARE', 'BEN', 'VAR', 'LOW', 'HPQ', 'PSA', 'USB', 'CI', 'CI', 'DIS', 'URI', 'GS', 'ROK', 'TRIP', 'MA', 'NWS', 'BLK', 'RMD', 'MA', 'WU', 'KIM', 'HD', 'JPM', 'IR', 'SO', 'CPB', 'DRE', 'BR', 'KR', 'CNP', 'STI', 'USB', 'TXT', 'PM', 'LOW', 'HFC', 'ANTM', 'KO', 'BR', 'ADP', 'CRM', 'LOW', 'CRM', 'HD', 'KHC', 'CME', 'USB', 'KIM', 'EA', 'IP', 'CI', 'NI', 'CI', 'ED', 'KEY', 'DISH', 'MSCI', 'ALL', 'TXT', 'KHC', 'EL', 'CL', 'KHC', 'FE', 'MGM', 'EW', 'ED', 'BA', 'COST', 'MAS', 'TEL', 'OMC', 'COP', 'MO', 'IBM', 'DVA', 'ANTM', 'STT', 'IRM', 'HAS', 'ABC', 'RL', 'CVS', 'TWTR', 'ABT', 'XOM', 'TEL', 'KR', 'IT', 'CME', 'CMG', 'MO', 'IBM', 'BAC', 'USB', 'EA', 'IRM', 'HAL', 'FB', 'GPS', 'MAS', 'COST', 'NOV', 'CRM', 'JPM', 'COP', 'WAT', 'DIS', 'FOX', 'USB', 'MAT', 'DRI', 'ANTM', 'UPS', 'PM', 'CPB', 'MMC', 'ED', 'MSFT', 'AMD', 'CME', 'NOC', 'ALL', 'QCOM', 'KIM', 'MA', 'CAT', 'SEE', 'COP', 'SEE', 'PEP', 'BA', 'GS', 'BAC', 'FB', 'ADI', 'HPE', 'ANTM', 'ABC', 'CAT', 'AIG', 'MU', 'MA', 'M', 'IT', 'MET', 'BA', 'TAP', 'LEN', 'ES', 'APA', 'NEM', 'VAR', 'MSCI', 'NOC', 'BA', 'IP', 'CMA', 'MS', 'WAT', 'CI', 'TMO', 'CME', 'UPS', 'COP', 'BK', 'CMA', 'PNC', 'CNC', 'MA', 'OXY', 'EBAY', 'GS', 'MSCI', 'DRE', 'WELL', 'KIM', 'HAL', 'NOV', 'USB', 'LOW', 'CBRE', 'WAT', 'HAS', 'UA', 'FAST', 'ED', 'PRU', 'BEN', 'TXN', 'MTD', 'EA', 'MA', 'M', 'ED', 'ED', 'TMK', 'TRIP', 'SRE', 'FB', 'COST', 'MS', 'RMD', 'ALL', 'WELL', 'DRE', 'MSCI', 'EMR', 'EMR', 'MMC', 'CMA', 'CF', 'ARE', 'MAR', 'IT', 'PFE', 'PNR', 'CB', 'AIG', 'AMD', 'ARE', 'DFS', 'CMA', 'AIG', 'HES', 'AMD', 'HRS', 'IT', 'COP', 'UPS', 'DRE', 'IT', 'IT', 'PM', 'NEM', 'EBAY', 'MET', 'IBM', 'HAS', 'ROK', 'IBM', 'CVS', 'WELL', 'CF', 'MET', 'CMS', 'M', 'PNC', 'STT', 'COP', 'NFLX', 'M', 'NEM', 'AIG', 'BEN', 'COF', 'PKI', 'CAT', 'PM', 'SO', 'FL', 'HAL', 'MMC', 'FB', 'COF', 'ANTM', 'RF', 'HAS', 'MA', 'USB', 'BAC', 'BA', 'USB', 'PNC', 'HPE', 'CF', 'RMD', 'COF', 'ACN', 'TAP', 'EA', 'APC', 'MET', 'FE', 'FAST', 'AIG', 'MET', 'HFC', 'IBM', 'MAT', 'WELL', 'NVDA', 'LEN', 'IT', 'IR', 'IT', 'ALL', 'CBRE', 'MRO', 'MET', 'USB', 'TRIP', 'NFLX', 'USB', 'WELL', 'DFS', 'FE', 'IFF', 'IT', 'MET', 'SEE', 'CMA', 'IR', 'RL', 'BR', 'TWTR', 'AMP', 'DRE', 'TAP', 'UPS', 'BA', 'MAR', 'FB', 'HAL', 'FAST', 'CMA', 'KEY', 'IR', 'TEL', 'FB', 'HAS', 'CF', 'WAT', 'MGM', 'ALL', 'TAP', 'BA', 'CVS', 'ADP', 'ED', 'STT', 'FL', 'MAC', 'CBS', 'ABC', 'ARE', 'MA', 'PNC', 'ABC', 'CB', 'CF', 'MAT', 'FOX', 'PFE', 'ROP', 'VAR', 'APA', 'ABC', 'MAT', 'KO', 'FB', 'SEE', 'AIG', 'JPM', 'GS', 'LOW', 'CMA', 'RMD', 'HD', 'TXN', 'COP', 'APA', 'IRM', 'USB', 'JPM', 'UPS', 'NEM', 'TSS', 'SRE', 'ARE', 'MAR', 'VZ', 'IBM', 'CF', 'IT', 'KO', 'BR', 'WAT', 'MPC', 'BAC', 'HAL', 'LH', 'KHC', 'APA', 'IRM', 'NOV', 'UPS', 'MS', 'ICE', 'COST', 'AON', 'MAR', 'COP', 'ED', 'MAT', 'IT', 'M', 'PEP', 'MO', 'BEN', 'NSC', 'CB', 'FAST', 'PNC', 'TEL', 'IBM', 'M', 'HES', 'TSS', 'IP', 'COO', 'BEN', 'NOC', 'STT', 'SO', 'CRM', 'RMD', 'GE', 'HD', 'DXC', 'STT', 'CTL', 'NVDA', 'BK', 'HAL', 'CME', 'NOC', 'FB', 'LOW', 'MO', 'FOX', 'EBAY', 'SO', 'NVDA', 'RMD', 'WU', 'PH', 'HIG', 'USB', 'STT', 'GE', 'ACN', 'SEE', 'FB', 'AIG', 'MAT', 'CE', 'IRM', 'TAP', 'ABC', 'PNC', 'MO', 'HAL', 'TEL', 'ROK']

    inputt_total = [x + ":2018:1:1" for x in l98]

    # Get configuration
    work_num = args.worker[0]
    ip = args.ip[0]
    port = args.port[0]
    system = args.system[0]
 
    # Create the Queue object
    queue = Queue()
     
    # Create a lock object to synchronize resource access
    lock = Lock()
 
    producers = []
    consumers = []
 
    thismodule = sys.modules[__name__]

    for i in range(work_num):

        # Slice the input_total to $work_num lists
        inputt_list = inputt_total[i::work_num]
        # Create our producer processes by passing the producer function and it's arguments
        producers.append(Process(target=producer, args=(queue, lock, ip, port, inputt_list, getattr(thismodule, system))))
 
    # Create consumer processes
    #for i in range(work_num):
    #    p = Process(target=consumer, args=(queue, lock))
         
        # This is critical! The consumer function has an infinite loop
        # Which means it will never exit unless we set daemon to true
    #    p.daemon = True
    #    consumers.append(p)
 
    # Start the producers and consumer
    # The Python VM will launch new independent processes for each Process object

    start = time.time()

    for p in producers:
        p.start()
 
    #for c in consumers:
    #    c.start()
 
    # Like threading, we have a join() method that synchronizes our program
    for p in producers:
        p.join()
 
    end = time.time()

    print('Finished %d requests with time:'%(len(inputt_total)))
    print(end-start)
    print('Parent process exiting...')



if __name__ == '__main__':
    main()
