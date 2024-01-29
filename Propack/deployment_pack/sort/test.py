import boto3
import time
import random 
import numpy as np 
import threading
import os

def run(a):
   BUCKET_NAME = 'app-bucket-rohan-new'
   BUCKET_FILE_NAME = 'unsorted_list.txt'
   LOCAL_FILE_NAME = '/tmp/unsorted_list'+str(a)+'.txt'
   s3 = boto3.client('s3')
   s3.download_file(BUCKET_NAME, BUCKET_FILE_NAME, LOCAL_FILE_NAME)
   with open('/tmp/unsorted_list'+str(a)+'.txt','r') as f:
   	x=f.readlines()
   x=[float(x[i]) for i in range(len(x))]
   y=np.sort(x)
   with open('/tmp/sorted_list'+str(a)+'.txt', 'w') as f:
    for item in y:
        f.write("%s\n" % item)
   s3.upload_file('/tmp/sorted_list'+str(a)+'.txt', 'app-bucket-rohan-new', 'sorted_list.txt')
   os.remove('/tmp/sorted_list'+str(a)+'.txt')
   return 0

def main(event, context):
    start_time=time.time()
    threads=[]
    j=0
    while j < int(event['key1']):
        t1 = threading.Thread(target=run,args=([j]))
        t1.start()
        threads.append(t1)
        j+=1
    for t in threads:
        t.join()
    return start_time, time.time()
