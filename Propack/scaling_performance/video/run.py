# import boto3
# import paramiko
import threading,time,shlex
from subprocess import DEVNULL, STDOUT, check_call
import subprocess as sp
import shlex
import os, glob
import sys
import shutil
import statistics
FNULL = open(os.devnull, 'w')

st_list=[]
scale_list=[]

def run_lam(nj, parallel):
    num=int(nj/parallel)
    i=1
    pro_list=[]
    start_time=time.time()
    while i <=num:
        command= "aws lambda invoke --function-name video --payload \'{\"key1\":\""+str(parallel)+"\"}\' response" +str(i)+".txt"
        p=sp.Popen(shlex.split(command),stdout=FNULL )
        pro_list.append(p)
        i+=1
    scaling_time=time.time()-start_time #
    for p in pro_list:
        while (1):
            poll = p.poll()
            if poll == None:
                    continue
            else:
                    break
    service_time=time.time()-start_time #
    filenames=["response"+str(i+1)+".txt" for i in range(num)]
    with open('response-concurrency'+str(nj)+'.txt', 'w') as outfile:
        for fname in filenames:
            try:
                with open(fname) as infile:
                        outfile.write(infile.read())
                        outfile.write("\n")
            except:
                pass

    for file in filenames:
            try:
                    command="sudo rm "
                    command+=file+" "
                    sp.check_output(shlex.split(command))
            except:
                    pass           
    st_list.append(service_time)
    scale_list.append(scaling_time)
# concurrency_list=[100]
#concurrency_list=[100, 500]
concurrency_list=[100,500,1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000]
for con in concurrency_list:
    run_lam(con,1)
    print(con)

with open('service_time.txt', 'w') as f:
    for item in st_list:
        f.write("%s\n" % item)
with open('scale_time.txt', 'w') as f:
    for item in scale_list:
        f.write("%s\n" % item)
