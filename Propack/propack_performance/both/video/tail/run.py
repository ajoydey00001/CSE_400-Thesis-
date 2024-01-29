import boto3
import paramiko
import threading,time,shlex
from subprocess import DEVNULL, STDOUT, check_call
import subprocess as sp
import shlex
import os, glob
import sys
import shutil
import statistics
FNULL = open(os.devnull, 'w')

def run_lam(nj, parallel):
    num=int(nj/parallel)
    i=1
    pro_list=[]
    start_time=time.time()
    while i <=num:
        command= "aws lambda invoke --function-name scaling_test_video --payload \'{\"key1\":\""+str(parallel)+"\"}\' response" +str(i)+".txt" ##
        p=sp.Popen(shlex.split(command),stdout=FNULL )
        pro_list.append(p)
        i+=1
    for p in pro_list:
        while (1):
            poll = p.poll()
            if poll == None:
                    continue
            else:
                    break
    filenames=["response"+str(i+1)+".txt" for i in range(num)]

    if  parallel==1:n_num="1"
    else: n_num="optima"

    filename='response_tail_'+str(nj)+'_'+n_num+'.txt'  ##
    with open(filename, 'w') as outfile:
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

    with open(filename) as f:
            l=[x.strip(',') for x in f.readlines()]
    i=0
    end_list=[]
    start_list=[]
    while i < len(l):
        end_list.append(float(l[i][1:-2].split(",")[1]))
        start_list.append(float(l[i][1:-2].split(",")[0]))
        i+=1
    scale_time=start_list[int(len(start_list)*0.9)]-start_time ## 
    service_time=end_list[int(len(end_list)*0.9)]-start_time  ##
    cost_list=[(end_list[i]-start_list[i])*cost_of_lambda for i in range(len(end_list))]
    cost=sum(cost_list)
    llist=[scale_time, service_time, cost]
    with open('final_output_'+str(nj)+'_'+n_num+'.txt', 'w') as outfile:
       	for listitem in llist:
             outfile.write('%s\n' % listitem) 

##main##
concurrency_list=[100,500,1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000]
#concurrency_list=[100,500]
opt_list=[5,9,12,15,15,15,15,16,20,20,20] ##
cost_of_lambda=0.0001667 ##lambda cost per sec 10 GB
for curr in concurrency_list:
   run_lam(curr,1)
   run_lam(curr, opt_list[concurrency_list.index(curr)])
   print(curr)
##
## final output: scale_time, service_time, cost
