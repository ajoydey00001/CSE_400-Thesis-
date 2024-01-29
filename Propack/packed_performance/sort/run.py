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

service_list=[]
parallel_list=[]
def run_lam(parallel, val):
    start_time=time.time()
    command= "aws lambda invoke --function-name scaling_test_sort --payload \'{\"key1\":\""+str(parallel)+"\"}\' response.txt"
    start_time=time.time()
    p=sp.check_output(shlex.split(command))
    if val==1: ##
    	service_list.append(time.time()-start_time) ##
    	parallel_list.append(parallel) ##
    command = "sudo rm response.txt"
    sp.check_output(shlex.split(command))
    return 0

n_max=15	 #

while n_max>0:
    run_lam(n_max,0) ##false
    print(n_max) ##
    run_lam(n_max,1) ##true
    print(n_max)
    n_max-=1
    
with open('service_time.txt', 'w') as filehandle:
    for listitem in service_list:
        filehandle.write('%s\n' % listitem)
with open('parallel.txt', 'w') as filehandle:
    for listitem in parallel_list:
        filehandle.write('%s\n' % listitem)

