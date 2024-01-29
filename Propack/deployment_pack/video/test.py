import boto3 
import time 
import random 
from PIL import Image,ImageOps
import numpy
import threading 
import os

def run(j):
   a = numpy.random.rand(5000,5000,3) * 255
   im_out = Image.fromarray(a.astype('uint8')).convert('RGB')
   im_out.save('/tmp/im'+str(j)+'.jpg')
   im = Image.open("/tmp/im"+str(j)+".jpg")
   width, height = im.size
   left = 4
   top = height / 5
   right = 154
   bottom = 3 * height / 5
   im1 = im.crop((left, top, right, bottom))
   im_invert = ImageOps.invert(im1)
   os.remove("/tmp/im"+str(j)+".jpg")
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
