from PIL import Image
import numpy
import os
import time
import threading
import pywren

def run(j):
    nn1=time.time()
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
    newsize = (300, 300)
    im1 = im1.resize(newsize)
    os.remove("/tmp/im"+str(j)+".jpg")
    nn2=time.time()
    return ((nn2-nn1)*0.0001667)



test_list=[1,100,500,1000,1500,2000,2500,3000,3500,4000,4500,5000]
#test_list=[1,10]
main_list=[]
for tt in test_list:
	wrenexec = pywren.default_executor()
	#future = wrenexec.call_async(my_function, 3)
	futures = wrenexec.map(run, range(tt))
	res=pywren.get_all_results(futures)
	ret_list=[i for i in res]
	main_list.append(sum(ret_list))
	print(sum(ret_list))

with open('cost.txt', 'w') as filehandle:
    for listitem in main_list:
        filehandle.write('%s\n' % listitem)
