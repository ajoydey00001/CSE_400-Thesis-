### Description :
this script is a Lambda function designed to download an unsorted list from S3, sort it, upload the sorted list back to S3, and measure the execution time. The use of `threads` allows multiple instances of the run function to execute in parallel.

### Code with explaination :
```python
import boto3
import time
import random 
import numpy as np 
import threading
import os

# Function to download, sort, and upload a file to/from AWS S3
def run(a):
   BUCKET_NAME = 'app-bucket-rohan-new'
   BUCKET_FILE_NAME = 'unsorted_list.txt'
   LOCAL_FILE_NAME = '/tmp/unsorted_list'+str(a)+'.txt'
   s3 = boto3.client('s3')
   
   # Download unsorted_list.txt from S3
   s3.download_file(BUCKET_NAME, BUCKET_FILE_NAME, LOCAL_FILE_NAME)
   
   # Read the file, convert its content to floats, and sort the list
   with open('/tmp/unsorted_list'+str(a)+'.txt','r') as f:
    x = f.readlines()
   x = [float(x[i]) for i in range(len(x))]
   y = np.sort(x)
   
   # Write the sorted list to a new file
   with open('/tmp/sorted_list'+str(a)+'.txt', 'w') as f:
    for item in y:
        f.write("%s\n" % item)
   
   # Upload the sorted file back to S3
   s3.upload_file('/tmp/sorted_list'+str(a)+'.txt', 'app-bucket-rohan-new', 'sorted_list.txt')
   
   # Remove the temporary sorted file
   os.remove('/tmp/sorted_list'+str(a)+'.txt')
   
   return 0

# Main Lambda function
def main(event, context):
    start_time = time.time()
    threads = []
    j = 0
    
    # Create threads to execute the run function in parallel
    while j < int(event['key1']):
        t1 = threading.Thread(target=run, args=([j]))
        t1.start()
        threads.append(t1)
        j += 1
    
    # Wait for all threads to complete
    for t in threads:
        t.join()
    
    # Return the start time and end time
    return start_time, time.time()

```
