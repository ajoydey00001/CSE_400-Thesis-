### `run_lam` Function:
- Parameters:
    - `nj`: The total number of invocations or concurrency level.
    - `parallel`: The number of invocations to be performed in parallel.
- Body:
    - A loop runs to invoke AWS Lambda functions in parallel. The aws lambda invoke command is used with a payload, and the response is redirected to a file. Each invocation is performed with a subprocess.
    - `scaling_time` is calculated as the time taken for the parallel invocations to complete.
    - A loop waits for all the Lambda invocations to complete by checking the status of each process in `pro_list`.
    - `service_time` is calculated as the total time taken for all invocations.
    - Responses from Lambda invocations are concatenated and written to a file named 'response-concurrency' + str(nj) + '.txt'.

### Code with Explaination :
```python

# Open a file object for writing to /dev/null (a null device)
FNULL = open(os.devnull, 'w')

# Lists to store service times and scaling times
st_list = []
scale_list = []

# Function to run AWS Lambda in parallel
def run_lam(nj, parallel):
    num = int(nj / parallel)
    i = 1
    pro_list = []
    start_time = time.time()

    # Invoke AWS Lambda functions in parallel
    while i <= num:
        command = "aws lambda invoke --function-name scaling_test_sort --payload \'{\"key1\":\"" + str(
            parallel) + "\"}\' response" + str(i) + ".txt"
        p = sp.Popen(shlex.split(command), stdout=FNULL)
        pro_list.append(p)
        i += 1

    scaling_time = time.time() - start_time  # Time taken for scaling
    # Wait for all Lambda invocations to complete
    for p in pro_list:
        while True:
            poll = p.poll()
            if poll is None:
                continue
            else:
                break

    service_time = time.time() - start_time  # Total service time
    filenames = ["response" + str(i + 1) + ".txt" for i in range(num)]

    # Write Lambda responses to a file
    with open('response-concurrency' + str(nj) + '.txt', 'w') as outfile:
        for fname in filenames:
            try:
                with open(fname) as infile:
                    outfile.write(infile.read())
                    outfile.write("\n")
            except:
                pass

    # Remove response files
    for file in filenames:
        try:
            command = "sudo rm "
            command += file + " "
            sp.check_output(shlex.split(command))
        except:
            pass

    st_list.append(service_time)
    scale_list.append(scaling_time)



```
