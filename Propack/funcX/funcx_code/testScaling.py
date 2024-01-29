from funcx.sdk.client import FuncXClient
import time
import json

# Functions to be run on the serverless endpoint
###########
def dummyWorkload(n):
    import time
    currTime = time.time()
    if n%2 == 0:
        a = "even"
    else:
        a = "odd"
    return currTime

def getNWorkers():
    import re
    pattern = re.compile("(m+a+x+_+w+o+r+k+e+r+s+_+p+e+r+_+n+o+d+e+=+[0-9]*)")
    with open("/home/ubuntu/.funcx/testEndpoint/config.py") as f:
        matches = re.findall(pattern,f.read())
    nWorkers = matches[0].split("=")[-1]
    return nWorkers


###########

def startScaling(client, endpoint, nWorkers):
    funcId = client.register_function(dummyWorkload)
    workers = list(range(nWorkers))
    batch = client.create_batch()

    for worker in workers:
        batch.add(worker, endpoint_id = endpoint, function_id = funcId)

    startTime = time.time()
    batch_res = client.batch_run(batch)
    return (startTime, batch_res)


def callGetNWorkers(client, endpoint):
    func_id = client.register_function(getNWorkers)
    res = client.run(endpoint_id = endpoint, function_id = func_id)
    return res

def getResult(client, res, nTries=10):
    tries = 0
    while tries < nTries:
        try:
            print("Waiting For Result")
            return(client.get_result(res))
            break
        except Exception:
            time.sleep(2)
            tries += 1

    raise TimeoutError("Endpoint Timed Out")

def _getNWorkers(client, endpoint):
    res = callGetNWorkers(client, endpoint)
    nWorkers = getResult(client, res)
    return nWorkers


def calculateScaleTime(startTime, results):
    # Returns the scaling time in seconds: startTime - lastInstanceBootTime
    endTime = results[-1]
    return (endTime - startTime)

def isDone(task):
    if task['pending'] == False and task['status'] == 'success':
        return True
    else:
        try:
            bogus = task['result']
            return True
        except KeyError:
            return False

def isTimedOut(sleepTime, timeLimit):
    if ((time.time() + sleepTime) < timeLimit):
        return False
    else:
        return True

def waitForTasks(client, res, timeout):
    status = client.get_batch_status(res)

    taskIds = list(status)
    finalTask = status[taskIds[-1]]

    timeLimit = time.time() + timeout

    for taskId in taskIds:
        task = status[taskId]
        # wait for each task to be done
        while (not isDone(task)):
            if (isTimedOut(5, timeLimit)):
                raise TimeoutError("Endpoint Timeout")
            else:
                time.sleep(5)
                print(task)
                status = client.get_batch_status(res)
                task = status[taskId]

    return status

def getBatchResults(client, res, timeout=120):
    status = waitForTasks(client, res, timeout)
    results = []
    taskIds = list(status)

    for taskId in taskIds:
        task = status[taskId]
        results.append(task['result'])

    return results

def startTest(endpoint, concurrentCalls):
    fxc = FuncXClient()

    scaleTimes = {}
    nWorkers = _getNWorkers(fxc, endpoint)

    print(f'Endpoint has {nWorkers} workers')

    for n in concurrentCalls:
        startTime, res = startScaling(fxc, endpoint, n)
        try:
            print(f'Making {n} concurrent calls')
            results = getBatchResults(fxc, res)
            scaleTime = calculateScaleTime(startTime, results)
            scaleTimes[n] = scaleTime
            print("Done")
        except TimeoutError:
            # TODO
            # Results that aren't retrieved take up space in FuncX's redis cluster. It's auto cleared after a week but
            # if too many functions are called, the authors will need to manually purge it.
            # Save batch request ids to disk and run cleanup when startTest() runs?
            print("Endpoint Timed Out")
            break

    return nWorkers, scaleTimes

def saveResults(nWorkers, scaleTimes):
    saveTime = time.strftime("%a, %d %b %Y %H:%M:%S", time.gmtime())
    runResults = {
        "time": saveTime,
        "nWorkers": nWorkers,
        "results": scaleTimes
    }
    with open("data/results.txt","a+") as f:
        f.write(f'[{saveTime}]\n')
        f.write(f'Max Workers at Endpoint: {nWorkers}\n')
        for n in scaleTimes:
            f.write(f'{n}: {scaleTimes[n]}\n')
        f.write("\n")
    return False


if __name__ == "__main__":
    fxc = FuncXClient()

    # My local endpoint
    endpoint = "86ea8dad-6418-4ae8-ab5e-4fadeb052d71"

    # Testing Amazon Linux 2 t2.micro Endpoint
    remoteEndpoint = "7f21d309-1006-48e4-8e23-b8872c82cf82"

    # Amazon Linux 2 m5.xlarge endpoint
    xlEndpoint = "02218f15-1936-4038-bc12-262942f10907"

    # Ubuntu18.04 m5.xlarge Endpoint
    ubuntuEndpoint = "7c286486-3872-44dd-8f5e-a52c1bba27b6"

    # Ubuntu18.04 t2.micro endpoint
    smallUbuntuEndpoint = "f9de936d-66f5-46bf-8946-5b26ef6b7cd1"

    concurrentCalls= [1,100,500,1000,1500,2000,2500,3000,3500,4000,4500,5000]
    #concurrentCalls = [1, 5, 10, 15, 20, 30, 35, 40]

    nWorkers, times = startTest(smallUbuntuEndpoint, concurrentCalls)
    saveResults(nWorkers, times)
