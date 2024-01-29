# ProPack: Executing Concurrent Serverless Functions Faster and Cheaper

If you use ProPack in your research, please cite our HPDC 2023 paper:

*Rohan Basu Roy, Tirthak Patel, Richmond Liew, Yadu Nand Babuji, Ryan Chard, and Devesh Tiwari. 2023. "ProPack: Executing Concurrent Serverless Functions Faster and Cheaper". In Proceedings of ACM International Symposium on High-Performance Parallel and Distributed Computing (HPDC 2023).*

### Description
At high levels of concurrency, service time of serverless applications depend on the scaling time to reach the high concurrency. ProPack packs multiple serverless functions inside one function instance to reduce the scaling time. In this process it also reduces the execution expense incurred to the end user as the number of spawned function instances decrease. ProPack solves a joint optimization problem by optimizing for both service time and expense.</br> 

### Evaluated Benchmarks
We use commonly used embarrasingly parallel benchmarks which are frequently used as serverless applications and are also part of HPC workflows. The benchmarks we use are (1) [Thousand Island Scanner](https://github.com/qianl15/this)(video) -- which is a distributed video processing benchmark, (2) [Map reduce Sort](https://github.com/Intel-bigdata/HiBench) -- which is a Hadoop implementation of sorting algorithm, and (3) [Stateless Cost](https://github.com/SJTU-IPADS/ServerlessBench/tree/master/Testcase10-Stateless-costs) -- which is a typical parallel serverless application performing image processing. </br>

### Implementation Details
In its performance estimation phase, ProPack uses its analytical model to calcuate the optimal packing degree for different application at different levels of concurrency. Accordingly, ProPack spawns serverless function instances on the serverless platform by packing optimal number of functions inside them. ProPack is implemented in Python3.6. and is easily portable to use with multiple cloud  providers like Amazon Web Services (AWS),Google Cloud, and Microsoft Azure. As a dependency, it only requires the command line interface (CLI) of the respective cloud provider to be installed and configured with the user account credentials. ProPack resides in a local machine and interacts with the cloud provider's serverless platform with the cloud CLI. ProPack stores the information of the calculated optimal packing degree for all future executions of the serverless function instances. </br>

### Experimental platform
 We use AWS Lambda, which is a commercial serverless platform, for our main set of experiments. We invoke Lambdas concurrently using AWS Step Functions. For storage and also for communicating between Lambdas we use Amazon S3 buckets. We also evaluate ProPack in other commercial serverless platforms, like Google Cloud Functions and Microsoft Azure Functions. Each serverless function instance has 6 vCPU cores, 10 GB of memory, running Amazon Linux 2, and they have an execution time limit of 900s. In our experimentation, upto 5000 such serverless function instances were spawned. The cost of running these function instances is $0.0000001667/ms. </br>

### Setup and ProPack Execution Procedure
(1) *Set up AWS CLI access keys and permissions:* 
        
        pip3 install boto3
        pip3 install awscli
        aws configure

(2) *Set up AWS execution role:* </br>
     Set up an execution role from AWS dashboard with full access of Lambda to S3.</br> 


(3) *Set up the benchmarks in AWS Lambda:* </br>
```
     
cd ./deployment_pack/<benchmark_name>/      

   aws lambda create-function --function-name <benchmark_name> --role <lambda_s3_role_id> --handler test.main --memory-size 10000 --runtime python3.6 --timeout 900 --zip-file fileb://test.zip

```
    
Inside each <benchmark_name> directory under *deployment_pack*, the *pre-compiled binaries* for the benchamrks are inside the *test.zip* file. The above command ships the files of all the tasks to AWS Lambda serverless platform for deployment. For the <benchmark_name>, provide the name of the benchmark and for <lambda_s3_role_id> provide the execution role id created in setp (2). After deployment, the input files of the tasks are stored in a S3 bucket.</br>

(4) *Determine the scaling time and service time:* </br>
```
python3 ./scaling_performance/<benchmark_name>/run.py 

```
This determines the scaling time of a benchmark on different levels of concurrency upto 5000. The generated file *scale_time.txt* files contains the records of the scaling time, and *service_time.txt* records the service time. The *response-concurreny<x>.txt* records the scaling time and service time of each of the function instances upto a concurrency of 5000. </br>

(5) *Determine the delay due to packing:* </br>
```
python3 ./packed_performance/<benchmark_name>/run.py 

```
This determines the delay due to packing multiple functions inside one function intance. It generates the file *parallel.txt* which records the packing degree and *service_time.txt* which records the service time for a given packing degree. </br>

(6) *Run ProPack:* </br>
```
python3 ./propack_performance/<optimization_objective>/<benchmark_name>/<metric>/run.py 

```
From steps (4) and (5) this command finds the packing degree and runs ProPack, which packs multiple functions inside one function instance. <optimization_objective> can be to optimize for only *cost* or only *time* or *both* cost and time. <metric> can be *tail*, *median* (med), or *total* (net) to mimimize tail latency (95th percentile), median latency or total latency of service time, respectively. The generated files are *final_output_x_1.txt*, and *final_output_x_optima.txt* which contains the scaling time, service time and cost of running propack and their optimal (Oracle) values, respectively. It also generates *response.txt* files which contain the service time and scaling time of each individual function instances separately. </br>

### Directory structure

```
./deployment_pack/<benchmark_name> 

```
It contains the pre-compiled bnchmark binaries. </br>

```
./funcX

```
It contains the execution results of ProPack on FuncX. </br>
```
./packed_performance/<benchmark_name>

```
It contains the service time delay due to packing multiple functions in one instance. Packing performed upto maximum packing degree. </br>
```
./scaling_performance/<benchmark_name>

```
It contains the scaling time and service time for different levels of concurrency upto 5000. </br>
```
./propack_performance/<optimization_objective>/<benchmark_name>/<metric>/

```
It contains ProPack's perforamnce and optimal (Oracle) performance for different objectives (optimizing for cost or execution time or both) and for different metrics (total, tail, and median). </br>
```
./pywren

```
It contains the comparison of ProPack with Pywren

### Competing Techniques
ProPack packs multiple serverless functions together inside one function instance. Its main idea is to optimally find the number of functions to pack inside one instance (packing degree) to reduce the service time and expense at a high concurrency. We express all our results as a percentage of improvement over no packing, which is the traditional way of executing serverless functions. We perform exhaustive offline search to obtain the optimal packing degree and compare it with ProPack's performance.

We also compare ProPack with the state-of-the-art serverless workload manager [Pywren](http://pywren.io/).

In our evaluation, we evaluate the effectiveness of ProPack with other HPC focused serverless deployments like [FuncX](https://funcx.org/).



















