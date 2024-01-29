### Here this `run` function is same as the `run` function in `` drive . Here some additional code is added .

The script loops through each concurrency level, running the `run_lam` function with `parallelism` set to 1 and the `optimized value`.

### Code :
```python
#For optimized parallelism 
opt_list = [2, 4, 5, 5, 12, 12, 12, 12, 12, 12, 12]  ##
cost_of_lambda = 0.0001667  ## Lambda cost per second for 10 GB

# The script loops through each concurrency level, running the `run_lam` function with `parallelism` set to 1 and the `optimized value`.
for curr in concurrency_list:
    run_lam(curr, 1)  # Run with parallelism = 1
    run_lam(curr, opt_list[concurrency_list.index(curr)])  # Run with optimized parallelism
    print(curr)
```
