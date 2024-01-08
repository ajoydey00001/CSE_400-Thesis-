This Lambda function performs map-reduce operations on data stored in S3, and the details of the operations are provided in the event parameter. The data is read, processed, and written back to S3.

### `read_partitioned(file_desc)`:
- Parameters:
    - `file_desc`: A dictionary containing information about the file, including 'bucket' and 'key'.
- Body:
    - Constructs the S3 path using 'bucket' and 'key'.
    - Reads a CSV file from S3 using pandas.
    - Returns a pandas DataFrame.
### `read_partitioned_parallel(file_descriptions)`:
- Parameters:
    - `file_descriptions`: A list of dictionaries, each describing a file (e.g., 'Import' files).
- Body:
    - Uses ThreadPoolExecutor to read multiple partitions concurrently.
    - Calls `read_partitioned` for each file description.
    - Concatenates the DataFrames obtained from each partition.
    - Returns a pandas DataFrame obtained by concatenating results from different partitions.
### `map(event)`
- Parameters:
    - `event`: A dictionary containing information about the map operation, including 'Import' and 'Export' details.
- Body:
    - Calls `read_partitioned_parallel` to read data from 'Import' partitions.
    - Filters the DataFrame based on the condition `df["l_shipdate"] <= "1998-09-02"`.
    - Performs some calculations on the DataFrame.
    - Writes the resulting DataFrame to an S3 location specified in the 'Export' details.
- Code : 
```python
def map(event):
    # Declare global variables to track time metrics
    global IMPORT_TIME, CALC_TIME, EXPORT_TIME
    
    # Read data in parallel from 'Import' partitions
    df = read_partitioned_parallel(event["Import"])
    
    # Filter data based on the condition: l_shipdate <= "1998-09-02"
    df = df[df["l_shipdate"] <= "1998-09-02"]
    
    # Record the import time
    IMPORT_TIME = time.time()
    
    # Perform calculations on the DataFrame
    df["pre_1"] = df["l_extendedprice"] * (1 - df["l_discount"])
    df["pre_2"] = df["pre_1"] * (1 + df["l_tax"])
    
    # Group the DataFrame by 'l_returnflag' and 'l_linestatus'
    grouped = df.groupby(["l_returnflag", "l_linestatus"])
    
    # Create a new DataFrame with aggregated results
    map_tpc_h_q1 = pd.DataFrame(
        {
            "sum_qty": grouped["l_quantity"].sum(),
            "sum_base_price": grouped["l_extendedprice"].sum(),
            "sum_disc_price": grouped["pre_1"].sum(),
            "sum_charge": grouped["pre_2"].sum(),
            "avg_qty": grouped["l_quantity"].sum(),
            "avg_price": grouped["l_extendedprice"].sum(),
            "avg_disc": grouped["l_discount"].sum(),
            "count_order": grouped.size(),
        }
    ).reset_index()
    
    # Record the calculation time
    CALC_TIME = time.time()
    
    # TODO: handle cases where partition ends up being empty (e.g., add null element so that a row group can be created)
    
    # Determine the number of output partitions for export
    out_partitions = event["Export"]["number_partitions"]
    
    # Add a 'partition' column based on the given formula
    map_tpc_h_q1["partition"] = (
        map_tpc_h_q1["l_returnflag"].apply(ord) // 2
        + map_tpc_h_q1["l_linestatus"].apply(ord) // 3
    ) % out_partitions
    
    # Sort the DataFrame based on the 'partition' column
    map_tpc_h_q1.sort_values("partition", inplace=True)
    
    # Write the resulting DataFrame to an S3 location specified in 'Export' details
    map_tpc_h_q1.to_csv(f"s3://{event['Export']['bucket']}/{event['Export']['key']}")
    
    # Record the export time
    EXPORT_TIME = time.time()

```
### `reduce(event)` :
- Parameters:
    - `event`: A dictionary containing information about the reduce operation, including 'Import' and 'Export' details.
- Body:
    - Calls `read_partitioned_parallel` to read data from 'Import' partitions.
    - Groups the DataFrame by 'l_returnflag' and 'l_linestatus'.
    - Performs aggregation on the grouped data.
    - Writes the resulting DataFrame to an S3 location specified in the 'Export' details.
    - No explicit return; data is written to S3.
- Code:
```python
def reduce(event):
    # Declare global variables to track time metrics
    global IMPORT_TIME, CALC_TIME, EXPORT_TIME
    
    # Read data in parallel from 'Import' partitions
    df = read_partitioned_parallel(event["Import"])
    
    # Record the import time
    IMPORT_TIME = time.time()
    
    # Initialize an empty dictionary to store values for each unique key
    KEY_VAL = {}
    
    # Iterate over rows of the DataFrame and organize data into the dictionary
    for i, row in enumerate(zip(df["l_returnflag"], df["l_linestatus"])):
        values_for_key = KEY_VAL.setdefault(row, [])
        values_for_key.append(df.iloc[i])

    # Initialize a list to store aggregated results
    return_df_list = []
    
    # Iterate over keys and their associated values in the dictionary
    for key, values in KEY_VAL.items():
        # Deep copy the first value for aggregation
        agg = deepcopy(values[0])
        
        # Iterate over the remaining values for aggregation
        for val in values[1:]:
            # Aggregate numerical columns
            for x in [
                "sum_qty",
                "sum_base_price",
                "sum_disc_price",
                "sum_charge",
                "avg_qty",
                "avg_price",
                "avg_disc",
                "count_order",
            ]:
                agg[x] += val[x]
        
        # Calculate averages for aggregated values
        agg["avg_qty"] /= agg["count_order"]
        agg["avg_price"] /= agg["count_order"]
        agg["avg_disc"] /= agg["count_order"]
        
        # Append the aggregated result to the list
        return_df_list.append(agg)

    # Create a DataFrame from the list and sort by 'l_returnflag' and 'l_linestatus'
    df = pd.DataFrame(return_df_list).sort_values(["l_returnflag", "l_linestatus"])
    
    # Record the calculation time
    CALC_TIME = time.time()
    
    # Write the resulting DataFrame to an S3 location specified in 'Export' details
    df.to_csv(f"s3://{event['Export']['bucket']}/{event['Export']['key']}")
    
    # Record the export time
    EXPORT_TIME = time.time()

```
### `lambda_handler(event, context)` :
- Parameters:
    - `event`: A dictionary containing information about the Lambda invocation, including 'mode' (map or reduce).
    - `context`: Lambda execution context.
- Body:
    - Records the start time.
    - Calls either `map` or `reduce` based on the 'mode' specified in the event.
    - Returns a dictionary with a success message and time metrics for different phases of the processing.




