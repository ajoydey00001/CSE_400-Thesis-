
These functions together form a Lambda function that reads, processes, and writes Parquet data based on the specified mode (map or reduce). The `map` function applies a filter, performs calculations, and writes the result, while the `reduce` function performs aggregation and writes the result. The Lambda handler decides which function to call based on the mode provided in the input event.

### `read_partitioned` Function:
- Parameters:
    - `file_desc`: Information about the file (bucket, key, partitions).
    - `columns`: Columns to be read from the Parquet file.
    - `opener`: File opener for Parquet files.
    - `filter`: Boolean flag indicating whether to apply a filter during reading.
- Body:
    - Reads a partitioned Parquet file based on the provided file description.
    - Optionally applies a filter on the 'l_shipdate' column if `filter` is True.
    - Returns a Pandas DataFrame.
- code:
```python

def read_partitioned(file_desc, columns, opener, filter=False):
    # Create an empty DataFrame to store the read data
    df = pd.DataFrame()

    # Construct the full path to the Parquet file using the bucket and key from file_desc
    input_file = f"{file_desc['bucket']}/{file_desc['key']}"

    # Extract the list of partitions from the file description
    partitions = file_desc["partitions"]

    # Open the Parquet file using ParquetFile and the provided opener
    pf = ParquetFile(input_file, open_with=opener)

    # Iterate over each row group (rg) in the Parquet file
    for i, rg in enumerate(pf):  # read only said partitions:

        # Check if specific partitions are specified or if the list is empty (read all partitions)
        if partitions == [] or i in partitions:
            # Check if a filter is specified
            if filter:
                # Apply a filter to the 'l_shipdate' column and concatenate the result to the DataFrame
                partition_data = rg.to_pandas(
                    columns=columns, filters=[("l_shipdate", "<=", "1998-09-02")]
                )
                df = pd.concat(
                    [df, partition_data[partition_data["l_shipdate"] <= "1998-09-02"]]
                )
            else:
                # Concatenate the entire row group to the DataFrame
                df = pd.concat([df, rg.to_pandas(columns=columns)])

    # Return the final DataFrame containing the read data
    return df

```
### `wrapper_read_partitioned` Function:
- Parameters:
    - `columns`: Columns to be read from the Parquet file.
    - `opener`: File opener for Parquet files.
    - `filter`: Boolean flag indicating whether to apply a filter during reading.
- Body:
    - Returns a partially applied version of read_partitioned with specified    columns, opener, and filter. 
### `read_partitioned_parallel` Function:
- Parameters: 
    - `file_descriptions`: List of file descriptions.
    - `columns`: Columns to be read from the Parquet file.
    - `opener`: File opener for Parquet files.
    - `filter`: Boolean flag indicating whether to apply a filter during reading.
- Body:
    - Reads multiple partitioned Parquet files in parallel using multithreading.
    - Uses `wrapper_read_partitioned` to create a partially applied version with specified columns, opener, and filter.
    - Returns a concatenated Pandas DataFrame.
### `map Function`:
- Parameters:
    - `columns`: Columns to be read from the Parquet file.
    - `s3`: S3 file system object.
    - `event`: Lambda event containing import/export details.
    - `myopen`: File opener for S3 files.
- Body:
    - Reads partitioned Parquet files in parallel, applies calculations to create new columns (`pre_1` and `pre_2`).
    - Performs groupby operations and exports the result as a new Parquet file.
    - Records time metrics for import, calculation, and export phases.
- Code:
```python
def map(columns, s3, event, myopen):
    # Declare global variables to track time metrics
    global IMPORT_TIME, CALC_TIME, EXPORT_TIME

    # Read the partitioned data in parallel using the read_partitioned_parallel function
    df = read_partitioned_parallel(event["Import"], columns, myopen, filter=True)

    # Record the start time for the import phase
    IMPORT_TIME = time.time()

    # Perform additional computations on the DataFrame
    df["pre_1"] = df["l_extendedprice"] * (1 - df["l_discount"])
    df["pre_2"] = df["pre_1"] * (1 + df["l_tax"])

    # Group the DataFrame based on specified columns
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

    # Record the start time for the calculation phase
    CALC_TIME = time.time()

    # TODO: handle cases where partition ends up being empty (e.g. add null element so that row group can be created)

    # Determine the number of output partitions specified in the event
    out_partitions = event["Export"]["number_partitions"]

    # Calculate a 'partition' column based on specified logic
    map_tpc_h_q1["partition"] = (
        map_tpc_h_q1["l_returnflag"].apply(ord) // 2
        + map_tpc_h_q1["l_linestatus"].apply(ord) // 3
    ) % out_partitions

    # Sort the DataFrame based on the 'partition' column
    map_tpc_h_q1.sort_values("partition", inplace=True)

    # Determine the offsets of row groups for writing to output partitions
    row_group_offsets = map_tpc_h_q1["partition"].searchsorted(
        range(out_partitions), side="left"
    )

    # Write the DataFrame to the specified S3 location in Parquet format
    write(
        f"{event['Export']['bucket']}/{event['Export']['key']}",
        map_tpc_h_q1,
        row_group_offsets=row_group_offsets,
        open_with=myopen,
        write_index=False
    )

    # Record the start time for the export phase
    EXPORT_TIME = time.time()

```
### `reduce Function`:
- Parameters:
    - `columns`: Columns to be read from the Parquet file.
    - `s3`: S3 file system object.
    - `event`: Lambda event containing import/export details.
    - `myopen`: File opener for S3 files.
- Body:
    - Reads partitioned Parquet files in parallel.
    - Performs a custom reduction operation and exports the result as a new Parquet file.
    - Records time metrics for import, calculation, and export phases.
    - Returns `None`.
- Code:
```python
def reduce(columns, s3, event, myopen):
    # Declare global variables to track time metrics
    global IMPORT_TIME, CALC_TIME, EXPORT_TIME

    # Read the partitioned data in parallel using the read_partitioned_parallel function
    df = read_partitioned_parallel(event["Import"], columns, myopen)

    # Record the start time for the import phase
    IMPORT_TIME = time.time()

    # Initialize an empty dictionary to store values grouped by a key
    KEY_VAL = {}

    # Iterate through the DataFrame and group values based on specified columns
    for i, row in enumerate(zip(df["l_returnflag"], df["l_linestatus"])):
        values_for_key = KEY_VAL.setdefault(row, [])
        values_for_key.append(df.iloc[i])

    # Initialize an empty list to store aggregated values
    return_df_list = []

    # Iterate through the grouped values and calculate aggregations
    for key, values in KEY_VAL.items():
        agg = deepcopy(values[0])
        for val in values[1:]:
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
        agg["avg_qty"] /= agg["count_order"]
        agg["avg_price"] /= agg["count_order"]
        agg["avg_disc"] /= agg["count_order"]
        return_df_list.append(agg)

    # Create a DataFrame from the aggregated values and sort it
    df = pd.DataFrame(return_df_list).sort_values(["l_returnflag", "l_linestatus"])

    # Record the start time for the calculation phase
    CALC_TIME = time.time()

    # Write the DataFrame to the specified S3 location in Parquet format
    write(f"{event['Export']['bucket']}/{event['Export']['key']}", df, open_with=myopen, write_index=False)

    # Record the start time for the export phase
    EXPORT_TIME = time.time()

```
### `lambda_handler` Function:
- Parameters:
    - `event`: Lambda event containing mode and other details.
    - `context`: Lambda context.
- Body:
    - Records the start timestamp.
    - Determines the operation mode (`map` or `reduce`) from the provided event.
    - Calls the corresponding `map` or `reduce` function.
    - Records timestamps for different phases (import, calculation, export).
    - Returns a dictionary containing a success message and timing information.
- Code:
```python
def lambda_handler(event, context):
    # Record the start time for the entire Lambda function
    TIMESTAMP = time.time()

    # Define the columns to be used in the processing
    columns = [
        "l_quantity",
        "l_extendedprice",
        "l_discount",
        "l_shipdate",
        "l_tax",
        "l_returnflag",
        "l_linestatus",
    ]

    # Create an S3FileSystem object for interacting with S3
    s3 = s3fs.S3FileSystem()

    # Define the open function for S3 file access
    myopen = s3.open

    # Check the processing mode specified in the event
    if event["mode"] == "map":
        # Call the map function for the "map" processing mode
        map(columns, s3, event, myopen)
    elif event["mode"] == "reduce":
        # Call the reduce function for the "reduce" processing mode
        reduce(None, s3, event, myopen)

    # Return a dictionary containing a success message and time metrics
    return {
        "message": "success",
        "times": [
            {
                "timestamp": TIMESTAMP * 1000,
                "Import": IMPORT_TIME * 1000,
                "Calculation": CALC_TIME * 1000,
                "Export": EXPORT_TIME * 1000,
            }
        ],
    }

```





