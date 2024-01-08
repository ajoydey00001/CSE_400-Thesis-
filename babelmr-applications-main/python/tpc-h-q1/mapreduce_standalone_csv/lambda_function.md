
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






