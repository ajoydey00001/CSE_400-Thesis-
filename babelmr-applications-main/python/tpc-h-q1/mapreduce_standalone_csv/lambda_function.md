
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






