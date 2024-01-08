### Description :
- `Import`: Reads partitioned data in parallel from an S3 location, specifically columns 'item1', 'item2', and 'count'.
- `Grouping`: Groups the pairs of items ('item1', 'item2') and sums the counts for each unique pair.
- `Reduction`: Filters out pairs with counts less than or equal to 50 and selects the top 50 pairs with the highest counts.
- `Export`: Writes the reduced result to a specified S3 location in Parquet format.
- `Timing`: Records and returns timing information (import, calculation, and export times).

### Code :
```python
def reduce(event, myopen):
    TIMESTAMP = time.time()  # Get the current timestamp
    pairs = read_partitioned_parallel(
        event["Import"], ["item1", "item2", "count"], myopen
    )  # Read partitioned data (columns: 'item1', 'item2', 'count') in parallel
    IMPORT_TIME = time.time()  # Record the import time

    # Group pairs by 'item1' and 'item2', then sum the 'count' for each group
    pairs = pairs.groupby(["item1", "item2"])

    # Sum the 'count' for each group, reset index, sort by count in descending order, and select the top 50
    reduced = (
        pairs["count"]
        .sum()
        .reset_index()
        .sort_values("count", ascending=False)
        .iloc[:50]
    )

    # Filter pairs with count greater than 50
    reduced = reduced[reduced["count"] > 50]
    CALC_TIME = time.time()  # Record the calculation time

    # Write the reduced result to the specified S3 location in Parquet format
    write(
        f"{event['Export']['bucket']}/{event['Export']['key']}",
        reduced,
        open_with=myopen,
        write_index=False
    )

    EXPORT_TIME = time.time()  # Record the export time

    # Return a dictionary with success message and timing information
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
