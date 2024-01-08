
### Description :
- Import: Reads partitioned store sales and items data from S3.
- Filtering: Filters items based on specified categories and store sales based on specified stores.
- Joining: Joins filtered store sales and items data on 'ss_item_sk' and 'i_item_sk'.
- Partitioning: Calculates the 'partition' column based on 'ss_ticket_number' and the specified number of partitions.
- Sorting: Sorts the data based on the 'partition' column.
- Row Group Offsets: Calculates row group offsets for the output partitions.
- Export: Writes the joined and partitioned data to the specified S3 location in Parquet format.
- Timing: Records and returns timing information (import, calculation, and export times).

### Code with explaination :
```python
def map1(event, myopen):
    TIMESTAMP = time.time()  # Record the current timestamp
    store_sales = read_partitioned_parallel(
        event["Sales"],
        ["ss_ticket_number", "ss_store_sk", "ss_item_sk"],
        myopen,
    )  # Read partitioned store sales data (columns: 'ss_ticket_number', 'ss_store_sk', 'ss_item_sk')

    items = read_partitioned_parallel(
        event["Items"],
        ["i_category_id", "i_item_sk"],
        myopen,
    )  # Read partitioned items data (columns: 'i_category_id', 'i_item_sk')

    IMPORT_TIME = time.time()  # Record the import time

    # Filter items based on specified categories and select 'i_item_sk'
    filtered_items = items[
        items["i_category_id"].isin(event.setdefault("Categories", [1, 2, 3]))
    ].filter(["i_item_sk"])

    # Filter store sales based on specified stores and select relevant columns
    filtered_sales = store_sales[
        store_sales["ss_store_sk"].isin(
            event.setdefault("Stores", [10, 20, 33, 40, 50])
        )
    ].filter(["ss_item_sk", "ss_ticket_number"])

    filtered_sales["item_id"] = filtered_sales["ss_item_sk"]

    # Join filtered sales and items data on 'ss_item_sk' and 'i_item_sk'
    joined = filtered_sales.set_index("ss_item_sk").join(
        filtered_items.set_index("i_item_sk"), how="inner"
    )

    out_partitions = event["Export"].setdefault("number_partitions", 10)  # Set the number of output partitions

    # Calculate 'partition' based on 'ss_ticket_number' and specified number of partitions
    joined["partition"] = joined["ss_ticket_number"] % out_partitions
    joined.sort_values("partition", inplace=True)

    # Calculate row group offsets for the output partitions
    row_group_offsets = joined["partition"].searchsorted(
        range(out_partitions), side="left"
    )

    CALC_TIME = time.time()  # Record the calculation time

    # Write the joined data to the specified S3 location in Parquet format
    write(
        f"{event['Export']['bucket']}/{event['Export']['key']}",
        joined,
        row_group_offsets=row_group_offsets,
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
