### Description :
This function reads item and sales data, filters the data based on specified criteria, identifies pairs of items sold together, and exports the top 50 pairs with the highest count to a specified S3 location in Parquet format. The timing information is recorded and included in the return statement.

```python
def lambda_handler(event, context):
    TIMESTAMP = time.time()  # Get the current timestamp
    s3 = s3fs.S3FileSystem()  # Create an S3FileSystem instance using s3fs library

    # Read 'Items' and 'Sales' Parquet files from S3 and convert them to Pandas DataFrames
    items = ParquetFile(
        event["Items"]["bucket"] + "/" + event["Items"]["key"], open_with=s3.open
    ).to_pandas()
    store_sales = ParquetFile(
        event["Sales"]["bucket"] + "/" + event["Sales"]["key"], open_with=s3.open
    ).to_pandas()
    IMPORT_TIME = time.time()  # Record the import time

    # Filter 'items' DataFrame based on 'i_category_id' and 'Stores' specified in the event
    filtered_items = items[
        items["i_category_id"].isin(event.setdefault("Stores", [1, 2, 3]))
    ].filter(["i_item_sk"])

    # Filter 'store_sales' DataFrame based on 'ss_store_sk' and 'Categories' specified in the event
    filtered_sales = store_sales[
        store_sales["ss_store_sk"].isin(
            event.setdefault("Categories", [10, 20, 33, 40, 50])
        )
    ].filter(["ss_item_sk", "ss_ticket_number"])
    filtered_sales["item_id"] = filtered_sales["ss_item_sk"]

    # Join 'filtered_sales' and 'filtered_items' DataFrames on 'ss_item_sk' and 'i_item_sk'
    joined = filtered_sales.set_index("ss_item_sk").join(
        filtered_items.set_index("i_item_sk"), how="inner"
    )

    # Group by 'ss_ticket_number' and create a Series of unique item sets sold together
    sold_together = joined.groupby("ss_ticket_number")["item_id"].unique()

    # Count pairs of items sold together using itertools combinations and update the counter
    counted_pairs = Counter()
    for items in sold_together:
        counted_pairs.update(itertools.combinations(sorted(items), 2))

    # Convert counted pairs to a DataFrame, filter pairs with count > 50, and select the top 50
    counted_pairs = (
        pd.DataFrame.from_dict(counted_pairs, orient="index")
        .reset_index()
        .rename(columns={"index": "item_pairs", 0: "count"})
    )
    counted_pairs[["item1", "item2"]] = pd.DataFrame(
        counted_pairs["item_pairs"].tolist(), index=counted_pairs.index
    )
    del counted_pairs["item_pairs"]
    counted_pairs = counted_pairs[counted_pairs["count"] > 50]
    top_pairs = counted_pairs.sort_values("count", ascending=False).iloc[:50]

    CALC_TIME = time.time()  # Record the calculation time

    # Write the result ('top_pairs') to the specified S3 location in Parquet format
    write(
        event["Export"]["bucket"] + "/" + event["Export"]["key"],
        top_pairs,
        open_with=s3.open,
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
