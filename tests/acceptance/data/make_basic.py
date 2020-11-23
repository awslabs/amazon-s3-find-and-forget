import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

"""
Script to generate the basic.parquet file used by acceptance tests
"""

filename = "basic.parquet"

df = pd.DataFrame(
    {
        "customer_id": ["12345", "23456", "34567"],
        "customerId": [12345, 23456, 34567],
        "user_info": [
            {"personal_information": {"email": "12345@test.com", "name": "matteo"}},
            {"personal_information": {"email": "23456@test.com", "name": "nick"}},
            {"personal_information": {"email": "34567@test.com", "name": "chris"}},
        ],
        "days_off": [
            ["2020-01-01", "2020-01-02"],
            ["2020-01-01", "2020-01-07"],
            ["2020-01-05"],
        ],
    }
)

table = pa.Table.from_pandas(df)
pq.write_table(table, filename)

parquet_file = pq.ParquetFile(filename)
schema = parquet_file.metadata.schema.to_arrow_schema().remove_metadata()

table2 = parquet_file.read()
df = table2.to_pandas()
print("File written. Data:\n\n{}".format(df))
