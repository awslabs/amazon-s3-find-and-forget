import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

"""
Script to generate the basic.parquet file used by acceptance tests
"""

filename = "basic.parquet"

# Content contains complex data structures (user_info is struct<name:string,email:string>

df = pd.DataFrame({
    'customer_id': [12345, 23456, 34567],
    'user_info': [
        {'name': 'matteo', 'email': '12345@test.com'},
        {'name': 'nick', 'email': '23456@test.com'},
        {'name': 'chris', 'email': '34567@test.com'}],
    'days_off': [['2020-01-01','2020-01-02'], ['2020-01-01','2020-01-07'], ['2020-01-05']]})

table = pa.Table.from_pandas(df)
pq.write_table(table, filename)
print("File created")

parquet_file = pq.ParquetFile(filename)
schema = parquet_file.metadata.schema.to_arrow_schema().remove_metadata()

# Metadata correctly shows n.columns=4
print("Metadata: {}".format(parquet_file.metadata))
print("Schema: {}".format(schema))

table2 = parquet_file.read()
df = table2.to_pandas()
print("Data:\n\n{}".format(df))
