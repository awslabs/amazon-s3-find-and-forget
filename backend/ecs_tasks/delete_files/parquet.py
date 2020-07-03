import logging
from collections import Counter

import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


def load_parquet(f):
    return pq.ParquetFile(f, memory_map=False)


def get_row_count(df):
    return len(df.index)


def delete_from_dataframe(row, to_delete):
    # iterate over flattened dataframe first in order to search
    # for complex structs columns, for example reports.user.id
    df = row.flatten().to_pandas()
    current_rows = get_row_count(df)
    indexes_to_delete = []
    for column in to_delete:
        indexes = df[column["Column"]].isin(column["MatchIds"])
        indexes_to_delete.append(indexes)
        # it's important to remove the indexes from the current df
        # to guarantee the next operation will operate on same 
        # indexes if iterating on multiple columns
        df = df[~indexes]
    # now operate with unflattened row to preserve original schema
    # and operate filtering in the same order as the previous one
    df = row.to_pandas()
    for indexes in indexes_to_delete:
        df=df[~indexes]
    deleted_rows = current_rows - get_row_count(df)
    return df, deleted_rows


def delete_matches_from_file(parquet_file, to_delete):
    """
    Deletes matches from Parquet file where to_delete is a list of dicts where
    each dict contains a column to search and the MatchIds to search for in
    that particular column
    """
    schema = parquet_file.metadata.schema.to_arrow_schema().remove_metadata()
    total_rows = parquet_file.metadata.num_rows
    stats = Counter({"ProcessedRows": total_rows, "DeletedRows": 0})
    with pa.BufferOutputStream() as out_stream:
        with pq.ParquetWriter(out_stream, schema) as writer:
            for row_group in range(parquet_file.num_row_groups):
                logger.info("Row group %s/%s", str(row_group + 1), str(parquet_file.num_row_groups))
                row = parquet_file.read_row_group(row_group)
                df, deleted_rows = delete_from_dataframe(row, to_delete)
                tab = pa.Table.from_pandas(df, schema=schema, preserve_index=False).replace_schema_metadata()
                writer.write_table(tab)
                stats.update({"DeletedRows": deleted_rows})
        return out_stream, stats
