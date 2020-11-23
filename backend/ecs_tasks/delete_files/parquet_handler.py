import logging
from collections import Counter

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


def load_parquet(f):
    return pq.ParquetFile(f, memory_map=False)


def get_row_indexes_to_delete_for_composite(table, identifiers, to_delete):
    """
    TODO
    """
    indexes = []
    data = {}
    for identifier in identifiers:
        column_first_level = identifier.split(".")[0]  # a.b.c => a
        if not column_first_level in data:
            data[column_first_level] = table.column(column_first_level).to_pylist()
    for i in range(table.num_rows):
        values_array = []
        for identifier in identifiers:
            segments = identifier.split(".")
            current = data[segments[0]][i]
            for j in range(1, len(segments)):
                current = current[segments[j]]
            values_array.append(str(current))
        composite_values = "____".join(values_array)
        indexes.append(composite_values in to_delete)
    return np.array(indexes)


def get_row_indexes_to_delete(table, identifier, to_delete):
    """
    Iterates over the values of a particular column and returns a
    numpy mask identifying the rows to delete. The column identifier
    can be simple like "customer_id" or complex like "user.info.id"
    """
    indexes = []
    segments = identifier.split(".")
    for obj in table.column(segments[0]).to_pylist():
        current = obj
        for i in range(1, len(segments)):
            current = current[segments[i]]
        indexes.append(current in to_delete)
    return np.array(indexes)


def delete_from_table(table, to_delete, composite_to_delete):
    """
    Deletes rows from a Arrow Table where any of the MatchIds is found as
    value in any of the columns
    """
    initial_rows = table.num_rows
    for column in to_delete:
        indexes = get_row_indexes_to_delete(table, column["Column"], column["MatchIds"])
        table = table.filter(~indexes)
    for column in composite_to_delete:
        indexes = get_row_indexes_to_delete_for_composite(
            table, column["Columns"], column["MatchIds"]
        )
        table = table.filter(~indexes)
    deleted_rows = initial_rows - table.num_rows
    return table, deleted_rows


def delete_matches_from_parquet_file(input_file, to_delete, composite_to_delete):
    """
    Deletes matches from Parquet file where to_delete is a list of dicts where
    each dict contains a column to search and the MatchIds to search for in
    that particular column and composite_to_delete is a list of dicts where each
    dict contains multiple columns to search and the MatchIds to search for in
    that particular list of columns
    """
    parquet_file = load_parquet(input_file)
    schema = parquet_file.metadata.schema.to_arrow_schema().remove_metadata()
    total_rows = parquet_file.metadata.num_rows
    stats = Counter({"ProcessedRows": total_rows, "DeletedRows": 0})
    with pa.BufferOutputStream() as out_stream:
        with pq.ParquetWriter(out_stream, schema) as writer:
            for row_group in range(parquet_file.num_row_groups):
                logger.info(
                    "Row group %s/%s",
                    str(row_group + 1),
                    str(parquet_file.num_row_groups),
                )
                table = parquet_file.read_row_group(row_group)
                table, deleted_rows = delete_from_table(
                    table, to_delete, composite_to_delete
                )
                writer.write_table(table)
                stats.update({"DeletedRows": deleted_rows})
        return out_stream, stats
