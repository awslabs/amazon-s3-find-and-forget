import io
import logging
from collections import Counter

import pyarrow as pa
import pyarrow.json as pj
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


def load_parquet(f):
    return pq.ParquetFile(f, memory_map=False)


def get_row_count(df):
    return len(df.index)


def needs_flattening(to_delete):
    """
    Lookup the columns to identify if any are nested inside structs
    (and therefore represented in a form like foo.bar).

    Operating on flattened columns is necessary when operating with 
    complex column types but it impacts performance during write as
    we need to re-allocate the data in-memory as unflattened to 
    preserve the initial schema's hierarchy after figuring out which rows
    need deletion. This is why we check first: it allows to do the
    re-allocation only if necessary.
    """
    return any("." in x["Column"] for x in to_delete)


def delete_from_table(table, to_delete):
    """
    Deletes rows from a Arrow Table where any of the MatchIds is found as
    value in any of the columns. Returns a Pandas DataFrame
    """
    print("matteo")
    needs_flattened_columns = needs_flattening(to_delete)
    df = (table.flatten() if needs_flattened_columns else table).to_pandas()
    initial_rows = get_row_count(df)
    indexes_to_delete = []
    for column in to_delete:
        indexes = df[column["Column"]].isin(column["MatchIds"])
        indexes_to_delete.append(indexes)
        df = df[~indexes]
    if needs_flattened_columns:
        df = table.to_pandas()
        for indexes in indexes_to_delete:
            df = df[~indexes]
    deleted_rows = initial_rows - get_row_count(df)
    return df, deleted_rows


def delete_matches_from_file(input_file, to_delete, file_format):
    """
    Deletes matches from file where to_delete is a list of dicts where
    each dict contains a column to search and the MatchIds to search for in
    that particular column
    """
    logger.info("Generating new file without matches")
    if file_format == "json":
        return delete_matches_from_json_file(input_file, to_delete)
    return delete_matches_from_parquet_file(input_file, to_delete)


def delete_matches_from_json_file(input_file, to_delete):
    json_file = pj.read_json(
        input_file, parse_options=pj.ParseOptions(newlines_in_values=True)
    )
    total_rows = json_file.num_rows
    stats = Counter({"ProcessedRows": total_rows, "DeletedRows": 0})
    with pa.BufferOutputStream() as out_stream:
        with io.StringIO() as json_stream:
            df, deleted_rows = delete_from_table(json_file, to_delete)
            df.to_json(json_stream, orient="records", lines=True)
            out_stream.write(json_stream.getvalue().encode())
            stats.update({"DeletedRows": deleted_rows})
        return out_stream, stats


def delete_matches_from_parquet_file(input_file, to_delete):
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
                df, deleted_rows = delete_from_table(table, to_delete)
                table = pa.Table.from_pandas(
                    df, schema=schema, preserve_index=False
                ).replace_schema_metadata()
                writer.write_table(table)
                stats.update({"DeletedRows": deleted_rows})
        return out_stream, stats
