import io
import logging
from collections import Counter

import pyarrow as pa
import pyarrow.json as pj
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


def load_parquet(f, file_format):
    return pq.ParquetFile(f, memory_map=False)


def get_row_count(df):
    return len(df.index)


def delete_from_dataframe(df, to_delete):
    for column in to_delete:
        df = df[~df[column["Column"]].isin(column["MatchIds"])]
    return df


def delete_matches_from_file(input_file, to_delete, file_format):
    """
    Deletes matches from file where to_delete is a list of dicts where
    each dict contains a column to search and the MatchIds to search for in
    that particular column
    """
    if file_format == 'json':
        return delete_matches_from_json_file(input_file, to_delete)
    return delete_matches_from_parquet_file(input_file, to_delete)


def delete_matches_from_json_file(input_file, to_delete):
    json_file = pj.read_json(input_file, parse_options=pj.ParseOptions(newlines_in_values=True))
    df = json_file.to_pandas()
    total_rows = get_row_count(df)
    stats = Counter({"ProcessedRows": total_rows, "DeletedRows": 0})
    with pa.BufferOutputStream() as out_stream:
        with io.StringIO() as json_stream:
            df = delete_from_dataframe(df, to_delete)
            new_rows = get_row_count(df)
            tab = pa.Table.from_pandas(df, preserve_index=False).replace_schema_metadata()
            df.to_json(json_stream, orient='records', lines=True)
            out_stream.write(json_stream.getvalue().encode())
            stats.update({"DeletedRows": total_rows - new_rows})
        return out_stream, stats


def delete_matches_from_parquet_file(input_file, to_delete):
    parquet_file = load_parquet(input_file)
    # Write new file in-memory
    logger.info("Generating new parquet file without matches")
    schema = parquet_file.metadata.schema.to_arrow_schema().remove_metadata()
    total_rows = parquet_file.metadata.num_rows
    stats = Counter({"ProcessedRows": total_rows, "DeletedRows": 0})
    with pa.BufferOutputStream() as out_stream:
        with pq.ParquetWriter(out_stream, schema) as writer:
            for row_group in range(parquet_file.num_row_groups):
                logger.info("Row group %s/%s", str(row_group + 1), str(parquet_file.num_row_groups))
                df = parquet_file.read_row_group(row_group).to_pandas()
                current_rows = get_row_count(df)
                df = delete_from_dataframe(df, to_delete)
                new_rows = get_row_count(df)
                tab = pa.Table.from_pandas(df, schema=schema, preserve_index=False).replace_schema_metadata()
                writer.write_table(tab)
                stats.update({"DeletedRows": current_rows - new_rows})
        return out_stream, stats
