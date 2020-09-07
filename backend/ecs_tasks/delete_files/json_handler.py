import gzip
import io
import json
import logging
from collections import Counter

import pyarrow as pa

logger = logging.getLogger(__name__)


def initialize(input_file, out_stream, compressed):
    if compressed:
        bytestream = io.BytesIO(input_file.read())
        input_file = gzip.GzipFile(None, "rb", fileobj=bytestream)
    gzip_stream = pa.CompressedOutputStream(out_stream, "gzip") if compressed else None
    writer = gzip_stream if compressed else out_stream
    return input_file, writer


def find_key(key, obj):
    """
    Athena openx SerDe is case insensitive, and converts by default each object's key
    to a lowercase value: https://docs.aws.amazon.com/athena/latest/ug/json-serde.html

    Here we convert the DataMapper value for the column identifier
    (for instance, customerid) to the JSON's object key (for instance, customerId).
    """
    if not obj:
        return None
    for found_key in obj.keys():
        if key.lower() == found_key.lower():
            return found_key


def delete_matches_from_json_file(input_file, to_delete, compressed=False):
    deleted_rows = 0
    with pa.BufferOutputStream() as out_stream:
        input_file, writer = initialize(input_file, out_stream, compressed)
        content = input_file.read().decode("utf-8")
        lines = content.splitlines()
        total_rows = len(lines)
        for line in lines:
            parsed = json.loads(line)
            should_delete = False
            for column in to_delete:
                record = parsed
                for segment in column["Column"].split("."):
                    current_key = find_key(segment, record)
                    if not current_key:
                        record = None
                        break
                    record = record[current_key]
                if record and record in column["MatchIds"]:
                    should_delete = True
                    break
            if should_delete:
                deleted_rows += 1
            else:
                writer.write(bytes(line + "\n", "utf-8"))
        if compressed:
            writer.close()
        stats = Counter({"ProcessedRows": total_rows, "DeletedRows": deleted_rows})
        return out_stream, stats
