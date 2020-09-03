import gzip
import io
import json
import logging
from collections import Counter

import pyarrow as pa

logger = logging.getLogger(__name__)


def initialize(input_file, out_stream, compressed):
    if compressed:
        input_file = gzip.GzipFile(fileobj=input_file)
    gzip_stream = pa.CompressedOutputStream(out_stream, "gzip") if compressed else None
    writer = gzip_stream if compressed else out_stream
    return input_file, writer


def delete_matches_from_json_file(input_file, to_delete, compressed=False):
    total_rows = deleted_rows = 0
    with pa.BufferOutputStream() as out_stream:
        input_file, writer = initialize(input_file, out_stream, compressed)
        while True:
            line = input_file.readline()
            if line:
                total_rows += 1
            else:
                break
            parsed = json.loads(line)
            should_delete = False
            for column in to_delete:
                record = parsed
                for segment in column["Column"].split("."):
                    if not segment in record:
                        record = None
                        break
                    record = record[segment]
                if record and record in column["MatchIds"]:
                    should_delete = True
                    break
            if should_delete:
                deleted_rows += 1
            else:
                writer.write(line)
        if compressed:
            writer.close()
        stats = Counter({"ProcessedRows": total_rows, "DeletedRows": deleted_rows})
        return out_stream, stats
