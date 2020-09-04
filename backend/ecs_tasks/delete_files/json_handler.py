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


def delete_matches_from_json_file(input_file, to_delete, compressed=False):
    deleted_rows = 0
    with pa.BufferOutputStream() as out_stream:
        input_file, writer = initialize(input_file, out_stream, compressed)
        content = input_file.read().decode("utf-8")
        lines = content.split("\n")[:-1]
        total_rows = len(lines)
        for line in lines:
            parsed = json.loads(line)
            should_delete = False
            for column in to_delete:
                record = parsed
                for segment in column["Column"].split("."):
                    if not record or not segment in record:
                        record = None
                        break
                    record = record[segment]
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
