import gzip
import io
import json
import logging
from collections import Counter

import pyarrow as pa

logger = logging.getLogger(__name__)


def delete_matches_from_json_file(input_file, to_delete, compressed=False):
    if compressed:
        input_file = gzip.GzipFile(fileobj=input_file)
    total_rows = deleted_rows = 0
    with pa.BufferOutputStream() as out_stream:
        gzip_stream = (
            pa.CompressedOutputStream(out_stream, "gzip") if compressed else None
        )
        writer = gzip_stream if compressed else out_stream
        while True:
            line = input_file.readline()
            if line:
                total_rows += 1
            else:
                break
            parsed = json.loads(line)
            for column in to_delete:
                record = parsed
                for segment in column["Column"].split("."):
                    record = record[segment]
                if record in column["MatchIds"]:
                    deleted_rows += 1
                else:
                    writer.write(line)
        if compressed:
            gzip_stream.close()
        stats = Counter({"ProcessedRows": total_rows, "DeletedRows": deleted_rows})
        return out_stream, stats
