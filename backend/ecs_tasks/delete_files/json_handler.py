from gzip import GzipFile
from io import BytesIO
import json
from collections import Counter

from pyarrow import BufferOutputStream, CompressedOutputStream


def initialize(input_file, out_stream, compressed):
    if compressed:
        bytestream = BytesIO(input_file.read())
        input_file = GzipFile(None, "rb", fileobj=bytestream)
    gzip_stream = CompressedOutputStream(out_stream, "gzip") if compressed else None
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


def get_value(key, obj):
    """
    Method to find a value given a nested key in an object. Example:
    key="user.Id"
    obj='{"user":{"id": 1234}}'
    result=1234
    """
    record = obj
    for segment in key.split("."):
        current_key = find_key(segment, record)
        if not current_key:
            return None
        record = record[current_key]
    return record


def delete_matches_from_json_file(
    input_file, to_delete, composite_to_delete, compressed=False
):
    deleted_rows = 0
    with BufferOutputStream() as out_stream:
        input_file, writer = initialize(input_file, out_stream, compressed)
        content = input_file.read().decode("utf-8")
        lines = content.split("\n")
        if lines[-1] == "":
            lines.pop()
        total_rows = len(lines)
        for i, line in enumerate(lines):
            try:
                parsed = json.loads(line)
            except (json.JSONDecodeError) as e:
                raise ValueError(
                    "Serialization error when processing JSON object: {}".format(
                        str(e).replace("line 1", "line {}".format(i + 1))
                    )
                )
            should_delete = False
            for column in to_delete:
                record = get_value(column["Column"], parsed)
                if record and record in column["MatchIds"]:
                    should_delete = True
                    break
            if not should_delete:
                for batch in composite_to_delete:
                    matched = []
                    for column in batch["Columns"]:
                        record = get_value(column, parsed)
                        if record:
                            matched.append(record)
                    if matched in batch["MatchIds"]:
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
