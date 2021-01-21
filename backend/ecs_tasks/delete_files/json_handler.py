from gzip import GzipFile
from io import BytesIO
import json
from collections import Counter

from pyarrow import BufferOutputStream, CompressedOutputStream

from redaction_handler import transform_json_rows


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
    for segment in key.split("."):
        current_key = find_key(segment, obj)
        if not current_key:
            return None
        obj = obj[current_key]
    return obj


def delete_matches_from_json_file(
    input_file, to_delete, data_mapper_id, compressed=False
):
    deleted_rows = 0
    transformed_rows = 0
    with BufferOutputStream() as out_stream:
        input_file, writer = initialize(input_file, out_stream, compressed)
        content = input_file.read().decode("utf-8")
        lines = content.split("\n")
        if lines[-1] == "":
            lines.pop()
        total_rows = len(lines)
        to_transform = []
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
                if column["Type"] == "Simple":
                    record = get_value(column["Column"], parsed)
                    if record and record in column["MatchIds"]:
                        should_delete = True
                        break
                else:
                    matched = []
                    for col in column["Columns"]:
                        record = get_value(col, parsed)
                        if record:
                            matched.append(record)
                    if matched in column["MatchIds"]:
                        should_delete = True
                        break
            if should_delete:
                # Before deleting, check if we need to redact instead
                to_transform.append(parsed)
            else:
                writer.write(bytes(line + "\n", "utf-8"))

        transformed = transform_json_rows(to_transform, data_mapper_id)

        for transformed_line in transformed:
            writer.write(bytes(json.dumps(transformed_line) + "\n", "utf-8"))
        redacted_rows = len(transformed)
        deleted_rows = len(to_transform) - redacted_rows

        if compressed:
            writer.close()
        stats = Counter(
            {
                "ProcessedRows": total_rows,
                "DeletedRows": deleted_rows,
                "RedactedRows": redacted_rows,
            }
        )
        return out_stream, stats
