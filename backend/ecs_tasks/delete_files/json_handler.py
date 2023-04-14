from gzip import GzipFile
from io import BytesIO
import json
from collections import Counter

from boto_utils import json_lines_iterator

from pyarrow import BufferOutputStream, CompressedOutputStream


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


def delete_matches_from_json_file(input_file, to_delete):
    deleted_rows = 0
    with BufferOutputStream() as out_stream:
        content = input_file.read().decode("utf-8")
        total_rows = 0
        for parsed, line in json_lines_iterator(content, include_unparsed=True):
            total_rows += 1
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
                deleted_rows += 1
            else:
                out_stream.write(bytes(line + "\n", "utf-8"))
        stats = Counter({"ProcessedRows": total_rows, "DeletedRows": deleted_rows})
        return out_stream, stats
