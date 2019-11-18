"""
Task to aggregate query results, grouping by file
"""

from collections import defaultdict

from decorators import with_logger


@with_logger
def handler(event, context):
    d = defaultdict(list)
    # Deduplicate paths which appear in multiple query results, concatenating Column lists for each paths
    for item in event:
        for obj in item["Objects"]:
            d[obj] = d[obj] + item["Columns"]

    transformed = []
    # For each path, deduplicate Columns with the same name, merging the MatchIds from each
    for path, cols in d.items():
        merged_cols = {}
        for col in cols:
            name = col["Column"]
            if merged_cols.get(name):
                merged_cols[name]["MatchIds"] = sorted(set(col["MatchIds"]) | set(merged_cols[name]["MatchIds"]))
            else:
                merged_cols[name] = {
                    "Column": col["Column"],
                    "MatchIds": col["MatchIds"]
                }
        transformed.append({
            "Object": path,
            "Columns": list(merged_cols.values())
        })
    return transformed
