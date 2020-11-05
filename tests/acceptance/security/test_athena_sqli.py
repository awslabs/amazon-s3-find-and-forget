import logging
import tempfile

import pytest
from boto3.dynamodb.conditions import Key, Attr

from tests.acceptance import query_parquet_file

logger = logging.getLogger()

pytestmark = [
    pytest.mark.acceptance,
    pytest.mark.athena,
    pytest.mark.security,
    pytest.mark.usefixtures("empty_lake", "empty_jobs"),
]


def test_it_handles_injection_attacks(
    del_queue_factory,
    job_factory,
    dummy_lake,
    glue_data_mapper_factory,
    data_loader,
    job_complete_waiter,
    job_table,
):
    """
    Makes a multiprocessing.

    Args:
        del_queue_factory: (todo): write your description
        job_factory: (todo): write your description
        dummy_lake: (str): write your description
        glue_data_mapper_factory: (todo): write your description
        data_loader: (todo): write your description
        job_complete_waiter: (bool): write your description
        job_table: (todo): write your description
    """
    # Generate a parquet file and add it to the lake
    glue_data_mapper_factory(
        "test",
        partition_keys=["year", "month", "day"],
        partitions=[["2019", "08", "20"]],
    )
    glue_data_mapper_factory(
        "test2", database="acceptancetest2", table="acceptancetest2"
    )
    legit_match_id = "12345"
    object_key = "test/2019/08/20/test.parquet"
    data_loader("basic.parquet", object_key)
    bucket = dummy_lake["bucket"]
    """
    Using single quotes as part of the match_id could be a SQL injection attack for reading information from other 
    tables. While this should be prevented by configuring IAM, it is appropriate to test that the query_handler properly
    escapes the quotes and Athena doesn't access other tables.
    """
    cross_db_access = "foo')) UNION (select * from acceptancetests2.acceptancetests2 where customer_id in ('12345"
    """
    Using single quotes as part of the match_id could be a SQL injection attack for reading information from other 
    tables. While this should be prevented by configuring IAM, it is appropriate to test that the query_handler properly
    escapes the quotes and Athena doesn't access other tables.
    """
    cross_db_escaped = "foo')) UNION (select * from acceptancetests2.acceptancetests2 where customer_id in ('12345"
    """
    Unicode smuggling is taken care out of the box. Here is a test with "ʼ", which is similar to single quote.
    """
    unicode_smuggling = "fooʼ)) UNION (select * from acceptancetests2.acceptancetests2 where customer_id in (ʼ12345"
    """
    Another common SQLi attack vector consists on fragmented attacks. Tamper the result of the select by commenting 
    out relevant match_ids by using "--" after a successful escape. This attack wouldn't work because Athena's
    way to escape single quotes are by doubling them rather than using backslash.
    Example: ... WHERE (user_id in ('foo', '\')) --','legit'))
    """
    commenting = ["'", ")) --", legit_match_id]
    new_lines = ["\n--", legit_match_id, "\n"]
    del_queue_items = []
    for i in [
        legit_match_id,
        cross_db_access,
        cross_db_escaped,
        unicode_smuggling,
        *commenting,
        *new_lines,
    ]:
        del_queue_items.append(del_queue_factory(i))
    job_id = job_factory(del_queue_items=del_queue_items)["Id"]
    # Act
    job_complete_waiter.wait(
        TableName=job_table.name, Key={"Id": {"S": job_id}, "Sk": {"S": job_id}}
    )
    # Assert
    tmp = tempfile.NamedTemporaryFile()
    bucket.download_fileobj(object_key, tmp)
    assert (
        "COMPLETED"
        == job_table.get_item(Key={"Id": job_id, "Sk": job_id})["Item"]["JobStatus"]
    )
    assert 0 == len(query_parquet_file(tmp, "customer_id", "12345"))
    assert 1 == len(
        job_table.query(
            KeyConditionExpression=Key("Id").eq(job_id),
            ScanIndexForward=True,
            Limit=20,
            FilterExpression=Attr("Type").eq("JobEvent")
            & Attr("EventName").eq("ObjectUpdated"),
            ExclusiveStartKey={"Id": job_id, "Sk": str(0)},
        )["Items"]
    )
