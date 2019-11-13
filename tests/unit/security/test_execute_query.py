import os
import re
from types import SimpleNamespace

import pytest
from mock import patch

from lambdas.src.tasks.execute_query import handler, make_query, escape_item

pytestmark = [pytest.mark.unit, pytest.mark.task]

def stringify(resp):
    return re.sub("[\x00-\x20]+", " ", resp.strip())

# Using single quotes as part of the match_id could be a SQL injection attack
# for reading information from other tables. While this should be prevented
# by configuring IAM, it is appropriate to escape the quotes as extra protection.

def test_it_escapes_match_ids_single_quotes_preventing_stealing_information():
    resp = make_query({
        "Database": "amazonreviews",
        "Table": "amazon_reviews_parquet",
        "Columns": [{"Column": "customer_id", "MatchIds": [
          "foo')) UNION ((select * from db2.table where column not in ('nope"
        ]}]
    })

    assert "SELECT \"$path\" " \
           "FROM \"amazonreviews\".\"amazon_reviews_parquet\" " \
           "WHERE (\"customer_id\" in ('456789'')) UNION " \
           "((select * from db2.table where column not in (''nope'))" == stringify(resp)

# This query without the Python escaping looks like:
#
# SELECT "$path" FROM "amazonreviews"."amazon_reviews_parquet"
# WHERE ("customer_id" in
# ('456789'')) UNION ((select * from db2.table where column not in (''nope'))
#
#  ^-----------------------------------------------------------------------^ this
# is considered as a whole customer_id match, which is correct as the union and the
# nested select are not executed.

# ============================================================================= #

# The following is for testing that escaped single quotes are handled as well.

def test_it_escapes_match_ids_escaped_single_quotes_preventing_stealing_information():
    resp = make_query({
        "Database": "amazonreviews",
        "Table": "amazon_reviews_parquet",
        "Columns": [{"Column": "customer_id", "MatchIds": [
          "456789\')) UNION ((select * from db2.table where column not in (\'nope"
        ]}]
    })

    assert "SELECT \"$path\" " \
           "FROM \"amazonreviews\".\"amazon_reviews_parquet\" " \
           "WHERE (\"customer_id\" in ('456789'')) UNION " \
           "((select * from db2.table where column not in (''nope'))" == stringify(resp)

# ============================================================================= #

# Another common SQLi attack vector consists on fragmented attacks. Tamper the
# result of the select by commenting out relevant match_ids by using "--"
# after a successful escape. This attack wouldn't work because Athena's 
# way to escape single quotes are by doubling them rather than using backslash.

def test_it_escapes_match_ids_escaped_single_quotes_preventing_bypassing_matches():
    resp = make_query({
        "Database": "amazonreviews",
        "Table": "amazon_reviews_parquet",
        "Columns": [{"Column": "customer_id", "MatchIds": [
          "\\",
          ")) --",
          "1234"
        ]}]
    })

    assert "SELECT \"$path\" " \
           "FROM \"amazonreviews\".\"amazon_reviews_parquet\" " \
           "WHERE (\"customer_id\" in ('\\', ')) --', '1234'))" == stringify(resp)

# This query without the Python escaping looks like:
#
# SELECT "$path" FROM "amazonreviews"."amazon_reviews_parquet"
# WHERE ("customer_id" in ('\', ')) --', '1234'))
#
# __________________________^ this is correctly handled by Athena.

# ============================================================================= #

def test_it_escapes_match_ids_escaped_single_quotes_preventing_bypassing_matches():
    resp = make_query({
        "Database": "amazonreviews",
        "Table": "amazon_reviews_parquet",
        "Columns": [{"Column": "customer_id", "MatchIds": [
          "\n--",
          "1234",
          "\n"
        ]}]
    })
    print(resp)
    assert "SELECT \"$path\" " \
           "FROM \"amazonreviews\".\"amazon_reviews_parquet\" " \
           "WHERE (\"customer_id\" in ('\\', ')) --', '1234'))" == stringify(resp)