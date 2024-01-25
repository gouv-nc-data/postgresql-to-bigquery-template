from postgresql_to_bigquery import query_factory
import pytest


def test_wit_schema_should_pass():
    schema = "test"
    stmt = query_factory(schema)
    assert stmt == "SELECT table_name FROM information_schema.tables where table_schema = '%s'" % schema

