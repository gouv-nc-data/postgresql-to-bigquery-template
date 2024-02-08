from typing import Optional
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from apache_beam.io.jdbc import ReadFromJdbc

import logging
import typing

# class TableUploader(beam.DoFn):

#     def __init__(self, dataset):
#         self.dataset = dataset

#     def process(self, element):
#         from google.cloud import bigquery
#         import io

#         print("TableUpload.process(%s)" % element["table_name"])
#         dataset = self.dataset
#         client = bigquery.Client()  #.from_service_account_json("credentials.json") # Pour execution en local bigquery.Client
#         with io.BytesIO() as stream:
#             df = element["df"]
#             df.write_parquet(stream,
#                              use_pyarrow=True,
#                              pyarrow_options={"allow_truncated_timestamps": True,
#                                               "coerce_timestamps": "ms"})
#             stream.seek(0)
#             job = client.load_table_from_file(
#                 stream,
#                 destination='%s.%s' % (dataset, "%s" % element["table_name"]),
#                 job_config=bigquery.LoadJobConfig(
#                     source_format=bigquery.SourceFormat.PARQUET,
#                 ),
#             )
#         print(job.result())
#         yield "###### TableUpload.process(%s) terminé #####" % element["table_name"]


# class TableReader(beam.DoFn):
#     def __init__(self, uri):
#         self.uri = uri

#     def process(self, element):
#         import polars as pl
#         import logging

#         logging.info("traitement de la table %s" % element)
#         query = "select * from %s" % element[0]
#         df = pl.read_database(query, self.uri)
#         logging.info("contenu récupéré")
#         logging.info(df.head())

#         yield {"table_name": element, "df": df}
from apache_beam.typehints.schemas import LogicalType


@LogicalType.register_logical_type
class db_bool(LogicalType):
    def __init__(self, argument=""):
        pass

    @classmethod
    def urn(cls):
        return "beam:logical_type:javasdk_bit:v1"

    @classmethod
    def language_type(cls):
        return bool

    def to_language_type(self, value):
        return bool(value)

    def to_representation_type(self, value):
        return bool(value)


@LogicalType.register_logical_type
class db_tmp(LogicalType):
    def __init__(self, argument=""):
        pass

    @classmethod
    def urn(cls):
        return "beam:coder:pickled_python:v1"

    @classmethod
    def language_type(cls):
        return str

    def to_language_type(self, value):
        print(value)
        return str(value)

    def to_representation_type(self, value):
        print(value)
        return str(value)


def query_factory(schema: str, exclude: str = None) -> str:
    if exclude != "":
        query = "SELECT table_name FROM information_schema.tables where table_schema = '%s' and table_name not in (%s)" % (schema, exclude)
    else:
        query = "SELECT table_name FROM information_schema.tables where table_schema = '%s'" % schema
    print(query)
    return query


def get_tables_list(schema: str, uri: str, exclude: str = None) -> list:
    import polars as pl
    query = query_factory(schema, exclude)
    df = pl.read_database(query, uri)
    res = df['table_name'].to_list()
    print("tables à migrer: %s" % res)
    return res


def run(
        schema: str, url: str, dataset: str, mode: str, exclude: str, tables: list,
        beam_options: Optional[PipelineOptions] = None,
        ) -> None:
    creds = url.split("?user=")
    uri = creds[0]
    username, password = creds[1].split("&password=")
    
    with beam.Pipeline(options=beam_options) as pipeline:
        # tables = (pipeline | 'Create table list' >> ReadFromJdbc(
        #                             query=query,
        #                             driver_class_name='org.postgresql.Driver',
        #                             jdbc_url='jdbc:%s' % uri,
        #                             username=username,
        #                             password=password,
        #                             table_name=""
        #                         )
        #                     | beam.combiners.ToList()    
        #                    #| "Read jdbc tables" >> beam.ParDo(TableReader(url))
        #                    #| "Write to bigQuery" >> beam.ParDo(TableUploader(dataset))
        #           ) 
        res = [
            (
                pipeline | "List to pCollection %s" % table >> beam.Create([table])
                         | "debug %s " % table >> beam.Map(logging.info)
                         | 'ReadTable' >> ReadFromJdbc(
                                                        driver_class_name='org.postgresql.Driver',
                                                        jdbc_url='jdbc:%s' % uri,
                                                        username=username,
                                                        password=password,
                                                        table_name="%s" % table
                                                       )
                         | "res %s " % table >> beam.Map(logging.info)
            ) for table in tables
        ]
        #  ['country_language', 'country_flag', 'city', 'country']
        # (pipeline | ReadFromJdbc(
        #                             #query="select * from ",
        #                             driver_class_name='org.postgresql.Driver',
        #                             jdbc_url='jdbc:%s' % uri,
        #                             username=username,
        #                             password=password,
        #                             table_name="country_language"
        #                         )
        #           #| "res" >> beam.Map(logging.warn)
        #          )


if __name__ == "__main__":
    import argparse

    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
            '--jdbc-url',
            type=str,
            dest='jdbc_url',
            required=True,
            help='URL JDBC vers la bdd source')

    parser.add_argument(
        '--schema',
        type=str,
        dest='schema',
        required=True,
        help='schéma à migrer')

    parser.add_argument(
        '--dataset',
        type=str,
        dest='dataset',
        required=True,
        help='schéma à migrer')

    parser.add_argument(
        '--mode',
        type=str,
        dest='mode',
        required=False,
        default="overwrite",
        help='schéma à migrer')

    parser.add_argument(
        '--exclude',
        type=str,
        dest='exclude',
        required=False,
        default="",
        help='tables à exclure de la migration')
    args, beam_args = parser.parse_known_args()
    pipeline_options = PipelineOptions(beam_args, save_main_session=True,
                                       streaming=False, sdk_location="container")
    beam_options = pipeline_options.view_as(GoogleCloudOptions)
    #args = beam_options.view_as(MyOptions)
    tables = get_tables_list(args.schema, args.jdbc_url, args.exclude)
    run(
        schema=args.schema,
        url=args.jdbc_url,
        dataset=args.dataset,
        mode=args.mode,
        exclude=args.exclude,
        tables=tables,
        beam_options=beam_options,
    )
