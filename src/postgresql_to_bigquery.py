from typing import Optional
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.jdbc import ReadFromJdbc

import logging


class TableUploader(beam.DoFn):

    def __init__(self, dataset):
        self.dataset = dataset

    def process(self, element):
        from google.cloud import bigquery
        import io

        print("TableUpload.process(%s)" % element["table_name"])
        dataset = self.dataset
        client = bigquery.Client()  #.from_service_account_json("credentials.json") # Pour execution en local bigquery.Client
        with io.BytesIO() as stream:
            df = element["df"]
            df.write_parquet(stream,
                             use_pyarrow=True,
                             pyarrow_options={"allow_truncated_timestamps": True,
                                              "coerce_timestamps": "ms"})
            stream.seek(0)
            job = client.load_table_from_file(
                stream,
                destination='%s.%s' % (dataset, "%s" % element["table_name"]),
                job_config=bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.PARQUET,
                ),
            )
        print(job.result())
        yield "###### TableUpload.process(%s) terminé #####" % element["table_name"]


class TableReader(beam.DoFn):

    def __init__(self, uri):
        self.uri = uri

    def process(self, element):
        import polars as pl
        import logging

        logging.info("traitement de la table %s" % element)
        query = "select * from %s" % element[0]
        df = pl.read_database_uri(query=query, uri=self.uri)
        logging.info("contenu récupéré")
        logging.info(df.head())

        yield ""# {"table_name": element, "df": df}


def query_factory(schema: str, exclude: str = None) -> str:
    if exclude != "":
        query = "SELECT table_name FROM information_schema.tables where table_schema = '%s' and table_name not in (%s)" % (schema, exclude)
    else:
        query = "SELECT table_name FROM information_schema.tables where table_schema = '%s'" % schema
    print(query)
    return query


def run(
        schema: str, url: str, dataset: str, mode: str, exclude: str,
        beam_options: Optional[PipelineOptions] = None,
        ) -> None:
    creds = url.split("?user=")
    uri = creds[0]
    username, password = creds[1].split("&password=")
    query = query_factory(schema, exclude)
    print(query)
    with beam.Pipeline(options=beam_options) as pipeline:
        result = (pipeline | 'Create table list' >> ReadFromJdbc(
                                    query=query,
                                    driver_class_name='org.postgresql.Driver',
                                    jdbc_url='jdbc:%s' % uri,
                                    username=username,
                                    password=password,
                                    table_name=""
                                ) 
                           | "Read jdbc tables" >> beam.ParDo(TableReader(url))
                           #| "Write to bigQuery" >> beam.ParDo(TableUploader(dataset))
                  )

        print(result)


class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
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


if __name__ == "__main__":
    import argparse

    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    beam_options = PipelineOptions(save_main_session=True, streaming=True, sdk_location="container")
    args = beam_options.view_as(MyOptions)

    run(
        schema=args.schema,
        url=args.jdbc_url,
        dataset=args.dataset,
        mode=args.mode,
        exclude=args.exclude,
        beam_options=beam_options,
    )
