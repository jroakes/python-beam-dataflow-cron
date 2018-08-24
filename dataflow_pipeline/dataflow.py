#!/usr/bin/env python
from __future__ import print_function, absolute_import
import argparse
import logging
import sys

import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.metrics.metric import Metrics
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions

import datetime as dt
from datetime import timedelta, date
import time
import re

# Change this to match your config file.  See config.py for template.
import config_adapt as config

logging.getLogger().setLevel(logging.INFO)

STAT_API_SCHEMA = [

    {
        'name' : 'Id',
        'type' : 'INTEGER',
        'mode' : 'REQUIRED'
    },
    {
        'name' : 'ProjectName',
        'type' : 'STRING',
        'mode' : 'REQUIRED'
    },
    {
        'name' : 'SiteUrl',
        'type' : 'STRING',
        'mode' : 'REQUIRED'
    },
    {
        'name' : 'Keyword',
        'type' : 'STRING',
        'mode' : 'REQUIRED'
    },
    {
        'name' : 'KeywordLocation',
        'type' : 'STRING',
        'mode' : 'NULLABLE'
    },
    {
        'name' : 'KeywordCategories',
        'type' : 'STRING',
        'mode' : 'NULLABLE'
    },
    {
        'name' : 'KeywordDevice',
        'type' : 'STRING',
        'mode' : 'NULLABLE'
    },
    {
        'name' : 'KeywordStats',
        'type' : 'RECORD',
        'mode' : 'NULLABLE',
        'fields': [
            {
                'name': 'AdvertiserCompetition',
                'type': 'FLOAT',
                'mode': 'NULLABLE'
            },
            {
                'name': 'GlobalSearchVolume',
                'type': 'INTEGER',
                'mode': 'NULLABLE'
            },
            {
                'name': 'TargetedSearchVolume',
                'type': 'INTEGER',
                'mode': 'NULLABLE'
            },
            {
                'name': 'CPC',
                'type': 'FLOAT',
                'mode': 'NULLABLE'
            }
        ]
    },
    {
        'name' : 'CreatedAt',
        'type' : 'DATETIME',
        'mode' : 'REQUIRED'
    },
    {
        'name' : 'Ranking',
        'type' : 'RECORD',
        'mode' : 'REQUIRED',
        'fields': [
            {
                'name': 'date',
                'type': 'DATETIME',
                'mode': 'REQUIRED'
            },
            {
                'name': 'type',
                'type': 'STRING',
                'mode': 'NULLABLE'
            },
            {
                'name' : 'Google',
                'type' : 'RECORD',
                'mode' : 'NULLABLE',
                'fields': [
                    {
                        'name': 'Rank',
                        'type': 'INTEGER',
                        'mode': 'NULLABLE'
                    },
                    {
                        'name': 'BaseRank',
                        'type': 'INTEGER',
                        'mode': 'NULLABLE'
                    },
                    {
                        'name': 'Url',
                        'type': 'STRING',
                        'mode': 'NULLABLE'
                    }


                ]
            }

        ]
    }
]



class StatAPI():
    def __init__(self, data={}):
        self.num_api_errors = Metrics.counter(self.__class__, 'num_api_errors')
        self.data = data

    def get_job(self):
        import requests
        endpoint = "https://{0}.getstat.com/api/v2/{1}/bulk/ranks".format(self.data.subdomain,self.data.api_key)
        logging.info("Endpoint: {}".format(str(endpoint)))
        params = dict(
                        date = self.data.date,
                        rank_type = 'highest',
                        crawled_keywords_only = 'true',
                        format = 'json'
                      )
        try:
            for i in range(0,3):
                res = requests.get(endpoint,params=params)
                if res.status_code == 200:
                    break
                time.sleep(3)
            logging.info("Reponse: {}".format(str(res.text)))
            json_data = res.json()
            if 'Response' in json_data:
                response = json_data['Response']
                if response['responsecode'] == "200":
                    return [response['Result']['Id']]
                else:
                    return []


        except Exception as e:
            # Log and count parse errors
            self.num_api_errors.inc()
            logging.error('Exception: {}'.format(e))
            logging.error('Extract error on "%s"', 'StatAPI')


class IterProjects(beam.DoFn):
    def __init__(self, data={}):
        super(IterProjects, self).__init__()
        self.num_api_errors = Metrics.counter(self.__class__, 'num_api_errors')
        self.data = data

    def process(self, id):
        logging.info("Processing id: {}".format(str(id)))

        if len(id) < 1:
            logging.error('No Id passed from job')
            yield []

        import requests
        endpoint = "https://{0}.getstat.com/bulk_reports/stream_report/{1}?key={2}".format( self.data.subdomain, str(id), self.data.api_key)

        try:
            json_data = requests.get(endpoint, stream=True).json()

            if 'Response' in json_data:
                response = json_data['Response']
                if response['responsecode'] == '200':
                    projects = response['Project']
                    if not isinstance(projects, list):
                        projects = [projects]
                    logging.info("Projects length: {}".format(str(len(projects))))
                    for project in projects:
                        yield project


        except Exception as e:
            # Log and count parse errors
            self.num_api_errors.inc()
            logging.error('Exception: {}'.format(e))
            logging.error('Extract error on "%s"', str(id))


class IterSites(beam.DoFn):
    def __init__(self):
        super(IterSites, self).__init__()
        self.num_parse_errors = Metrics.counter(self.__class__, 'num_parse_errors')

    def process(self, project):
        logging.info("Running project: {}".format(str(project['Name'])))
        try:
            if 'Site' in project:
                sites = project['Site']
                if not isinstance(sites, list):
                    sites = [sites]
                logging.info("Sites length: {}".format(str(len(sites))))
                for site in sites:
                    site['ProjectName'] = project['Name']
                    yield site

        except Exception as e:
            # Log and count parse errors
            self.num_parse_errors.inc()
            logging.error('Exception: {}'.format(e))
            logging.error('Parse error on "%s"', project)



class IterKeywords(beam.DoFn):
    def __init__(self):
        super(IterKeywords, self).__init__()
        self.num_parse_errors = Metrics.counter(self.__class__, 'num_parse_errors')

    def process(self, site):

        try:
            output = []
            if 'Keyword' in site:
                keywords = site['Keyword']
                if not isinstance(keywords, list):
                    keywords = [keywords]
                logging.info("Keywords length: {}".format(str(len(keywords))))
                for keyword in keywords:
                    keyword['ProjectName'] = site['ProjectName']
                    keyword['SiteUrl'] = site['Url']
                    yield keyword

        except Exception as e:
            # Log and count parse errors
            self.num_parse_errors.inc()
            logging.error('Exception: {}'.format(e))
            logging.error('Parse error on "%s"', site)


class ConvertBigQueryRowFn(beam.DoFn):
    def __init__(self, schema):
        super(ConvertBigQueryRowFn, self).__init__()
        self.schema = schema

    def process(self, keyword):
        #for keyword in keywords:
        yield self.convert(self.schema, keyword)

    def convert(self, schema, keyword):
        from datetime import datetime
        output = {}
        for field in schema:
            column = field['name']
            if field['type'] == 'DATETIME':
                # Convert YYYY-MM-DDTHH:MM:SS
                output[column] = datetime.strptime(keyword[column], '%Y-%m-%d').strftime('%Y-%m-%dT%X')
            elif field['type'] == 'RECORD':
                output[column] = self.convert(field['fields'], keyword[column])
            else:
                output[column] = keyword[column]

        return output

class WriteToBigQuery(beam.PTransform):

    """Generate, format, and write BigQuery table row information."""
    def __init__(self, table, dataset, schema):
        """Initializes the transform.
        Args:
          table_name: Name of the BigQuery table to use.
          dataset: Name of the dataset to use.
          schema: Dictionary in the format {'column_name': 'bigquery_type'}
        """
        super(WriteToBigQuery, self).__init__()
        self.table_name = table
        self.dataset = dataset
        self.schema = schema

    def get_bq_table_schema(self):
        table_schema = bigquery.TableSchema()
        for field in self.schema:
            field_schema = self.get_bq_field_schema(field)
            table_schema.fields.append(field_schema)
        return table_schema

    def get_bq_field_schema(self, field):
        field_schema = bigquery.TableFieldSchema()
        field_schema.name = field['name']
        field_schema.type = field['type']
        field_schema.mode = field['mode']
        if field['type'] == 'RECORD':
            for nested_field in field['fields']:
                nested_field_schema = self.get_bq_field_schema(nested_field)
                field_schema.fields.append(nested_field_schema)
        return field_schema

    def expand(self, pcoll):
        project = pcoll.pipeline.options.view_as(GoogleCloudOptions).project
        return (
            pcoll
            | 'ConvertBigQueryRowFn' >> beam.ParDo(ConvertBigQueryRowFn(self.schema))
            | beam.io.WriteToBigQuery(
                self.table_name, self.dataset, project, self.get_bq_table_schema()))



def run(argv=None):

    """Main entry point; defines and runs the hourly_team_score pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--subdomain',
        type=str,
        default=config.SUBDOMAIN,
        help='Sub-domain for Stat API.')
    parser.add_argument('--api_key',
        type=str,
        default=config.API_KEY,
        help='API key for Stat API.')
    parser.add_argument('--date',
        type=str,
        default=(dt.date.today()-dt.timedelta(days = config.PRIOR_DAYS)).strftime("%Y-%m-%d"),
        help='Run date in YYYY-MM-DD format.')
    parser.add_argument('--dataset',
        type=str,
        default=config.DATASET,
        help='BigQuery Dataset to write tables to. Must already exist.')
    parser.add_argument('--table_name',
        type=str,
        default=config.TABLE_NAME,
        help='The BigQuery table name. Should not already exist.')
    parser.add_argument('--project',
        type=str,
        default=config.PROJECT,
        help='Your GCS project.')
    parser.add_argument('--runner',
        type=str,
        default="DataflowRunner",
        help='Type of DataFlow runner.')

    args, pipeline_args = parser.parse_known_args(argv)

    # Create and set your PipelineOptions.
    options = PipelineOptions(pipeline_args)

    # For Cloud execution, set the Cloud Platform project, job_name,
    # staging location, temp_location and specify DataflowRunner.
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = args.project
    google_cloud_options.job_name = ("{0}-{1}".format(args.project, str(dt.datetime.today().strftime("%m%dt%H%M")) ))
    google_cloud_options.staging_location = "gs://{0}/binaries".format(args.project)
    google_cloud_options.temp_location = "gs://{0}/temp".format(args.project)
    options.view_as(StandardOptions).runner = args.runner



    with beam.Pipeline(options=options) as pipeline:
        # Read projects from Stat API
        api = (
            pipeline
            | 'create' >> beam.Create(StatAPI(data=args).get_job())
            | 'IterProjects'  >> beam.ParDo(IterProjects(data=args))
        )

        # Iteates Sites in Projects
        keywords = (
            api
            | 'IterSites' >> beam.ParDo(IterSites())
            | 'IterKeywords' >> beam.ParDo(IterKeywords())
        )

        # Write to bigquery based on specified schema
        BQ = (keywords | "WriteToBigQuery" >> WriteToBigQuery(args.table_name, args.dataset, STAT_API_SCHEMA))
