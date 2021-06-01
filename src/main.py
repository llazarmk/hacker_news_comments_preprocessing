import argparse
import logging
import apache_beam as beam

from queries.hacker_news_comments import get_hacker_news_comments_query

from schema.schema import COMMENTS_TEXT_PROCESSING_SCHEMA
from schema.schema_converter import BQSchema
from io_handler.big_query import write_to_bq, read_bq

from options.df_options import DataflowOptions, CustomOptions
from ptransforms.processor import HNCommentsProcessing


def run_text_processing_comments(pipeline, custom_options):
    feature_columns_text_analysis = {'id': 'comment_id',
                                     'feature': 'comment_text',
                                     'output_prefix': 'comment'
                                     }

    query = get_hacker_news_comments_query(custom_options.bq_input_table)

    source = read_bq(pcollection=pipeline, query=query)

    processed_data = (source
                      | HNCommentsProcessing(feature_columns=feature_columns_text_analysis, batch_size=200)
                      )
    logging.info(" output table is " + custom_options.bq_output_table)

    bq_schema = BQSchema(schema=COMMENTS_TEXT_PROCESSING_SCHEMA).get_schema()

    write_to_bq(pcollection_input=processed_data,
                table=custom_options.bq_output_table,
                schema=bq_schema,
                partition={'type': 'YEAR', 'field': 'comment_date'}
                )


def run(args):
    """Main entry point"""

    _temp_gcs = f'gs://{args.input_bucket}/temp'
    _stage_gcs = f'gs://{args.input_bucket}/staging'

    # ts = datetime.now()
    pipeline_options = DataflowOptions.get_options(
        environment=args.environment,
        project=args.project,
        job_name=args.job_name,
        staging_location=_stage_gcs,
        temp_location=_temp_gcs,

    )
    custom_options = pipeline_options.view_as(CustomOptions)

    pipeline = beam.Pipeline(options=pipeline_options)

    run_text_processing_comments(pipeline, custom_options=custom_options)

    result = pipeline.run()
    if args.environment == 'local':
        result.wait_until_finish()


def get_args(argv=None):
    """

    :param argv: environment variables
    :return:
    """
    parser = argparse.ArgumentParser()

    parser.add_argument('--input_bucket',
                        required=True,
                        help='cloud storage bucket for stage and temp folder for dataflow.')
    parser.add_argument('--project',
                        required=True,
                        help='google cloud project name')
    parser.add_argument('--environment',
                        help='environment variable to run the dataflow job [local,gcp]',
                        default='local')
    parser.add_argument('--job_name',
                        default='hacker-news-comments-processing',
                        help='job name')

    known_args, pipeline_args = parser.parse_known_args(argv)

    return known_args, pipeline_args


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    known_args, pipeline_args = get_args()
    run(args=known_args)
