#!/usr/bin/python
import argparse
import os
import sys
import time

from pyspark.sql import SparkSession

from jobs import init

if os.path.exists('jobs.zip'):
    sys.path.insert(0, 'jobs.zip')
else:
    sys.path.insert(0, './jobs')


def main():
    parser = argparse.ArgumentParser(description='Run a PySpark job')
    parser.add_argument('--job-args',
                        nargs='*',
                        help="Extra arguments to send to the PySpark job (example: --job-args foo=bar)")

    args = parser.parse_args()

    job_args = dict()
    if args.job_args:
        job_args_tuples = [arg_str.split('=') for arg_str in args.job_args]
        job_args = {a[0]: a[1] for a in job_args_tuples}

    spark_session = SparkSession.builder.appName("aida-insights").getOrCreate()

    # this will log to the console but not to files.
    log4j_logger = spark_session._jvm.org.apache.log4j  # pylint:disable=protected-access
    logger = log4j_logger.LogManager.getLogger("aida-insights")

    start = time.time()
    init.analyze(spark_session, logger, **job_args)
    end = time.time()

    logger.info("\nExecution of AIDA-INSIGHTS for client %s took %s seconds" % (job_args["client_name"], end - start))


if __name__ == '__main__':
    main()
