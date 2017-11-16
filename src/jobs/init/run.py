import datetime

from jobs.classification.classify import classify, get_classification_subcategory_df, \
    apply_event_lookback_to_classified_leads
from jobs.consumer_insights.consumer_insights_processing import retrieve_leads_from_consumer_graph
from jobs.init.config import get_application_config_df
from jobs.input.input_processing import process_input_file
from jobs.output.output_processing import write_output, build_output_csv_folder_name
from jobs.pii_hashing.pii_hashing import transform_raw_inputs
from jobs.scoring.scoring import score_file


def analyze(spark, logger, **job_args):  # pylint:disable=too-many-statements,too-many-locals
    """
    Takes the spark context launched in main.py and runs the AIDA Insights application. The application will
    take an input file, get the canonical hash values for phones, emails, and devices, retrieve associated
    lead ids, classify those leads, attempt to score the leads, and finally write the result to a CSV location
    that is based on the client name and environment.
    :param spark: The spark context
    :param logger: The underlying JVM logger
    :param job_args: A Dict of job arguments, currently client_name and environment
    """
    client_name = job_args["client_name"]
    environment = job_args["environment"]

    time_stamp = datetime.datetime.utcnow()

    logger.info("STARTING UP APPLICATION")
    logger.info("USING THE FOLLOWING JOB ARGUMENTS")
    logger.info("CLIENT NAME: " + client_name)
    logger.info("ENVIRONMENT: " + environment)

    app_config_df = get_application_config_df(spark, environment, client_name, logger)

    logger.info("RAW INPUT FILE START")
    raw_input_data_frame = process_input_file(spark, logger, client_name, environment)
    logger.info("RAW INPUT FILE END")
    logger.info("INPUT_DATA_FRAME PARTITION SIZE: {size}".format(size=raw_input_data_frame.rdd.getNumPartitions()))

    logger.info("PII HASHING START")
    input_data_frame = transform_raw_inputs(spark, logger, raw_input_data_frame, environment)
    input_data_frame.cache()
    input_data_frame.collect()
    input_data_frame.show(50, False)
    logger.info("PII HASHING END")

    logger.info("CONSUMER INSIGHTS START")
    consumer_insights_df = retrieve_leads_from_consumer_graph(spark, environment, input_data_frame)
    consumer_insights_df.cache()
    consumer_insights_df.collect()
    consumer_insights_df.show(50, False)
    logger.info("CONSUMER INSIGHTS END")
    logger.info("CONSUMER_INSIGHTS_DF PARTITION SIZE: {size}".format(
        size=consumer_insights_df.rdd.getNumPartitions()))

    logger.info("CLASSIFICATION START")
    raw_classification_data_frame = classify(spark, logger, consumer_insights_df, environment)
    filtered_classification_df = apply_event_lookback_to_classified_leads(raw_classification_data_frame,
                                                                          app_config_df,
                                                                          time_stamp)

    logger.info("CLASSIFICATION_DATA_FRAME PARTITION SIZE: {size}".format(
        size=filtered_classification_df.rdd.getNumPartitions()))

    # repartition on record_id before we score all values
    logger.info("REPARTITIONING CLASSIFICATION RESULTS")
    classification_data_frame = filtered_classification_df.repartition("record_id")
    logger.info("REPARTITION OF CLASSIFICATION RESULTS DONE")
    classification_data_frame.cache()
    classification_data_frame.collect()
    classification_data_frame.show(50, False)
    logger.info("CLASSIFICATION END")

    logger.info("SCORING START")
    classify_subcategory_df = get_classification_subcategory_df(spark, environment, logger)
    scored_data_frame = score_file(classify_subcategory_df, classification_data_frame)
    logger.info(
        "CLASSIFY_SUBCATEGORY_DF PARTITION SIZE: {size}".format(size=classify_subcategory_df.rdd.getNumPartitions()))
    logger.info("SCORED_DATA_FRAME PARTITION SIZE: {size}".format(size=scored_data_frame.rdd.getNumPartitions()))

    # cache final Scores (need the show statements to ensure action is fired)
    scored_data_frame.cache()
    scored_data_frame.collect()
    scored_data_frame.show(50, False)
    logger.info("SCORING END")
    # end caching

    logger.info("WRITE OUTPUT START")
    output_path = build_output_csv_folder_name(environment, client_name, time_stamp)
    logger.info("WRITING OUTPUT FILE TO {path}".format(path=output_path))
    write_output(output_path, classify_subcategory_df, scored_data_frame)
    logger.info("WRITE OUTPUT END")
