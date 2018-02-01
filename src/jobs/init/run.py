import datetime

from jobs.classification.classify import classify, get_classification_subcategory_df, \
    apply_event_lookback_to_classified_leads, restrict_industry_by_config
from jobs.consumer_insights.consumer_insights_processing import retrieve_leads_from_consumer_graph
from jobs.consumer_insights.publisher_permissions import apply_publisher_permissions_to_lead_campaigns
from jobs.init.config import get_application_config_df, get_as_of_timestamp, get_schema_location_dict
from jobs.input.input_processing import process_input_file
from jobs.output.output_processing import write_output, transform_scoring_columns_for_output
from jobs.pii_hashing.pii_hashing import transform_raw_inputs
from jobs.scoring.scoring import score_file, apply_thresholds_to_scored_df
from shared.constants import OutputFileNames, Schemas
from shared.file_summary import summarize_output_df, summarize_input_df


def analyze(spark, logger, **job_args):  # pylint:disable=too-many-locals, too-many-statements
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
    job_run_id = job_args["job_run_id"]
    time_stamp = datetime.datetime.utcnow()

    logger.info("STARTING UP APPLICATION")
    logger.info("USING THE FOLLOWING JOB ARGUMENTS")
    logger.info("CLIENT NAME: " + client_name)
    logger.info("ENVIRONMENT: " + environment)
    logger.info("TIMESTAMP: " + str(time_stamp))
    logger.info("JOB RUN ID: " + job_run_id)

    logger.info("RAW INPUT FILE START")
    raw_input_data_frame = process_input_file(spark, logger, client_name, environment, job_run_id)
    input_summary_df = summarize_input_df(raw_input_data_frame)
    write_output(environment, client_name, job_run_id, input_summary_df, OutputFileNames.INPUT_SUMMARY)
    logger.info("RAW INPUT FILE END")

    app_config_df = get_application_config_df(spark, environment, client_name, job_run_id)
    as_of_timestamp = get_as_of_timestamp(app_config_df, time_stamp)
    schema_locations = get_schema_location_dict(app_config_df)

    logger.info("PII HASHING START")
    input_data_frame = transform_raw_inputs(spark, raw_input_data_frame, schema_locations[Schemas.HASH_MAPPING])
    input_data_frame.cache()
    input_data_frame.show(50, False)
    logger.info("PII HASHING END")

    logger.info("CONSUMER INSIGHTS START")
    consumer_insights_df = retrieve_leads_from_consumer_graph(spark,
                                                              schema_locations,
                                                              input_data_frame,
                                                              as_of_timestamp)
    consumer_insights_df.cache()
    consumer_insights_df.show(50, False)
    logger.info("CONSUMER INSIGHTS END")

    # Now that we have the campaign keys from the consumer view, apply publisher permissions to remove
    # any leads that are not allowed to participate in aida insights.
    cis_permissions_applied = apply_publisher_permissions_to_lead_campaigns(
        spark,
        schema_locations[Schemas.PUBLISHER_PERMISSIONS],
        consumer_insights_df
    )

    logger.info("CLASSIFICATION START")
    raw_classification_data_frame = classify(spark, cis_permissions_applied, schema_locations)
    filtered_classification_df = apply_event_lookback_to_classified_leads(raw_classification_data_frame,
                                                                          app_config_df,
                                                                          as_of_timestamp)

    # repartition on record_id before we score all values
    logger.info("REPARTITIONING CLASSIFICATION RESULTS")
    classification_data_frame = filtered_classification_df.repartition("record_id")
    logger.info("REPARTITION OF CLASSIFICATION RESULTS DONE")
    classification_data_frame.cache()
    classification_data_frame.show(50, False)
    logger.info("CLASSIFICATION END")

    logger.info("SCORING START")
    classify_subcategory_df = get_classification_subcategory_df(spark, schema_locations[Schemas.CLASSIF_SUBCATEGORY])
    internal_scored_df = score_file(classify_subcategory_df, classification_data_frame)

    # Once we have the scored data frame, get the columns the client is expecting
    # and apply the frequency thresholds to get the external customer file
    external_columns_df = restrict_industry_by_config(internal_scored_df, app_config_df)
    external_scored_df = apply_thresholds_to_scored_df(external_columns_df, app_config_df)

    # cache final Scores (need the show statements to ensure action is fired)
    internal_scored_df.cache()
    internal_scored_df.collect()
    internal_scored_df.show(50, False)

    external_scored_df.cache()
    external_scored_df.collect()
    external_scored_df.show(50, False)
    logger.info("SCORING END")
    # end caching

    logger.info("WRITE OUTPUT START")
    internal_output_df = transform_scoring_columns_for_output(classify_subcategory_df, internal_scored_df)
    external_output_df = transform_scoring_columns_for_output(classify_subcategory_df, external_scored_df)
    output_summary_df = summarize_output_df(external_output_df)

    write_output(environment, client_name, job_run_id, internal_output_df, OutputFileNames.INTERNAL)
    write_output(environment, client_name, job_run_id, external_output_df, OutputFileNames.EXTERNAL)
    write_output(environment, client_name, job_run_id, output_summary_df, OutputFileNames.OUTPUT_SUMMARY)
    logger.info("WRITE OUTPUT END")
