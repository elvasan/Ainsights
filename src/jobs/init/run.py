import datetime

from jobs.classification.classify import classify, get_classification_subcategory_df
from jobs.input.input_processing import process_input_file
from jobs.output.output_processing import write_output, build_output_csv_folder_name
from jobs.scoring.scoring import score_file


def analyze(spark, logger, **job_args):
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

    logger.info("READING INPUT FILE")
    input_data_frame1 = process_input_file(spark, logger, client_name, environment)
    logger.info("INPUT_DATA_FRAME PARTITION SIZE: {size}".format(size=input_data_frame1.rdd.getNumPartitions()))

    # HACK!!! temp hack to add input_id so rest of processing works this will be replaced when we do
    # pii_hashing work
    input_data_frame = input_data_frame1.withColumn("input_id", input_data_frame1.input_id_raw) \
        .select("record_id", "input_id_raw", "input_id", "input_id_type")
    # END HACK

    # Uncomment me when not in production
    input_data_frame.show(30, False)

    logger.info("CLASSIFYING FILE INPUTS")
    classification_data_frame = classify(spark, logger, input_data_frame, environment)
    logger.info("CLASSIFICATION_DATA_FRAME PARTITION SIZE: {size}".format(
        size=classification_data_frame.rdd.getNumPartitions()))

    logger.info("SCORING RESULTS")
    classify_subcategory_df = get_classification_subcategory_df(spark, environment, logger)
    scored_data_frame = score_file(classify_subcategory_df, classification_data_frame)
    logger.info(
        "CLASSIFY_SUBCATEGORY_DF PARTITION SIZE: {size}".format(size=classify_subcategory_df.rdd.getNumPartitions()))
    logger.info("SCORED_DATA_FRAME PARTITION SIZE: {size}".format(size=scored_data_frame.rdd.getNumPartitions()))

    output_path = build_output_csv_folder_name(environment, client_name, time_stamp)
    logger.info("WRITING OUTPUT FILE TO {path}".format(path=output_path))
    write_output(output_path, classify_subcategory_df, scored_data_frame)
