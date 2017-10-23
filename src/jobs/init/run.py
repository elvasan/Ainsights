from jobs.classification.classify import classify, get_classification_subcategory_df
from jobs.input.input_processing import process_input_file
from jobs.scoring.scoring import score_file


def analyze(spark, logger, **job_args):
    client_name = job_args["client_name"]
    environment = job_args["environment"]

    aws_region = 'us-east-1'
    logger_prefix = "AIDA_INSIGHTS: "

    logger.info(logger_prefix + "USING THE FOLLOWING JOB ARGUMENTS")
    logger.info(logger_prefix + "CLIENT NAME: " + client_name)
    logger.info(logger_prefix + "ENVIRONMENT: " + environment)

    # read the input file into a dataframe
    logger.info(logger_prefix + "READING INPUT FILE")
    input_data_frame = process_input_file(spark, logger, client_name, environment, aws_region)
    logger.debug(input_data_frame.show(25, False))

    logger.info(logger_prefix + "CLASSIFYING FILE INPUTS")
    classification_data_frame = classify(spark, logger, input_data_frame, environment, aws_region)
    logger.debug(classification_data_frame.show(15, True))

    logger.info(logger_prefix + "SCORING RESULTS")
    classify_subcategory_df = get_classification_subcategory_df(spark, environment, aws_region)
    scored_data_frame = score_file(classify_subcategory_df, classification_data_frame)
    logger.debug(scored_data_frame.show(15, True))

    # logger.info("#### WRITING OUTPUT FILE ####")
    # Write values
    # output_csv \
    #     .coalesce(1) \
    #     .write \
    #     .csv(path=output_dir, mode="overwrite", header="True")

    logger.info(logger_prefix + "STOPPING APPLICATION")
    spark.stop()
