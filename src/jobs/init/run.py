from jobs.classification.classify import classify
from jobs.input.input_processing import process_input_file


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

    # logger.info("#### SCORING RESULTS ####")
    # output_csv = score(spark, classification_data_frame, subcategory_data_frame)
    # output_csv.show(15, False)

    # logger.info("#### WRITING OUTPUT FILE ####")
    # Write values
    # output_csv \
    #     .coalesce(1) \
    #     .write \
    #     .csv(path=output_dir, mode="overwrite", header="True")

    logger.info(logger_prefix + "STOPPING APPLICATION")
    spark.stop()
