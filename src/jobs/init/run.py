from jobs.input.input_processing import process_input_file


def analyze(spark, logger, **job_args):
    client_name = job_args["client_name"]
    environment = job_args["environment"]

    aws_region = 'us-east-1'

    logger.info("aida_insights: USING THE FOLLOWING JOB ARGUMENTS")
    logger.info("aida_insights: CLIENT NAME: " + client_name)
    logger.info("aida_insights: ENVIRONMENT: " + environment)

    # read the input file into a dataframe
    logger.info("aida_insights: READING INPUT FILE")
    input_data_frame = process_input_file(spark, logger, client_name, environment, aws_region)

    # logger.info("#### CLASSIFYING FILE INPUTS #####")
    # classification_dataframe = classify(spark, input_dataframe, classification_file)
    # classification_dataframe.show(15, False)

    # logger.info("#### SCORING RESULTS ####")
    # output_csv = score(classification_dataframe)
    # output_csv.show(15, False)

    input_data_frame.show(25, False)

    # logger.info("#### WRITING OUTPUT FILE ####")
    # Write values
    # output_csv \
    #     .coalesce(1) \
    #     .write \
    #     .csv(path=output_dir, mode="overwrite", header="True")

    logger.info("aida_insights: STOPPING APPLICATION")
    spark.stop()
