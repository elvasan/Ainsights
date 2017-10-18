def analyze(spark, logger, **job_args):
    # input_file_location = job_args["input_file_name"]
    # classification_file = job_args["classification_file"]
    # output_dir = job_args["output_dir"]

    logger.info("#### USING THE FOLLOWING JOB ARGUMENTS ####")
    # logger.info("#### INPUT FILE LOCATION: " + input_file_location)
    # logger.info("#### OUTPUT FILE LOCATION: " + output_dir)
    # logger.info("#### CLASSIFICATIONS FILE: " + classification_file)

    # read in input_csv file and Classifications DF
    # logger.info("#### READING FILE INPUTS #####")
    # input_dataframe = input_file(spark, logger, input_file_location)
    # input_dataframe.show(15, False)

    # logger.info("#### CLASSIFYING FILE INPUTS #####")
    # classification_dataframe = classify(spark, input_dataframe, classification_file)
    # classification_dataframe.show(15, False)

    # logger.info("#### SCORING RESULTS ####")
    # output_csv = score(classification_dataframe)
    # output_csv.show(15, False)

    # logger.info("#### WRITING OUTPUT FILE ####")
    # Write values
    # output_csv \
    #     .coalesce(1) \
    #     .write \
    #     .csv(path=output_dir, mode="overwrite", header="True")

    logger.info("#### STOPPING APPLICATION ####")
    spark.stop()
