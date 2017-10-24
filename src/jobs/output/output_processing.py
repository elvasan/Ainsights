from pyspark.sql.functions import col

from shared.utilities import GenericColumnNames, Environments, ClassificationColumnNames, InputColumnNames, \
    OutputFileNames


def transform_output_scoring_columns(classification_category_df, raw_results_df):
    # get column rows as dictionary { 'auto_sales': 'Auto Sales', 'education': 'Education' }
    class_dict = get_classifications_as_dictionary(classification_category_df)
    # transform all columns using dictionary (if not there just existing value)
    results_df = raw_results_df.select([col(c).alias(class_dict.get(c, c)) for c in raw_results_df.columns])
    return results_df


def get_classifications_as_dictionary(classification_category_df):
    # pull out arrays of [ (subcategory, display), (subcategory, display) ]
    row_list = [[i.subcategory_nm, i.display_nm] for i in
                classification_category_df.select(ClassificationColumnNames.SUBCATEGORY_NAME,
                                                  ClassificationColumnNames.DISPLAY_NAME).collect()]
    class_dict = dict(row_list)
    # add in record_id mapping
    class_dict[InputColumnNames.RECORD_ID] = GenericColumnNames.RECORD_ID
    return class_dict


def build_output_csv_folder_name(environment, client_name, time_stamp):
    # folder name should be environment, aws_region, client_name
    # S3://jornaya-dev-us-east-1-aida-insights/beestest/output/beestest_aidainsights_201710241320
    # ../samples/beestest/output/beestest_aidainsights_201710241320
    if environment == Environments.LOCAL:
        bucket_prefix = Environments.LOCAL_BUCKET_PREFIX
    else:
        bucket_prefix = 'S3://jornaya-{0}-{1}-aida-insights/'.format(environment, Environments.AWS_REGION)
    time_stamp_formatted = time_stamp.strftime(OutputFileNames.TIME_FORMAT)
    name = '{0}{1}/output/{2}_{3}_{4}'.format(bucket_prefix, client_name, client_name,
                                              OutputFileNames.PRODUCT_NAME,
                                              time_stamp_formatted)
    return name


def write_output(environment, client_name, time_stamp, classification_subcategory_df, scored_results_df, logger):
    results_df = transform_output_scoring_columns(classification_subcategory_df, scored_results_df)
    output_path = build_output_csv_folder_name(environment, client_name, time_stamp)
    logger.debug("output_processing: Writing to {path})".format(path=output_path))
    logger.debug(results_df.show(15, True))
    results_df \
        .coalesce(1) \
        .write \
        .csv(path=output_path, mode="overwrite", header="True")
