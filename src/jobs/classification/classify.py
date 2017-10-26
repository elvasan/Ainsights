from shared.utilities import Environments, ClassificationLead, ClassificationSetElementXref, \
    ClassificationSubcategory, InputColumnNames


def classify(spark_session, logger, input_df, environment):
    """
    Classify the leads that are provided in the input DataFrame.

    :param spark_session: Spark session created from our main file.
    :param logger: The underlying spark log4j logger
    :param input_df: A DataFrame consisting of leads that need to be classified
    :param environment: The current environment (local, dev, qa, prod)
    :return: A DataFrame that contains the leads from the input and their classifications
    """
    lead_df = get_classification_lead_df(spark_session, environment)
    logger.debug('Printing classif_lead DataFrame.')
    logger.debug(lead_df.show(15, True))

    classification_set_df = get_classification_set_elem_xref_df(spark_session, environment)
    logger.debug('Printing classif_set_elem_xref DataFrame.')
    logger.debug(classification_set_df.show(15, True))

    # Transform the input DataFrame by joining it to the classif_lead DataFrame
    input_lead_df = join_input_to_lead_df(input_df, lead_df)
    logger.debug('Printing input joined to classif_lead DataFrame.')
    logger.debug(input_lead_df.show(15, True))

    # Transform the input and lead DataFrame by joining it to the classif_set DataFrame
    input_lead_set_df = join_input_to_classification_set_df(input_lead_df, classification_set_df)
    logger.debug('Printing transformed input joined to classif_set DataFrame.')
    logger.debug(input_lead_set_df.show(15, True))

    return input_lead_set_df


def join_input_to_lead_df(input_df, lead_df):
    """
    Joins the input DataFrame received from pii hashing to the classification lead table based on lead id.
    :param input_df: A DataFrame consisting of leads that need to be classified
    :param lead_df: The classif_lead table as a DataFrame
    :return: A DataFrame consisting of the initial input DataFrame joined to the Classification Lead DataFrame minus
    timestamps.
    """
    join_expression = input_df.input_id == lead_df.token
    return input_df \
        .join(lead_df, join_expression, "left") \
        .drop(InputColumnNames.INPUT_ID_TYPE,
              ClassificationLead.TOKEN)


def join_input_to_classification_set_df(input_df, classification_set_df):
    """

    :param input_df: The original input DataFrame joined with the lead table that includes classif_set_key
        so we can join into the classif_set_element_xref table
    :param classification_set_df: The classif_set_element_xref table as a DataFrame
    :return: A DataFrame consisting of the transformed input DataFrame joined to the Classification Set DataFrame minus
    unnecessary columns.
    """
    join_expression = input_df.classif_set_key == classification_set_df.classif_set_key
    return input_df \
        .join(classification_set_df, join_expression, "left") \
        .drop(ClassificationSetElementXref.CLASSIF_SET_KEY,
              ClassificationSetElementXref.CLASSIF_ELEMENT_KEY,
              ClassificationSetElementXref.CLASSIF_CATEGORY_KEY,
              ClassificationSetElementXref.ELEMENT_CD,
              ClassificationSetElementXref.ELEMENT_DISPLAY_NM,
              ClassificationSetElementXref.SUBCATEGORY_CD,
              ClassificationSetElementXref.SUBCATEGORY_DISPLAY_NM,
              ClassificationSetElementXref.CATEGORY_CD,
              ClassificationSetElementXref.CATEGORY_DISPL_NM,
              ClassificationSetElementXref.CLASSIF_OWNER_NM,
              ClassificationSetElementXref.INSERT_TS,
              ClassificationSetElementXref.INSERT_JOB_RUN_ID,
              ClassificationSetElementXref.INSERT_BATCH_RUN_ID,
              ClassificationSetElementXref.LOAD_ACTION_IND,
              InputColumnNames.HAS_ERROR,
              InputColumnNames.ERROR_MESSAGE,
              InputColumnNames.AS_OF_TIME)


def get_classification_lead_df(spark_session, environment):
    """
    Returns a DataFrame consisting of the classification leads table.
    :param spark_session: Spark session created from our main file.
    :param environment: The current environment (local, dev, qa, prod)
    :return: The Classification Lead table as a DataFrame
    """
    schema_location = get_classification_schema_location(environment, ClassificationLead.SCHEMA_NAME)
    return load_parquet_into_df(spark_session, schema_location)


def get_classification_set_elem_xref_df(spark_session, environment):
    """
    Returns a DataFrame consisting of the classification set_element_xref table.
    :param spark_session: Spark session created from our main file.
    :param environment: The current environment (local, dev, qa, prod)
    :return: The Classification Set Element XREF table as a DataFrame
    """
    schema_location = get_classification_schema_location(environment, ClassificationSetElementXref.SCHEMA_NAME)
    return load_parquet_into_df(spark_session, schema_location)


def get_classification_subcategory_df(spark_session, environment):
    """
    Returns a DataFrame consisting of the classification subcategory table.
    :param spark_session: Spark session created from our main file.
    :param environment: The current environment (local, dev, qa, prod)
    :return: The Classification Subcategory table as a DataFrame
    """
    schema_location = get_classification_schema_location(environment, ClassificationSubcategory.SCHEMA_NAME)
    return load_parquet_into_df(spark_session, schema_location)


def get_classification_schema_location(environment, schema_name):
    """
    Builds an absolute path to a file for classifications

    :param environment: The current environment (local, dev, qa, prod)
    :param schema_name: The name of the table being retrieved
    :return: A string for locating classification parquet files.
    """
    if environment == Environments.LOCAL:
        bucket_prefix = Environments.LOCAL_BUCKET_PREFIX
    else:
        bucket_prefix = 's3://jornaya-{0}-{1}-prj/'.format(environment, Environments.AWS_REGION)
    return bucket_prefix + 'classification/' + schema_name + '/'


def load_parquet_into_df(spark_session, file_location):
    """
    Loads a parquet file into a DataFrame.
    :param spark_session: The spark context initialized in on the application start up
    :param file_location: The absolute path to the parquet file
    :return: A DataFrame from the parquet location
    """
    return spark_session.read.parquet(file_location)
