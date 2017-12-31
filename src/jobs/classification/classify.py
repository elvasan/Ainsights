from jobs.input.input_processing import load_parquet_into_df
from shared.constants import ClassificationLead, ClassificationSubcategory, InputColumnNames, ConfigurationOptions, \
    JoinTypes, Schemas


class TransformationColumns:  # pylint:disable=too-few-public-methods
    SUBCATEGORY_KEY = "subcategory_key"
    LOOKBACK_DATES = "lookback_dates"


def restrict_industry_by_config(internal_scored_df, app_config_df):
    """
    Removes industry columns which are not present in the configuration file.

    :param internal_scored_df: A DataFrame with internal scored results.
    :param app_config_df: A DataFrame containing the application configuration
    :return: A DataFrame with internal scored values with columns restricted to industry config values.
    """
    industry_results = app_config_df.filter(app_config_df.option == ConfigurationOptions.INDUSTRY_RESULT) \
        .select(app_config_df.value)

    industry_list = [result.value for result in industry_results.collect()]
    industry_list.insert(0, InputColumnNames.RECORD_ID)

    return internal_scored_df.select(*industry_list)


def classify(spark_session, input_df, schema_locations):
    """
    Classify the leads that are provided in the input DataFrame.

    :param spark_session: Spark session created from our main file.
    :param input_df: A DataFrame consisting of leads that need to be classified
    :param schema_locations: A dict of classification schema locations
    :return: A DataFrame that contains the leads from the input and their classifications
    """
    lead_df = load_parquet_into_df(spark_session, schema_locations[Schemas.CLASSIF_LEAD])
    lead_df.show()
    classification_set_df = get_classification_set_elem_xref_df(spark_session,
                                                                schema_locations[Schemas.CLASSIF_SET_ELEM_XREF])
    classification_set_df.show()

    # Transform the input DataFrame by joining it to the classif_lead DataFrame
    input_lead_df = join_input_to_lead_df(input_df, lead_df)

    # Transform the input and lead DataFrame by joining it to the classif_set DataFrame
    return join_input_to_classification_set_df(input_lead_df, classification_set_df)


def apply_event_lookback_to_classified_leads(classified_leads_df, app_config_df, as_of_time):
    """
    Applies an event lookback period based on industry subcategories to all of the classified leads. If a lead falls out
    of the event lookback window, we will null out the value to prevent processing when we move to the scoring module.
    :param classified_leads_df: A DataFrame of classified leads containing the record_id and classif_subcategory_key
    :param app_config_df: A DataFrame of app configuration. For this function we filter on event_lookback.
    :param as_of_time: The time the job should run as of, which is used to calculate the lookback date per industry.
    :return: A DataFrame consisting of the original classifed leads. Any value which falls out of the lookback window
    is nulled out.
    """
    # Example Classification Input:
    # record_id | classif_subcategory_key | creation_ts |
    # 100, 1, 2017-01-01 00:00:00
    # 100, 2, 2016-01-01 00:00:00

    # With an as_of_time of 2017-01-01 00:00:00 and a lookback of 3 months only the first record would return a value
    # and the second record would return null

    # Example Output:
    # record_id|classif_subcategory_key|
    # 100, 1
    # 100, null

    # filter out app_config_df by event_lookback values
    # convert our as_of_time to a unix timestamp for comparison
    # then get the lookback dates based on the value and current timestamp
    as_of_time = int(as_of_time.strftime("%s"))
    event_lookback_df = app_config_df.filter(app_config_df.option == ConfigurationOptions.EVENT_LOOKBACK) \
        .selectExpr('*',
                    'from_unixtime(CAST({as_of_time} - value * 86400 as BIGINT)) as lookback_dates'
                    .format(as_of_time=as_of_time)) \
        .select(ClassificationSubcategory.CLASSIF_SUBCATEGORY_KEY, TransformationColumns.LOOKBACK_DATES)

    # Rename the subcategory key column here so we dont have conflicts during our comparison
    classif_df = classified_leads_df.withColumnRenamed(ClassificationSubcategory.CLASSIF_SUBCATEGORY_KEY,
                                                       TransformationColumns.SUBCATEGORY_KEY)

    # Finally join in the two DataFrames based on subcategory key and if the created date is within the lookback window.
    join_expression = [classif_df.subcategory_key == event_lookback_df.classif_subcategory_key,
                       classif_df.creation_ts >= event_lookback_df.lookback_dates]
    return classif_df.join(event_lookback_df, join_expression, JoinTypes.LEFT_JOIN) \
        .select(InputColumnNames.RECORD_ID, ClassificationSubcategory.CLASSIF_SUBCATEGORY_KEY)


def join_input_to_lead_df(input_df, lead_df):
    """
    Joins the input DataFrame received from pii hashing to the classification lead table based on lead id.
    :param input_df: A DataFrame consisting of leads that need to be classified
    :param lead_df: The classif_lead table as a DataFrame
    :return: A DataFrame consisting of the initial input DataFrame joined to the Classification Lead DataFrame.
    """
    join_expression = input_df.input_id == lead_df.lead_id
    return input_df \
        .join(lead_df, join_expression, JoinTypes.LEFT_JOIN) \
        .drop(ClassificationLead.LEAD_ID)


def join_input_to_classification_set_df(input_df, classification_set_df):
    """

    :param input_df: The original input DataFrame joined with the lead table that includes classif_set_key
        so we can join into the classif_set_element_xref table
    :param classification_set_df: The classif_set_element_xref table as a DataFrame
    :return: A DataFrame consisting of the transformed input DataFrame joined to the Classification Set DataFrame minus
    unnecessary columns.
    """
    modified_input = input_df.drop(InputColumnNames.INPUT_ID)
    join_expression = modified_input.classif_set_key == classification_set_df.classif_set_key
    return modified_input \
        .join(classification_set_df, join_expression, JoinTypes.LEFT_JOIN) \
        .drop(ClassificationLead.CLASSIF_SET_KEY)


def get_classification_set_elem_xref_df(spark_session, schema_location):
    """
    Returns a DataFrame consisting of the classification set_element_xref table.
    :param spark_session: Spark session created from our main file.
    :param schema_location: The location of the classif_set_elem_xref table
    :return: The Classification Set Element XREF table as a DataFrame
    """
    classification_set_elem_xref_df = load_parquet_into_df(spark_session, schema_location)
    return classification_set_elem_xref_df.select(classification_set_elem_xref_df.classif_set_key,
                                                  classification_set_elem_xref_df.classif_subcategory_key)


def get_classification_subcategory_df(spark_session, schema_location):
    """
    Returns a DataFrame consisting of the classification subcategory table.
    :param spark_session: Spark session created from our main file.
    :param schema_location: The location of the classif_subcategory table
    :return: The Classification Subcategory table as a DataFrame
    """
    classif_subcategory_df = load_parquet_into_df(spark_session, schema_location)
    return classif_subcategory_df.select(classif_subcategory_df.classif_subcategory_key,
                                         classif_subcategory_df.subcategory_cd,
                                         classif_subcategory_df.subcategory_display_nm)
