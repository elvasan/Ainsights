import datetime

import pytest

from jobs.classification import classify
from jobs.init import config
from shared.constants import ClassificationSetElementXref, ConfigurationOptions
from tests.helpers import extract_rows_for_col
from tests.jobs.classification import schema
from tests.jobs.scoring.test_scoring import flat_classified_results_schema, all_expected_subcategory_column_names

spark_session_enabled = pytest.mark.usefixtures("spark_session")


def test_join_input_to_lead_data_frame_returns_expected_columns(spark_session):
    input_row = [(1, 'LLAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA', "2014-08-01 04:41:52.500")]
    input_df = spark_session.createDataFrame(input_row, schema.expected_input_schema())

    lead_row = [('LLAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA', 5)]
    lead_dataframe = spark_session.createDataFrame(lead_row, schema.classification_lead_schema())

    result = classify.join_input_to_lead_df(input_df, lead_dataframe)

    assert sorted(schema.expected_input_lead_transformed_schema().names) == sorted(result.schema.names)


def test_single_join_input_to_lead_df_with_no_matching_lead_id_returns_null_for_classification_key(spark_session):
    input_set = [(1, 'LLAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA', "2014-08-01 04:41:52.500")]
    input_df = spark_session.createDataFrame(input_set, schema.expected_input_schema())

    lead_row = [('LLBBBBBB-BBBB-BBBB-BBBB-BBBBBBBBBBBB', 5)]
    lead_dataframe = spark_session.createDataFrame(lead_row, schema.classification_lead_schema())

    result = classify.join_input_to_lead_df(input_df, lead_dataframe)
    extracted_results = extract_rows_for_col(result, ClassificationSetElementXref.CLASSIF_SET_KEY)
    expected_col_results = [None]

    assert sorted(schema.expected_input_lead_transformed_schema().names) == sorted(result.schema.names)
    assert expected_col_results == extracted_results


def test_multiple_join_input_to_lead_df_with_no_matching_lead_id_returns_null_for_classification_key(spark_session):
    input_set = [(1, 'LLAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA', "2014-08-01 04:41:52.500"),
                 (2, 'LLBBBBBB-BBBB-BBBB-BBBB-BBBBBBBBBBBB', "2017-11-12 14:28:34.000")]
    input_df = spark_session.createDataFrame(input_set, schema.expected_input_schema())

    lead_row = [('LLAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA', '5')]
    lead_dataframe = spark_session.createDataFrame(lead_row, schema.classification_lead_schema())

    result = classify.join_input_to_lead_df(input_df, lead_dataframe)
    extracted_results = extract_rows_for_col(result, ClassificationSetElementXref.CLASSIF_SET_KEY)
    expected_col_results = [None, '5']

    assert sorted(schema.expected_input_lead_transformed_schema().names) == sorted(result.schema.names)
    assert expected_col_results == extracted_results


def test_join_input_to_classification_set_df_returns_expected_columns(spark_session):
    input_row = [(1, 'LLAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA', 5, "2017-11-12 14:28:34.000")]
    input_df = spark_session.createDataFrame(input_row, schema.expected_input_lead_transformed_schema())

    classification_row = [(5, 1)]
    classification_df = spark_session.createDataFrame(classification_row, schema.classification_set_elem_xref_schema())

    result = classify.join_input_to_classification_set_df(input_df, classification_df)

    assert sorted(schema.expected_classification_result_schema().names) == sorted(result.schema.names)


def test_join_input_to_classification_set_df_with_null_returns_expected_columns(spark_session):
    input_row = [(1, 'LLAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA', 5, "2017-11-12 14:28:34.000"),
                 (2, 'LLBBBBBB-BBBB-BBBB-BBBB-BBBBBBBBBBBB', None, "2017-11-12 14:28:34.000")]
    input_df = spark_session.createDataFrame(input_row, schema.expected_input_lead_transformed_schema())

    classification_row = [(5, 1)]
    classification_df = spark_session.createDataFrame(classification_row, schema.classification_set_elem_xref_schema())

    result = classify.join_input_to_classification_set_df(input_df, classification_df)
    extracted_results = extract_rows_for_col(result, ClassificationSetElementXref.CLASSIF_SUBCATEGORY_KEY)
    expected_col_results = [None, 1]

    assert sorted(schema.expected_classification_result_schema().names) == sorted(result.schema.names)
    assert extracted_results == expected_col_results


def test_apply_event_lookback_to_classified_leads_returns_null_values_for_events_outside_lookback_window(spark_session):
    time_stamp = datetime.datetime.strptime("2017-11-20 12:00:00.000", "%Y-%m-%d %H:%M:%S.%f")

    classification_data = [(1, "1", "2017-11-10 14:28:34.000"),
                           # The following should return null as it is 1 second before lookback window
                           (1, "2", "2017-11-10 11:59:59.000"),
                           # The following should return a value as it is equal to the lookback window
                           (1, "3", "2017-11-10 12:00:00.000"),
                           # The following should return a value as it is 1 second after the lookback window
                           (2, "1", "2017-11-10 12:00:01.000"),
                           (2, "2", "2017-11-09 12:00:00.000"),
                           (3, "1", "2017-11-06 14:28:34.000"),
                           (3, "3", "2017-11-12 14:28:34.000"),
                           (4, "5", "2017-11-12 14:28:34.000"),
                           (5, "4", "2017-10-09 14:28:34.000"),
                           (6, None, None)]
    classified_leads_df = spark_session.createDataFrame(classification_data,
                                                        schema.expected_classification_result_schema())

    config_data = [(ConfigurationOptions.EVENT_LOOKBACK, "auto_sales", "10", "1"),
                   (ConfigurationOptions.EVENT_LOOKBACK, "financial_services", "10", "2"),
                   (ConfigurationOptions.EVENT_LOOKBACK, "other", "10", "3"),
                   (ConfigurationOptions.EVENT_LOOKBACK, "senior_living", "10", "4"),
                   (ConfigurationOptions.EVENT_LOOKBACK, "jobs", "10", "5"),
                   (ConfigurationOptions.FREQUENCY_THRESHOLD, "auto_sales", "3", "1"),
                   (ConfigurationOptions.ASOF, "asof", "10", None)]
    app_config_df = spark_session.createDataFrame(config_data, schema.expected_transformed_configuration_schema())

    result_df = classify.apply_event_lookback_to_classified_leads(classified_leads_df, app_config_df, time_stamp)
    assert 10 == result_df.count()

    expected_result = [(1, '1'),
                       (1, None),
                       (1, '3'),
                       (2, '1'),
                       (2, None),
                       (3, None),
                       (3, '3'),
                       (4, '5'),
                       (5, None),
                       (6, None)]

    expected_df = spark_session.createDataFrame(expected_result, schema.expected_classif_lookback_result_schema())
    assert 0 == result_df.subtract(expected_df).count()


def test_restrict_industry_by_config_returns_only_record_id_when_no_industry_values_exist_in_config(spark_session):
    config_data = [(ConfigurationOptions.EVENT_LOOKBACK, "auto_sales", "10"),
                   (ConfigurationOptions.FREQUENCY_THRESHOLD, "auto_sales", "3"),
                   (ConfigurationOptions.ASOF, "asof", "10")]
    app_config_df = spark_session.createDataFrame(config_data, config.configuration_schema())

    scored_values = [(100, 0, 1, 4, 4, 0, 0, 5, 2, 0),
                     (101, 0, 1, 4, 4, 0, 0, 5, 2, 0)]
    scored_df = spark_session.createDataFrame(scored_values, flat_classified_results_schema())

    result = classify.restrict_industry_by_config(scored_df, app_config_df)

    assert result.columns == ['record_id']
    assert result.count() == 2


def test_restrict_industry_by_config_returns_all_columns_when_all_are_present_in_config(spark_session):
    config_data = [(ConfigurationOptions.INDUSTRY_RESULT, "auto_sales", "auto_sales"),
                   (ConfigurationOptions.INDUSTRY_RESULT, "education", "education"),
                   (ConfigurationOptions.INDUSTRY_RESULT, "insurance", "insurance"),
                   (ConfigurationOptions.INDUSTRY_RESULT, "financial_services", "financial_services"),
                   (ConfigurationOptions.INDUSTRY_RESULT, "real_estate", "real_estate"),
                   (ConfigurationOptions.INDUSTRY_RESULT, "jobs", "jobs"),
                   (ConfigurationOptions.INDUSTRY_RESULT, "legal", "legal"),
                   (ConfigurationOptions.INDUSTRY_RESULT, "home_services", "home_services"),
                   (ConfigurationOptions.INDUSTRY_RESULT, "other", "other")]
    app_config_df = spark_session.createDataFrame(config_data, config.configuration_schema())

    scored_values = [(100, 0, 1, 4, 4, 0, 0, 5, 2, 0),
                     (101, 0, 1, 4, 4, 0, 0, 5, 2, 0)]
    scored_df = spark_session.createDataFrame(scored_values, flat_classified_results_schema())

    result = classify.restrict_industry_by_config(scored_df, app_config_df)

    assert sorted(result.columns) == sorted(all_expected_subcategory_column_names())
    assert result.count() == 2


def test_restrict_industry_by_config_returns_only_columns_present_in_config(spark_session):
    config_data = [(ConfigurationOptions.INDUSTRY_RESULT, "insurance", "insurance"),
                   (ConfigurationOptions.INDUSTRY_RESULT, "financial_services", "financial_services"),
                   (ConfigurationOptions.INDUSTRY_RESULT, "real_estate", "real_estate"),
                   (ConfigurationOptions.INDUSTRY_RESULT, "home_services", "home_services")]
    app_config_df = spark_session.createDataFrame(config_data, config.configuration_schema())

    scored_values = [(100, 0, 1, 4, 4, 0, 0, 5, 2, 0),
                     (101, 0, 1, 4, 4, 0, 0, 5, 2, 0)]
    scored_df = spark_session.createDataFrame(scored_values, flat_classified_results_schema())

    result = classify.restrict_industry_by_config(scored_df, app_config_df)

    assert sorted(result.columns) == sorted([
        'record_id',
        'insurance',
        'financial_services',
        'real_estate',
        'home_services']
    )
    assert result.count() == 2
