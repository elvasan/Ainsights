from datetime import datetime

import pytest
from pyspark.sql.types import StructField, StructType, StringType

from jobs.init import config
from src.shared.constants import Environments, ConfigurationSchema, ClassificationSubcategory, Schemas, \
    ConfigurationOptions, Test
from tests.helpers import extract_rows_for_col

# define mark (need followup if need this)
spark_session_enabled = pytest.mark.usefixtures("spark_session")


def test_get_application_defaults_location_returns_correct_local_schema():
    result = config.get_application_defaults_location(Environments.LOCAL)
    assert '../samples/pyspark/config/local/application_defaults.csv' == result


def test_get_application_defaults_location_returns_correct_dev_schema():
    result = config.get_application_defaults_location(Environments.DEV)
    assert 's3://jornaya-dev-us-east-1-aida-insights/pyspark/config/dev/application_defaults.csv' == result


def test_get_application_defaults_location_returns_correct_qa_schema():
    result = config.get_application_defaults_location(Environments.QA)
    assert 's3://jornaya-qa-us-east-1-aida-insights/pyspark/config/qa/application_defaults.csv' == result


def test_get_application_defaults_location_returns_correct_staging_schema():
    result = config.get_application_defaults_location(Environments.STAGING)
    assert 's3://jornaya-staging-us-east-1-aida-insights/pyspark/config/staging/application_defaults.csv' == result


def test_get_application_defaults_location_returns_correct_prod_schema():
    result = config.get_application_defaults_location(Environments.PROD)
    assert 's3://jornaya-prod-us-east-1-aida-insights/pyspark/config/prod/application_defaults.csv' == result


def test_get_client_config_location_returns_correct_local_schema():
    result = config.get_client_config_location(Environments.LOCAL, Test.CLIENT_NAME, Test.JOB_RUN_ID)
    assert '../samples/app_data/beestest/beestest_2018_01_02/input/client_config.csv' == result


def test_get_client_config_location_returns_correct_dev_schema():
    result = config.get_client_config_location(Environments.DEV, Test.CLIENT_NAME, Test.JOB_RUN_ID)
    assert 's3://jornaya-dev-us-east-1-aida-insights/app_data/beestest/beestest_2018_01_02/input/client_config.csv' == result


def test_get_client_config_location_returns_correct_qa_schema():
    result = config.get_client_config_location(Environments.QA, Test.CLIENT_NAME, Test.JOB_RUN_ID)
    assert 's3://jornaya-qa-us-east-1-aida-insights/app_data/beestest/beestest_2018_01_02/input/client_config.csv' == result


def test_get_client_config_location_returns_correct_staging_schema():
    result = config.get_client_config_location(Environments.STAGING, Test.CLIENT_NAME, Test.JOB_RUN_ID)
    assert 's3://jornaya-staging-us-east-1-aida-insights/app_data/beestest/beestest_2018_01_02/input/client_config.csv' == result


def test_get_client_config_location_returns_correct_prod_schema():
    result = config.get_client_config_location(Environments.PROD, Test.CLIENT_NAME, Test.JOB_RUN_ID)
    assert 's3://jornaya-prod-us-east-1-aida-insights/app_data/beestest/beestest_2018_01_02/input/client_config.csv' == result


def test_merge_client_config_with_app_config_returns_correct_config_when_client_config_is_empty(spark_session):
    default_rows = [("event_lookback", "auto_sales", 30),
                    ("event_lookback", "financial_services", 15),
                    ("frequency_threshold", "auto_sales", 5),
                    ("frequency_threshold", "financial_services", 7)]
    app_default_df = spark_session.createDataFrame(default_rows, config.configuration_schema())

    client_config_rows = []
    client_config_df = spark_session.createDataFrame(client_config_rows, config.configuration_schema())

    result_df = config.merge_client_config_with_app_config(client_config_df, app_default_df)
    expected_columns = [ConfigurationSchema.OPTION, ConfigurationSchema.CONFIG_ABBREV, ConfigurationSchema.VALUE]
    assert sorted(result_df.columns) == sorted(expected_columns)
    assert result_df.count() == 4

    extracted_values = extract_rows_for_col(result_df, ConfigurationSchema.VALUE)
    expected_values = ["30", "15", "5", "7"]
    assert sorted(expected_values) == sorted(extracted_values)


def test_merge_client_config_with_app_config_returns_correct_config_when_client_config_is_not_empty(spark_session):
    default_rows = [("event_lookback", "auto_sales", 30),
                    ("event_lookback", "financial_services", 15),
                    ("frequency_threshold", "auto_sales", 5),
                    ("frequency_threshold", "financial_services", 7)]
    app_default_df = spark_session.createDataFrame(default_rows, config.configuration_schema())

    client_config_rows = [("event_lookback", "auto_sales", 88),
                     ("event_lookback", "financial_services", 77),
                     ("frequency_threshold", "auto_sales", 66),
                     ("industry_results", "auto_sales", "auto_sales"),
                     ("asof", "asof", "2017-11-17 12:00:00.0")]
    client_config_df = spark_session.createDataFrame(client_config_rows, config.configuration_schema())

    result_df = config.merge_client_config_with_app_config(client_config_df, app_default_df)
    expected_columns = [ConfigurationSchema.OPTION, ConfigurationSchema.CONFIG_ABBREV, ConfigurationSchema.VALUE]
    assert sorted(result_df.columns) == sorted(expected_columns)
    assert result_df.count() == 6

    extracted_values = extract_rows_for_col(result_df, ConfigurationSchema.VALUE)
    expected_values = ["auto_sales", "88", "77", "66", "7", "2017-11-17 12:00:00.0"]
    assert sorted(expected_values) == sorted(extracted_values)


def test_get_subcategory_key_for_config_abbreviation(spark_session):
    config_rows = [("event_lookback", "auto_sales", 30),
                   ("event_lookback", "financial_services", 20),
                   ("event_lookback", "jobs", 20),
                   ("frequency_threshold", "auto_sales", 10),
                   ("frequency_threshold", "legal", 10),
                   ("asof", "asof", "2017-11-17 12:00:00.0")]
    config_df = spark_session.createDataFrame(config_rows, config.configuration_schema())

    subcategory_rows = [(11, "auto_sales"),
                        (22, "financial_services"),
                        (33, "jobs"),
                        (44, "legal")]
    subcategory_df = spark_session.createDataFrame(subcategory_rows, classif_subcategory_schema())

    result_df = config.get_subcategory_key_for_config_abbreviation(config_df, subcategory_df)
    assert result_df.count() == 6

    extracted_keys = extract_rows_for_col(result_df, ClassificationSubcategory.CLASSIF_SUBCATEGORY_KEY)
    expected_keys = ["44", None, "22", "33", "11", "11"]

    for i in extracted_keys:
        assert i in expected_keys


def test_get_as_of_timestamp_returns_current_utc_datetime_when_no_config_value_present(spark_session):
    config_rows = [("event_lookback", "auto_sales", 30),
                   ("frequency_threshold", "auto_sales", 4),
                   ("industry_result", "auto_sales", "auto_sales")]
    config_df = spark_session.createDataFrame(config_rows, config.configuration_schema())

    now = datetime.utcnow()
    result = config.get_as_of_timestamp(config_df, now)

    assert isinstance(result, datetime)
    assert result == now


def test_get_as_of_timestamp_returns_iso_formatted_datetime(spark_session):
    config_row = [("asof", "asof", "10/16/17 12:00")]
    config_df = spark_session.createDataFrame(config_row, config.configuration_schema())

    now = datetime.utcnow()
    result = config.get_as_of_timestamp(config_df, now)

    assert result != now
    assert isinstance(result, datetime)
    assert str(result) == "2017-10-16 12:00:00"


def test_get_as_of_timestamp_accepts_two_digit_years(spark_session):
    config_row = [("asof", "asof", "1/2/17 8:00")]
    config_df = spark_session.createDataFrame(config_row, config.configuration_schema())

    now = datetime.utcnow()
    result = config.get_as_of_timestamp(config_df, now)

    assert result != now
    assert isinstance(result, datetime)
    assert str(result) == "2017-01-02 08:00:00"


def test_get_schema_location_dict_returns_only_schema_locations(spark_session):
    config_row = [("dummy_value", "some_value", "some_location"),
                  (ConfigurationOptions.SCHEMA_LOCATION, Schemas.HASH_MAPPING, "../samples/hash_mapping")]
    config_df = spark_session.createDataFrame(config_row, config.configuration_schema())
    result = config.get_schema_location_dict(config_df)

    assert len(result) == 1
    assert result[Schemas.HASH_MAPPING] == "../samples/hash_mapping"


def test_get_schema_location_dict_returns_all_locations_from_config(spark_session):
    config_row = [
        (ConfigurationOptions.SCHEMA_LOCATION, Schemas.CLASSIF_LEAD, "../samples/classification/classif_lead"),
        (ConfigurationOptions.SCHEMA_LOCATION, Schemas.CONSUMER_VIEW, "../samples/cis/consumer_view"),
        (ConfigurationOptions.SCHEMA_LOCATION, Schemas.LEAD_EVENT, "../samples/cis/lead_event"),
        (ConfigurationOptions.SCHEMA_LOCATION, Schemas.HASH_MAPPING, "../samples/hash_mapping")
    ]
    config_df = spark_session.createDataFrame(config_row, config.configuration_schema())
    result = config.get_schema_location_dict(config_df)

    assert len(result) == 4
    assert result[Schemas.CLASSIF_LEAD] == "../samples/classification/classif_lead"
    assert result[Schemas.CONSUMER_VIEW] == "../samples/cis/consumer_view"
    assert result[Schemas.LEAD_EVENT] == "../samples/cis/lead_event"
    assert result[Schemas.HASH_MAPPING] == "../samples/hash_mapping"


def test_get_as_of_timestamp_accepts_four_digit_years(spark_session):
    config_row = [("asof", "asof", "1/2/2017 4:30")]
    config_df = spark_session.createDataFrame(config_row, config.configuration_schema())

    now = datetime.utcnow()
    result = config.get_as_of_timestamp(config_df, now)

    assert result != now
    assert isinstance(result, datetime)
    assert str(result) == "2017-01-02 04:30:00"


def test_get_as_of_timestamp_returns_value_error_when_invalid_date_format(spark_session):
    config_row = [("asof", "asof", "1/2/7 4:30")]
    config_df = spark_session.createDataFrame(config_row, config.configuration_schema())

    now = datetime.utcnow()
    with pytest.raises(ValueError):
        config.get_as_of_timestamp(config_df, now)


def classif_subcategory_schema():
    return StructType(
        [StructField(ClassificationSubcategory.CLASSIF_SUBCATEGORY_KEY, StringType()),
         StructField(ClassificationSubcategory.SUBCATEGORY_CD, StringType())])
