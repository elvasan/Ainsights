import pytest
from pyspark.sql.types import StructField, StructType, StringType

from jobs.init import config
from src.shared.constants import Environments, ConfigurationSchema, ClassificationSubcategory
from tests.helpers import extract_rows_for_col

CLIENT_NAME = 'beestest'

# define mark (need followup if need this)
spark_session_enabled = pytest.mark.usefixtures("spark_session")


def test_get_application_defaults_location_returns_correct_local_schema():
    result = config.get_application_defaults_location(Environments.LOCAL)
    assert '../samples/pyspark/config/application_defaults.csv' == result


def test_get_application_defaults_location_returns_correct_dev_schema():
    result = config.get_application_defaults_location(Environments.DEV)
    assert 's3://jornaya-dev-us-east-1-aida-insights/pyspark/config/application_defaults.csv' == result


def test_get_application_defaults_location_returns_correct_qa_schema():
    result = config.get_application_defaults_location(Environments.QA)
    assert 's3://jornaya-qa-us-east-1-aida-insights/pyspark/config/application_defaults.csv' == result


def test_get_application_defaults_location_returns_correct_staging_schema():
    result = config.get_application_defaults_location(Environments.STAGING)
    assert 's3://jornaya-staging-us-east-1-aida-insights/pyspark/config/application_defaults.csv' == result


def test_get_application_defaults_location_returns_correct_prod_schema():
    result = config.get_application_defaults_location(Environments.PROD)
    assert 's3://jornaya-prod-us-east-1-aida-insights/pyspark/config/application_defaults.csv' == result


def test_get_client_overrides_location_returns_correct_local_schema():
    result = config.get_client_overrides_location(Environments.LOCAL, CLIENT_NAME)
    assert '../samples/beestest/input/client_overrides.csv' == result


def test_get_client_overrides_location_returns_correct_dev_schema():
    result = config.get_client_overrides_location(Environments.DEV, CLIENT_NAME)
    assert 's3://jornaya-dev-us-east-1-aida-insights/beestest/input/client_overrides.csv' == result


def test_get_client_overrides_location_returns_correct_qa_schema():
    result = config.get_client_overrides_location(Environments.QA, CLIENT_NAME)
    assert 's3://jornaya-qa-us-east-1-aida-insights/beestest/input/client_overrides.csv' == result


def test_get_client_overrides_location_returns_correct_staging_schema():
    result = config.get_client_overrides_location(Environments.STAGING, CLIENT_NAME)
    assert 's3://jornaya-staging-us-east-1-aida-insights/beestest/input/client_overrides.csv' == result


def test_get_client_overrides_location_returns_correct_prod_schema():
    result = config.get_client_overrides_location(Environments.PROD, CLIENT_NAME)
    assert 's3://jornaya-prod-us-east-1-aida-insights/beestest/input/client_overrides.csv' == result


def test_merge_client_config_with_app_config_returns_correct_config_when_client_override_is_empty(spark_session):
    default_rows = [("event_lookback", "auto_sales", 30),
                    ("event_lookback", "financial_services", 15),
                    ("frequency_threshold", "auto_sales", 5),
                    ("frequency_threshold", "financial_services", 7)]
    app_default_df = spark_session.createDataFrame(default_rows, config.configuration_schema())

    override_rows = []
    client_override_df = spark_session.createDataFrame(override_rows, config.configuration_schema())

    result_df = config.merge_client_config_with_app_config(client_override_df, app_default_df)
    expected_columns = [ConfigurationSchema.OPTION, ConfigurationSchema.CONFIG_ABBREV, ConfigurationSchema.VALUE]
    assert sorted(result_df.columns) == sorted(expected_columns)
    assert result_df.count() == 4

    extracted_values = extract_rows_for_col(result_df, ConfigurationSchema.VALUE)
    expected_values = ["30", "15", "5", "7"]
    assert sorted(expected_values) == sorted(extracted_values)


def test_merge_client_config_with_app_config_returns_correct_config_when_client_override_is_not_empty(spark_session):
    default_rows = [("event_lookback", "auto_sales", 30),
                    ("event_lookback", "financial_services", 15),
                    ("frequency_threshold", "auto_sales", 5),
                    ("frequency_threshold", "financial_services", 7)]
    app_default_df = spark_session.createDataFrame(default_rows, config.configuration_schema())

    override_rows = [("event_lookback", "auto_sales", 88),
                     ("event_lookback", "financial_services", 77),
                     ("frequency_threshold", "auto_sales", 66),
                     ("asof", "asof", "2017-11-17 12:00:00.0")]
    client_override_df = spark_session.createDataFrame(override_rows, config.configuration_schema())

    result_df = config.merge_client_config_with_app_config(client_override_df, app_default_df)
    expected_columns = [ConfigurationSchema.OPTION, ConfigurationSchema.CONFIG_ABBREV, ConfigurationSchema.VALUE]
    assert sorted(result_df.columns) == sorted(expected_columns)
    assert result_df.count() == 5

    extracted_values = extract_rows_for_col(result_df, ConfigurationSchema.VALUE)
    expected_values = ["88", "77", "66", "7", "2017-11-17 12:00:00.0"]
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


def classif_subcategory_schema():
    return StructType(
        [StructField(ClassificationSubcategory.CLASSIF_SUBCATEGORY_KEY, StringType()),
         StructField(ClassificationSubcategory.SUBCATEGORY_CD, StringType())])
