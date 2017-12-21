import pytest

from jobs.consumer_insights import publisher_permissions as pub
from shared.constants import Environments, PublisherPermissions, LeadEventSchema, InputColumnNames
from tests.helpers import extract_rows_for_col
from tests.jobs.consumer_insights import schema

spark_session_enabled = pytest.mark.usefixtures("spark_session")


def test_build_v_campaign_opt_in_state_schema_location_returns_correct_local_schema():
    result = pub.build_v_campaign_opt_in_state_schema_location(Environments.LOCAL)
    assert '../samples/publisher_permissions/v_campaign_opt_in_state' == result


def test_build_v_campaign_opt_in_state_schema_location_returns_correct_dev_schema():
    result = pub.build_v_campaign_opt_in_state_schema_location(Environments.DEV)
    assert 's3://jornaya-dev-us-east-1-prj/publisher_permissions/v_campaign_opt_in_state' == result


def test_build_v_campaign_opt_in_state_schema_location_returns_correct_qa_schema():
    result = pub.build_v_campaign_opt_in_state_schema_location(Environments.QA)
    assert 's3://jornaya-qa-us-east-1-prj/publisher_permissions/v_campaign_opt_in_state' == result


def test_build_v_campaign_opt_in_state_schema_location_returns_correct_staging_schema():
    result = pub.build_v_campaign_opt_in_state_schema_location(Environments.STAGING)
    assert 's3://jornaya-staging-us-east-1-prj/publisher_permissions/v_campaign_opt_in_state' == result


def test_build_v_campaign_opt_in_state_schema_location_returns_correct_prod_schema():
    result = pub.build_v_campaign_opt_in_state_schema_location(Environments.PROD)
    assert 's3://jornaya-prod-us-east-1-prj/publisher_permissions/v_campaign_opt_in_state' == result


def test_filter_campaign_opt_in_state_returns_only_campaign_keys_for_aida_insights(spark_session):
    # Create test data where all apps are opted in but we have a mix off application keys (1 & 2)
    view_data = [('CKAAA', Environments.AIDA_INSIGHTS_APP_CODE, PublisherPermissions.OPT_IN_VALUE, None, None),
                 ('CKBBB', Environments.AIDA_INSIGHTS_APP_CODE, PublisherPermissions.OPT_IN_VALUE, None, None),
                 ('CKDDD', 1, PublisherPermissions.OPT_IN_VALUE, None, None),
                 ('CKEEE', Environments.AIDA_INSIGHTS_APP_CODE, PublisherPermissions.OPT_IN_VALUE, None, None),
                 ('CKFFF', 2, PublisherPermissions.OPT_IN_VALUE, None, None)]
    campaign_opt_in = spark_session.createDataFrame(view_data, schema.v_campaign_opt_in_state_schema())
    result_df = pub.filter_campaign_opt_in_state(campaign_opt_in)
    result = extract_rows_for_col(result_df, PublisherPermissions.VIEW_CAMPAIGN_KEY)
    assert sorted(result) == ['CKAAA', 'CKBBB', 'CKEEE']


def test_filter_campaign_opt_in_state_returns_only_opted_in_campaign_keys(spark_session):
    # Create test data where all apps are opted in but we have a mix off application keys (1 & 2)
    view_data = [('CKAAA', Environments.AIDA_INSIGHTS_APP_CODE, PublisherPermissions.OPT_IN_VALUE, None, None),
                 ('CKBBB', Environments.AIDA_INSIGHTS_APP_CODE, PublisherPermissions.OPT_OUT_VALUE, None, None),
                 ('CKDDD', Environments.AIDA_INSIGHTS_APP_CODE, PublisherPermissions.OPT_IN_VALUE, None, None),
                 ('CKEEE', Environments.AIDA_INSIGHTS_APP_CODE, PublisherPermissions.OPT_OUT_VALUE, None, None),
                 ('CKFFF', Environments.AIDA_INSIGHTS_APP_CODE, PublisherPermissions.OPT_IN_VALUE, None, None)]
    campaign_opt_in = spark_session.createDataFrame(view_data, schema.v_campaign_opt_in_state_schema())
    result_df = pub.filter_campaign_opt_in_state(campaign_opt_in)
    result = extract_rows_for_col(result_df, PublisherPermissions.VIEW_CAMPAIGN_KEY)
    assert sorted(result) == ['CKAAA', 'CKDDD', 'CKFFF']


def test_apply_campaign_filter_returns_expected_dataframe_given_missing_campaign_keys(spark_session):
    view_data = [['CKAAA'],
                 ['CKDDD'],
                 ['CKFFF']]
    campaign_opt_in = spark_session.createDataFrame(view_data, schema.filtered_campaign_opt_in_view())

    consumer_view_data = [(1, 'LLAAA', '2017-12-01', 'CKAAA'),
                          (2, 'LLBBB', '2017-12-01', 'CKBBB'),
                          (3, 'LLDDD', '2017-12-01', 'CKDDD'),
                          (4, 'LLEEE', '2017-12-01', 'CKEEE'),
                          (5, 'LLFFF', '2017-12-01', 'CKFFF')]
    consumer_view = spark_session.createDataFrame(consumer_view_data, schema.expected_consumer_insights_result_schema())
    results_df = pub.apply_campaign_filter(consumer_view, campaign_opt_in)
    expected_result_data = [(1, 'LLAAA', '2017-12-01'),
                            (2, None, '2017-12-01'),
                            (3, 'LLDDD', '2017-12-01'),
                            (4, None, '2017-12-01'),
                            (5, 'LLFFF', '2017-12-01')]

    expected_result_df = spark_session.createDataFrame(expected_result_data,
                                                       schema.expected_publisher_permissions_result_schema())

    assert results_df.count() == 5
    assert sorted(results_df.columns) == [LeadEventSchema.CREATION_TS,
                                          InputColumnNames.INPUT_ID,
                                          InputColumnNames.RECORD_ID]
    assert 0 == results_df.subtract(expected_result_df).count()
