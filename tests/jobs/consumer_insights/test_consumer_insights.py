import pytest

from src.jobs.consumer_insights import consumer_insights_processing as cis
from src.shared.constants import Environments, ConsumerViewSchema, PiiHashingColumnNames, LeadEventSchema, \
    IdentifierTypes as Type
from tests.jobs.consumer_insights import schema

spark_session_enabled = pytest.mark.usefixtures("spark_session")


def test_build_consumer_view_schema_location_returns_correct_local_schema():
    result = cis.build_consumer_view_schema_location(Environments.LOCAL)
    assert '../samples/cis/consumer_view' == result


def test_build_consumer_view_schema_location_returns_correct_dev_schema():
    result = cis.build_consumer_view_schema_location(Environments.DEV)
    assert 's3://jornaya-dev-us-east-1-prj/cis/consumer_view' == result


def test_build_consumer_view_schema_location_returns_correct_qa_schema():
    result = cis.build_consumer_view_schema_location(Environments.QA)
    assert 's3://jornaya-qa-us-east-1-prj/cis/consumer_view' == result


def test_build_consumer_view_schema_location_returns_correct_staging_schema():
    result = cis.build_consumer_view_schema_location(Environments.STAGING)
    assert 's3://jornaya-staging-us-east-1-prj/cis/consumer_view' == result


def test_build_consumer_view_schema_location_returns_correct_prod_schema():
    result = cis.build_consumer_view_schema_location(Environments.PROD)
    assert 's3://jornaya-prod-us-east-1-prj/cis/consumer_view' == result


def test_build_lead_event_schema_location_returns_correct_local_schema():
    result = cis.build_lead_event_schema_location(Environments.LOCAL)
    assert '../samples/cis/lead_event' == result


def test_build_lead_event_schema_location_returns_correct_dev_schema():
    result = cis.build_lead_event_schema_location(Environments.DEV)
    assert 's3://jornaya-dev-us-east-1-prj/cis/lead_event' == result


def test_build_lead_event_schema_location_returns_correct_qa_schema():
    result = cis.build_lead_event_schema_location(Environments.QA)
    assert 's3://jornaya-qa-us-east-1-prj/cis/lead_event' == result


def test_build_lead_event_schema_location_returns_correct_staging_schema():
    result = cis.build_lead_event_schema_location(Environments.STAGING)
    assert 's3://jornaya-staging-us-east-1-prj/cis/lead_event' == result


def test_build_lead_event_schema_location_returns_correct_prod_schema():
    result = cis.build_lead_event_schema_location(Environments.PROD)
    assert 's3://jornaya-prod-us-east-1-prj/cis/lead_event' == result


def test_join_pii_hashing_to_consumer_view_df_returns_expected_values_for_one_entry(spark_session):
    # Create PII Hashing DataFrame
    pii_hashing_data = [(100, 'PPAAA', 'CPAAA', Type.PHONE)]
    pii_hashing_df = spark_session.createDataFrame(pii_hashing_data, schema.expected_pii_hashing_schema())

    # Create consumer view DataFrame
    consumer_view_data = [(Type.PHONE, 'CPAAA', 7, '2017-01-11 22:28:34.000')]
    consumer_view_df = spark_session.createDataFrame(consumer_view_data, schema.consumer_view_schema())

    # Get the result of joining the pii hashing and consumer view DataFrames
    result_df = cis.join_pii_hashing_to_consumer_view_df(pii_hashing_df, consumer_view_df)
    expected_column_names = [PiiHashingColumnNames.RECORD_ID, ConsumerViewSchema.CLUSTER_ID]
    assert sorted(expected_column_names) == sorted(result_df.columns)

    # Transform the result into a Dict so we can assert based on key/value
    result_dict = result_df.collect()[0].asDict()

    assert result_dict['record_id'] == 100
    assert result_dict['cluster_id'] == 7


def test_join_pii_hashing_to_consumer_view_df_returns_one_cluster_value_per_record_for_multiple_entries(spark_session):
    # Create PII Hashing DataFrame
    pii_hashing_data = [
        (100, 'PPAAA', 'CPAAA', Type.PHONE),
        (100, 'EEAAA', 'CEAAA', Type.EMAIL),
        (200, 'LLBBB', 'LLBBB', Type.LEADID),
        (200, 'EEBBB', 'CEBBB', Type.EMAIL),
        (300, 'PPCCC', 'CPCCC', Type.PHONE)
    ]
    pii_hashing_df = spark_session.createDataFrame(pii_hashing_data, schema.expected_pii_hashing_schema())

    # Create consumer view DataFrame
    consumer_view_data = [
        (Type.PHONE, 'CPAAA', 7, '2017-01-11 22:28:34.000'),
        (Type.EMAIL, 'CEAAA', 7, '2017-01-11 22:28:34.000'),
        (Type.LEADID, 'LLBBB', 4, '2017-01-11 22:28:34.000'),
        (Type.EMAIL, 'CEBBB', 4, '2017-01-11 22:28:34.000')
    ]
    consumer_view_df = spark_session.createDataFrame(consumer_view_data, schema.consumer_view_schema())

    # Get the result of joining the pii hashing and consumer view DataFrames
    result_df = cis.join_pii_hashing_to_consumer_view_df(pii_hashing_df, consumer_view_df) \
        .orderBy(PiiHashingColumnNames.RECORD_ID)
    expected_column_names = [PiiHashingColumnNames.RECORD_ID, ConsumerViewSchema.CLUSTER_ID]
    assert sorted(expected_column_names) == sorted(result_df.columns)

    # Get the cluster id values and assert that there should only be one value for each record
    # and no value found for record id 300
    cluster_id_rows = extract_rows_for_col(result_df, ConsumerViewSchema.CLUSTER_ID)

    assert [7, 4, None] == cluster_id_rows


def test_get_leads_from_cluster_id_df_returns_expected_values_for_one_entry(spark_session):
    # Create a dummy consumer view DataFrame
    consumer_view_data = [(Type.LEADID, 'LLAAA', 7, '2017-01-11 22:28:34.000')]
    consumer_view_df = spark_session.createDataFrame(consumer_view_data, schema.consumer_view_schema())

    # Create a dummy cluster id DataFrame
    cluster_id_data = [(100, 7)]
    cluster_id_df = spark_session \
        .createDataFrame(cluster_id_data, schema.expected_pii_hashing_consumer_view_transformed_schema())

    result_df = cis.get_leads_from_cluster_id_df(consumer_view_df, cluster_id_df)
    expected_column_names = [PiiHashingColumnNames.RECORD_ID,
                             PiiHashingColumnNames.INPUT_ID,
                             LeadEventSchema.CREATION_TS]
    assert sorted(expected_column_names) == sorted(result_df.columns)

    result_dict = result_df.collect()[0].asDict()
    assert result_dict['record_id'] == 100
    assert result_dict['input_id'] == 'LLAAA'
    assert result_dict['creation_ts'] == '2017-01-11 22:28:34.000'


def test_get_leads_from_cluster_id_df_returns_expected_values_for_multiple_entries(spark_session):
    # Create a dummy consumer view DataFrame
    consumer_view_data = [
        (Type.LEADID, 'LLAAA', 7, '2017-01-11 22:28:34.000'),
        (Type.PHONE, 'CPAAA', 7, '2017-01-11 22:28:34.000'),
        (Type.EMAIL, 'CEAAA', 7, '2017-01-11 22:28:34.000'),
        (Type.LEADID, 'LLXXX', 7, '2017-01-11 22:28:34.000'),
        (Type.LEADID, 'LLBBB', 4, '2017-01-11 22:28:34.000'),
        (Type.EMAIL, 'CEBBB', 4, '2017-01-11 22:28:34.000'),
        (Type.LEADID, 'LLYYY', 4, '2017-01-11 22:28:34.000'),
        (Type.LEADID, 'LLZZZ', 4, '2017-01-11 22:28:34.000'),
        (Type.PHONE, 'CPCCC', 1, '2017-01-11 22:28:34.000'),
        (Type.LEADID, 'LLTTT', 1, '2017-01-11 22:28:34.000'),
        (Type.LEADID, 'LLUUU', 14, '2017-01-11 22:28:34.000'),
        (Type.LEADID, 'LLVVV', 14, '2017-01-11 22:28:34.000'),
        (Type.LEADID, 'LLWWW', 14, '2017-01-11 22:28:34.000'),
        (Type.LEADID, 'LLPPP', 20, '2017-01-11 22:28:34.000'),
    ]
    consumer_view_df = spark_session.createDataFrame(consumer_view_data, schema.consumer_view_schema())

    # Create a dummy cluster id DataFrame
    cluster_id_data = [
        (100, 7),
        (200, 4),
        (300, None),
        (400, 1),
        (500, 14)
    ]
    cluster_id_df = spark_session \
        .createDataFrame(cluster_id_data, schema.expected_pii_hashing_consumer_view_transformed_schema())

    # Get the results and assert on the column names and the number of expected rows.
    result_df = cis.get_leads_from_cluster_id_df(consumer_view_df, cluster_id_df)
    expected_column_names = [PiiHashingColumnNames.RECORD_ID,
                             PiiHashingColumnNames.INPUT_ID,
                             LeadEventSchema.CREATION_TS]
    assert sorted(expected_column_names) == sorted(result_df.columns)
    # 2 for record id 7, 3 for record id 4, 1 for record id 1, 3 for record id 14, and 1 row for the null value
    assert 10 == result_df.count()

    # Build a result DataFrame to check if the result is valid
    expected_result_data = [(100, 'LLXXX', '2017-01-11 22:28:34.000'),
                            (100, 'LLAAA', '2017-01-11 22:28:34.000'),
                            (200, 'LLYYY', '2017-01-11 22:28:34.000'),
                            (200, 'LLZZZ', '2017-01-11 22:28:34.000'),
                            (200, 'LLBBB', '2017-01-11 22:28:34.000'),
                            (300, None, None),
                            (400, 'LLTTT', '2017-01-11 22:28:34.000'),
                            (500, 'LLUUU', '2017-01-11 22:28:34.000'),
                            (500, 'LLVVV', '2017-01-11 22:28:34.000'),
                            (500, 'LLWWW', '2017-01-11 22:28:34.000'),
                            ]
    expected_result_df = spark_session.createDataFrame(expected_result_data,
                                                       schema.expected_consumer_insights_result_schema())
    assert 0 == result_df.subtract(expected_result_df).count()


def test_join_lead_ids_to_lead_event_returns_expected_values(spark_session):
    lead_id_data = [(1, 'LLAAA'), (1, 'LLBBB'), (2, 'LLCCC'), (3, 'LLDDD'), (4, None)]
    lead_id_df = spark_session.createDataFrame(lead_id_data, schema.lead_id_schema())
    lead_event_data = [('LLAAA', '2017-01-11 22:28:34.000'),
                       ('LLBBB', '2017-02-11 22:28:34.000'),
                       ('LLCCC', '2017-03-11 22:28:34.000'),
                       ('LLDDD', '2017-04-11 22:28:34.000')]
    lead_event_df = spark_session.createDataFrame(lead_event_data, schema.lead_event_schema())

    result_df = cis.join_lead_ids_to_lead_event(lead_id_df, lead_event_df)
    assert 5 == result_df.count()

    expected_result_data = [(1, 'LLAAA', '2017-01-11 22:28:34.000'),
                            (1, 'LLBBB', '2017-02-11 22:28:34.000'),
                            (2, 'LLCCC', '2017-03-11 22:28:34.000'),
                            (3, 'LLDDD', '2017-04-11 22:28:34.000'),
                            (4, None, None)
                            ]
    expected_result_df = spark_session.createDataFrame(expected_result_data,
                                                       schema.expected_consumer_insights_result_schema())

    assert 0 == result_df.subtract(expected_result_df).count()


# TODO: DRY
def extract_rows_for_col(data_frame, col_name):
    return [i[col_name] for i in data_frame.select(col_name).collect()]
