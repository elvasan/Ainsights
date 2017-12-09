from pyspark.sql.functions import col

from jobs.input.input_processing import load_parquet_into_df
from shared.constants import Environments, ConsumerViewSchema, PiiHashingColumnNames, IdentifierTypes, \
    LeadEventSchema, JoinTypes


def retrieve_leads_from_consumer_graph(spark, environment, pii_hashing_df):
    """
    Retrieves all of the leads that are associated with a record id.
    :param spark: The spark session
    :param environment: The current development environment (local, dev, qa, etc...)
    :param pii_hashing_df: The DataFrame received from the pii hashing module
    :return: A DataFrame consisting of a record id and associated input ids
    """
    consumer_view_schema_location = build_consumer_view_schema_location(environment)
    consumer_view_df = get_consumer_view_df(spark, consumer_view_schema_location)

    cluster_id_df = join_pii_hashing_to_consumer_view_df(pii_hashing_df, consumer_view_df)
    lead_id_df = get_leads_from_cluster_id_df(consumer_view_df, cluster_id_df)

    lead_event_location = build_lead_event_schema_location(environment)
    lead_event_df = get_lead_event_df(spark, lead_event_location)

    return join_lead_ids_to_lead_event(lead_id_df, lead_event_df)


def join_lead_ids_to_lead_event(lead_id_df, lead_event_df):
    """
    Joins the record_id and lead_id DataFrame with the lead_id and creation_ts DataFrame
    :param lead_id_df: A DataFrame consisting of record_id and lead_id
    :param lead_event_df: A DataFrame consisting of lead_id and creation_ts
    :return: A DataFrame consisting of record_id, lead_id, and creation_ts
    """
    return lead_id_df.join(lead_event_df, lead_id_df.input_id == lead_event_df.lead_id, JoinTypes.LEFT_JOIN) \
        .select(PiiHashingColumnNames.RECORD_ID, PiiHashingColumnNames.INPUT_ID, LeadEventSchema.CREATION_TS,
                LeadEventSchema.CAMPAIGN_KEY)


def get_leads_from_cluster_id_df(consumer_view_df, cluster_id_df):
    """
    Using the cluster_id retrieved in the cluster_id DataFrame we join in to the original consumer_view
    to get all of the lead ids associated with a record.
    :param consumer_view_df: The consumer view DataFrame
    :param cluster_id_df: A DataFrame consisting of a record_id and cluster_id
    :return: A DataFrame containing record_ids and associated input_ids (lead ids)
    """
    # Ran into a warning here where it couldn't distinguish between cluster_id in the join so I had to use aliases.
    cis_df = consumer_view_df.alias("cis_df")
    cluster_df = cluster_id_df.alias("cluster_df")
    filtered_cis_df = cis_df \
        .filter(col(ConsumerViewSchema.NODE_TYPE_CD) == IdentifierTypes.LEADID) \
        .drop(ConsumerViewSchema.NODE_TYPE_CD)

    join_condition = filtered_cis_df.cluster_id == cluster_df.cluster_id
    return cluster_df.join(filtered_cis_df, join_condition, JoinTypes.LEFT_JOIN) \
        .drop(ConsumerViewSchema.CLUSTER_ID) \
        .withColumnRenamed(ConsumerViewSchema.NODE_VALUE, PiiHashingColumnNames.INPUT_ID) \
        .distinct()


def join_pii_hashing_to_consumer_view_df(pii_hashing_df, consumer_view_df):
    """
    Joins the DataFrame received from the PII Hashing service with the consumer view DataFrame to get the
    cluster id that references the leads for each individual row.
    :param pii_hashing_df:
    :param consumer_view_df:
    :return: A DataFrame consisting of a record id associated to a cluster id.
    """
    filtered_pii_hashing_df = pii_hashing_df.drop(PiiHashingColumnNames.INPUT_ID_RAW)
    join_condition = [filtered_pii_hashing_df.input_id_type == consumer_view_df.node_type_cd,
                      filtered_pii_hashing_df.input_id == consumer_view_df.node_value]
    return filtered_pii_hashing_df.join(consumer_view_df, join_condition, JoinTypes.LEFT_JOIN) \
        .select(PiiHashingColumnNames.RECORD_ID, ConsumerViewSchema.CLUSTER_ID) \
        .distinct()


def get_consumer_view_df(spark, schema_location):
    """
    Retrieves the consumer view table as a DataFrame
    :param spark: The Spark Session
    :param schema_location: The absolute path to the location of the consumer view parquet files
    :return: A DataFrame consisting of the identifier type, the canonical value, and cluster id.
    """
    unfiltered_consumer_view_df = load_parquet_into_df(spark, schema_location)
    return unfiltered_consumer_view_df.select(ConsumerViewSchema.NODE_TYPE_CD,
                                              ConsumerViewSchema.NODE_VALUE,
                                              ConsumerViewSchema.CLUSTER_ID) \
        .filter(unfiltered_consumer_view_df.cluster_id != 0) \
        .filter(unfiltered_consumer_view_df.node_type_cd != "device_id")


def get_lead_event_df(spark, schema_location):
    """
    Retrieves the lead event table as a DataFrame
    :param spark: The Spark Session
    :param schema_location: The absolute path to the location of the lead event parquet files
    :return: A DataFrame consisting of a lead id and it's creation timestamp
    """
    lead_event_df = load_parquet_into_df(spark, schema_location)
    return lead_event_df.select(LeadEventSchema.LEAD_ID, LeadEventSchema.CAMPAIGN_KEY, LeadEventSchema.SERVER_GMT_TS) \
        .withColumnRenamed(LeadEventSchema.SERVER_GMT_TS, LeadEventSchema.CREATION_TS)


def build_consumer_view_schema_location(environment):
    """
    Builds an absolute path to the consumer insights schema

    :param environment: The current environment (local, dev, qa, prod)
    :return: A string for locating consumer insights parquet files.
    """
    if environment == Environments.LOCAL:
        bucket_prefix = Environments.LOCAL_BUCKET_PREFIX
    else:
        bucket_prefix = 's3://jornaya-{0}-{1}-prj/'.format(environment, Environments.AWS_REGION)

    # Temp workaround in DEV for CI Team to use alternate view
    if environment == Environments.DEV:
        return bucket_prefix + 'cis/consumer_view_papaya'
    return bucket_prefix + 'cis/consumer_view'


def build_lead_event_schema_location(environment):
    """
    Builds an absolute path to the lead event schema

    :param environment: The current environment (local, dev, qa, prod)
    :return: A string for locating lead event parquet files.
    """
    if environment == Environments.LOCAL:
        bucket_prefix = Environments.LOCAL_BUCKET_PREFIX
    else:
        bucket_prefix = 's3://jornaya-{0}-{1}-prj/'.format(environment, Environments.AWS_REGION)
    return bucket_prefix + 'cis/lead_event'
