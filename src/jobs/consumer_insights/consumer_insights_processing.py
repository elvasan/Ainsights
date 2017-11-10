from pyspark.sql.functions import col

from jobs.input.input_processing import load_parquet_into_df
from shared.utilities import Environments, ConsumerViewSchema, PiiHashingColumnNames, IdentifierTypes


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
    return get_leads_from_cluster_id_df(consumer_view_df, cluster_id_df)


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
    return cluster_df.join(filtered_cis_df, join_condition, "left") \
        .drop(ConsumerViewSchema.CLUSTER_ID) \
        .withColumnRenamed(ConsumerViewSchema.VALUE, PiiHashingColumnNames.INPUT_ID) \
        .distinct()


def join_pii_hashing_to_consumer_view_df(pii_hashing_df, consumer_view_df):
    """
    Joins the DataFrame received from the PII Hashing service with the consumer view DataFrame to get the
    cluster id that references the leads for each individual row.
    :param pii_hashing_df:
    :param consumer_view_df:
    :return:
    """
    modified_pii_hashing_df = pii_hashing_df.drop(PiiHashingColumnNames.INPUT_ID_RAW)
    join_condition = [pii_hashing_df.input_id_type == consumer_view_df.node_type_cd,
                      pii_hashing_df.input_id == consumer_view_df.value]
    return modified_pii_hashing_df.join(consumer_view_df, join_condition, "left") \
        .select(PiiHashingColumnNames.RECORD_ID, ConsumerViewSchema.CLUSTER_ID) \
        .distinct()


def get_consumer_view_df(spark, schema_location):
    """
    Retrieves the consumer view table as a DataFrame
    :param spark: The Spark Session
    :param schema_location: The absolute path to the location of the consumer view parquet files
    :return: A DataFrame consisting of the identifier type, the canonical value, and a cluster id.
    """
    unfiltered_consumer_view_df = load_parquet_into_df(spark, schema_location)
    return unfiltered_consumer_view_df.select(ConsumerViewSchema.NODE_TYPE_CD,
                                              ConsumerViewSchema.VALUE,
                                              ConsumerViewSchema.CLUSTER_ID) \
        .filter(unfiltered_consumer_view_df.node_type_cd != "device_id")


def build_consumer_view_schema_location(environment):
    """
    Builds an absolute path to the consumer insights schema`

    :param environment: The current environment (local, dev, qa, prod)
    :return: A string for locating consumer insights parquet files.
    """
    if environment == Environments.LOCAL:
        bucket_prefix = Environments.LOCAL_BUCKET_PREFIX
    else:
        bucket_prefix = 's3://jornaya-{0}-{1}-prj/'.format(environment, Environments.AWS_REGION)
    return bucket_prefix + 'cis/consumer_graph/consumer_view'
