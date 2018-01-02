from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import TimestampType

from jobs.input.input_processing import load_parquet_into_df
from shared.constants import ConsumerViewSchema, PiiHashingColumnNames, IdentifierTypes, \
    LeadEventSchema, JoinTypes, InputColumnNames, Schemas


def retrieve_leads_from_consumer_graph(spark, schema_locations, pii_hashing_df, as_of_datetime):
    """
    Retrieves all of the leads that are associated with a record id in addition to the associated campaign key,
    and created timestamp.

    :param spark: The spark session
    :param schema_locations: A dictionary containing the location for consumer view and lead event
    :param pii_hashing_df: The DataFrame received from the pii hashing module
    :param as_of_datetime: The date as of which the file is being processed
    :return: A DataFrame consisting of rows of record ids, leadids, created timestamps, and campaign keys
    """
    consumer_view_schema_location = schema_locations[Schemas.CONSUMER_VIEW]
    consumer_view_df = get_consumer_view_df(spark, consumer_view_schema_location)

    cluster_id_df = join_pii_hashing_to_consumer_view_df(pii_hashing_df, consumer_view_df)
    lead_id_df = get_leads_from_cluster_id_df(consumer_view_df, cluster_id_df)

    lead_event_location = schema_locations[Schemas.LEAD_EVENT]
    lead_event_df = get_lead_event_df(spark, lead_event_location)

    view_df = join_lead_ids_to_lead_event(lead_id_df, lead_event_df)

    return apply_as_of_date_to_consumer_view_results(view_df, as_of_datetime)


def apply_as_of_date_to_consumer_view_results(consumer_view, as_of_datetime):
    """
    Takes an 'as of' date and filters (nulls out) leads which where created after the as of date.

    created_ts = '2017-11-10 12:00:00'
    as_of_time = '2017-11-05 12:00:00'
    Since created_ts > as_of_time, then input_id = null
    :param consumer_view: A DataFrame representing the consumer view
    :param as_of_datetime: The date as of which the file is being processed
    :return: A DataFrame with as_of dates applied
    """
    as_of_view = consumer_view.withColumn(InputColumnNames.AS_OF_TIME, lit(as_of_datetime)) \
        .withColumn(LeadEventSchema.CREATION_TS, consumer_view.creation_ts.cast(TimestampType()))

    return as_of_view.withColumn(InputColumnNames.INPUT_ID,
                                 when(as_of_view[LeadEventSchema.CREATION_TS] > as_of_view[InputColumnNames.AS_OF_TIME],
                                      None)
                                 .otherwise(as_of_view[InputColumnNames.INPUT_ID])) \
        .drop(InputColumnNames.AS_OF_TIME)


def join_lead_ids_to_lead_event(lead_id_df, lead_event_df):
    """
    Joins the record_id and lead_id DataFrame with the lead_id and creation_ts DataFrame
    :param lead_id_df: A DataFrame consisting of record_id and lead_id
    :param lead_event_df: A DataFrame consisting of lead_id, creation_ts, and campaign_key
    :return: A DataFrame consisting of record_id, lead_id, creation_ts, and campaign_key
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
