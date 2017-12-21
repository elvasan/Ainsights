from pyspark.sql.functions import when, col

from jobs.input.input_processing import load_parquet_into_df
from shared.constants import Environments, JoinTypes, LeadEventSchema, InputColumnNames, PublisherPermissions


def apply_publisher_permissions_to_lead_campaigns(spark, environment, consumer_insights_df):
    """
    Apply publisher permissions to the consumer insights DataFrame so that campaigns which are opted out are filtered
    from consideration when classifying or scoring leads.

    :param spark: The application Spark Session
    :param environment: The current environment (local, dev, qa, prod)
    :param consumer_insights_df: A DataFrame consisting of record_id, input_id, creation_ts, and campaign_key
    :return: The original consumer insights DataFrame with the publisher permission filter applied resulting in null
        values for input_id where campaigns are opted out.
    """
    campaign_opt_in_view = load_campaign_opt_in_state_view(spark, environment)
    campaign_opt_in = filter_campaign_opt_in_state(campaign_opt_in_view)
    return apply_campaign_filter(consumer_insights_df, campaign_opt_in)


def apply_campaign_filter(consumer_insights_df, campaign_opt_in):
    """
    Apply the campaign filter by joining on campaign key and nulling out input ids for opted out campaigns

    :param consumer_insights_df: A DataFrame consisting of record_id, input_id, creation_ts, and campaign_key
    :param campaign_opt_in: A DataFrame consisting of opted in campaign keys with the column name 'view_campaign_key'
    :return: A DataFrame consisting of null values for input_id where campaigns are opted out.
    """
    filtered_cis = consumer_insights_df.join(campaign_opt_in,
                                             consumer_insights_df.campaign_key == campaign_opt_in.view_campaign_key,
                                             JoinTypes.LEFT_JOIN)
    return filtered_cis.withColumn(InputColumnNames.INPUT_ID,
                                   (when(col(PublisherPermissions.VIEW_CAMPAIGN_KEY).isNotNull(), filtered_cis.input_id)
                                    .otherwise(None))) \
        .select(InputColumnNames.RECORD_ID, InputColumnNames.INPUT_ID, LeadEventSchema.CREATION_TS)


def filter_campaign_opt_in_state(campaign_opt_in_state_df):
    """
    Filter out unnecessary information in the view to just the data we need: Campaigns that are opted in.

    :param campaign_opt_in_state_df: A DataFrame representing the campaign opt in view. Contains campaign_key,
        application_key, and opt_in_ind
    :return: A DataFrame representing the filtered campaign opt in view. Contains campaign_keys that are opted in.
    """
    return campaign_opt_in_state_df \
        .filter((campaign_opt_in_state_df.application_key == Environments.AIDA_INSIGHTS_APP_CODE) &
                (campaign_opt_in_state_df.opt_in_ind == PublisherPermissions.OPT_IN_VALUE)) \
        .select(PublisherPermissions.CAMPAIGN_KEY) \
        .withColumnRenamed(PublisherPermissions.CAMPAIGN_KEY, PublisherPermissions.VIEW_CAMPAIGN_KEY)


def load_campaign_opt_in_state_view(spark, environment):
    """
    Loads the view from an external location in parquet format into a DataFrame

    :param spark: The application Spark Session
    :param environment: The current environment (local, dev, qa, prod)
    :return: A DataFrame representing the campaign opt in view. Contains campaign_key, application_key, and opt_in_ind
    """
    campaign_opt_in_schema_location = build_v_campaign_opt_in_state_schema_location(environment)
    return load_parquet_into_df(spark, campaign_opt_in_schema_location)


def build_v_campaign_opt_in_state_schema_location(environment):
    """
    Builds an absolute path to the campaign opt in event schema.

    :param environment: The current environment (local, dev, qa, prod)
    :return: A string for locating campaign opt in event view parquet files.
    """
    if environment == Environments.LOCAL:
        bucket_prefix = Environments.LOCAL_BUCKET_PREFIX
    else:
        bucket_prefix = 's3://jornaya-{0}-{1}-prj/'.format(environment, Environments.AWS_REGION)
    return bucket_prefix + 'publisher_permissions/v_campaign_opt_in_state'
