from pyspark.sql.functions import when, col

from jobs.input.input_processing import load_parquet_into_df
from shared.constants import Environments, JoinTypes, LeadEventSchema, InputColumnNames, PublisherPermissions


def apply_publisher_permissions_to_lead_campaigns(spark, schema_location, consumer_insights_df):
    """
    Apply publisher permissions to the consumer insights DataFrame so that campaigns which are opted out are filtered
    from consideration when classifying or scoring leads.

    :param spark: The application Spark Session
    :param schema_location: The location of the campaign_opt_in_view
    :param consumer_insights_df: A DataFrame consisting of record_id, input_id, creation_ts, and campaign_key
    :return: The original consumer insights DataFrame with the publisher permission filter applied resulting in null
        values for input_id where campaigns are opted out.
    """
    campaign_opt_in_view = load_parquet_into_df(spark, schema_location)
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
