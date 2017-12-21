from pyspark.sql.types import StructType, StructField, StringType

from jobs.classification.classify import get_classification_subcategory_df
from jobs.input.input_processing import load_csv_file
from shared.constants import Environments, ConfigurationSchema, ClassificationSubcategory, JoinTypes


def get_application_defaults_location(environment):
    """
    Builds an absolute path to the application defaults file.

    :param environment: The current environment (local, dev, qa, prod)
    :return: A string for locating the application default csv.
    """
    if environment == Environments.LOCAL:
        bucket_prefix = Environments.LOCAL_BUCKET_PREFIX
    else:
        bucket_prefix = 's3://jornaya-{0}-{1}-aida-insights/'.format(environment, Environments.AWS_REGION)
    return bucket_prefix + 'pyspark/config/application_defaults.csv'


def get_client_overrides_location(environment, client_name):
    """
    Builds an absolute path to the client overrides file.

    :param environment: The current environment (local, dev, qa, prod)
    :param client_name: The name of the client running the application
    :return: A string for locating the client override csv.
    """
    if environment == Environments.LOCAL:
        bucket_prefix = Environments.LOCAL_BUCKET_PREFIX
    else:
        bucket_prefix = 's3://jornaya-{0}-{1}-aida-insights/'.format(environment, Environments.AWS_REGION)
    return '{0}{1}/input/client_overrides.csv'.format(bucket_prefix, client_name)


def get_application_default_df(spark, environment, logger):
    """
    Gets the application default configuration settings.
    :param spark: The application Spark Session
    :param environment: The current environment (local, dev, qa, staging, prod)
    :param logger: The Spark log4j logger
    :return: A DataFrame containing the application default configuration settings
    """
    defaults_location = get_application_defaults_location(environment)
    logger.info("Reading application defaults config from {location}".format(location=defaults_location))
    return load_csv_file(spark, defaults_location, configuration_schema())


def get_client_override_df(spark, environment, client_name, logger):
    """
    Gets the client override configuration settings.
    :param spark: The application Spark Session
    :param environment: The current environment (local, dev, qa, staging, prod)
    :param client_name: The name of the client which the app is running
    :param logger: The Spark log4j logger
    :return: A DataFrame containing the client override configuration settings
    """
    overrides_location = get_client_overrides_location(environment, client_name)
    logger.info("Reading client overrides config from {location}".format(location=overrides_location))
    return load_csv_file(spark, overrides_location, configuration_schema())


def merge_client_config_with_app_config(client_override_df, app_default_df):
    """
    Function for merging the application configuration with the client overrides
    :param client_override_df: DataFrame containing the client overrides
    :param app_default_df: DataFrame containing the application defaults
    :return: A DataFrame of application configuration
    """
    join_condition = [app_default_df.option == client_override_df.option,
                      app_default_df.config_abbrev == client_override_df.config_abbrev]

    # Get all of the config values from app defaults that aren't in client overrides
    missing_client_config_df = app_default_df.join(client_override_df, join_condition, JoinTypes.LEFT_ANTI_JOIN)

    # Union the client overrides and the missing config options with the application defaults
    return missing_client_config_df.union(client_override_df)


def get_subcategory_key_for_config_abbreviation(config_df, subcategory_df):
    """
    Function for obtaining the subcategory key for a given config abbreviation so that we can apply event lookbacks
    and frequency thresholds to an appropriate subcategory.
    :param config_df: The application config DataFrame
    :param subcategory_df: The classification subcategory DataFrame
    :return: A DataFrame containing the columns from the application config and an associated subcategory key.
    """
    subcategory_abbrev_join = [config_df.config_abbrev == subcategory_df.subcategory_cd]

    return config_df.join(subcategory_df, subcategory_abbrev_join, JoinTypes.LEFT_JOIN) \
        .drop(ClassificationSubcategory.SUBCATEGORY_CD)


def get_application_config_df(spark, environment, client_name, logger):
    """
    Gets the overall application configuration by combining the application defaults and client overrides. Then it
    converts the config abbreviation to the classification subcategory key and returns a DataFrame.
    :param spark: The application Spark Session
    :param environment: The current environment (local, dev, qa, staging, prod)
    :param client_name: The name of the client which the app is running
    :param logger: The Spark log4j logger
    :return: A DataFrame containing the overall application configuration settings
    """
    application_defaults_df = get_application_default_df(spark, environment, logger)
    client_overrides_df = get_client_override_df(spark, environment, client_name, logger)

    configuration_df = merge_client_config_with_app_config(client_overrides_df, application_defaults_df)

    # Get the subcategories dataframe so that we can get the id value of the config abbreviation
    classif_subcategory_df = get_classification_subcategory_df(spark, environment, logger) \
        .drop(ClassificationSubcategory.SUBCATEGORY_DISPLAY_NM)

    return get_subcategory_key_for_config_abbreviation(configuration_df, classif_subcategory_df)


def configuration_schema():
    """
    Defines the schema for the configuration file.
    :return: A StructType object defining schema for configuration files
    """
    return StructType(
        [StructField(ConfigurationSchema.OPTION, StringType()),
         StructField(ConfigurationSchema.CONFIG_ABBREV, StringType()),
         StructField(ConfigurationSchema.VALUE, StringType())])
