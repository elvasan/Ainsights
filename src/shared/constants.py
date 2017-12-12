class Environments:  # pylint:disable=too-few-public-methods
    QA = "qa"  # pylint:disable=invalid-name
    DEV = "dev"
    STAGING = "staging"
    PROD = "prod"
    LOCAL = "local"
    LOCAL_BUCKET_PREFIX = "../samples/"
    AWS_REGION = 'us-east-1'
    AIDA_INSIGHTS_APP_CODE = 3


class InputColumnNames:  # pylint:disable=too-few-public-methods
    RECORD_ID = "record_id"
    LEAD_ID = "lead_id"
    HAS_ERROR = "has_error"
    ERROR_MESSAGE = "error_message"
    INPUT_ID = "input_id"
    INPUT_ID_TYPE = "input_id_type"
    AS_OF_TIME = "as_of_time"
    INPUT_ID_RAW = "input_id_raw"


class GenericColumnNames:  # pylint:disable=too-few-public-methods
    LEAD_ID = "leadid"
    PHONE = "phone"
    EMAIL = "email"
    RECORD_ID = "recordid"


class OutputFileNames:  # pylint:disable=too-few-public-methods
    INTERNAL = 'results_internal'
    EXTERNAL = 'results_external'
    TIME_FORMAT = "%Y%m%d%H%M"
    PRODUCT_NAME = 'aidainsights'


class ClassificationColumnNames:  # pylint:disable=too-few-public-methods
    SUBCATEGORY_CD = "subcategory_cd"
    CLASSIF_SUBCATEGORY_KEY = "classif_subcategory_key"
    CLASSIF_CATEGORY_KEY = "classif_category_key"
    CLASSIF_OWNER_NM = "classif_owner_nm"
    INSERT_JOB_RUN_ID = "insert_job_run_id"
    INSERT_BATCH_RUN_ID = "insert_batch_run_id"
    LOAD_ACTION_IND = "load_action_ind"
    CLASSIF_SET_KEY = "classif_set_key"
    SUBCATEGORY_DISPLAY_NM = "subcategory_display_nm"
    INSERT_TS = "insert_ts"


class ClassificationLead(ClassificationColumnNames):  # pylint:disable=too-few-public-methods
    SCHEMA_NAME = "classif_lead/classif_lead"
    LEAD_ID = "lead_id"


class ClassificationSetElementXref(ClassificationColumnNames):  # pylint:disable=too-few-public-methods
    SCHEMA_NAME = "classif_set_element_xref"
    CLASSIF_ELEMENT_KEY = "classif_element_key"
    ELEMENT_CD = "element_cd"
    ELEMENT_DISPLAY_NM = "element_display_nm"
    CATEGORY_CD = "category_cd"
    CATEGORY_DISPL_NM = "category_displ_nm"


class ClassificationSubcategory(ClassificationColumnNames):  # pylint:disable=too-few-public-methods
    SCHEMA_NAME = "classif_subcategory"


class ClassificationCategoryAbbreviations:  # pylint:disable=too-few-public-methods
    AUTO_SALES = 'auto_sales'
    EDUCATION = 'education'
    INSURANCE = 'insurance'
    FINANCIAL_SERVICES = 'financial_services'
    REAL_ESTATE = 'real_estate'
    JOBS = 'jobs'
    LEGAL = 'legal'
    HOME_SERVICES = 'home_services'
    OTHER = 'other'


class ClassificationCategoryDisplayNames:  # pylint:disable=too-few-public-methods
    AUTO_SALES = 'Auto Sales'
    EDUCATION = 'Education'
    INSURANCE = 'Insurance'
    FINANCIAL_SERVICES = 'Financial Services'
    REAL_ESTATE = 'Real Estate'
    JOBS = 'Jobs'
    LEGAL = 'Legal'
    HOME_SERVICES = 'Home Services'
    OTHER = 'Other'


class IdentifierTypes:  # pylint:disable=too-few-public-methods
    PHONE = "phone"
    EMAIL = "email"
    LEADID = "leadid"


class RawInputCSVColumnNames:  # pylint:disable=too-few-public-methods
    RECORD_ID = "record_id"
    PHONE_1 = "phone_1"
    PHONE_2 = "phone_2"
    PHONE_3 = "phone_3"
    PHONE_4 = "phone_4"
    EMAIL_1 = "email_1"
    EMAIL_2 = "email_2"
    EMAIL_3 = "email_3"
    LEAD_1 = "lead_1"
    LEAD_2 = "lead_2"
    LEAD_3 = "lead_3"


class HashMappingColumnNames:  # pylint:disable=too-few-public-methods
    CANONICAL_HASH_VALUE = "canonical_hash_value"
    HASH_VALUE = "hash_value"
    HASH_TYPE_CD = "hash_type_cd"


class PiiHashingColumnNames:  # pylint:disable=too-few-public-methods
    RECORD_ID = 'record_id'
    INPUT_ID_RAW = 'input_id_raw'
    INPUT_ID = 'input_id'
    INPUT_ID_TYPE = 'input_id_type'


class ConsumerViewSchema:  # pylint:disable=too-few-public-methods
    NODE_TYPE_CD = "node_type_cd"
    NODE_VALUE = "node_value"
    CLUSTER_ID = "cluster_id"


class ConfigurationSchema:  # pylint:disable=too-few-public-methods
    OPTION = "option"
    CONFIG_ABBREV = "config_abbrev"
    VALUE = "value"


class ConfigurationOptions:  # pylint:disable=too-few-public-methods
    EVENT_LOOKBACK = "event_lookback"
    FREQUENCY_THRESHOLD = "frequency_threshold"
    ASOF = "asof"


class LeadEventSchema:  # pylint:disable=too-few-public-methods
    LEAD_ID = "lead_id"
    SERVER_GMT_TS = "server_gmt_ts"
    GENERATOR_ACCOUNT_KEY = "generator_account_key"
    CAMPAIGN_KEY = "campaign_key"
    CREATION_TS = "creation_ts"


class JoinTypes:  # pylint:disable=too-few-public-methods
    LEFT_JOIN = "left"
    LEFT_ANTI_JOIN = "left_anti"
    LEFT_OUTER_JOIN = "left_outer"


class ThresholdValues:  # pylint:disable=too-few-public-methods
    NOT_SEEN = "NOT_SEEN"
    IN_MARKET = "IN_MARKET"
    IN_MARKET_HIGH = "IN_MARKET_H"


class PublisherPermissions:  # pylint:disable=too-few-public-methods
    CAMPAIGN_KEY = "campaign_key"
    OPT_IN_IND = "opt_in_ind"
    APPLICATION_KEY = "application_key"
    INSERT_JOB_RUN_ID = "insert_job_run_id"
    INSERT_TS = "insert_ts"
    VIEW_CAMPAIGN_KEY = "view_campaign_key"
    OPT_IN_VALUE = 1
    OPT_OUT_VALUE = 0
