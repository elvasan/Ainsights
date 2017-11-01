class Environments:  # pylint:disable=too-few-public-methods
    QA = "qa"  # pylint:disable=invalid-name
    DEV = "dev"
    STAGING = "staging"
    PROD = "prod"
    LOCAL = "local"
    LOCAL_BUCKET_PREFIX = "../samples/"
    AWS_REGION = 'us-east-1'


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
    SCHEMA_NAME = "classif_lead"
    TOKEN = "token"


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
