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


class GenericColumnNames:  # pylint:disable=too-few-public-methods
    LEAD_ID = "leadid"
    PHONE = "phone"
    EMAIL = "email"
    RECORD_ID = "recordid"


class OutputFileNames:  # pylint:disable=too-few-public-methods
    TIME_FORMAT = "%Y%m%d%H%M"
    PRODUCT_NAME = 'aidainsights'


class ClassificationSchemaNames:  # pylint:disable=too-few-public-methods
    LEAD = "classif_lead"
    SET_ELEMENT_XREF = "classif_set_element_xref"
    SUBCATEGORY = "classif_subcategory"


class ClassificationColumnNames:  # pylint:disable=too-few-public-methods
    SUBCATEGORY_KEY = "classif_subcategory_key"
    CATEGORY_KEY = "classif_category_key"
    ELEMENT_KEY = "classif_element_key"
    SET_KEY = "classif_set_key"
    SUBCATEGORY_NAME = "subcategory_nm"
    DISPLAY_NAME = "display_nm"
    CLASSIF_TIMESTAMP = "classif_ts"
    INSERTED_TIMESTAMP = "inserted_ts"


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
