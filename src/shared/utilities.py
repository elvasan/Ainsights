# Environment Definitions
ENV_QA = "qa"
ENV_DEV = "dev"
ENV_STAGING = "staging"
ENV_PROD = "prod"
ENV_LOCAL = "local"

# File constants
# TODO: Need another way to get to samples dir. This could vary depending on where we are in the code.
LOCAL_BUCKET_PREFIX = "../samples/"

# Column constant values
ID_TYPE_LEAD_ID = "leadid"
ID_TYPE_PHONES = "phone"
ID_TYPE_EMAIL = "email"

# Column constant names
COL_NAME_RECORD_ID = "record_id"
COL_NAME_LEAD_ID = "lead_id"
COL_NAME_HAS_ERROR = "has_error"
COL_NAME_ERROR_MESSAGE = "error_message"
COL_NAME_INPUT_ID = "input_id"
COL_NAME_INPUT_ID_TYPE = "input_id_type"
COL_NAME_AS_OF_TIME = "as_of_time"
COL_NAME_INSERTED_TIMESTAMP = "inserted_ts"
COL_NAME_CLASSIF_TIMESTAMP = "classif_ts"

COL_NAME_CLASSIF_SET_KEY = "classif_set_key"
COL_NAME_CLASSIF_ELEMENT_KEY = "classif_element_key"
COL_NAME_CLASSIF_CATEGORY_KEY = "classif_category_key"
COL_NAME_CLASSIF_SUBCATEGORY_KEY = "classif_subcategory_key"
COL_NAME_SUBCATEGORY_NAME = "subcategory_nm"
COL_NAME_DISPLAY_NAME = "display_nm"

CLASSIFICATION_LEAD_SCHEMA_NAME = "classif_lead"
CLASSIFICATION_SET_ELEMENT_XREF_SCHEMA_NAME = "classif_set_element_xref"
CLASSIFICATION_SUBCATEGORY_SCHEMA_NAME = "classif_subcategory"
