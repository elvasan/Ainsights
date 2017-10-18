import logging
import sys

import pytest
from pyspark.context import SparkContext, SparkConf
from pyspark.sql import SparkSession

sys.path.insert(0, 'src')


def quiet_py4j():
    """ Turn down spark logging for the test context """
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.DEBUG)


@pytest.fixture(scope="session", name="spark_session")
def spark_session_setup(request):
    """ fixture for creating a spark context
    Args:
        request: pytest.FixtureRequest object
    """
    conf = (SparkConf().setMaster("local[*]").setAppName("aida_insights_testing"))
    spark_context = SparkContext(conf=conf)
    sc = SparkSession(sparkContext=spark_context).builder.getOrCreate()
    request.addfinalizer(lambda: sc.stop())

    quiet_py4j()
    return sc
