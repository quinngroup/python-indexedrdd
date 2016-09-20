import pytest
import logging
from pyspark import SparkContext
from pyspark.conf import SparkConf
from indexedrdd import *

def quiet_py4j():
    """ turn down spark logging for the test context """
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)

@pytest.fixture(scope="session")
def spark_context(request):
    """ fixture for creating a spark context
    Args:
        request: pytest.FixtureRequest object
    """
    conf = (SparkConf().setMaster("local[2]").setAppName("pytest-pyspark-local-testing"))
    sc = SparkContext(conf=conf)
    request.addfinalizer(lambda: sc.stop())

    quiet_py4j()
    return sc    

pytestmark = pytest.mark.usefixtures("spark_context")    

def test_initialization(spark_context):
	rdd0 = spark_context.parallelize(range(5)).map(lambda x: (x, x*x))
	rdd1 = IndexedRDD(rdd0).cache()
	actual_results = rdd1.collect()
	expected_results = [ (0,0), (2,4), (4,16), (1,1), (3,9)]
	wrong_results = [ (0,0), (2,2), (4,4), (1,1), (3,3)]
	assert actual_results == expected_results
	assert actual_results != wrong_results

def test_get(spark_context):
	rdd0 = spark_context.parallelize(range(5)).map(lambda x: (x, x*x))
	rdd1 = IndexedRDD(rdd0).cache()
	actual_results = rdd1.get(2)
	expected_results = [4]
	wrong_results = [2]
	assert actual_results == expected_results
	assert actual_results != wrong_results

def test_put(spark_context):
	rdd0 = spark_context.parallelize(range(2)).map(lambda x: (x, x*x))
	rdd1 = IndexedRDD(rdd0).cache()
	actual_results = rdd1.put((2,4)).collect()
	expected_results = [ (0,0), (2,4), (1,1)]
	wrong_results = [ (0,0), (2,2), (1,1)]
	assert actual_results == expected_results
	assert actual_results != wrong_results

def test_delete(spark_context):
	rdd0 = spark_context.parallelize(range(5)).map(lambda x: (x, x*x))
	rdd1 = IndexedRDD(rdd0).cache()
	actual_results = rdd1.delete(2).collect()
	expected_results = [ (0,0), (4,16), (1,1), (3,9)]
	wrong_results = [ (0,0), (2,4), (4,16), (1,1), (3,9)]
	assert actual_results == expected_results
	assert actual_results != wrong_results	

def test_filter(spark_context):
	rdd0 = spark_context.parallelize(range(5)).map(lambda x: (x, x*x))
	rdd1 = IndexedRDD(rdd0).cache()
	actual_results = rdd1.filter(lambda (x):(x[0]==2)).collect()
	expected_results = [(2,4)]
	wrong_results = [ (0,0), (4,16), (1,1), (3,9)]
	assert actual_results == expected_results
	assert actual_results != wrong_results
