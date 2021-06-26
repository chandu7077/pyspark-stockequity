import configparser
from pyspark import SparkConf


def get_spark_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")
    for (k, v) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(k, v)
    return spark_conf
