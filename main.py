from pyspark.sql import *
from lib.logger import Log4J
from config import get_spark_config
from utils import load_df, group_by, filter_by, select

if __name__ == '__main__':
    spark_conf = get_spark_config()
    spark = SparkSession.builder \
        .config(conf=spark_conf) \
        .getOrCreate()

    logger = Log4J(spark)
    filename = "data/Equity.csv"

    df = load_df(spark, filename)
    df1 = filter_by(df, col="Face_Value", val="10", op=" > ")
    df1 = select(df1, ["Security_Name"])
    print("SECURITIES WHOSE FACE VALUE IS GREATER THAN 10")
    print(df1.collect())

    df2 = group_by(df, "Industry")
    df2 = df2.count()
    print("NO OF SECURITIES IN EACH INDUSTRY")
    df2.show()

    spark.stop()
