
def load_df(spark, filename):
    df = spark.read.csv(filename, header=True, inferSchema=True)
    cols = df.columns
    cols = [col.replace(" ","_") for col in cols]
    df = df.toDF(*cols)
    return df


def filter_by(df, col, val, op):
    print(col+op+val)
    return df.where(col + op + val)


def select(df, cols):
    return df.select(*cols)


def group_by(df, col):
    return df.groupBy(col)
