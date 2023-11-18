from pyspark.sql import functions as F


def assert_no_null(df, columns: list):
    if not columns:
        return
    
    null_counts = df.select([F.sum(df[c].isNull().cast("int")).alias(c) for c in columns]).toPandas()
    if null_counts.sum().sum() != 0:
        null_counts.display()
        raise Exception("df contain nulls.")

def assert_pk(df, pk):
    count = df.groupBy(pk).count().filter("count > 1")
    if count.count() > 0:
        display(count)
        raise Exception(f"{pk} is not unique.")