# Author: Alexander H O Wada
# email: alexanderhidekioniwa@gmail.com
# Original source code: https://github.com/alexanderhowada/data_engineering


from delta.tables import DeltaTable
from typing import List


def table_exists(table, spark_session=None):
    """
    Check if delta table exists.

    :param table: delta table
    :param spark_session: spark session
    :return: True/False
    """

    if spark_session is None:
        global spark
    else:
        spark = spark_session

    table = table.split('.')
    database, table = table[0], table[1]

    if spark._jsparkSession.catalog().tableExists(database, table):
        return True
    else:
        return False


def generate_where_clause(df, column_list: List[str]):
    """
    Generate a SQL-style WHERE clause for the given column name, based on the unique values in the DataFrame.

    :param df: the DataFrame to generate the WHERE clause from.
    :param column_name: the name of the column to generate the WHERE clause for.
    :return: a string representing the SQL-style WHERE clause.
    """

    where_clause = []

    for c in column_list:

        unique_values = df.select(c).distinct().rdd.flatMap(lambda x: x).collect()
        unique_values.sort()
        dt = str(df.schema[c].dataType).lower()

        if 'date' in dt or 'timestamp' in dt:
            unique_values = [str(uv) for uv in unique_values]

        if not unique_values:
            continue

        if len(unique_values) == 1 and ('string' in dt or 'date' in dt or 'timestamp' in dt):
            where_clause.append(f"{c} = '{unique_values[0]}'")
        elif len(unique_values) == 1:
            where_clause.append(f"{c} = {unique_values[0]}")
        else:
            where_clause.append(f"{c} in {str(tuple(unique_values))}")

    return where_clause


def merge(df, target_table, pk, spark_session=None, partition=[]):
    """
    Merge spark datafram into a delta table.

    :param df: spark dataframe
    :param target_table: target delta table to merge
    :param pk: primary keys to merge
    :param spark_session: spark sessions
    :param partition: columns to partition.
                      Partitioned columns will be selected in merge clause for performance.
                      For optimal perfomance, df must contain only unique partitions.
    """

    if spark_session is None:
        global spark
    else:
        spark = spark_session

    if table_exists(target_table):
        dt = DeltaTable.forName(spark, target_table)

        condition = [f't.{k}=s.{k}' for k in pk]
        condition += ['t.' + wc for wc in generate_where_clause(df, partition)]
        condition = ' and '.join(condition)

        dt.alias('t').merge(
            df.alias('s'), condition
        ).whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        if len(partition) > 0:
            df.write.partitionBy(*partition).mode('overwrite').format('delta').saveAsTable(target_table)
        else:
            df.write.mode('overwrite').format('delta').saveAsTable(target_table)


if __name__ == '__main__':
    from pyspark.sql import SparkSession

    # Create a SparkSession
    spark = SparkSession.builder.appName("test").getOrCreate()

    # Create a sample DataFrame
    data = [("John", "Doe", 25),
            ("Jane", "Doe", 30),
            ("Bob", "Doe", 45),
            ("Alice", "Doe", 35),
            ("Mike", "Doe", 20)]

    df = spark.createDataFrame(data, ["first_name", "last_name", "age"])
    df.show()

    print(generate_where_clause(df, ['first_name', 'last_name']))