import numpy as np
from pyspark.sql import functions as F
from functools import reduce


class LinearTrend:
    def __init__(self, spark):
        self.spark = spark

    def linear_trend(self, y):
        x = np.arange(len(y))
        a, b = np.polyfit(x, y, 1)
        return x, a*x+b

    def df_linear_trend(self, df, x_col, y_col, label_col, new_label):
        xy = df.select([x_col, y_col]).orderBy(x_col).toPandas()
        x, y = self.linear_trend(xy[y_col].values)
        xy[y_col] = y
        
        xy = self.spark.createDataFrame(xy)
        xy = xy.withColumn(label_col, F.lit(new_label))
        return df.unionByName(xy, allowMissingColumns=True)

    def df_all_labels(self, df, x_col, y_col, label_col, new_label):
        df.persist()
        new_df = []
        for t in  df.select(label_col).distinct().toPandas()[label_col].tolist():
            temp = df.filter(f"{label_col} = '{t}'")
            temp = self.df_linear_trend(temp, x_col, y_col, label_col, new_label=f"{new_label}{t}")
            new_df.append(temp)

        f = lambda x, y: x.unionByName(y, allowMissingColumns=True)
        df_output = reduce(f, new_df)

        df.unpersist()
        return df_output