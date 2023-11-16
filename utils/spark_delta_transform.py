import pyspark
from pyspark.sql.types import StructType
from pyspark.sql import functions as F


def unnest_struct(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """Unnest structs into new columns.
    
    Args:
        df: pyspark dataframe to unnest.
        
    Returns:
        Unnested dataframe.
    """
    
    to_drop = []
    for s in df.schema:
        if not isinstance(s.dataType, StructType):
            continue

        to_drop.append(s.name)
        for field in s.dataType.fieldNames():
            df = df.withColumn(f"{s.name}_{field}", df[f"{s.name}.{field}"])
    
    df = df.drop(*to_drop)
    return df

def transform_column_names(df: pyspark.sql.DataFrame):
    for c in df.columns:
        new_c = c.replace('$', '').strip('_')
        df = df.withColumnRenamed(c, new_c)
    return df