from pyspark.sql.functions import split

def split_columns(df):
    """Split raw log records into different columns."""
    transformed_df = df.withColumn("value", split(df["value"], " ")) \
        .selectExpr("value[0] AS IP",
                    "substring(value[3], 2, length(value[3])) AS Date",
                    "substring(value[4], 1, length(value[4])-1) AS Time",
                    "substring(value[5], 2, length(value[5])) AS Method",
                    "value[6] AS Endpoint",
                    "substring(value[7], 1, length(value[7])-1) AS Protocol",
                    "value[8] AS Status",
                    "value[9] AS Size")
    return transformed_df