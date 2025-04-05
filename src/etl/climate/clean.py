from pyspark.sql.functions import avg, to_date, current_timestamp

def dmi_climate_clean_and_transform(df):
    """
    Transforms dmi climate data by pivoting parameterId and changing metadata.
    """
    
    df = df.groupBy('observed','stationId','ingestion_timestamp').pivot('parameterId').agg(avg('value'))
    df = df.withColumnRenamed('observed', 'observed_timestamp')
    df = df.withColumn("observed_date", to_date("observed_timestamp"))
    df = df.withColumn('transformation_timestamp', current_timestamp())
    return df