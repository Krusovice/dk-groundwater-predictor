from pyspark.sql.functions import avg, to_date, current_timestamp, to_timestamp, col, when

def pivot_dataframe(df):
    '''
    Transforms dmi climate data by pivoting parameterId and changing metadata.

    Takes and returns dataframe.
    '''
    df = df.groupBy('observed','stationId','ingestion_timestamp').pivot('parameterId').agg(avg('value'))
    return df

def rename_cols(df):
    '''
    Rename the columns of the dataframe.

    Takes and returns dataframe.
    '''
    df = df.withColumnRenamed('observed', 'observed_timestamp')
    df = df.withColumnRenamed('stationId', 'station_id')
    return df

def format_timestamps(df):
    '''
    Formats the timestamp columns values to proper timestamp format.
    Also adds transformation timestamp.

    Takes and returns dataframe.
    '''
    
    df = df.withColumn("observed_timestamp", to_timestamp("observed_timestamp"))
    df = df.withColumn("ingestion_timestamp", to_timestamp("ingestion_timestamp"))
    df = df.withColumn("observed_date", to_date("observed_timestamp"))
    df = df.withColumn('transformation_timestamp', current_timestamp())
    return df

def verify_temp(df, *args):
    '''
    Takes in a dataframe and arguments of columns to be checked.
    Checking for outliers of temperature, that values are in between -40 and 60.

    If an ourlier exists, the value is set to null.

    Takes and returns dataframe.
    '''
    for i in args:
        df = df.withColumn(i, when((col(i) < -40) | (col(i) > 60), None).otherwise(col(i)))
    return df

def verify_sun(df, *args):
    '''
    Takes in a dataframe and arguments of columns to be checked.
    Checking for outliers of sun, are positive and below 500

    If an ourlier exists, the value is set to null.

    Takes and returns dataframe.
    '''
    for i in args:
        df = df.withColumn(i, when((col(i) < 0) | (col(i) > 500), None).otherwise(col(i)))
    return df

def verify_rain(df, *args):
    '''
    Takes in a dataframe and arguments of columns to be checked.
    Checking for outliers of rain, are positive and below 500

    If an ourlier exists, the value is set to null.

    Takes and returns dataframe.
    '''
    for i in args:
        df = df.withColumn(i, when((col(i) < 0) | (col(i) > 500), None).otherwise(col(i)))
    return df
