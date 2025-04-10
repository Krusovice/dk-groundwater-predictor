from pyspark.sql.functions import avg, unix_timestamp, when, col
from pyspark.sql.window import Window

def average_column_over_window(df,exist_col_name,new_col_name,time_days):
    '''
    Function that makes a new column in a dataframe, which contains
    windowed results from an existing column over a given number of days.

    Args:
        df: Spark DataFrame, which contains column "observed_date" as date format.
        exist_col_name: Column which should be windowed and averaged over.
        new_col_name: Name for the new column created with windowed results.
        time_days: Time which the window functions runs over, given in days.

    Returns:
        df: Same Spark DataFrame given as argument, but with the new column added.
    '''

    # Creating unix timestamp
    df = df.withColumn("date_ts", unix_timestamp("observed_date"))

    # Creating window over number of dates
    window_spec = Window.orderBy("date_ts").rangeBetween((-1*time_days+1) * 24*3600, 0)
    df = df.withColumn(new_col_name, avg(exist_col_name).over(window_spec))
    df = df.drop('date_ts')
    return df


def evaluate_null_columns(df,value):
    """
    Adds binary columns for each column that contains a null value. 
    The binary column has existing col name followed _notnull.
    
    Also substitutes the null values with a given value.

    Args:
        df: Spark DataFrame.
        value: Value that substitutes null values.
    
    Returns:
        df: Spark Dataframe
    """
    for i in df.columns:
        if df.filter(col(f"{i}").isNull()).count() > 0:
            df = df.withColumn(f"{i}_notnull", when(col(i).isNotNull(), 1).otherwise(0))
    
    df = df.fillna(value)
    return df