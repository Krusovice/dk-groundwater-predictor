from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, TimestampType, FloatType, StringType
from pyspark.sql.functions import lit, col, round, current_timestamp, to_date
from pyspark.sql import SparkSession


def parse_station_file(station_file: str) -> dict[str, str]:
    """
    Parsing station id file data from txt format into a dictionary.
    
    The returned dictionary takes the station_id as argument and returns station location info.
    """

    with open(station_file.replace('dbfs:','/dbfs'), "r") as f:
        lines = f.readlines()
    
    station_dict = {}
    for i in lines:
        key = i.split(',')[0]
        values = i.split(',')[1:]
        values_merged = ", ".join(s.strip() for s in values)
        station_dict[key] = values_merged
    
    return station_dict


def load_groundwater_data(groundwater_data_files: list[str], station_dict: dict[str, str]) -> DataFrame:
    """
    Loads groundwater observation data from multiple CSV files and returns a unified Spark DataFrame.

    Each filename must start with "grundvandsstand_" and include the station_id.
    Files must contain 'observed_timestamp' and 'waterlevel' columns.

    Parameters:
        groundwater_data_files (list[str]): List of file paths to CSV files.
        station_dict (dict[str, str]): Mapping of station_id to station_name.

    Returns:
        DataFrame: Spark DataFrame with columns [observed_timestamp, waterlevel, station_id, station_name].
    """

    # Creating an empty Spark DataFrame
    schema = StructType([
        StructField("observed_timestamp", TimestampType(), True),
        StructField("waterlevel", FloatType(), True),
        StructField("station_id", StringType(), True),
        StructField("station_name", StringType(), True),
    ])

    empty_data = []

    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(empty_data, schema)

    for i in groundwater_data_files:
        station_id = i.split('grundvandsstand_')[1].split('.csv')[0]
        df_inc = spark.read.option("header", "true").schema(schema).csv(i)
        df_inc = df_inc.withColumn("waterlevel", round(col("waterlevel"),2))
        df_inc = df_inc.withColumn("station_id", lit(station_id))
        df_inc = df_inc.withColumn("station_name", lit(station_dict[station_id]))
        df = df.union(df_inc)
    return df


def transform_groundwater_df(groundwater_df: DataFrame) -> DataFrame:
    """
    Adds ingestion_timestamp and observed_date to an existing groundwater_df.

    Parameters:
        groundwater_df: Groundwater DataFrame after loading from raw data.

    Returns:
        groundwater_df: Same DataFrame with added columns: ingestion_timestamp and observed_date.
    """
    
    groundwater_df = groundwater_df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("observed_date", to_date(col("observed_timestamp")))

    return groundwater_df
