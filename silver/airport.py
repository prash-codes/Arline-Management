#######################################################################################################################
# Name  :   Airport.py
# Description   :   Pyspark script to clean,transform raw data into silver layer
# Date  :   2024-07-30
# Version   :   0
# Modified Date :   2025-02-06
# Modification Description  :
#######################################################################################################################

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, upper
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, BooleanType
from all_utils.utils import Utils

if __name__ == '__main__':

    # Initialization Spark Session object
    spark = SparkSession.builder.master("local[*]").appName("Airport").getOrCreate()

    # Generate object of Utils
    utils_obj = Utils()

    # Generate DataFrame from input files
    try:
        df1 = utils_obj.generate_dataframe(spark,
                                           r"D:\3.Spark\Project_airport\airline_management\dataset_schema\airport_schema.txt",
                                           r"D:\3.Spark\Project_airport\airline_management\dataset\raw\airport\airport_1.csv")

        df2 = utils_obj.generate_dataframe(spark,
                                           r"D:\3.Spark\Project_airport\airline_management\dataset_schema\airport_schema.txt",
                                           r"D:\3.Spark\Project_airport\airline_management\dataset\raw\airport\airport_2.csv")

        df3 = utils_obj.generate_dataframe(spark,
                                           r"D:\3.Spark\Project_airport\airline_management\dataset_schema\airport_schema.txt",
                                           r"D:\3.Spark\Project_airport\airline_management\dataset\raw\airport\airport_3.csv")

        df4 = utils_obj.generate_dataframe(spark,
                                           r"D:\3.Spark\Project_airport\airline_management\dataset_schema\airport_schema.txt",
                                           r"D:\3.Spark\Project_airport\airline_management\dataset\raw\airport\airport_4.csv")

        df5 = utils_obj.generate_dataframe(spark,
                                           r"D:\3.Spark\Project_airport\airline_management\dataset_schema\airport_schema.txt",
                                           r"D:\3.Spark\Project_airport\airline_management\dataset\raw\airport\airport_5.csv")

        df6 = utils_obj.generate_dataframe(spark,
                                           r"D:\3.Spark\Project_airport\airline_management\dataset_schema\airport_schema.txt",
                                           r"D:\3.Spark\Project_airport\airline_management\dataset\raw\airport\airport_6.csv")

        df7 = utils_obj.generate_dataframe(spark,
                                           r"D:\3.Spark\Project_airport\airline_management\dataset_schema\airport_schema.txt",
                                           r"D:\3.Spark\Project_airport\airline_management\dataset\raw\airport\airport_7.csv")

        df8 = utils_obj.generate_dataframe(spark,
                                           r"D:\3.Spark\Project_airport\airline_management\dataset_schema\airport_schema.txt",
                                           r"D:\3.Spark\Project_airport\airline_management\dataset\raw\airport\airport_8.csv")

        print(f"Input Dataframe is successfully Created")
    except Exception as e:
        print(f"Failed while generating dataframe. Error:{e}")

    # ------------------------------------------------------------------------------------------------------------------
    # Dst column Transformation
    # Daylight savings time. One of E (Europe), A (US/Canada),
    # S (South America), O (Australia), Z (New Zealand), N
    # (None) or U (Unknown). See also: Help: Time

    df = df1.withColumn("DST",
                        when(upper(col("dst")) == "E", "Europe").
                        when(upper(col("dst")) == "A", "US/Canada").
                        when(upper(col("dst")) == "S", "South America").
                        when(upper(col("dst")) == "O", "Australia").
                        when(upper(col("dst")) == "Z", "New Zealand").
                        when(upper(col("dst")).isin("N", "U"), "Unknown"))
    print(df.show())

    # ------------------------------------------------------------------------------------------------------------------
    # Tz_database_time_zone column Transformation
    # Timezone in "tz" (Olson) format, eg .
    # "America/Los_Angeles".
    # If the value is equal to null or \N then replace with IST timezone.

    df = df1.withColumn("Tz_database_time_zone",
                        when(col("Tz_database_time_zone") == "", "IST").
                        when(col("Tz_database_time_zone") == "\\N", "IST").
                        otherwise(col("Tz_database_time_zone")))
    print(df.show())
    # ------------------------------------------------------------------------------------------------------------------

    # existing DF = existing data
    # incremental update - df,existing_df SCD Type1.
    # overwrite output location with SCD type 1 output dataframe.

    df1.write.mode("overwrite").parquet(r"D:\3.Spark\Project_airport\airline_management\dataset\silver\airport")

    print(r"silver layer data is generated at location : D:\3.Spark\Project_airport\airline_management\dataset\silver"
          r"\airport")

    # ------------------------------------------------------------------------------------------------------------------

