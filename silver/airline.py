#######################################################################################################################
# Name  :   Airline.py
# Description   :   Pyspark script to clean,transform raw data into silver layer
# Date  :   2024-07-30
# Version   :   0
# Modified Date :   2025-02-06
# Modification Description  :
#######################################################################################################################

from pyspark.sql import SparkSession
from all_utils import utils
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, BooleanType
from pyspark.sql.functions import when, col, upper, lower, filter, isnull
from all_utils.utils import Utils

if __name__ == '__main__':
    # Initialization Spark Session Object
    spark = SparkSession.builder.master("local[*]").appName("Airline").getOrCreate()

    # Generate object of Utils
    utils_obj = Utils()

    # Generate DataFrame from input file
    df = utils_obj.generate_dataframe(spark, r"D:\3.Spark\Project_airport\airline_management\dataset_schema"
                                             r"\airline_schema.txt",
                                      r"D:\3.Spark\Project_airport\airline_management\dataset\raw\airline"
                                      r"\airline.csv")
    print(df.printSchema())
    print(df.show())

    # All null check on name, IATA, ICAO, ID
    null_df = df.filter(col("name").isNull() |
                        col("iata").isNull() |
                        col("icao").isNull() |
                        col("airline_id").isNull()
                        )
    print(null_df.show())

    # Airline_id Column Transformation.
    # Unique OpenFlights identifiers for this airline.
    # if id is -1 then it is bad record.

    bad_df = df.filter(col("airline_id") == -1)
    print(bad_df.show())

    fd = df.filter(col("Status").isin(["Y", "N"]))
    print(fd.show())
