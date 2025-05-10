#######################################################################################################################
# Name  :   Plane.py
# Description   :   Pyspark script to clean,transform raw data into silver layer
# Date  :   2024-07-30
# Version   :   0
# Modified Date :   2025-02-06
# Modification Description  :
#######################################################################################################################


from pyspark.sql import SparkSession
from all_utils.utils import Utils
from pyspark.sql.functions import when, col, upper, lower
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType

if __name__ == '__main__':
    # initializing spark session object
    spark = SparkSession.builder.master("local[*]").appName("plane").getOrCreate()

    # Generate object of utils.
    utils_obj = Utils()

    # Generate dataframe from input file
    df = utils_obj.generate_dataframe(spark, r"D:\3.Spark\Project_airport\airline_management\dataset_schema"
                                             r"\plane_schema.txt",
                                      r"D:\3.Spark\Project_airport\airline_management\dataset\raw\plane\plane.csv")

    print(df.printSchema())
    print(df.show(truncate=False))
