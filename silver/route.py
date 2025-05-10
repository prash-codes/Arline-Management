#######################################################################################################################
# Name  :   Route.py
# Description   :   Pyspark script to clean,transform raw data into silver layer
# Date  :   2024-07-30
# Version   :   0
# Modified Date :   2025-02-06
# Modification Description  :
#######################################################################################################################

from pyspark.sql import SparkSession
from all_utils.utils import Utils
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType

if __name__ == '__main__':
    # initializing spark session object
    spark = SparkSession.builder.master("local[*]").appName("route").getOrCreate()

    # Generate object of utils.
    utils_obj = Utils()

    # Generate dataframe from input file.
    df_1 = utils_obj.generate_dataframe(spark, r"D:\3.Spark\Project_airport\airline_management\dataset_schema"
                                               r"\route_schema.txt",
                                        r"D:\3.Spark\Project_airport\airline_management\dataset\raw\routes\routes_1.csv")

    df2 = utils_obj.generate_dataframe(spark, r"D:\3.Spark\Project_airport\airline_management\dataset_schema"
                                              r"\route_schema.txt",
                                       r"D:\3.Spark\Project_airport\airline_management\dataset\raw\routes\routes_2.csv")

    df3 = utils_obj.generate_dataframe(spark, r"D:\3.Spark\Project_airport\airline_management\dataset_schema"
                                              r"\route_schema.txt",
                                       r"D:\3.Spark\Project_airport\airline_management\dataset\raw\routes\routes_3.csv")

    df4 = utils_obj.generate_dataframe(spark, r"D:\3.Spark\Project_airport\airline_management\dataset_schema"
                                              r"\route_schema.txt",
                                       r"D:\3.Spark\Project_airport\airline_management\dataset\raw\routes\routes_4.csv")

    df5 = utils_obj.generate_dataframe(spark, r"D:\3.Spark\Project_airport\airline_management\dataset_schema"
                                              r"\route_schema.txt",
                                       r"D:\3.Spark\Project_airport\airline_management\dataset\raw\routes\routes_5.csv")

    df6 = utils_obj.generate_dataframe(spark, r"D:\3.Spark\Project_airport\airline_management\dataset_schema"
                                              r"\route_schema.txt",
                                       r"D:\3.Spark\Project_airport\airline_management\dataset\raw\routes\routes_6.csv")

    df7 = utils_obj.generate_dataframe(spark, r"D:\3.Spark\Project_airport\airline_management\dataset_schema"
                                              r"\route_schema.txt",
                                       r"D:\3.Spark\Project_airport\airline_management\dataset\raw\routes\routes_7.csv")

    print(df7.printSchema())
    print(df7.show())
