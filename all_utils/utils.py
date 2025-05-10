#######################################################################################################################
# Name : utils.py
# Description : Contains all comman code through the project.
# Date : 28-07-2024
# Version : 0
# Modified Date :
# Modification Description :
#######################################################################################################################

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType


#
class Utils:

    def generate_schema(self, schema_path, schema_type_sep=' '):
        """
        This Method will generate a spark schema based on input file should have column name and data type.
        :param schema_path: text file path.
        :return: spark schema object.
        """

        # Generate Schema using schema files.
        schema_content = open(f"{schema_path}", "r").readlines()

        # Generate struct field value from schema list
        str_field = []
        for val in schema_content:
            data = val.split(f"{schema_type_sep}")
            col_nm = data[0]
            datatype = data[1]
            if 'int' in datatype.lower():
                str_field.append(StructField(col_nm, IntegerType()))
            elif 'string' in datatype.lower():
                str_field.append(StructField(col_nm, StringType()))
            elif 'decimal' in datatype.lower():
                str_field.append(StructField(col_nm, DecimalType()))

        return str_field

    def generate_dataframe(self, spark, schema_path, input_file_path, file_formate= "csv"):
        """
        The method will generate pyspark dataframe based on input location.
        :param spark: spark session object.
        :param schema_path: input schema path.
        :param input_file_path: input file path.
        :param file_formate: input file format.
        :return: pyspark dataframe.
        """
        str_field = self.generate_schema(f"{schema_path}")
        # Generate pyspark schema object
        input_schema = StructType(str_field)
        print(input_schema)

        # Generate dataframe from input data and input schema
        df = spark.read.format(f"{file_formate}").schema(input_schema).load(f"{input_file_path}")
        print(df.printSchema())

        return df
