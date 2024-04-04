import json
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType


class CommonSpark:
    def __init__(self, spark, logger):
        self.spark = spark
        self.logger = logger

    @staticmethod
    def create_dataframe_with_schema(schema_file):
        with open(schema_file, 'r') as file:
            schema_data = json.load(file)
            fields = schema_data['fields']

        # Create the PySpark StructType schema
        spark_schema = StructType([
            StructField(field['name'], StringType() if field['type'] == 'string' else
            TimestampType() if field['type'] == 'timestamp' else FloatType(), True)
            for field in fields
        ])

        return spark_schema

    @staticmethod
    def write_to_db(df, url, driver, table, user, password):
        df.write.format("jdbc") \
            .option("url", url) \
            .option("driver", driver) \
            .option("dbtable", table) \
            .option("user", user) \
            .option("password", password) \
            .mode("overwrite") \
            .save()
