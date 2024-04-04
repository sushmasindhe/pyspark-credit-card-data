import random
import warnings
from datetime import datetime

from pyspark.sql import SparkSession

from src.utils.common_utils import CommonUtils
from src.utils.faker_utils import CommonFaker
from src.utils.spark_utils import CommonSpark
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

warnings.filterwarnings("ignore")


class GenerateHistDate:
    def __init__(self, spark, common_merchant_names, common_account_numbers, transactions_per_account, n_years, common_utils,
                 spark_utils, faker_utils, logger):
        self.spark = spark
        self.common_merchant_names = common_merchant_names
        self.common_account_numbers = common_account_numbers
        self.transactions_per_account = transactions_per_account
        self.n_years = n_years
        self.common_utils = common_utils
        self.spark_utils = spark_utils
        self.faker_utils = faker_utils
        self.logger = logger

    def generate_static_data(self):
        users_data = []
        credit_card_data = []
        user_schema = self.spark_utils.create_dataframe_with_schema(hist_config['user_schema_path'])
        credit_cards_schema = self.spark_utils.create_dataframe_with_schema(hist_config['credit_card_schema_path'])
        for account_number in self.common_account_numbers:
            # Generate user data
            user_details = self.faker_utils.generate_user_details(account_number)
            users_data.append((user_details["account_number"], user_details["name"], user_details["address"],
                               user_details["email"], user_details["phone_number"]))

            # Generate credit card data
            credit_details = self.faker_utils.generate_credit_card(account_number)
            credit_card_data.append((credit_details["account_number"], credit_details["card_number"],
                                     credit_details["expiration_date"], credit_details["cvv"]))

        credit_cards_df = self.spark.createDataFrame(credit_card_data, credit_cards_schema)
        users_df = self.spark.createDataFrame(users_data, user_schema)
        users_df.write.mode("overwrite").parquet(f"users_data/")
        credit_cards_df.write.mode("overwrite").parquet(f"credit_cards_data/")
        self.logger.info(f"Generated user data ")
        self.logger.info(f"Generated credit card data")

    def generate_monthly_data(self):
        transaction_schema = self.spark_utils.create_dataframe_with_schema(hist_config['transaction_schema_path'])
        for year in range(datetime.now().year - self.n_years, datetime.now().year):
            for month in range(1, 3):
                logger.info(f"Generating data for {year}-{month}...")
                transactions_data = []
                for account_number in self.common_account_numbers:
                    # Generate transactions data for each account
                    transactions_data.extend(
                        [self.faker_utils.generate_transaction(account_number, year, month) for _ in
                         range(transactions_per_account)])
                    logger.info(transactions_data)
                transactions_df = self.spark.createDataFrame(transactions_data, transaction_schema)
                logger.info(f"Generated transaction data for {year}-{month}")
                logger.info(f"Saving data for {year}-{month}...")
                transactions_df.write.mode("overwrite").parquet(f"transactions_data/{year}/{month}")
                logger.info(f"Saved data for {year}-{month}")


if __name__ == '__main__':
    common_utils = CommonUtils()
    logger = common_utils.create_logger()
    spark = SparkSession.builder.appName("GenerateHistData").getOrCreate()
    # .config("spark.driver.extraClassPath","mysql-connector-j-8.2.0.jar").getOrCreate()
    spark_utils = CommonSpark(spark, logger)
    hist_config = common_utils.json_reader("src/configs/hist_config.json")
    num_accounts = hist_config['num_accounts']
    total_records_per_month = hist_config['total_records_per_month']
    common_merchant_names = hist_config['common_merchant_names']
    n_years = hist_config['n_years']
    # Create account numbers for individual accounts
    common_account_numbers = [random.randint(1000000000, 9999999999) for _ in range(num_accounts)]
    # Calculate transactions per account
    transactions_per_account = total_records_per_month // num_accounts
    faker_utils = CommonFaker(common_account_numbers, transactions_per_account, common_merchant_names, logger)
    logger.info("Data generation started.")
    generate_hist_data = GenerateHistDate(spark, common_merchant_names, common_account_numbers, transactions_per_account,
                                          n_years, common_utils, spark_utils, faker_utils, logger)
    generate_hist_data.generate_static_data()
    generate_hist_data.generate_monthly_data()
    logger.info("Data generation completed.")
    spark.stop()