from faker import Faker
import random
from datetime import datetime, timedelta


class CommonFaker:
    # Dictionary to store unique account details for each account
    account_card_details = {}

    # Dictionary to store unique user details for each account
    account_user_details = {}

    fake = Faker()

    def __init__(self, common_account_numbers, transactions_per_account, common_merchant_names, logger):
        self.common_account_numbers = common_account_numbers
        self.transactions_per_account = transactions_per_account
        self.common_merchant_names = common_merchant_names
        self.logger = logger

    def generate_user_details(self, account_number):
        # Generate and retain user details (name, email, phone number, and address) per account
        if account_number not in self.account_user_details:
            name = self.fake.name()
            email = self.fake.email()
            phone_number = self.fake.numerify(text='(###) ###-####')
            address = self.fake.address()

            self.account_user_details[account_number] = {
                "name": name,
                "email": email,
                "phone_number": phone_number,
                "address": address
            }

        return {
            "account_number": str(account_number),
            "name": self.account_user_details[account_number]["name"],
            "email": self.account_user_details[account_number]["email"],
            "phone_number": self.account_user_details[account_number]["phone_number"],
            "address": self.account_user_details[account_number]["address"]
        }

    def generate_credit_card(self, account_number):
        # Generate and retain a unique card number, expiration date, and CVV per account
        if account_number not in self.account_card_details:
            card_number = self.fake.credit_card_number(
                card_type='visa')  # Generate a unique card number for each account
            expiration_date = self.fake.credit_card_expire()  # Generate a self.fake expiration date
            cvv = str(random.randint(100, 999))  # Generate a random CVV code
            self.account_card_details[account_number] = {
                "card_number": card_number,
                "expiration_date": expiration_date,
                "cvv": cvv
            }

        return {
            "account_number": str(account_number),
            "card_number": self.account_card_details[account_number]["card_number"],
            "expiration_date": self.account_card_details[account_number]["expiration_date"],
            "cvv": self.account_card_details[account_number]["cvv"]
        }

    def generate_transaction(self, account_number, year, month):
        # Calculate the start and end date of the month
        start_date = datetime(year, month, 1)
        end_date = start_date.replace(day=28) + timedelta(days=4)  # Move to the 4th day of the next month

        # Generate a transaction date within the same month
        transaction_date = self.fake.date_time_between(start_date=start_date, end_date=end_date)
        merchant_name = random.choice(self.common_merchant_names)  # Select a random merchant name from the list
        transaction_amount = round(random.uniform(10, 500), 2)  # Generate a random transaction amount

        return {
            "account_number": str(account_number),
            "transaction_date": transaction_date,
            "merchant_name": merchant_name,
            "transaction_amount": transaction_amount
        }
