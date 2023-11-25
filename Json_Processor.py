import pandas as pd
from datetime import datetime
import json
import ast


class JSONProcessor:
    def __init__(self):
        # Initialize empty lists to store flattened records for each nested sub-JSON
        self.flattened_sub_items = []
        self.flattened_item_tiers = []
        self.flattened_coupons = []
        self.flattened_subscriptions = []
        self.flattened_customers = []
        self.json_data = None

    def load_json_file(self, file_path):
        try:
            # Open the JSON file in read mode with UTF-8 encoding
            with open(file_path, 'r', encoding='utf-8') as file:
                # Use ast.literal_eval to parse the content as a Python literal
                content_literal = ast.literal_eval(file.read())

                # Use json.dumps to convert the Python literal to a JSON-formatted string
                json_string = json.dumps(content_literal)

                # Use json.loads to parse the JSON-formatted string and create a JSON object
                self.json_data = json.loads(json_string)

        except (SyntaxError, json.JSONDecodeError) as e:
            # Handle errors related to syntax or JSON decoding
            print(f"Error: {e}")

    def process_subscription(self, subscription_data):
        # Extract and append the main 'subscription' data
        self.flattened_subscriptions.append(subscription_data)

        # Extract and append 'subscription_items'
        if 'subscription_items' in subscription_data:
            for item in subscription_data['subscription_items']:
                item['subscription_id'] = subscription_data['id']  # Add subscription_id as a common key
                self.flattened_sub_items.append(item)

        # Extract and append 'item_tiers'
        if 'item_tiers' in subscription_data:
            for tier in subscription_data['item_tiers']:
                tier['subscription_id'] = subscription_data['id']  # Add subscription_id as a common key
                self.flattened_item_tiers.append(tier)

        # Extract and append 'coupons'
        if 'coupons' in subscription_data:
            for coupon in subscription_data['coupons']:
                coupon['subscription_id'] = subscription_data['id']  # Add subscription_id as a common key
                self.flattened_coupons.append(coupon)

    def process_customer(self, customer_data):
        # Extract and append the main 'customer' data
        self.flattened_customers.append(customer_data)

        # Extract information from the nested dictionary within 'customer' and create new columns
        if 'billing_address' in customer_data:
            billing_address_data = customer_data['billing_address']
            for key, value in billing_address_data.items():
                new_column_name = f"billing_{key}"
                customer_data[new_column_name] = value

    def process_json_data(self):
        # Process each record in the JSON data
        if self.json_data:
            data_list = self.json_data.get('list', [])
            for record in data_list:
                # Check if 'subscription' key is present in the record
                if 'subscription' in record:
                    self.process_subscription(record['subscription'])

                # Check if 'customer' key is present in the record
                if 'customer' in record:
                    self.process_customer(record['customer'])

    def create_tables(self):
        # Create DataFrames for the flattened records
        subscriptions_table = pd.DataFrame(self.flattened_subscriptions)
        subscription_items_table = pd.DataFrame(self.flattened_sub_items)
        item_tiers_table = pd.DataFrame(self.flattened_item_tiers)
        coupons_table = pd.DataFrame(self.flattened_coupons)
        customers_table = pd.DataFrame(self.flattened_customers)

        return subscriptions_table, subscription_items_table, item_tiers_table, coupons_table, customers_table

    def remove_columns(df, columns_to_remove):
        # Remove specific columns from a DataFrame
        return df.drop(columns=columns_to_remove, errors='ignore')

    def convert_to_dates(df, date_columns):
        # Convert specific columns in a DataFrame to datetime format
        for column in date_columns:
            df[column] = pd.to_datetime(df[column], unit='s', origin='unix', errors='coerce')
        return df


# # Usage example:
# # Instantiate the class
# json_processor = JSONProcessor()
#
# # Load and process the JSON data from the file
# json_processor.load_json_file('/Users/dimitriskatsogiannis/Downloads/analytics-engineer-test/etl.json')
# json_processor.process_json_data()
#
# # Create tables
# subscriptions, subscription_items, item_tiers, coupons, customers = json_processor.create_tables()
#
# # Process 1: Remove specific columns from tables
# subscriptions = JSONProcessor.remove_columns(subscriptions, ['subscription_items', 'item_tiers', 'coupons', 'object'])
# subscription_items = JSONProcessor.remove_columns(subscription_items, ['object'])
# item_tiers = JSONProcessor.remove_columns(item_tiers, ['object'])
# coupons = JSONProcessor.remove_columns(coupons, ['object'])
# customers = JSONProcessor.remove_columns(customers, ['object', 'billing_address'])
#
# # Process 2: Convert specific values to dates
# subscriptions = JSONProcessor.convert_to_dates(subscriptions,
#                                                ['current_term_start', 'current_term_end', 'next_billing_at',
#                                                 'created_at', 'started_at', 'activated_at', 'updated_at', 'due_since'])
# coupons = JSONProcessor.convert_to_dates(coupons, ['apply_till'])
# customers = JSONProcessor.convert_to_dates(customers, ['created_at', 'updated_at'])
