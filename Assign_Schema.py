import pandas as pd

class SchemaAssigner:
    def __init__(self):
        # Define the predefined column schemas
        self.columns_coupons = [
            {'name': 'coupon_id', 'type': 'STRING'},
            {'name': 'apply_till', 'type': 'TIMESTAMP'},
            {'name': 'applied_count', 'type': 'INT64'},
            {'name': 'subscription_id', 'type': 'STRING'}
        ]

        self.columns_customer = [
            {'name': 'id', 'type': 'STRING'},
            {'name': 'first_name', 'type': 'STRING'},
            {'name': 'last_name', 'type': 'STRING'},
            {'name': 'email', 'type': 'STRING'},
            {'name': 'company', 'type': 'STRING'},
            {'name': 'auto_collection', 'type': 'BOOL'},
            {'name': 'offline_payment_method', 'type': 'STRING'},
            {'name': 'net_term_days', 'type': 'INT64'},
            {'name': 'allow_direct_debit', 'type': 'BOOL'},
            {'name': 'created_at', 'type': 'TIMESTAMP'},
            {'name': 'created_from_ip', 'type': 'INT64'},
            {'name': 'taxability', 'type': 'STRING'},
            {'name': 'updated_at', 'type': 'TIMESTAMP'},
            {'name': 'pii_cleared', 'type': 'STRING'},
            {'name': 'channel', 'type': 'STRING'},
            {'name': 'resource_version', 'type': 'INT64'},
            {'name': 'deleted', 'type': 'BOOL'},
            {'name': 'card_status', 'type': 'STRING'},
            {'name': 'promotional_credits', 'type': 'INT64'},
            {'name': 'refundable_credits', 'type': 'INT64'},
            {'name': 'excess_payments', 'type': 'INT64'},
            {'name': 'unbilled_charges', 'type': 'INT64'},
            {'name': 'preferred_currency_code', 'type': 'STRING'},
            {'name': 'mrr', 'type': 'INT64'},
            {'name': 'tax_providers_fields', 'type': 'STRING'},
            {'name': 'auto_close_invoices', 'type': 'BOOL'},
            {'name': 'cf_payment_id', 'type': 'INT64'},
            {'name': 'billing_first_name', 'type': 'STRING'},
            {'name': 'billing_last_name', 'type': 'STRING'},
            {'name': 'billing_email', 'type': 'STRING'},
            {'name': 'billing_company', 'type': 'STRING'},
            {'name': 'billing_line1', 'type': 'STRING'},
            {'name': 'billing_city', 'type': 'STRING'},
            {'name': 'billing_country', 'type': 'STRING'},
            {'name': 'billing_zip', 'type': 'INT64'},
            {'name': 'billing_validation_status', 'type': 'STRING'},
            {'name': 'billing_object', 'type': 'STRING'},
            {'name': 'billing_state_code', 'type': 'STRING'},
            {'name': 'billing_state', 'type': 'STRING'}
        ]

        self.columns_item_tiers = [
            {'name': 'item_price_id', 'type': 'STRING'},
            {'name': 'starting_unit', 'type': 'INT64'},
            {'name': 'ending_unit', 'type': 'INT64'},
            {'name': 'price', 'type': 'INT64'},
            {'name': 'subscription_id', 'type': 'STRING'}
        ]

        self.columns_subscription_items = [
            {'name': 'item_price_id', 'type': 'STRING'},
            {'name': 'item_type', 'type': 'STRING'},
            {'name': 'quantity', 'type': 'FLOAT64'},
            {'name': 'unit_price', 'type': 'FLOAT64'},
            {'name': 'amount', 'type': 'FLOAT64'},
            {'name': 'free_quantity', 'type': 'INT64'},
            {'name': 'subscription_id', 'type': 'STRING'},
            {'name': 'metered_quantity', 'type': 'INT64'}
        ]

        self.columns_subscription = [
            {'name': 'id', 'type': 'STRING'},
            {'name': 'billing_period', 'type': 'INT64'},
            {'name': 'billing_period_unit', 'type': 'STRING'},
            {'name': 'customer_id', 'type': 'STRING'},
            {'name': 'status', 'type': 'STRING'},
            {'name': 'current_term_start', 'type': 'TIMESTAMP'},
            {'name': 'current_term_end', 'type': 'TIMESTAMP'},
            {'name': 'next_billing_at', 'type': 'TIMESTAMP'},
            {'name': 'created_at', 'type': 'TIMESTAMP'},
            {'name': 'started_at', 'type': 'TIMESTAMP'},
            {'name': 'activated_at', 'type': 'TIMESTAMP'},
            {'name': 'created_from_ip', 'type': 'INT64'},
            {'name': 'updated_at', 'type': 'TIMESTAMP'},
            {'name': 'has_scheduled_changes', 'type': 'BOOL'},
            {'name': 'channel', 'type': 'STRING'},
            {'name': 'resource_version', 'type': 'INT64'},
            {'name': 'deleted', 'type': 'BOOL'},
            {'name': 'coupon', 'type': 'STRING'},
            {'name': 'currency_code', 'type': 'STRING'},
            {'name': 'due_invoices_count', 'type': 'INT64'},
            {'name': 'due_since', 'type': 'TIMESTAMP'},
            {'name': 'total_dues', 'type': 'INT64'},
            {'name': 'mrr', 'type': 'INT64'},
            {'name': 'exchange_rate', 'type': 'INT64'},
            {'name': 'base_currency_code', 'type': 'STRING'},
            {'name': 'has_scheduled_advance_invoices', 'type': 'BOOL'},
            {'name': 'create_pending_invoices', 'type': 'BOOL'},
            {'name': 'auto_close_invoices', 'type': 'BOOL'},
            {'name': 'auto_collection', 'type': 'BOOL'},
            {'name': 'offline_payment_method', 'type': 'STRING'}
        ]


    def assign_schema(self, df_name):
        # Check if the DataFrame name is in the specified list
        if df_name.lower() == 'subscriptions':
            return self.columns_subscription
        elif df_name.lower() == 'subscription_items':
            return self.columns_subscription_items
        elif df_name.lower() == 'item_tiers':
            return self.columns_item_tiers
        elif df_name.lower() == 'coupons':
            return self.columns_coupons
        elif df_name.lower() == 'customers':
            return self.columns_customers
        else:
            print(f"No schema assigned for DataFrame {df_name}")
            return None

# Example usage
