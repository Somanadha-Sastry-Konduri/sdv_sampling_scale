
from sdv.metadata import MultiTableMetadata
import pandas as pd
from sdv.multi_table import HMASynthesizer
from sdv.evaluation.multi_table import evaluate_quality, run_diagnostic
import warnings
import logging

logging.basicConfig(filename='/home/ubuntu/datastes/log_file.txt', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


orders_df = pd.read_csv(
    r"/home/ubuntu/datastes/raw_data/orders.csv")
transactions_df = pd.read_csv(
    r"/home/ubuntu/datastes/raw_data/transactions.csv")

logging.info("Data read from local")

# Check for duplicates and drop it if any
orders_df = orders_df.drop_duplicates(keep="last")

# fetch the date columns and fixed that
date_columns = list(orders_df.columns)[5:]

# Convert these columns to datetime
orders_df[date_columns] = orders_df[date_columns].apply(pd.to_datetime)

# There are some orders where order_approved_at date is greater than order_delivered_carrier date, so exclude that
orders_df = orders_df[
    ~(orders_df["order_approved_at"] >
    orders_df["order_delivered_carrier_date"])
]

"""
There is a record where order_delivered_carrier date is greater than order_delivered_customer date,
thus it's not possible because first the order will shipped and then it will be delivered to customer
"""
orders_df = orders_df[
    ~(
        orders_df["order_delivered_carrier_date"]
        > orders_df["order_delivered_customer_date"]
    )
]

# Remove customer_id from orders table, as we don't want to generate it via synthetic data
orders_df.drop("customer_id", axis=1, inplace=True)

# Check for duplicates and drop it if any
orders_df = orders_df.drop_duplicates(keep="last")
orders_df = orders_df.dropna()

# Check for duplicates and drop it if any
transactions_df = transactions_df.drop_duplicates(keep="last")

# There are records where qty is 0, but in actual there won't be any transactions where quantity is 0
transactions_df = transactions_df[transactions_df["quantity"] > 0]

# fetch the discount values where discount < 0.8
transactions_df = transactions_df[transactions_df["discount"] < 0.8]

# exclude blank values from profit_amt of transactions column
transactions_df = transactions_df[~(transactions_df["profit"].isna())]

# Remove product_id from transactions table, as we don't want to generate it via synthetic data
transactions_df.drop("product_id", axis=1, inplace=True)

# Take the sample of first 1000 records of orders
orders_df = pd.concat(
    [
        orders_df[orders_df["order_status"] == status].head(1000)
        for status in orders_df["order_status"].unique()
    ]
)

# Now fetch all the unique order_id
orders = list(orders_df["order_id"].unique())

# Only fetch those records where order_id of transactions is there in orders table
transactions_df = transactions_df[transactions_df["order_id"].isin(orders)]

# fetch the sample of transaction table
transactions_df = transactions_df.head(1000)

transactions_df = transactions_df.dropna()

orders_df.reset_index(drop=True, inplace=True)
transactions_df.reset_index(drop=True, inplace=True)

metadata = MultiTableMetadata()

metadata.detect_table_from_dataframe(
    table_name='orders',
    data=orders_df
)
metadata.detect_table_from_dataframe(
    table_name='transactions',
    data=transactions_df
)

# 1. Update orders table metadata
metadata.update_column(
    table_name="orders",
    column_name="order_id",
    sdtype="id",
    regex_format=r"[A-Z]{2}-\d{4}-\d{5}",
)

metadata.update_column(
    table_name="orders",
    column_name="order_purchase_date",
    sdtype="datetime",
    datetime_format=r"%Y-%m-%d %H:%M:%S",
)

metadata.update_column(
    table_name="orders",
    column_name="order_approved_at",
    sdtype="datetime",
    datetime_format=r"%Y-%m-%d %H:%M:%S",
)

metadata.update_column(
    table_name="orders",
    column_name="order_delivered_carrier_date",
    sdtype="datetime",
    datetime_format=r"%Y-%m-%d %H:%M:%S",
)

metadata.update_column(
    table_name="orders",
    column_name="order_delivered_customer_date",
    sdtype="datetime",
    datetime_format=r"%Y-%m-%d %H:%M:%S",
)

metadata.update_column(
    table_name="orders",
    column_name="order_estimated_delivery_date",
    sdtype="datetime",
    datetime_format=r"%Y-%m-%d",
)

# 2. Update transactions table metadata
metadata.update_column(
    table_name="transactions", column_name="transaction_id", sdtype="id", regex_format=r"\d{7}"
)

metadata.update_column(
    table_name="transactions",
    column_name="order_id",
    sdtype="id",
    regex_format=r"[A-Z]{2}-\d{4}-\d{5}",
)

metadata.update_column(
    table_name="transactions",
    column_name="sales",
    sdtype="numerical",
    computer_representation="Float",
)

metadata.update_column(
    table_name="transactions",
    column_name="quantity",
    sdtype="numerical",
    computer_representation="Int64",
)

metadata.update_column(
    table_name="transactions",
    column_name="discount",
    sdtype="numerical",
    computer_representation="Float",
)

metadata.update_column(
    table_name="transactions",
    column_name="profit",
    sdtype="numerical",
    computer_representation="Float",
)

# Set primary key of order and transactions table
metadata.set_primary_key(table_name="orders", column_name="order_id")

metadata.set_primary_key(table_name="transactions",
                        column_name="transaction_id")

# Add relationships between both tables
metadata.add_relationship(
    parent_table_name="orders",
    child_table_name="transactions",
    parent_primary_key="order_id",
    child_foreign_key="order_id",
)

synthesizer = HMASynthesizer(metadata)

# Add constraints to orders table
synthesizer.add_constraints(
    constraints=[
        {
            "constraint_class": "Inequality",
            "table_name": "orders",
            "constraint_parameters": {
                "low_column_name": "order_purchase_date",
                "high_column_name": "order_approved_at",
            },
        },
        {
            "constraint_class": "Inequality",
            "table_name": "orders",
            "constraint_parameters": {
                "low_column_name": "order_purchase_date",
                "high_column_name": "order_delivered_carrier_date",
            },
        },
        {
            "constraint_class": "Inequality",
            "table_name": "orders",
            "constraint_parameters": {
                "low_column_name": "order_purchase_date",
                "high_column_name": "order_delivered_customer_date",
            },
        },
        {
            "constraint_class": "Inequality",
            "table_name": "orders",
            "constraint_parameters": {
                "low_column_name": "order_purchase_date",
                "high_column_name": "order_estimated_delivery_date",
            },
        },
        {
            "constraint_class": "Inequality",
            "table_name": "orders",
            "constraint_parameters": {
                "low_column_name": "order_approved_at",
                "high_column_name": "order_delivered_carrier_date",
            },
        },
        {
            "constraint_class": "Inequality",
            "table_name": "orders",
            "constraint_parameters": {
                "low_column_name": "order_approved_at",
                "high_column_name": "order_delivered_customer_date",
            },
        },
        {
            "constraint_class": "Inequality",
            "table_name": "orders",
            "constraint_parameters": {
                "low_column_name": "order_approved_at",
                "high_column_name": "order_estimated_delivery_date",
            },
        },
        {
            "constraint_class": "Inequality",
            "table_name": "orders",
            "constraint_parameters": {
                "low_column_name": "order_delivered_carrier_date",
                "high_column_name": "order_delivered_customer_date",
            },
        },
    ]
)

# Add constraints to transactions table
synthesizer.add_constraints(
    constraints=[
        {
            "constraint_class": "Positive",
            "table_name": "transactions",
            "constraint_parameters": {
                "column_name": "sales",
                "strict_boundaries": True,
            },
        },
        {
            "constraint_class": "Positive",
            "table_name": "transactions",
            "constraint_parameters": {
                "column_name": "quantity",
                "strict_boundaries": True,
            },
        },
        {
            "constraint_class": "Inequality",
            "table_name": "transactions",
            "constraint_parameters": {
                "low_column_name": "profit",
                "high_column_name": "sales",
                "strict_boundaries": True,
            },
        },
    ]
)

real_data = {"orders": orders_df, "transactions": transactions_df}


synthesizer.fit(real_data)

logging.info("model training complete")

for i in range(10):
    synthetic_data = synthesizer.sample(scale=10)
    
    logging.info(f"sample data ready for iteration {i+1} is ready")
    
    synthetic_data_orders_df = synthetic_data["orders"]    
    synthetic_data_orders_df.to_csv(f"/home/ubuntu/datastes/synthetic_data/synthetic_orders_{i+1}", index=False)
    logging.info(f"synthetic orders data for iteration {i+1} written to destination")  

    synthetic_data_transactions_df = synthetic_data["transactions"]
    synthetic_data_transactions_df.to_csv(f"/home/ubuntu/datastes//synthetic_data/synthetic_transactions_{i+1}", index=False)
    logging.info(f"synthetic transactions data for iteration {i+1} written to destination")

