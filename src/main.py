import json
import os

import pandas as pd


# Creates three directories for the medallion architecture, gold, silver, and bronze, within the data directory
def create_layers():
    data_directory = "data"
    directories = ["bronze", "silver", "gold"]

    for directory in directories:
        directory_path = os.path.join(data_directory, directory)
        if check_directory(directory_path):
            os.makedirs(directory_path)


# ----------------- Bronze Layer -----------------


# Open the file and load the data
def open_file(file):
    with open(file) as f:
        data = json.load(f)
        return data


# Check if directories exist before creating them
def check_directory(directory_path):
    if not os.path.exists(directory_path):
        return True
    else:
        return False


# Converts JSON data into a tabular format using pandas
def convert_to_tabular(data):
    df = pd.json_normalize(data)
    return df


# From the dataframe of data generated, get me basic statistics of every row
def get_basic_statistics(df):
    return df.describe()


# Create a folder with date of the import, within the bronze directory, and drop there the dataframe as csv file
def save_bronze_data(df_bronze_data):
    today = pd.Timestamp("today").strftime("%Y-%m-%d")
    directory_path = f"data/bronze/{today}"
    os.makedirs(directory_path, exist_ok=True)
    df_bronze_data.to_csv(f"{directory_path}/data.csv", index=False)

# Create the bronze layer. This runs on daily basis


def create_bronze_layer():
    data = open_file("data/customers.json")
    customer_data = data["customers"]
    df_bronze_data = convert_to_tabular(customer_data)
    save_bronze_data(df_bronze_data)


# ----------------- Silver Layer -----------------


# Load the data from the bronze layer
def load_bronze_data():
    today = pd.Timestamp("today").strftime("%Y-%m-%d")
    directory_path = f"data/bronze/{today}"
    file_path = f"{directory_path}/data.csv"
    return pd.read_csv(file_path)


# Rearrage the data from the bronze to sort it by transactions. Data is stored denormalized
def rearrange_data(df):
    # Convert DataFrame rows to tuples
    data_tuples = df.apply(dict, axis=1).tolist()

    transactions = []
    for row in data_tuples:

        for transaction in json.loads(row["transactions"].replace("'", '"')):
            transaction["customer_id"] = row["id"]
            transaction["customer_name"] = row["name"]
            transaction["customer_email"] = row["email"]
            transaction["signup_date"] = row["signup_date"]
            transaction["last_purchase"] = row["last_purchase"]
            transaction["total_spent"] = row["total_spent"]
            transactions.append(transaction)

    return pd.DataFrame(transactions)

# Create raw tables for transactions, that can be used for further analysis


def create_transaction_table(df):
    return df[["transaction_id", "date", "amount", "product_id", "customer_id"]]


# Create raw tables for customers, that can be used for further analysis
def create_customer_table(df):
    df_customer = df.groupby(["customer_id", "customer_name",
                             "customer_email", "signup_date"]).size().reset_index(name="count")
    df_customer = df_customer[["customer_id",
                               "customer_name", "customer_email", "signup_date"]]
    return df_customer

# Create raw tables for product, that can be used for further analysis


def create_product_table(df):
    df_product = df.groupby(["product_id", "product_name"]
                            ).size().reset_index(name="count")
    df_product = df_product[["product_id", "product_name"]]
    return df_product


# Serialize the data to a csv file
def save_silver_data(df, table_name):
    today = pd.Timestamp("today").strftime("%Y-%m-%d")
    directory_path = f"data/silver/{today}"
    os.makedirs(directory_path, exist_ok=True)
    df.to_csv(
        f"{directory_path}/{table_name}.csv", index=False)


# ---------------------- Sanity Check ----------------------
# Sanity check for the data on the denormliazed table, this sanity check, will be saved in the sanity table

# Process the transactions column
def get_last_transaction(x):
    try:
        transaction_object = json.loads(x.replace("'", '"'))
        date = None
        for transaction in transaction_object:
            transaction["date"] = pd.to_datetime(transaction["date"])
            if date is None or transaction["date"] > date:
                date = transaction["date"]

        return date
    except Exception as e:
        print(e)
        return 0

# Process the total amount per customer


def get_total_spent(x):
    try:
        transaction_object = json.loads(x.replace("'", '"'))
        total = 0
        for transaction in transaction_object:
            total += pd.to_numeric(transaction["amount"])
        return total
    except Exception as e:
        print(e)
        return 0


def sanity_check(df):
    # create a clone of the dataframe
    df_sanity = df.copy()

    df_sanity["signup_date"] = pd.to_datetime(df["signup_date"])
    df_sanity["last_purchase"] = pd.to_datetime(df["last_purchase"])
    df_sanity["total_spent"] = pd.to_numeric(df["total_spent"])

    # Check that the last_purchase date is greater than the signup_date
    df_sanity["last_purchase_vs_signup"] = df_sanity["last_purchase"] > df_sanity["signup_date"]

    df_sanity["last_transaction_date"] = df_sanity["transactions"].apply(
        lambda x: get_last_transaction(x))
    df_sanity["total_transaction_spent"] = df_sanity["transactions"].apply(
        lambda x: get_total_spent(x))

    # Check that the last_purchase date is the same or greater than the transaction date
    df_sanity["last_purchase_vs_transaction"] = df_sanity["last_purchase"] >= df_sanity["last_transaction_date"]

    # Check that the total_spent is the sum of all the transactions for the customer
    df_sanity["total_spent_vs_transaction"] = df_sanity["total_spent"] == df_sanity["total_transaction_spent"]

    # Check that the email is a valid email
    df_sanity["email_valid"] = df_sanity["email"].str.contains(r'[^@]+@[^@]+\.[^@]+')

    return df_sanity


# Create the silver layer. This runs on daily basis
def create_silver_layer():
    df_bronze_data = load_bronze_data()
    df_denormalized = rearrange_data(df_bronze_data)
    df_sanitized = sanity_check(df_bronze_data)

    df_transactions = create_transaction_table(df_denormalized)
    df_customers = create_customer_table(df_denormalized)
    df_products = create_product_table(df_denormalized)

    save_silver_data(df_sanitized, "sanitation")
    save_silver_data(df_denormalized, "denormalized")
    save_silver_data(df_transactions, "transactions")
    save_silver_data(df_customers, "customers")
    save_silver_data(df_products, "products")


# ----------------- Golden Layer -----------------

# Load the data from the silver layer
def load_silver_data(table_name):
    today = pd.Timestamp("today").strftime("%Y-%m-%d")
    directory_path = f"data/silver/{today}"
    file_path = f"{directory_path}/{table_name}.csv"
    return pd.read_csv(file_path)

# Append the data to an existing table, only if the data is new


def append_to_table(df, file_path):
    existing_data = pd.read_csv(file_path)

    df_combined = pd.merge(existing_data, df, how='outer', indicator=True)
    new_rows = df_combined[df_combined['_merge'] == 'right_only']

    # Drop the _merge column
    new_data = new_rows.drop(columns=['_merge'])

    # Append the new data to the existing table
    if not new_data.empty:
        print(existing_data)
        # existing_data = existing_data.append(new_data, ignore_index=True)
        existing_data = pd.concat([existing_data, new_data], ignore_index=True)
        existing_data.to_csv(file_path, index=False)


# Save the data to the golden layer, in an incremental way
def save_golden_data(df, table_name):
    directory_path = "data/gold/"
    file_path = f"{directory_path}/{table_name}.csv"
    if os.path.exists(file_path):
        append_to_table(df, file_path)
    else:
        df.to_csv(file_path, index=False)


# Create the golden layer. This runs on daily basis, but it is incremental, there is no need to create the golden layer from scratch
def create_update_golden_layer():
    entities = [
        {"table_name": "transactions", "type": "fact"},
        {"table_name": "customers", "type": "dimension"},
        {"table_name": "products", "type": "dimension"}
    ]

    for entity in entities:
        df_silver_data = load_silver_data(entity["table_name"])
        save_golden_data(df_silver_data, f"{entity['type']}_{entity['table_name']}")


if __name__ == "__main__":
    create_layers()

    # Create the bronze layer
    create_bronze_layer()

    # Create the silver layer
    create_silver_layer()

    # Crete the golden layer
    create_update_golden_layer()
