import pandas as pd
import numpy as np
from faker import Faker
from pathlib import Path
import argparse
import gc

fake = Faker()

def generate_vectorized_data_chunk(chunk_size, categories, payment_methods):
    # Simple strings for speed
    product_names = [f"Product {i}" for i in range(10000)]
    shipping_addresses = [f"Address {i}" for i in range(10000)]
    cities = [f"City {i}" for i in range(10000)]
    states = [f"State {i}" for i in range(10000)]

    # Vectorized generation
    user_ids = np.array([f"user_{i}" for i in range(chunk_size)])  # Simple IDs
    user_names = np.array([f"User {i}" for i in range(chunk_size)])
    emails = np.array([f"user{i}@example.com" for i in range(chunk_size)])
    ages = np.random.randint(18, 81, size=chunk_size)
    product_ids = np.array([f"prod_{i}" for i in range(chunk_size)])
    product_names_sampled = np.random.choice(product_names, size=chunk_size)
    categories_sampled = np.random.choice(categories, size=chunk_size)
    prices = np.round(np.random.uniform(10.0, 1000.0, size=chunk_size), 2)
    quantities = np.random.randint(1, 11, size=chunk_size)
    total_amounts = np.round(prices * quantities, 2)

    # Dates
    order_dates = pd.to_datetime(
        np.random.randint(
            pd.to_datetime('2020-01-01').value // 10**9,
            pd.to_datetime('2023-12-31').value // 10**9,
            size=chunk_size
        ),
        unit='s'
    )
    delivery_dates = order_dates + pd.to_timedelta(np.random.randint(1, 31, size=chunk_size), unit='D')

    payment_methods_sampled = np.random.choice(payment_methods, size=chunk_size)
    shipping_addresses_sampled = np.random.choice(shipping_addresses, size=chunk_size)
    cities_sampled = np.random.choice(cities, size=chunk_size)
    states_sampled = np.random.choice(states, size=chunk_size)
    review_scores = np.random.randint(1, 6, size=chunk_size)
    discount_applied = np.random.choice([True, False], size=chunk_size)
    is_returned = np.random.choice([True, False], size=chunk_size)

    df = pd.DataFrame({
        'user_id': user_ids,
        'user_name': user_names,
        'email': emails,
        'age': ages,
        'product_id': product_ids,
        'product_name': product_names_sampled,
        'category': categories_sampled,
        'price': prices,
        'quantity': quantities,
        'total_amount': total_amounts,
        'order_date': order_dates,
        'delivery_date': delivery_dates,
        'payment_method': payment_methods_sampled,
        'shipping_address': shipping_addresses_sampled,
        'city': cities_sampled,
        'state': states_sampled,
        'country': 'USA',
        'review_score': review_scores,
        'discount_applied': discount_applied,
        'is_returned': is_returned
    })
    return df

def generate_sales_data(num_rows, output_path, chunk_size=200000):
    Path("data").mkdir(exist_ok=True)
    categories = ['Electronics', 'Clothing', 'Home & Garden', 'Books', 'Sports']
    payment_methods = ['Credit Card', 'Debit Card', 'PayPal', 'Cash']

    # Generate in chunks to manage memory
    dfs = []
    for i in range(0, num_rows, chunk_size):
        current_chunk_size = min(chunk_size, num_rows - i)
        print(f"Generating chunk {i // chunk_size + 1} with {current_chunk_size} rows...")
        chunk_df = generate_vectorized_data_chunk(current_chunk_size, categories, payment_methods)
        dfs.append(chunk_df)
        gc.collect()  # Free memory

    # Concatenate and save
    full_df = pd.concat(dfs, ignore_index=True)
    full_df.to_parquet(output_path, index=False)
    print(f"Generated {num_rows} rows of data to {output_path}")
    del full_df
    gc.collect()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic sales data")
    parser.add_argument("--rows", type=int, required=True, help="Number of rows to generate")
    args = parser.parse_args()

    output_path = f"data/sales_{args.rows}.parquet"
    generate_sales_data(args.rows, output_path)