import pandas as pd
from faker import Faker
import random
from pathlib import Path

def generate_sales_data(num_rows, output_path):
    fake = Faker()
    data = []
    categories = ['Electronics', 'Clothing', 'Home & Garden', 'Books', 'Sports']
    payment_methods = ['Credit Card', 'Debit Card', 'PayPal', 'Cash']

    for _ in range(num_rows):
        user_id = fake.uuid4()
        user_name = fake.name()
        email = fake.email()
        age = random.randint(18, 80)
        product_id = fake.uuid4()
        product_name = fake.sentence(nb_words=3)
        category = random.choice(categories)
        price = round(random.uniform(10, 1000), 2)
        quantity = random.randint(1, 10)
        total_amount = round(price * quantity, 2)
        order_date = fake.date_this_year()
        delivery_date = fake.date_between(start_date=order_date)
        payment_method = random.choice(payment_methods)
        shipping_address = fake.address()
        city = fake.city()
        state = fake.state()
        country = 'USA'
        review_score = random.randint(1, 5)
        discount_applied = random.choice([True, False])
        is_returned = random.choice([True, False])

        data.append({
            'user_id': user_id,
            'user_name': user_name,
            'email': email,
            'age': age,
            'product_id': product_id,
            'product_name': product_name,
            'category': category,
            'price': price,
            'quantity': quantity,
            'total_amount': total_amount,
            'order_date': order_date,
            'delivery_date': delivery_date,
            'payment_method': payment_method,
            'shipping_address': shipping_address,
            'city': city,
            'state': state,
            'country': country,
            'review_score': review_score,
            'discount_applied': discount_applied,
            'is_returned': is_returned
        })

    df = pd.DataFrame(data)
    df.to_parquet(output_path, index=False)
    print(f"Generated {num_rows} rows of data to {output_path}")

if __name__ == "__main__":
    Path("data").mkdir(exist_ok=True)
    generate_sales_data(100000, "data/sales_100k.parquet")
    # generate_sales_data(1000000, "data/sales_1000k.parquet")  # Skip for now to avoid timeout