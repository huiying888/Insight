import random
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from faker import Faker
import makedirectory

fake = Faker()
random.seed(42)
Faker.seed(42)
pd.random_state = 42
np.random.seed(42)

product_list = pd.read_csv("data/master_product.csv")

def sample_products(platform_id, num_products):
    selected_products = product_list.sample(n=num_products).reset_index(drop=True)
    selected_products.rename(columns={"master_product_code":"sku","base_cost":"cost","base_price":"price"}, inplace=True)
    selected_products['product_id'] = selected_products.index.map(lambda x: f"{platform_id}-PROD{x+1:04d}")
    selected_products['currency'] = "MYR"
    selected_products['sku'] = selected_products['sku'].map(lambda x: f"{platform_id}-{x}")
    selected_products['updated_at'] = fake.date_time_between(start_date="-2y", end_date="now")
    selected_products = selected_products.reindex(columns=["product_id","sku","name","category","brand","cost","price","currency","updated_at"])
    return selected_products

def random_inventory(products):
    df = pd.DataFrame(products['product_id'], columns=["product_id"])
    df['stock_qty'] = df['product_id'].map(lambda x: random.randint(1, 200))
    df['updated_at'] = products['updated_at'].map(lambda x: fake.date_time_between(start_date="-2y", end_date=x))
    return df

# Generate products for each platform
lazada_products = sample_products("LAZ", 80)
shopee_products = sample_products("SHP", 80)
tiktok_products = sample_products("TIK", 50)
pos_products = sample_products("POS", 20)

# Generate inventory for each platform
lazada_inventory = random_inventory(lazada_products)
shopee_inventory = random_inventory(shopee_products)
tiktok_inventory = random_inventory(tiktok_products)
pos_inventory = random_inventory(pos_products)

# Save to CSV
lazada_products.to_csv("data/src_lazada/products.csv", index=False)
shopee_products.to_csv("data/src_shopee/products.csv", index=False)
tiktok_products.to_csv("data/src_tiktok/products.csv", index=False)
pos_products.to_csv("data/src_pos/products.csv", index=False)

print("Generated src_lazada/products.csv")
print("Generated src_shopee/products.csv")
print("Generated src_tiktok/products.csv")
print("Generated src_pos/products.csv")

lazada_inventory.to_csv("data/src_lazada/inventory.csv", index=False)
shopee_inventory.to_csv("data/src_shopee/inventory.csv", index=False)
tiktok_inventory.to_csv("data/src_tiktok/inventory.csv", index=False)
pos_inventory.to_csv("data/src_pos/inventory.csv", index=False)

print("Generated src_lazada/inventory.csv")
print("Generated src_shopee/inventory.csv")
print("Generated src_tiktok/inventory.csv")
print("Generated src_pos/inventory.csv")