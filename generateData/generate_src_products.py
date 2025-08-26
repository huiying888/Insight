import random
import pandas as pd
import numpy as np
import datetime
import makedirectory

random.seed(42)
pd.random_state = 42
np.random.seed(42)

product_list = pd.read_csv("data/master_product.csv")

def sample_products(platform_id, num_products):
    selected_product = product_list.sample(n=num_products).reset_index(drop=True)
    selected_product.rename(columns={"master_product_code":"sku","base_cost":"cost","base_price":"price"}, inplace=True)
    selected_product['product_id'] = selected_product.index.map(lambda x: f"{platform_id}-PROD{x+1:04d}")
    selected_product['currency'] = "MYR"
    selected_product['sku'] = selected_product['sku'].map(lambda x: f"{platform_id}-{x}")
    selected_product = selected_product.reindex(columns=["product_id","sku","name","category","brand","cost","price","currency"])
    return selected_product

def random_inventory(product_ids):
    df = pd.DataFrame(product_ids, columns=["product_id"])
    df['stock_qty'] = df['product_id'].map(lambda x: random.randint(1, 200))
    return df

# Generate products for each platform
shopee_products = sample_products("SHP", 80)
lazada_products = sample_products("LAZ", 80)
tiktok_products = sample_products("TIK", 50)
pos_products = sample_products("POS", 20)

# Generate inventory for each platform
shopee_inventory = random_inventory(shopee_products['product_id'])
lazada_inventory = random_inventory(lazada_products['product_id'])
tiktok_inventory = random_inventory(tiktok_products['product_id'])
pos_inventory = random_inventory(pos_products['product_id'])

# Save to CSV
shopee_products.to_csv("data/src_shopee/products.csv", index=False)
lazada_products.to_csv("data/src_lazada/products.csv", index=False)
tiktok_products.to_csv("data/src_tiktok/products.csv", index=False)
pos_products.to_csv("data/src_pos/products.csv", index=False)

shopee_inventory.to_csv("data/src_shopee/inventory.csv", index=False)
lazada_inventory.to_csv("data/src_lazada/inventory.csv", index=False)
tiktok_inventory.to_csv("data/src_tiktok/inventory.csv", index=False)
pos_inventory.to_csv("data/src_pos/inventory.csv", index=False)