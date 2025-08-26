import random
import pandas as pd
import numpy as np
import datetime
import makedirectory

random.seed(42)
pd.random_state = 42
np.random.seed(42)

lazada_products = pd.read_csv("data/src_lazada/products.csv")
shopee_products = pd.read_csv("data/src_shopee/products.csv")
tiktok_products = pd.read_csv("data/src_tiktok/products.csv")
pos_products = pd.read_csv("data/src_pos/products.csv")
j=1

def generate_orders(platform_id, products_df, num_orders, j):
    for i in range(1, num_orders + 1):
        num_items = random.randint(1, 5)
        order_items = products_df.sample(n=num_items).reset_index(drop=True)[['product_id','name','sku','category','price']]
        order_items['order_id'] = f"{platform_id}-ORD{i:08d}"
        order_items['item_id'] = f"{platform_id}-ITEM{j:08d}"
        order_items['qty'] = order_items['product_id'].map(lambda x: random.randint(1, 3))
        order_items['discount'] = order_items['price'].map(lambda x: np.random.randint(0,10) if x > 50 else 0)
        order_items['shipping_fee'] = order_items['price'].map(lambda x: 0 if x > 75 else np.random.randint(0,15))
        order_items['tax'] = order_items.apply(lambda x: round(x['price'] * x['qty'] * 0.06, 2), axis=1)
        order_items = order_items.reindex(columns=['order_id','item_id','product_id','name','category','sku','qty','price','discount','shipping_fee','tax'])
        order_items.rename(columns={'name':'product_name'}, inplace=True)

        if i == 1:
            all_orders = order_items
        else:
            all_orders = pd.concat([all_orders, order_items], ignore_index=True)
        
        j += 1

    return all_orders


def generate_receipt(products_df, num_orders, j):
    for i in range(1, num_orders + 1):
        num_items = random.randint(1, 5)
        receipt_items = products_df.sample(n=num_items).reset_index(drop=True)[['product_id','name','sku','category','price']]
        receipt_items['receipt_id'] = f"REC{i:08d}"
        receipt_items['line_id'] = f"LINE{j:08d}"
        receipt_items['qty'] = receipt_items['product_id'].map(lambda x: random.randint(1, 3))
        receipt_items['line_discount'] = receipt_items['price'].map(lambda x: np.random.randint(0,10) if x > 50 else 0)
        receipt_items['line_tax'] = receipt_items.apply(lambda x: round(x['price'] * x['qty'] * 0.06, 2), axis=1)
        receipt_items['line_total'] = receipt_items.apply(lambda x: round(x['price'] * x['qty'] + x['line_tax'], 2), axis=1)
        receipt_items = receipt_items.reindex(columns=['receipt_id','line_id','product_id','name','category','sku','qty','price','line_discount','line_tax'])
        receipt_items.rename(columns={'name':'product_name','price':'unit_price'}, inplace=True)

        if i == 1:
            all_orders = receipt_items
        else:
            all_orders = pd.concat([all_orders, receipt_items], ignore_index=True)
        
        j += 1

    return all_orders

generate_orders("LAZ", lazada_products, 100, j).to_csv("data/src_lazada/order_items.csv", index=False)
generate_orders("SHP", shopee_products, 100, j).to_csv("data/src_shopee/order_items.csv", index=False)
generate_orders("TIK", tiktok_products, 100, j).to_csv("data/src_tiktok/order_items.csv", index=False)
generate_receipt(pos_products, 100, j).to_csv("data/src_pos/receipt_lines.csv", index=False)