import random
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from faker import Faker
import makedirectory

Faker.seed(42)
fake = Faker()
random.seed(42)
pd.random_state = 42
np.random.seed(42)

lazada_products = pd.read_csv("data/src_lazada/products.csv")
lazada_customers = pd.read_csv("data/src_lazada/customers.csv")
shopee_products = pd.read_csv("data/src_shopee/products.csv")
shopee_customers = pd.read_csv("data/src_shopee/customers.csv")
tiktok_products = pd.read_csv("data/src_tiktok/products.csv")
tiktok_customers = pd.read_csv("data/src_tiktok/customers.csv")
tiktok_influencers = pd.read_csv("data/src_tiktok/influencers.csv")
tiktok_campaigns = pd.read_csv("data/src_tiktok/campaigns.csv")
pos_products = pd.read_csv("data/src_pos/products.csv")
pos_customers = pd.read_csv("data/src_pos/customers.csv")
pos_terminals = pd.read_csv("data/src_pos/terminals.csv")
pos_cashiers = pd.read_csv("data/src_pos/cashiers.csv")

refund_reasons = [
    "Customer Request",
    "Damaged Item",
    "Wrong Item Sent",
    "Item Not Delivered",
    "Size Too Small",
    "Size Too Large",
    "Defective Item",
    "Changed Mind",
    "Late Delivery",
    "Different from Description",
    "Color Mismatch",
    "Received Extra Item",
    "Missing Parts/Accessories",
    "Quality Not Satisfactory",
    "Duplicate Order",
    "Payment Issue",
    "Unauthorized Purchase",
    "Other"
]

def generate_orders(platform_id, products_df, customers_df, min_orders):
    """
    Generate orders:
    1. Always create a minimum number of random orders.
    2. Ensure all customers appear at least once.
    """
    all_orders, i, j = pd.DataFrame(), 1, 1
    customer_ids = customers_df["buyer_id"].tolist()

    # helper to create one order for a given buyer
    def create_order(buyer_id, order_idx, item_idx):
        num_items = random.randint(1, 5)
        order_items = products_df.sample(n=num_items).reset_index(drop=True)
        order_items = order_items[["product_id","name","sku","category","price"]].copy()
        order_items["order_id"] = f"{platform_id}-ORD{order_idx:08d}"
        order_items["buyer_id"] = buyer_id
        order_items["item_id"] = [
            f"{platform_id}-ITEM{k:08d}" for k in range(item_idx, item_idx + num_items)
        ]
        order_items["qty"] = [random.randint(1, 3) for _ in range(num_items)]
        order_items["discount"] = order_items["price"].apply(
            lambda x: np.random.randint(0, 10) if x > 50 else 0
        )
        order_items["shipping_fee"] = order_items["price"].apply(
            lambda x: 0 if x > 75 else np.random.randint(0, 15)
        )
        order_items["tax"] = (order_items["price"] * order_items["qty"] * 0.06).round(2)
        order_items.rename(columns={"name": "product_name"}, inplace=True)
        return order_items, order_idx + 1, item_idx + num_items

    # Phase 1: generate min_orders
    for _ in range(min_orders):
        order_items, i, j = create_order(random.choice(customer_ids), i, j)
        all_orders = pd.concat([all_orders, order_items], ignore_index=True)

    # Phase 2 & 3: handle missing customers
    missing = set(customer_ids) - set(all_orders["buyer_id"])
    for buyer_id in missing:
        order_items, i, j = create_order(buyer_id, i, j)
        all_orders = pd.concat([all_orders, order_items], ignore_index=True)

    return all_orders.reindex(columns=["order_id","item_id","product_id","product_name","category","sku","qty","price","discount","shipping_fee","tax"])

def generate_receipts(products_df, customers_df, min_receipts=50):
    """
    Generate receipts:
    1. Always create a minimum number of random receipts.
    2. Ensure all customers appear at least once.
    """
    all_receipts, i, j = pd.DataFrame(), 1, 1
    customer_ids = customers_df["customer_id"].tolist()

    # helper: create a receipt for one customer
    def create_receipt(customer_id, receipt_idx, line_idx):
        num_items = random.randint(1, 5)
        receipt_items = products_df.sample(n=num_items).reset_index(drop=True)
        receipt_items = receipt_items[["product_id","name","sku","category","price"]].copy()
        receipt_items["receipt_id"] = f"REC{receipt_idx:08d}"
        receipt_items["customer_id"] = customer_id
        receipt_items["line_id"] = [
            f"LINE{k:08d}" for k in range(line_idx, line_idx + num_items)
        ]
        receipt_items["qty"] = [random.randint(1, 3) for _ in range(num_items)]
        receipt_items["line_discount"] = receipt_items["price"].apply(
            lambda x: np.random.randint(0, 10) if x > 50 else 0
        )
        receipt_items["line_tax"] = (receipt_items["price"] * receipt_items["qty"] * 0.06).round(2)
        receipt_items["line_total"] = (receipt_items["price"] * receipt_items["qty"] + receipt_items["line_tax"]).round(2)
        receipt_items.rename(columns={"name": "product_name","price": "unit_price"}, inplace=True)
        return receipt_items, receipt_idx + 1, line_idx + num_items

    # Phase 1: minimum random receipts
    for _ in range(min_receipts):
        receipt_items, i, j = create_receipt(random.choice(customer_ids), i, j)
        all_receipts = pd.concat([all_receipts, receipt_items], ignore_index=True)

    # Phase 2 + 3: handle missing customers
    missing = set(customer_ids) - set(all_receipts["customer_id"])
    for customer_id in missing:
        receipt_items, i, j = create_receipt(customer_id, i, j)
        all_receipts = pd.concat([all_receipts, receipt_items], ignore_index=True)

    return all_receipts.reindex(columns=["receipt_id","line_id","product_id","product_name","category","sku","qty","unit_price","line_discount","line_tax","line_total"])


def calculate_orders(order_items, customers_df):
    for x in order_items['order_id'].unique():
        order = pd.DataFrame()
        customer = customers_df.sample(n=1).reset_index(drop=True)
        order['order_id'] = [x]
        order['buyer_id'] = customer['buyer_id']
        order['created_at'] = fake.date_time_between(start_date=pd.to_datetime(customer.loc[0, 'created_at']), end_date="now")
        order['updated_at'] = fake.date_time_between(start_date=pd.to_datetime(order.loc[0, 'created_at']), end_date="now")
        order['status'] = random.choices(['PENDING', 'PAID', 'SHIPPED', 'COMPLETED', 'CANCELLED', "REFUNDED"], [0.02, 0.02, 0.08, 0.7, 0.1, 0.08])
        order['currency'] = 'MYR'
        order['total_amount'] = round(order_items[order_items['order_id'] == x].apply(lambda row: (row['price'] * row['qty']) - row['discount'] + row['shipping_fee'] + row['tax'], axis=1).sum(),2)
        order['shipping_fee'] = order_items[order_items['order_id'] == x]['shipping_fee'].sum()
        order['tax_total'] = round(order_items[order_items['order_id'] == x]['tax'].sum(),2)
        order['voucher_amount'] = order_items[order_items['order_id'] == x]['discount'].sum()
        order['market_region'] = 'MALAYSIA'

        if x == order_items['order_id'].unique()[0]:
            all_orders = order
        else:
            all_orders = pd.concat([all_orders, order], ignore_index=True)

    return all_orders

def calculate_receipts(receipt_lines, customers_df):
    for x in receipt_lines['receipt_id'].unique():
        receipt = pd.DataFrame()
        customer = customers_df.sample(n=1).reset_index(drop=True)
        terminal = pos_terminals.sample(n=1).reset_index(drop=True)
        receipt['receipt_id'] = [x]
        receipt['store_id'] = terminal['store_id']
        receipt['terminal_id'] = terminal['terminal_id']
        receipt['cashier_id'] = pos_cashiers.sample(n=1).reset_index(drop=True)['cashier_id']
        receipt['sold_at'] = fake.date_time_between(start_date=pd.to_datetime(customer.loc[0, 'created_at']), end_date="now")
        receipt['status'] = random.choices(['COMPLETED', 'CANCELLED', "REFUNDED"], [0.99, 0.05, 0.05])
        receipt['customer_id'] = customer['customer_id']
        receipt['currency'] = 'MYR'
        receipt['subtotal'] = receipt_lines[receipt_lines['receipt_id'] == x].apply(lambda row: (row['unit_price'] * row['qty']), axis=1).sum()
        receipt['discount_total'] = receipt_lines[receipt_lines['receipt_id'] == x]['line_discount'].sum()
        receipt['tax_total'] = round(receipt_lines[receipt_lines['receipt_id'] == x]['line_tax'].sum(),2)
        receipt['shipping_fee'] = 0 if np.random.rand() > 0.05 else np.random.randint(0,15)
        receipt['grand_total'] = receipt['subtotal'] - receipt['discount_total'] + receipt['tax_total'] + receipt['shipping_fee']

        if x == receipt_lines['receipt_id'].unique()[0]:
            all_receipts = receipt
        else:
            all_receipts = pd.concat([all_receipts, receipt], ignore_index=True)

    return all_receipts

def generate_refunds(platform_id, orders_df, order_items_df):
    refund_list = []
    k=1
    refund_df = orders_df[orders_df['status'].isin(['CANCELLED', 'REFUNDED'])].sample(n=10, random_state=42)
    for idx, order in refund_df.iterrows():
        items = order_items_df[order_items_df['order_id'] == order['order_id']].sample(n=1, random_state=42)
        for _, item in items.iterrows():
            refund_amount = (item['price'] * item['qty']) - item['discount'] + item['shipping_fee'] + item['tax']
            refund_list.append({
                'refund_id': f"{platform_id}-REF{k:08d}",
                'order_id': order['order_id'],
                'item_id': item['item_id'],
                'amount': round(refund_amount, 2),
                'reason': random.choice(refund_reasons),
                'processed_at': fake.date_time_between(start_date=pd.to_datetime(order['created_at']), end_date="now")
            })
            k+=1
    return pd.DataFrame(refund_list)

def generate_payments(receipts_df):
    payment_methods = [["CASH","CASH"],["CREDIT CARD","CRDC"],["DEBIT CARD","DBTC"],["GRAB PAY","GRAB"],["TOUCH N GO","TNGO"]]
    payment_list = []
    k=1
    for idx, receipt in receipts_df.iterrows():
        payment_method = random.choices(payment_methods, [0.4, 0.3, 0.2, 0.05, 0.05])[0],
        payment_list.append({
            'payment_id': f"PAY{k:08d}",
            'receipt_id': receipt['receipt_id'],
            'method': payment_method[0][0],
            'amount': receipt['grand_total'],
            'ref_no': payment_method[0][1] + fake.bothify(text='##########'),
            'paid_at': receipt['sold_at'] + timedelta(minutes=random.randint(1,10))
        })
        k+=1
    return pd.DataFrame(payment_list)

def assign_last_updated(order_items_df, orders_df):
    order_items_df['updated_at'] = None
    for idx, item in order_items_df.iterrows():
        order = orders_df[orders_df['order_id'] == item['order_id']].reset_index(drop=True)
        order_items_df.at[idx, 'updated_at'] = fake.date_time_between(start_date=pd.to_datetime(order.loc[0, 'created_at']), end_date=pd.to_datetime(order.loc[0, 'updated_at']))
    order_items_df = order_items_df.reindex(columns=['order_id','item_id','product_id','product_name','category','sku','qty','price','discount','shipping_fee','tax','updated_at'])
    return order_items_df

def assign_campaigns_influencers(orders_df, campaigns_df, influencers_df):
    orders_df['campaign_id'] = None
    orders_df['influencer_id'] = None
    for idx, order in orders_df.sample(n=20).iterrows():
        campaign = campaigns_df.sample(n=1).reset_index(drop=True)
        orders_df.at[idx, 'campaign_id'] = campaign.loc[0, 'campaign_id']
    for idx, order in orders_df.sample(n=20).iterrows():
        influencer = influencers_df.sample(n=1).reset_index(drop=True)
        orders_df.at[idx, 'influencer_id'] = influencer.loc[0, 'influencer_id']
    orders_df = orders_df.reindex(columns=['order_id','buyer_id','created_at','updated_at','status','currency','total_amount','shipping_fee','tax_total','voucher_amount','campaign_id','influencer_id','market_region'])
    return orders_df

laz_order_items = generate_orders("LAZ", lazada_products, lazada_customers, 100)
shp_order_items = generate_orders("SHP", shopee_products, shopee_customers, 100)
tik_order_items = generate_orders("TIK", tiktok_products, tiktok_customers, 100)
pos_receipt_lines = generate_receipts(pos_products, pos_customers, 100)

laz_orders = calculate_orders(laz_order_items, lazada_customers)
shp_orders = calculate_orders(shp_order_items, shopee_customers)
tik_orders = calculate_orders(tik_order_items, tiktok_customers)
tik_orders = assign_campaigns_influencers(tik_orders, tiktok_campaigns, tiktok_influencers)
pos_receipts = calculate_receipts(pos_receipt_lines, pos_customers)

laz_order_items = assign_last_updated(laz_order_items, laz_orders)
shp_order_items = assign_last_updated(shp_order_items, shp_orders)
tik_order_items = assign_last_updated(tik_order_items, tik_orders)

laz_refunds = generate_refunds("LAZ", laz_orders, laz_order_items)
shp_refunds = generate_refunds("SHP", shp_orders, shp_order_items)
tik_refunds = generate_refunds("TIK", tik_orders, tik_order_items)

pos_payments = generate_payments(pos_receipts)

laz_order_items.to_csv("data/src_lazada/order_items.csv", index=False)
shp_order_items.to_csv("data/src_shopee/order_items.csv", index=False)
tik_order_items.to_csv("data/src_tiktok/order_items.csv", index=False)
pos_receipt_lines.to_csv("data/src_pos/receipt_lines.csv", index=False)

print("Generated src_lazada/order_items.csv")
print("Generated src_shopee/order_items.csv")
print("Generated src_tiktok/order_items.csv")
print("Generated src_pos/receipt_lines.csv")

laz_orders.to_csv("data/src_lazada/orders.csv", index=False)
shp_orders.to_csv("data/src_shopee/orders.csv", index=False)
tik_orders.to_csv("data/src_tiktok/orders.csv", index=False)
pos_receipts.to_csv("data/src_pos/receipts.csv", index=False)

print("Generated src_lazada/orders.csv")
print("Generated src_shopee/orders.csv")
print("Generated src_tiktok/orders.csv")
print("Generated src_pos/receipts.csv")

laz_refunds.to_csv("data/src_lazada/refunds.csv", index=False)
shp_refunds.to_csv("data/src_shopee/refunds.csv", index=False)
tik_refunds.to_csv("data/src_tiktok/refunds.csv", index=False)

print("Generated src_lazada/refunds.csv")
print("Generated src_shopee/refunds.csv")
print("Generated src_tiktok/refunds.csv")

pos_payments.to_csv("data/src_pos/payments.csv", index=False)

print("Generated src_pos/payments.csv")