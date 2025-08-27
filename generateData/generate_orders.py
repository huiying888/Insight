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

lazada_products = pd.read_csv("data/src_lazada/products.csv")
lazada_customers = pd.read_csv("data/src_lazada/customers.csv")
shopee_products = pd.read_csv("data/src_shopee/products.csv")
shopee_customers = pd.read_csv("data/src_shopee/customers.csv")
tiktok_products = pd.read_csv("data/src_tiktok/products.csv")
tiktok_customers = pd.read_csv("data/src_tiktok/customers.csv")
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
    for idx, order in orders_df.iterrows():
        if order['status'] in ['CANCELLED', 'REFUNDED']:
            items = order_items_df[order_items_df['order_id'] == order['order_id']]
            for _, item in items.iterrows():
                refund_amount = (item['price'] * item['qty']) - item['discount'] + item['shipping_fee'] + item['tax']
                refund_list.append({
                    'refund_id': f"{platform_id}-REF{item['item_id']}",
                    'order_id': order['order_id'],
                    'item_id': item['item_id'],
                    'amount': round(refund_amount, 2),
                    'reason': random.choice(refund_reasons),
                    'processed_at': fake.date_time_between(start_date=pd.to_datetime(order['created_at']), end_date="now")
                })
    return pd.DataFrame(refund_list)

def generate_payments(receipts_df):
    payment_methods = [["CASH","CASH"],["CREDIT CARD","CRDC"],["DEBIT CARD","DBTC"],["GRAB PAY","GRAB"],["TOUCH N GO","TNGO"]]
    payment_list = []
    for idx, receipt in receipts_df.iterrows():
        payment_method = random.choices(payment_methods, [0.4, 0.3, 0.2, 0.05, 0.05])[0],
        payment_list.append({
            'payment_id': f"PAY{receipt['receipt_id'][3:]}",
            'receipt_id': receipt['receipt_id'],
            'method': payment_method[0][0],
            'amount': receipt['grand_total'],
            'ref_no': payment_method[0][1] + fake.bothify(text='##########'),
            'paid_at': receipt['sold_at'] + timedelta(minutes=random.randint(1,10))
        })
    return pd.DataFrame(payment_list)

laz_order_items = generate_orders("LAZ", lazada_products, 100, j)
shp_order_items = generate_orders("SHP", shopee_products, 100, j)
tik_order_items = generate_orders("TIK", tiktok_products, 100, j)
pos_receipt_lines = generate_receipt(pos_products, 100, j)

laz_orders = calculate_orders(laz_order_items, lazada_customers)
shp_orders = calculate_orders(shp_order_items, shopee_customers)
tik_orders = calculate_orders(tik_order_items, tiktok_customers)
pos_receipts = calculate_receipts(pos_receipt_lines, pos_customers)

laz_refunds = generate_refunds("LAZ", laz_orders, laz_order_items)
shp_refunds = generate_refunds("SHP", shp_orders, shp_order_items)
tik_refunds = generate_refunds("TIK", tik_orders, tik_order_items)

pos_payments = generate_payments(pos_receipts)

laz_order_items.to_csv("data/src_lazada/order_items.csv", index=False)
shp_order_items.to_csv("data/src_shopee/order_items.csv", index=False)
tik_order_items.to_csv("data/src_tiktok/order_items.csv", index=False)
pos_receipt_lines.to_csv("data/src_pos/receipt_lines.csv", index=False)

laz_orders.to_csv("data/src_lazada/orders.csv", index=False)
shp_orders.to_csv("data/src_shopee/orders.csv", index=False)
tik_orders.to_csv("data/src_tiktok/orders.csv", index=False)
pos_receipts.to_csv("data/src_pos/receipts.csv", index=False)

laz_refunds.to_csv("data/src_lazada/refunds.csv", index=False)
shp_refunds.to_csv("data/src_shopee/refunds.csv", index=False)
tik_refunds.to_csv("data/src_tiktok/refunds.csv", index=False)

pos_payments.to_csv("data/src_pos/payments.csv", index=False)
