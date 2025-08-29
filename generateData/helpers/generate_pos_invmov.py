import random
from datetime import datetime, timedelta
import pandas as pd
from faker import Faker

Faker.seed(42)
fake = Faker()
random.seed(42)
pd.random_state = 42

# Configuration
receipt_lines = pd.read_csv("data/src_pos/receipt_lines.csv")
receipts = pd.read_csv("data/src_pos/receipts.csv")
stores = pd.read_csv("data/src_pos/stores.csv")['store_id'].tolist()
products = pd.read_csv("data/src_pos/products.csv")['product_id'].tolist()
movement_types = ["Sale", "Stock In", "Return", "Adjustment"]

# Storage
data = []
j = 1
stockin_id = 1
return_id = 1
adjustment_id = 1

for idx, x in receipt_lines.iterrows():
    movement_type = ""
    while movement_type != "Sale":
        movement_id = f"IMV{j:08d}"
        if j==1:
            movement_type = "Sale"
        else:
            movement_type = random.choices(movement_types, weights=[0.7, 0.2, 0.05, 0.05])[0]

        # qty_delta rules
        if movement_type == "Sale": # Sale
            product_id = x['product_id']
            store_id = receipts.loc[receipts['receipt_id'] == x['receipt_id'], 'store_id'].values[0]
            qty_delta = x['qty'] * -1
            reference_id = receipts.loc[receipts['receipt_id'] == x['receipt_id'], 'receipt_id'].values[0]
            moved_at = receipts.loc[receipts['receipt_id'] == x['receipt_id'], 'sold_at'].values[0]
            note = f"Sold {product_id} via {reference_id}"
        elif movement_type == "Stock In": # Stock In
            product_id = products[random.randint(0, len(products)-1)]
            store_id = stores[random.randint(0, len(stores)-1)]
            qty_delta = random.randint(1, 20) * 5
            reference_id = f"PO{stockin_id:04d}"
            stockin_id += 1
            moved_at = datetime.strptime(str(data[-1][6]), "%Y-%m-%d %H:%M:%S") + timedelta(minutes=random.randint(10, 1440))
            note = f"New stock delivery for {product_id}"
        elif movement_type == "Return": # Return
            product_id = products[random.randint(0, len(products)-1)]
            store_id = stores[random.randint(0, len(stores)-1)]
            qty_delta = random.randint(1, 3)
            reference_id = f"RTN{return_id:04d}"
            note = f"Customer return {product_id}"
            return_id += 1
            moved_at = datetime.strptime(str(data[-1][6]), "%Y-%m-%d %H:%M:%S") + timedelta(minutes=random.randint(10, 1440))
        else:  # Adjustment
            product_id = products[random.randint(0, len(products)-1)]
            store_id = stores[random.randint(0, len(stores)-1)]
            qty_delta = random.randint(-5, 5)
            reference_id = f"ADJ{adjustment_id:04d}"
            note = f"Stock adjustment of {product_id}"
            adjustment_id += 1
            moved_at = datetime.strptime(str(data[-1][6]), "%Y-%m-%d %H:%M:%S") + timedelta(minutes=random.randint(10, 1440))

        data.append([
            movement_id,
            product_id,
            store_id,
            movement_type,
            round(float(qty_delta), 3),
            reference_id,
            moved_at,
            note
        ])

        j+=1

# Put into DataFrame for preview / CSV export
df = pd.DataFrame(data, columns=[
    "movement_id", "product_id", "store_id", "movement_type",
    "qty_delta", "reference_id", "moved_at", "note"
])

# Save to CSV
df.to_csv("data/src_pos/inventory_movements.csv", index=False)
print("Generated src_pos/inventory_movements.csv")