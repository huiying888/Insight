# Generate a synthetic fashion apparel e-commerce dataset and save as CSV
import random
import os
import pandas as pd
import numpy as np
from itertools import product
from datetime import datetime

random.seed(42)
np.random.seed(42)

# Configuration
num_products = 100

categories = [
    "T-Shirt","Shirt","Blouse","Dress","Jeans","Trousers","Skirt","Shorts",
    "Jacket","Coat","Hoodie","Sweater","Cardigan","Activewear","Loungewear",
    "Pajamas","Tracksuit","Jumpsuit","Romper","Leggings","Polo","Chinos",
    "Windbreaker","Raincoat","Vest","Track Jacket","Puffer Jacket","Denim Jacket",
    "Cargo Pants","Maxi Dress","Midi Dress","Mini Dress","Slip Dress","Bodycon Dress",
    "A-Line Skirt","Pencil Skirt","Pleated Skirt","Wide-Leg Pants","Culottes",
    "Overcoat","Blazer","Bomber Jacket","Athletic Tee","Sports Bra","Yoga Pants",
    "Running Shorts","Bike Shorts","Thermal Top","Thermal Bottoms","Henley","Tunic"
]

size_map = {
    # apparel alpha sizes
    "alpha": ["XS","S","M","L","XL","XXL"],
    # bottoms numeric sizes (denim/trousers)
    "denim": [str(s) for s in range(24, 35)],
    # dresses (alpha)
    "dress": ["XS","S","M","L","XL"],
    # unisex tops
    "unisex": ["XS","S","M","L","XL","XXL"],
    # skirts (alpha)
    "skirt": ["XS","S","M","L","XL"],
    # outerwear (alpha)
    "outer": ["XS","S","M","L","XL","XXL"]
}

colors = [
    "Black","White","Navy","Olive","Beige","Charcoal","Gray","Cream","Brown",
    "Tan","Khaki","Burgundy","Forest Green","Mustard","Teal","Cobalt",
    "Coral","Blush","Lavender","Lilac","Maroon","Sky Blue","Denim Blue",
    "Emerald","Mint","Rust","Terracotta","Sand","Ivory","Rose","Pearl"
]

adjectives = [
    "Classic","Premium","Relaxed","Tailored","Essential","Everyday","Signature",
    "Breathable","Stretch","Lightweight","Ultra-Soft","Ribbed","Textured",
    "Organic","Recycled","Moisture-Wicking","Quick-Dry","Wrinkle-Resistant",
    "Oversized","Slim-Fit","Boxy","Cropped","High-Rise","Mid-Rise","Low-Rise",
    "Seamless","Athleisure","Smart","Heritage","Modern","Minimal","Cozy","Thermal"
]

fabric_notes = [
    "cotton blend","100% cotton","organic cotton","linen blend","100% linen",
    "merino wool","cashmere blend","modal jersey","bamboo viscose","recycled polyester",
    "French terry","tech knit","performance mesh","corduroy","denim","twill",
    "sateen","poplin","chambray","rib knit"
]

feature_notes = [
    "tagless label for comfort","hidden zip pocket","reinforced seams",
    "breathable panels","four-way stretch","soft hand feel","machine washable",
    "sustainably sourced materials","easy-care finish","UV-protective finish",
    "moisture control","wrinkle-resistant finish","adjustable waistband",
    "buttery-soft feel","odor control","brushed interior","snap front",
    "button front","drawstring waist","elastic cuffs"
]

# Helper: choose size profile by category
def all_sizes(category):
    c = category.lower()
    if any(k in c for k in ["jeans","trousers","chinos","cargo","wide-leg","culottes","pants","bottoms"]):
        return size_map["denim"]
    if any(k in c for k in ["dress","jumpsuit","romper"]):
        return size_map["dress"]
    if any(k in c for k in ["skirt"]):
        return size_map["skirt"]
    if any(k in c for k in ["jacket","coat","hoodie","sweater","cardigan","blazer","bomber","overcoat","windbreaker","puffer"]):
        return size_map["outer"]
    return size_map["alpha"]

# Pricing heuristics by category family
def price_for(category):
    c = category.lower()
    if any(k in c for k in ["t-shirt","tee","henley","polo"]):
        return round(np.random.normal(29, 6), 2)
    if any(k in c for k in ["shirt","blouse","tunic"]):
        return round(np.random.normal(45, 10), 2)
    if any(k in c for k in ["jeans","trousers","chinos","cargo","wide-leg","culottes","leggings"]):
        return round(np.random.normal(65, 12), 2)
    if any(k in c for k in ["dress","jumpsuit","romper","skirt"]):
        return round(np.random.normal(79, 15), 2)
    if any(k in c for k in ["jacket","coat","overcoat","blazer","bomber","puffer","raincoat","windbreaker"]):
        return round(np.random.normal(119, 25), 2)
    if any(k in c for k in ["sweater","cardigan","hoodie"]):
        return round(np.random.normal(69, 14), 2)
    if any(k in c for k in ["activewear","athletic","yoga","running","bike","tracksuit","thermal"]):
        return round(np.random.normal(49, 10), 2)
    return round(np.random.normal(55, 10), 2)

def clamp_price(p):
    return float(np.clip(p, 9.99, 399.99))

# Build rows
products = []
inventory = []
used_names = set()

for i in range(1, num_products + 1):
    j = 0
    # Unique-ish product name
    category = random.choice(categories)
    adj = random.choice(adjectives)
    name = f"{adj} {category}"
    # Ensure unique name
    while name in used_names:
        category = random.choice(categories)
        adj = random.choice(adjectives)
        name = f"{adj} {category}"
    used_names.add(name)
    # Price and description
    price = clamp_price(price_for(category))
    desc = (
        f"{adj} {category.lower()} crafted from "
        f"{random.choice(fabric_notes)} with {random.choice(feature_notes)}."
    )
    base_colors = random.sample(colors, k=random.randint(1, 4))  # Pick 1-4 colors
    products.append({
        "ProductID": f"FA-{i:04d}-{j:02d}",
        "ProductName": name,
        "Category": category,
        "Price": round(price, 2),
        "Description": desc.capitalize(),
        "Available Color": base_colors,
    })
    
    for base_color in base_colors:
        color = base_color
        for size in all_sizes(category):
            # Random stock levels
            stock = int(max(0, np.random.normal(60, 40)))
            # Occasional sold-out / limited
            if random.random() < 0.08:
                stock = 0
            elif random.random() < 0.12:
                stock = random.randint(1, 10)

            inventory.append({
                "ProductID": f"FA-{i:04d}-{j:02d}",
                "Size": size,
                "Color": color,
                "RemainingStock": stock
            })

            j += 1

df_products = pd.DataFrame(products, columns=["ProductID","ProductName","Category","Price","Description","Available Color"])
df_inventory = pd.DataFrame(inventory, columns=["ProductID","Size","Color","RemainingStock"])

# Save to CSV
csv_path_products = "data/products.csv"
os.makedirs(os.path.dirname(csv_path_products), exist_ok=True)  # Ensure folder exists
df_products.to_csv(csv_path_products, index=False)

csv_path_inventory = "data/inventory.csv"
os.makedirs(os.path.dirname(csv_path_inventory), exist_ok=True)  # Ensure folder exists
df_inventory.to_csv(csv_path_inventory, index=False)
