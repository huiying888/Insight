# Generate a synthetic fashion apparel e-commerce dataset and save as CSV
import random
import pandas as pd
from faker import Faker
import makedirectory


Faker.seed(42)
fake = Faker()
random.seed(42)
pd.random_state = 42


# Configuration
num_products = 100

categories = [
    "Tops", "Bottoms", "Dresses", "Outerwear", "Activewear", "Shoes"
]

brands = {
    "Tops": {"Nike", "Adidas", "Puma", "Under Armour", "Reebok", "Tommy Hilfiger", "Calvin Klein"},
    "Bottoms": {"Levi's", "Wrangler", "Lee", "Dockers", "Gap", "Old Navy", "American Eagle"},
    "Dresses": {"Calvin Klein", "Tommy Hilfiger", "Banana Republic", "Ralph Lauren", "Guess", "Ann Taylor"},
    "Outerwear": {"The North Face", "Columbia", "Patagonia", "Marmot", "Calvin Klein", "Tommy Hilfiger"},
    "Activewear": {"Nike", "Adidas", "Lululemon", "Under Armour", "Puma", "Reebok", "Champion"},
    "Shoes": {"Nike", "Adidas", "Converse", "Vans", "New Balance", "Skechers", "Timberland"}
}

descriptors = {
    "Tops": ["SkinFit","CoolTouch","AirFlow","SmoothFit","SlimCore","FlexLite","Dri-FIT","TrueShape","QuickDry","Ribbed","Cropped","Oversized"],
    "Bottoms": ["SlimCore","PowerStretch","MotionFlex","ComfortStretch","TaperedFit","FlexLite","CoreFlex","EnduraFit","ProStretch","ActiveDry","Distressed","High-Rise"],
    "Dresses": ["Draped","Ruched","Cinched","FlowForm","Minimalist","Statement","Layered","ClassicCut","Sculpted","Contemporary","Floral","Pleated"],
    "Outerwear": ["StormShield","ThermalShield","StormGuard","StormProof","Insulated","Quilted","Padded","Oversized","UtilityStyle","LightMotion","Hooded","Lightweight"],
    "Activewear": ["Ultraboost","MaxSpeed","SkinFit","HyperDry","AeroCool","MotionWeave","RapidDry","BounceFlex","HeatGear","PowerMesh","Seamless","Breathable"],
    "Shoes": ["Ultraboost","AirMax","GlideStep","FreshFoam","MaxSpeed","GripLock","SmoothMotion","SpeedForm","LightMotion","SpeedTech","Slip-On","Lace-Up"],
}

items = {
    "Tops": {"T-Shirt", "Shirt", "Blouse", "Tank Top", "Polo Shirt", "Sweatshirt"},
    "Bottoms": {"Jeans", "Trousers", "Skirt", "Shorts", "Chinos", "Cargo Pants"},
    "Dresses": {"Dress", "Maxi Dress", "Midi Dress", "Mini Dress", "Shift Dress"},
    "Outerwear": {"Jacket", "Coat", "Hoodie", "Sweater", "Cardigan", "Parka"},
    "Activewear": {"Leggings", "Sports Bra", "Yoga Pants", "Running Shorts", "Athletic Tee", "Track Jacket"},
    "Shoes": {"Sneakers", "Boots", "Sandals", "Slip-Ons", "Running Shoes"},
}

# Build rows
products = []
used_names = set()

random.seed(42)

for i in range(1, num_products + 1):
    category = random.choice(categories)

    # Generate a unique product name
    while True:
        brand = random.choice(sorted(list(brands[category])))
        descriptor = random.choice(sorted(list(descriptors[category])))
        item = random.choice(sorted(list(items[category])))
    
        name = f"{brand} {descriptor} {item}"
        if name not in used_names:
            used_names.add(name)
            break
    
    cost = random.randint(4, 26) * 10
    price = cost + random.randint(1, 6) * 10
    starting_inventory = random.randint(5, 100) * 10

    products.append({
        "master_product_code": "SKU"+f"{i:04d}",
        "name": name,
        "category": category,
        "brand": brand,
        "is_active": random.choices([True, False], weights=[0.9, 0.1])[0],
        "created_at": fake.date_time_between(start_date="-1y", end_date="-6m"),
        "starting_inventory": starting_inventory,
        "base_cost": cost, # Base wholesale cost - cost to acquire
        "base_price": price, # Base retail price - price sold to customer
    })

pd.DataFrame(products).to_csv("data/master_product.csv", index=False)
print("Generated master_product.csv")