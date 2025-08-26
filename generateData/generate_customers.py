import random
import pandas as pd
from datetime import datetime, timedelta
from faker import Faker
import makedirectory

fake = Faker()
random.seed(42)
Faker.seed(42)

malay_prefix_male = ["Md","Mohd","Muhammad","Mohammed","Abdul","Ahmad","Syed",]
malay_prefix_female = ["Ain","Nur","Wan","Che","Siti"]
malay_given_male = ["Ahmad","Hafiz","Aiman","Farid","Amir","Azman","Haziq","Fazli","Syafiq","Amin","Irfan","Hakim","Fikri","Zulhilmi","Rizal"]
malay_given_female = ["Aisyah","Nurul","Siti","Farah","Liyana","Hani","Aina","Nadia","Amira","Izzati","Fatin","Huda","Mira","Lina"]
chinese_surnames = ["Tan","Lim","Lee","Wong","Chan","Chong","Ng","Ong","Low","Goh","Teo","Khoo","Lau","Ho","Cheah"]
chinese_given = ["Wei Ming","Xiu Lin","Li Jun","Mei Ling","Jia Hui","Qian Yi","Hui Min","Zhi Hao","Yue Ying","Chen Wei","Fang Ying","Jing Yi","Yu Xuan","Hao Ran","Si Ying"]
indian_given_male = ["Arun","Ravi","Kumar","Raj","Vijay","Suresh","Mani","Hari","Prakash","Ramesh","Ajay","Deepak","Anil","Sanjay"]
indian_given_female = ["Anita","Lakshmi","Priya","Kavita","Sita","Radha","Meena","Latha","Divya","Asha","Nisha","Sunita","Rekha"]

states = ["Kuala Lumpur","Selangor","Penang","Johor","Sabah","Sarawak","Negeri Sembilan","Perak","Pahang","Kedah","Kelantan","Terengganu","Melaka","Perlis"]
used_names = set()
used_id = set()

def random_name():
    et = random.choices(["Malay","Chinese","Indian"])[0]
    sex = random.choices(["M","F"])[0]
    
    while True:
        if et == "Malay":
            if sex == "F":
                name = f"{random.choice(malay_prefix_male)} {random.choice(malay_given_male)} bin {random.choice(malay_given_male)}"
            if sex == "M":
                name = f"{random.choice(malay_prefix_female)} {random.choice(malay_given_female)} binti {random.choice(malay_given_male)}"
        elif et == "Chinese":
            name = f"{random.choice(chinese_surnames)} {random.choice(chinese_given)}"
        else:
            if sex == "F":
                name = f"{random.choice(indian_given_female)} A/P {random.choice(indian_given_male)}"
            if sex == "M":
                name = f"{random.choice(indian_given_male)} A/L {random.choice(indian_given_male)}"
    
        if name not in used_names:
            used_names.add(name)
            break

    return name

def random_phone():
    prefixes = ["+6011", "+6012", "+6013", "+6014", "+6016", "+6017", "+6018", "+6019"]
    return random.choice(prefixes) + "".join(str(random.randint(0,9)) for _ in range(7))

def random_email(name):
    slug = "".join(ch.lower() for ch in name.split()[0] if ch.isalnum())
    domain = random.choice(["gmail.com","yahoo.com","hotmail.com","outlook.com"])
    return f"{slug}{random.randint(10,999)}@{domain}"

def random_customer(prefix):
    while True:
        id = prefix+f"{random.randint(1,99999999):08d}"
        if id not in used_id:
            used_id.add(id)
            break
    name = random_name()
    phone = random_phone()
    email = random_email(name)
    region = random.choice(["Kuala Lumpur","Selangor","Penang","Johor","Sabah","Sarawak"])
    created_at = fake.date_time_between(start_date="-2y", end_date="now")
    
    return (id, name, phone, email, region, created_at)

# Generate customers
shopee_customers = [random_customer("SHPC") for _ in range(100)]
lazada_customers = [random_customer("LAZC") for _ in range(100)]
tiktok_customers = [random_customer("TIKC") for _ in range(100)]
pos_customers = [random_customer("POSC") for _ in range(100)]

pd.DataFrame(shopee_customers, columns=["buyer_id","name","phone","email","region","created_at"]).to_csv("data/src_shopee/customers.csv", index=False)
pd.DataFrame(lazada_customers, columns=["buyer_id","name","phone","email","region","created_at"]).to_csv("data/src_lazada/customers.csv", index=False)
pd.DataFrame(tiktok_customers, columns=["buyer_id","name","phone","email","region","created_at"]).to_csv("data/src_tiktok/customers.csv", index=False)
pd.DataFrame(pos_customers, columns=["customer_id","name","phone","email","region","created_at"]).to_csv("data/src_pos/customers.csv", index=False)
print("Generated customers data")