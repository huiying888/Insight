import pandas as pd
import makedirectory

terminals = [
    {"terminal_id":"TERM001","store_id":"POS001","label":"Main Counter 1"},
    {"terminal_id":"TERM002","store_id":"POS001","label":"Main Counter 2"},
    {"terminal_id":"TERM003","store_id":"POS002","label":"Island Checkout"},
    {"terminal_id":"TERM004","store_id":"POS003","label":"Outlet Register"}
    ]

stores = [
    {"store_id":"POS001","name":"KL Flagship Store","region":"Kuala Lumpur","timezone":"Asia/Kuala_Lumpur"},
    {"store_id":"POS002","name":"Penang Island Boutique","region":"Penang","timezone":"Asia/Kuala_Lumpur"},
    {"store_id":"POS003","name":"Johor Premium Outlet","region":"Johor","timezone":"Asia/Kuala_Lumpur"}
    ]

cashiers = [
    {"cashier_id":"CASH001","name":"Ahmad bin Ali","employee_code":"EMP001","status":"Active"},
    {"cashier_id":"CASH002","name":"Siti Nurhaliza","employee_code":"EMP002","status":"Active"},
    {"cashier_id":"CASH003","name":"Lim Wei Xuan","employee_code":"EMP003","status":"Active"}
    ]


pd.DataFrame(terminals).to_csv("data/src_pos/terminals.csv", index=False)
pd.DataFrame(stores).to_csv("data/src_pos/stores.csv", index=False)
pd.DataFrame(cashiers).to_csv("data/src_pos/cashiers.csv", index=False)