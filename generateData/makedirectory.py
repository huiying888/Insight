 # Ensure folders exists
import os
def run():
    os.makedirs(os.path.dirname("data/"), exist_ok=True)
    os.makedirs(os.path.dirname("data/src_shopee/"), exist_ok=True)
    os.makedirs(os.path.dirname("data/src_lazada/"), exist_ok=True)
    os.makedirs(os.path.dirname("data/src_tiktok/"), exist_ok=True)
    os.makedirs(os.path.dirname("data/src_pos/"), exist_ok=True)
    os.makedirs(os.path.dirname("data/wh/"), exist_ok=True)

run()