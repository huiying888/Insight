import pandas as pd
import makedirectory

campaigns = [
    {"campaign_id":"TIK-CAMP001","name":"Fall Fashion Showcase","start_at":"2025-08-21 00:00:00+00","end_at":"2025-09-20 15:59:59+00","budget":"5000.00"},
    {"campaign_id":"TIK-CAMP002","name":"Mega Sale Launch","start_at":"2025-08-21 02:00:00+00","end_at":"2025-08-27 15:59:59+00","budget":"3000.00"}
    ]

influencers = [
    {"idx":0,"influencer_id":"TIK-INFL001","handle":"@FashionistaMY","name":"Maya Lee"},
    {"idx":1,"influencer_id":"TIK-INFL002","handle":"@StyleGuruKL","name":"Ben Tan"}
    ]

pd.DataFrame(campaigns).to_csv("data/src_tiktok/campaigns.csv", index=False)
pd.DataFrame(influencers).to_csv("data/src_tiktok/influencers.csv", index=False)