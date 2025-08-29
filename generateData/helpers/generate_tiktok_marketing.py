import pandas as pd
import makedirectory

campaigns = [
    {"campaign_id":"TIK-CAMP001","name":"Fall Fashion Showcase","start_at":"2025-08-21 00:00:00+00","end_at":"2025-09-20 15:59:59+00","budget":"5000.00"},
    {"campaign_id":"TIK-CAMP002","name":"Mega Sale Launch","start_at":"2025-08-21 02:00:00+00","end_at":"2025-08-27 15:59:59+00","budget":"3000.00"},
    {"campaign_id":"TIK-CAMP003","name":"Streetwear September","start_at":"2025-09-01 00:00:00+00","end_at":"2025-09-30 15:59:59+00","budget":"7000.00"},
    {"campaign_id":"TIK-CAMP004","name":"Beauty & Wellness Week","start_at":"2025-10-05 06:00:00+00","end_at":"2025-10-12 15:59:59+00","budget":"4000.00"},
    {"campaign_id":"TIK-CAMP005","name":"Holiday Glam Drive","start_at":"2025-12-01 00:00:00+00","end_at":"2025-12-31 15:59:59+00","budget":"10000.00"}
]

influencers = [
    {"influencer_id":"TIK-INFL001","handle":"@FashionistaMY","name":"Maya Lee"},
    {"influencer_id":"TIK-INFL002","handle":"@StyleGuruKL","name":"Ben Tan"},
    {"influencer_id":"TIK-INFL003","handle":"@TrendSetSarawak","name":"Alicia Wong"},
    {"influencer_id":"TIK-INFL004","handle":"@OOTDKL","name":"Jason Lim"},
    {"influencer_id":"TIK-INFL005","handle":"@ChicPenang","name":"Amira Zain"},
    {"influencer_id":"TIK-INFL006","handle":"@StreetwearSabah","name":"Rafiq Hakim"},
    {"influencer_id":"TIK-INFL007","handle":"@BeautyVibesMY","name":"Chloe Tan"},
    {"influencer_id":"TIK-INFL008","handle":"@FoodieFits","name":"Jared Ong"},
    {"influencer_id":"TIK-INFL009","handle":"@UrbanGlam","name":"Serena Yap"},
    {"influencer_id":"TIK-INFL010","handle":"@MinimalistChic","name":"Siti Nur"},
    {"influencer_id":"TIK-INFL011","handle":"@CasualVibesKL","name":"Daniel Koh"},
    {"influencer_id":"TIK-INFL012","handle":"@LuxuryLooksMY","name":"Fiona Goh"},
    {"influencer_id":"TIK-INFL013","handle":"@BatikRevival","name":"Aisyah Karim"},
    {"influencer_id":"TIK-INFL014","handle":"@DenimCulture","name":"Marcus Lee"},
    {"influencer_id":"TIK-INFL015","handle":"@HijabistaStyle","name":"Nurul Huda"},
    {"influencer_id":"TIK-INFL016","handle":"@EcoFashionMY","name":"Ivan Chan"},
    {"influencer_id":"TIK-INFL017","handle":"@FestivalFits","name":"Farah Anis"},
    {"influencer_id":"TIK-INFL018","handle":"@StreetChicMY","name":"Darren Lau"},
    {"influencer_id":"TIK-INFL019","handle":"@RunwayVibes","name":"Michelle Yong"},
    {"influencer_id":"TIK-INFL020","handle":"@VintageMuse","name":"Clara Lim"}
]


pd.DataFrame(campaigns).to_csv("data/src_tiktok/campaigns.csv", index=False)
pd.DataFrame(influencers).to_csv("data/src_tiktok/influencers.csv", index=False)

print("Generated src_tiktok/campaigns.csv")
print("Generated src_tiktok/influencers.csv")