##Additiol

import spacy
from flashtext import KeywordProcessor
import pandas as pd
import xlsxwriter

import sys
from Tim_Quinn_it_01 import go_tim_go

try:
        nlp = spacy.load(r"C:\Users\Low Kah\Downloads\NER models\en_core_web_lg-3.0.0")
except:
        nlp = spacy.load(r"C:\Users\TQ0003\Downloads\en_core_web_lg-3.0.0\en_core_web_lg\en_core_web_lg-3.0.0")

mkt_intel = pd.read_excel(r"J:\4. ISE\Market Intel\Data work\000 Dogbert 160489\zzUniversal_reference\Keyword_mkt_intel_list.xlsx", sheet_name =0)

kp = KeywordProcessor()
for item, key in zip(mkt_intel["Item"], mkt_intel["Label"]):
	kp.add_keyword(item, key)

###### Main data folder ---->
df = go_tim_go(r"J:\4. ISE\Market Intel\Data work\000 Dogbert 160489\Counsellor scraper\Data")
df.reset_index(inplace=True, drop=True)

##Matching the required format for pipeline to BAC (see updated.xlsx for example or data reception

df.rename(columns = {"Header2": "Heading",
                     "Categories": "Category"}, inplace=True)
##df["Text"] = df["Heading"] + " - " + df["Text"]


##Identify the Country/Body fields using Spacy, en_core_web_lg. In en_core_web_lg, pull out ORG and GPE tags
def gpe_org_tagger(text):
    doc = nlp(text)
    df = pd.DataFrame(data = {"Text"  :[ent.text for ent in doc.ents],
                              "Label" :[ent.label_ for ent in doc.ents],
                              "Count" :[ent.label_ for ent in doc.ents]})

    df = df.loc[(df["Label"] == "GPE"),:].copy(deep=True)

    df = pd.DataFrame(df.groupby("Text").count())
    df.sort_values(by = "Label", ascending = False, inplace=True)

    df.reset_index(inplace=True)
    df = df.T

    ##Assign each entity with the suffix "Country/Body" followed by the number
    col_head = []
    for col in range(1, df.shape[1]+1,1):
        x = f"Country/Body{col}"
        col_head.append(x)

    df.columns = col_head
    df = pd.DataFrame(df.iloc[0,:])
    df = df.T

    return df



def mkt_intel_detector(text):
    kp_df = pd.DataFrame(data = {"Label": kp.extract_keywords(text),
                                 "Count" : kp.extract_keywords(text)})

    kp_df = pd.DataFrame(kp_df.groupby("Label").count())
    kp_df.sort_values(by = "Count", ascending = False, inplace=True)
    kp_df.reset_index(inplace=True)
    kp_df = kp_df.T

    col_head = []
    for col in range(1, kp_df.shape[1]+1,1):
        x = f"Commodity {col}"
        col_head.append(x)

    kp_df.columns = col_head
    kp_df = pd.DataFrame(kp_df.iloc[0,:])
    kp_df = kp_df.T

    return kp_df
    
    
    
    


country_df = pd.DataFrame()
label_df = pd.DataFrame()

for text in df["Text"]:
    try:
        detect = gpe_org_tagger(text)
        country_df = country_df.append(detect)
    except:
        country_df = country.df.append(pd.DataFrame(data = {"Label":[""]}))
        print("Country error")

    try:
        detect = mkt_intel_detector(text)
        label_df = label_df.append(detect)
    except:
        label_df = label_df.append(pd.DataFrame(data = {"Label":[""]}))
        print("Detect error")

country_df.reset_index(drop = True, inplace=True)
label_df.reset_index(drop = True, inplace=True)

df = df.merge(country_df, how ="left", left_index=True, right_index=True).copy(deep=True)
df = df.merge(label_df, how = "left", left_index=True, right_index=True).copy(deep=True)
df["Issue"] =""

df["Text"] = df["Heading"] + df["Text"]

df = df[['Date', 'Month/Year', 'Country/Body','Country/Body1', 'Country/Body2', 'Country/Body3','Country/Body4',
          'Post', "Issue", 'Category', 'Heading','Text', 'Commodity 1', 'Commodity 2','Commodity 3', 'Commodity 4', 'Commodity 5', 'Commodity 6',
       'Commodity 7']]

##df["TitleBold"] = df["Heading"]
df.fillna(value = "", inplace=True)


df.to_excel(r"J:\4. ISE\Market Intel\Data work\000 Dogbert 160489\Counsellor scraper\Datadump.xlsx", sheet_name = "WOO", index=False)

    

    




