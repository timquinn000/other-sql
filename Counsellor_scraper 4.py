import pandas as pd
import re
from docx import *
import os
from bs4 import BeautifulSoup
import mammoth
import numpy as np
import nltk
nltk.data.path.append(r"J:\4. ISE\Market Intel\Data work\00445 Low 050218\Counsellor scraper\NLTK_resources")

import spacy
nlp = spacy.load("en_core_web_lg")

from nltk.corpus import stopwords
from flashtext import KeywordProcessor


###Counsellor scraper 2
 
"""
Objectives:
0)   Convert word document to html
1)   Read in the html that has one n x 2 table
1.5) Convert to table, whilst maintaining style tags
2)   Column 0 is header 1
3)   Bolded text in column 1 is header 2. If no header 2 exists, fill in header 1
4)   All dot points under bolded text until the next bolded text (that appears on a new line) must be captured
5)   If the text at the beginning ends with a colon, this indicates header 3
6)   If no header 3 exists, fill in header 2
"""

def unicode_killer(df, col_name):
    df[col_name] = df[col_name].str.replace("\<p\>","")
    df[col_name] = df[col_name].str.replace("\<\/p\>","")
    df[col_name] = df[col_name].str.replace("\<strong>","")
    df[col_name] = df[col_name].str.replace("\<ul\>\<li\>"," - ")
    df[col_name] = df[col_name].str.replace("\<(.+?)\>","")

    return df



#0) convert word document to html
def scraper(docfile): 
    result = mammoth.convert_to_html(docfile)
    sauce = result.value

    #1) Read in the html that has one n x 2 table 
    soup = BeautifulSoup(sauce, "xml")
    table = soup.find_all("table")[0]

    #1.5) Convert to table, whilst maintaing style tags
    ##Source: https://stackoverflow.com/questions/48247029/is-it-possible-to-read-html-tables-into-pandas-with-style-tag

    #2)  Column 0 is header 1 (with tag 5)
    df = pd.DataFrame()
    rows = table.find_all('tr')[:]
    for row in rows:
        total = []
        cols = row.find_all('td')
        #Header 0
        Header0 = cols[0].findChildren()

        #Return second column
        texts  = cols[1].findChildren()
        for text in texts:
            part = "".join(str(text))
            total.append(part)

        part_df = pd.DataFrame(data = {"Header0":[Header0[0].text],
                                       "Text": [total]})

        df = df.append(part_df)



    complete =pd.DataFrame()

    for rows in df["Text"]:
    ##    row = " ".join(row)
    #<strong>(.+?)(?=<strong>)
        clean_text = []
        for row in rows:
            row = "".join(row)
            subtext = re.findall("\<strong\>(.+?)(?=((\<strong\>)|(\<\/p\>$)))",row)
            

            if len(subtext) == 0:
                subtext = re.findall("\<p\>.*",row)
            else:
                pass
            try:
                clean_text.append(subtext[0][1])
                #clean_text.append(subtext[0][0])
                data = pd.DataFrame(data = {"Yup": clean_text})
                complete = complete.append(data)
            except:
                pass

    ##Convert list elements to string
    df["Combined"] = ["".join(i) if isinstance(i, list) else i for i in df["Text"]]

    df.reset_index(inplace=True, drop = True)

    part = df["Combined"].str.split(pat = "</strong>", expand=True)
    part.rename(columns = {0:"Header1"}, inplace=True)

    part["CombinedText"] = part[part.columns[1:]].apply(lambda x: "".join(x.values.astype(str)), axis = 1)

    df = df.merge(part, how = "left", left_index= True, right_index=True)

    df = df[["Header0", "Header1", "CombinedText"]]


    ###Header2"
    ##Identification of header 2
    df["Header2"] = df["CombinedText"].apply(lambda row :re.findall("\<\/p\>\<strong\>(.+?)(?=\<p\>)",row))


    ##Convert each element list into a separate row.
    df = df.explode(column = "Header2")
    df = df[["Header0", "Header1", "Header2", "CombinedText"]]
    df["CombinedText"] = df["CombinedText"].str.replace("None","")



    ##Identify each Header1 instance

    header1 = df.groupby("Header1").count().index


    ###Identify each header2 and the corresponding text. Stop at another instance of header2
    complete = pd.DataFrame()
    for x in header1:
        foo = df.loc[df["Header1"] == x,:].copy(deep=True)


        ##Second instance of header2 is the next header2

        foo["StopPhrase"] = foo["Header2"].shift(-1)
        foo["StopPhrase"].fillna(value="</p>", inplace=True)


        data = pd.DataFrame()

        for y in range(0,  foo.shape[0],1):

            try:
                part = foo.iloc[y,:]
                tx = part["CombinedText"]
                h0 = part["Header0"]
                h1 = part["Header1"]
                h2 = part["Header2"]
                sp = part["StopPhrase"]
                my_regex = f"{re.escape(h2)}(.*){re.escape(sp)}"
                ex = re.search(my_regex,tx).group(1)
                pa = pd.DataFrame(data = {"Header0":[h0],
                                          "Header1":[h1],
                                          "Header2":[h2],
                                          "Text"   :[ex]})
                data = data.append(pa)
            except:
                part = pd.DataFrame(foo.iloc[y,:]).T
                part.rename(columns = {"CombinedText":"Text"}, inplace=True)
                pa = part[["Header0","Header1", "Header2", "Text"]]
                data = data.append(pa)
        complete = complete.append(data)


    df = complete.copy(deep=True)



    df = unicode_killer(df, "Header0")
    df = unicode_killer(df, "Header1")
    df = unicode_killer(df, "Header2")
    df = unicode_killer(df, "Text")

    df.columns = [x.strip() for x in df.columns]

    ##For entries without header1, header2 or header3, then the text is under header1. Populate text column with header1
    df["Text"] = np.where(df["Text"] =="",df["Header1"], df["Text"])
    #For repeated text in header1 and text, replace header1 with "No category"
    df["Header1"] = np.where(df["Header1"] == df["Text"], "No category", df["Header1"])
    df["Header1"].fillna(value = "No category", inplace=True)

    ##For text with header1 but no header2, populate header2 with "No subcategory"
    df["Header2"].fillna(value = "No subcategory", inplace=True)
    df["Header2"] = np.where(df["Header2"] =="","No subcategory", df["Header2"])


    ##dataframe has Today's date and Country/Body, which should be a new column
    datefield   = df.loc[df["Header0"] == "Today’s date","Text"]
    datefield   = datefield.iloc[0]
    countrybody = df.loc[df["Header0"] == "Country/ Body","Text"]
    countrybody = countrybody.iloc[0]

    df["Date"] = datefield
    df["Country/Body"] = countrybody

    df.reset_index(inplace=True, drop =True)
    df = df.loc[(df["Header0"]!="Today’s date")&(df["Header0"]!="Country/ Body"),:]
    df["Text"].replace(df["Header2"],"", inplace=True)

    return df


data_dump = r"J:\4. ISE\Market Intel\Data work\00445 Low 050218\Counsellor scraper\Data"
os.chdir(data_dump)

df = pd.DataFrame()
for directories, folders, files in os.walk(data_dump):
    for file in files:
        try:
            part = scraper(file)
            print(f"{file} complete")
            df = df.append(part)
            
        except:
            print(f"{file} failed")
            pass

df.drop("Header1", axis =1 , inplace=True)


####Topic detection
mktintel = pd.read_excel(r"J:\4. ISE\Market Intel\Data work\000 Dogbert 160489\zzUniversal_reference\Keyword_mkt_intel_list.xlsx", sheet_name = 0)

mktintel["Item"] = mktintel["Item"].apply(lambda x: x.lower())
mktintel = mktintel.groupby("Label").agg({"Item" : ",".join}).reset_index().reindex(columns = mktintel.columns)
mktintel["Item"] = mktintel["Item"].apply(lambda x: x.split(","))
##


mktintel = dict(zip(mktintel["Label"],mktintel["Item"]))


region = pd.read_csv(r"J:\4. ISE\Market Intel\Data work\000 Dogbert 160489\zzUniversal_reference\Region.csv", encoding = "cp1252")
region = dict(zip(region["Country/ Body"], region["Post"]))


df["Text"] =  df["Text"].astype(str)
df["string"] = df["Text"].apply(lambda x: x.lower())


df["Header3"] = df["Header2"].shift(-1)
df.reset_index(inplace=True, drop =True)

for x in df.iterrows():
    beg = x[1]["Header2"]
    end = x[1]["Header3"]
    txt = x[1]["Text"]
    exp = "^" +re.escape(beg)
    txt =re.sub(exp,"", txt)
    try:
        exp = re.escape(end) + "$"
        x[1]["Text"] = re.sub(exp,"", txt)
    except:
        x[1]["Text"] = re.sub(exp,"","")
            

def replacer(row):
    x = re.sub(f"^{row['Header2']}", "", row["Text"])
    return x

def replacer2(row):
    x = re.sub(f"{row['Header3']}$", "", row["Text"])
    return x
    
                               
df["Text"] = df.apply(lambda row: replacer(row), axis = 1)
df["Actual"] = df["Text"]
df["Text"] = df["Text"].apply(lambda x: x.lower())
##df["Tokenize"] = df["Text"].apply(lambda x : nltk.tokenize.word_tokenize(x))

kp = KeywordProcessor()
kp.add_keywords_from_dict(mktintel)


df["Commodity"] = df["Text"].apply(lambda x: kp.extract_keywords(x, span_info=False))
df["Commodity"] = df["Commodity"].apply(lambda x: list(set(x)))
df["Commodity"] = df["Commodity"].astype(str)
df["Commodity"] = df["Commodity"].str.replace("\[","")
df["Commodity"] = df["Commodity"].str.replace("\]","")

df.reset_index(drop = True, inplace=True)



##Split the commodity into seperate categories

df["Commodity"] = df["Commodity"].astype(str)

commodity = df["Commodity"].str.split(pat = ",", expand=True)

num_of_commodity_splits = commodity.shape[1]+1
ls = []
for x in range(1, num_of_commodity_splits, 1):
    st = f"Commodity {str(x)}"
    ls.append(st)

commodity.columns = ls

df = df.merge(commodity, how = "left", left_index=True, right_index=True)

df.rename(columns = {"Header0":"Issue"}, inplace=True)

df["Post"] = df["Country/Body"].map(region)

data = df.copy(deep=True)


df.drop("Text", axis = 1, inplace=True)
df.rename(columns ={"Actual":"Text"}, inplace=True)

df = df[["Date", "Country/Body", "Post", "Issue", "Header2", "Text"] + ls]


df["Text"] = df["Text"].apply(lambda x : x.strip())

df["Text"] = df["Header2"] + " - " + df["Text"]
    



###Country NLP
nlp_df = pd.DataFrame()

for idx, x in df.iterrows():    
    doc = nlp(x["Text"])
    ent_ls = []
    for ent in doc.ents:
        if ent.label_ == "GPE":
            ent_ls.append(ent.text.strip())
        else:
            pass
        ent_ls = [str(x) for x in ent_ls]
        ent_ls = list(set(ent_ls))
        part = pd.DataFrame(data = {"Index" : [idx],
                                    "Entity": [list(ent_ls)]})
        nlp_df = nlp_df.append(part)
    
nlp_df["Entity"] = nlp_df["Entity"].astype(str)
nlp_df["Entity"] = nlp_df["Entity"].str.replace("[","")
nlp_df["Entity"] = nlp_df["Entity"].str.replace("]","")
nlp_df = nlp_df.groupby(by = "Index")["Entity"].apply(lambda x: ",".join(x)).reset_index()


nlp_df["Entity"] = nlp_df["Entity"].str.replace(pat = ",{2,}", repl= "")
nlp_df["Entity"] = nlp_df["Entity"].str.replace(pat = "^,", repl= "")
nlp_df["Entity"] = nlp_df["Entity"].str.replace(pat = ",,", repl= ",")
nlp_df["Entity"] = nlp_df["Entity"].str.replace(pat = "'", repl= "")





foo = pd.DataFrame()

for idx, x in nlp_df.iterrows():
    splitter = list(set(x["Entity"].split(sep =",")))
    part = pd.DataFrame(splitter)
    foo = foo.append(part.T)

for x in foo.columns:
	foo[x] = foo[x].str.strip()

foo_col_names = []

foo = foo.T

for x in range(1, len(foo.columns) +1 ,1):
    foo_col_names.append(f"foo {x}")

foo.columns = foo_col_names



def combiner (foo):
    woo = pd.DataFrame()
    for x in foo.columns:
            part = pd.DataFrame(data = {x: foo[x].str.strip()})
            part[x] = part[x].str.strip()
            part[x] = part[x].str.replace("^\s","foo", regex=True)
            part.dropna(inplace=True)
            part.fillna(value =np.nan, inplace=True)
            part.drop_duplicates(inplace=True)
            woo = pd.concat([woo, part], axis =1 , ignore_index=True)
    woo = woo.T
    return woo

woo = combiner(foo)




col_names = []
for x in range(1, len(woo.columns) + 1,1):
    col_names.append(f"Country/Body {x}")

##nlp_df = foo.copy(deep=True)    
 
woo.columns = col_names
woo.reset_index(inplace =True, drop =True)


nlp_df = nlp_df.merge(woo,  how = "left", left_index=True, right_index=True)

df = df.merge(nlp_df, how = "left", left_index=True, right_on ="Index").copy(deep=True)

df = df[["Date"] + col_names +  ["Post", "Issue", "Header2", "Text"] + ls]

##df.reset_index(inplace=True, drop=True)
##
##country = df[col_names].copy(deep=True)
##country = country.T
##country = combiner(country)
##
##df.drop(labels = col_names, axis = 1, inplace=True)
##df = df.merge(country, how = "left", left_index=True, right_index=True)
##df = df[["Date"] + col_names +  ["Post", "Issue", "Header2", "Text"] + ls]



df.to_excel(r"J:\4. ISE\Market Intel\Data work\00445 Low 050218\Counsellor scraper\Test.xlsx", sheet_name = "WOO", index=False)


del(file)


