import mammoth
import pandas as pd
import os
from bs4 import BeautifulSoup
import re
import numpy as np
import sys

def go_tim_go(data_folder):
    countrycounsellor = pd.read_excel(r"J:\4. ISE\Market Intel\Data work\000 Dogbert 160489\zzUniversal_reference\RTM Country to Post.xlsx", sheet_name = 0)
    countrycounsellor = dict(zip(countrycounsellor["Country/Body"], countrycounsellor["Post"]))


    blanktextreplace = {"Enter any emerging risks here"                               : "blanktext",
                        "Enter any content you want here."                            : "blanktext",
                        "Enter any content that you want here."                       : "blanktext",
                        "Enter any content that you want here"                        : "blanktext",  
                        "Enter any opportunities headline here"                       : "blanktext",
                        "Enter trend/development headline here"                       : "blanktext",
                        "Enter MO progress headline here"                             : "blanktext",
                        "Enter MO content you want here."                             : "blanktext",
                        "Enter any MO content you want here."                         : "blanktext",
                        "Enter any Recent Wins headline here"                         : "blanktext",
                        "Enter any recent wins content that you want here"            : "blanktext",
                        "Enter any General news/updates headline here"                : "blanktext",
                        "Enter any general news/updates content that you want here"   : "blanktext"
                        
        }



    def category_builder(number, code, ls):
        for x in range(1, number+1,1):
            title, body = f"{code}{x}t", f"{code}{x}b"
            ls.append(title)
            ls.append(body)
        return ls
        





    def tim_quinn_it(file):
        html_doc = mammoth.convert_to_html(file)
        parafix = html_doc.value

        for key in blanktextreplace.keys():
            parafix = parafix.replace(key, blanktextreplace[key])
            
    ##Accounts for paragraph breaks in the entry
        parafix = parafix.replace("<h4>blanktext</h4>"  , "ThIsiSpAtRiCk")
        parafix = parafix.replace("</h4><h4><strong>"   , "sPoNgEbOb")
        parafix = parafix.replace("</strong></h4><h4>"  , "mEmOnEy")
        parafix = parafix.replace("</h4><h4>"           , " ")
        parafix = parafix.replace("ThIsiSpAtRiCk"       , "<h4>blanktext</h4>")
        parafix = parafix.replace("sPoNgEbOb"           , "</h4><h4><strong>")
        parafix = parafix.replace("mEmOnEy"             , "</strong></h4><h4>")

        soup = BeautifulSoup(parafix, "html.parser")    
       
        h_list_i = []
        for i in range(1):
            list_h4 = []
            for element in soup.select('h4'):
                i = str(element.get_text())
                list_h4.append(element.get_text())
                h_list_i = pd.Series(list_h4)

        text_ser = []
        for i in h_list_i:
            text = i.replace("\n"," ").replace("  ","")
            text_ser.append(text)
        post_data = pd.Series(text_ser)

        df = pd.DataFrame(post_data)
        df.columns = [file]
        df = df.T


        ###Autodetect the number of entries  
        counter = pd.DataFrame()
        no = int(len(soup.select("tr")))
        for x in range(0,no, 1):
            for no in list(soup.select("tr")[x].select("h4")):
                part = pd.DataFrame(data = {"data":[soup.select("tr")[x].select("h1")[0].text], "Bolded": [len(no.select("strong"))]})
                counter = counter.append(part)

        counter = counter.groupby("data").count()
        counter = pd.DataFrame(counter)
        counter.reset_index(inplace=True)
        counter["data"] = counter["data"].str.strip()


        def category_counter(entry):
            try:
                number = counter.loc[counter["data"] == entry,:]["Bolded"]
                number = number.iloc[0]/2
            except:
                number = 0

            return int(number)
        
        
        riskno = category_counter("Emerging risks")
        mktono = category_counter("Opportunities for trade or market development")
        trenno = category_counter("Trends and important developments")
        progno = category_counter("Progress against Ministerial priorities")
        recwno = category_counter("Recent wins")
        geneno = category_counter("General news and updates")

        ##Ignore Country/Body and Month/Year. These will always be one

        column_foo = ["Country/Body", "Month/Year"]
                                      
        
        column_foo = category_builder(riskno, "eri", column_foo)
        column_foo = category_builder(mktono, "opm", column_foo)
        column_foo = category_builder(trenno, "tid", column_foo)
        column_foo = category_builder(progno, "pam", column_foo)
        column_foo = category_builder(recwno, "rew", column_foo)
        column_foo = category_builder(geneno, "gen", column_foo)    


        df.columns = column_foo
                      
                      
        def melter(df, regexp, value_name):
            ls = []

            column_foo = df.columns

            for x in column_foo:
                tgt = re.findall(regexp,x)
                ls.append(tgt)
            ls =[str(x) for x in ls]
            ls = [x.replace("[", "") for x in ls]
            ls = [x.replace("]", "") for x in ls]
            ls = [x.replace("'", "") for x in ls]
            ls = list(set(ls))
            ls = [x for x in ls if x!=""]
            id_vars = [x for x in column_foo if x not in ls]

            if df.melt(id_vars = id_vars,  var_name = "foo", value_name = value_name).shape[0] == 0:
                pass
            else:
                df = df.melt(id_vars = id_vars,  var_name = "foo", value_name = value_name)
                df.drop("foo", axis =1, inplace=True)




            return df

        

        df = melter(df, "eri[0-9]{1,}.*", "Emerging risks")
        df = melter(df, "opm[0-9]{1,}.*", "Opportunities for trade or market development")
        df = melter(df, "tid[0-9]{1,}.*", "Trends and important developments")
        df = melter(df, "pam[0-9]{1,}.*", "Progress against Ministerial priorities")
        df = melter(df, "rew[0-9]{1,}.*", "Recent wins")
        df = melter(df, "gen[0-9]{1}.*", "General news and updates")

     #   df.dropna(axis = 0, how = "all", inplace=True)
                      
        df = df.melt(id_vars = ['Country/Body', 'Month/Year'], var_name ="Categories", value_name = "Text")


        df.drop_duplicates(subset = ["Text"], inplace=True)
        df.reset_index(inplace=True, drop=True)
                           

        ##Pull out cat2 only
        title = df.iloc[0::2,:].copy(deep=True)

        #pull out body text
        text =  df.iloc[1::2,:].copy(deep=True)
          
        text.reset_index(inplace=True, drop = True)
        title.reset_index(inplace=True, drop =True)

        
        title.rename(columns = {"Text":"Header2"}, inplace=True)
        title = title[["Header2"]].copy(deep=True)

        df = text.merge(title, how = "left", left_index=True, right_index=True)
                       
            



        df["filename"] = file
                         
        return df



    os.chdir(data_folder)

    ls = []

    df = pd.DataFrame()
    for directories, folders, files in os.walk(data_folder):
        for file in files:
            try:
                data = tim_quinn_it(directories + "\\" +file)
                data = pd.DataFrame(data)
                
                ls.append(data)

                print(f"{file} processed")
            except:
                print(f"{file} failed")
                pass


    df = pd.DataFrame(ls[0])

    for x in ls[1:]:
        df = df.append(x)


    df["Post"] = df["Country/Body"].map(countrycounsellor)

    df["Date"] = pd.to_datetime(df["Month/Year"], infer_datetime_format=True)





    df = df[["Date", "Month/Year", "Country/Body", "Post", "Categories", "Header2", "Text", "filename"]]

    df   = df.loc[df["Text"]!="blanktext",:].copy(deep=True)


    return df

