# This is a template for a Python scraper on morph.io (https://morph.io)
# including some code snippets below that you should find helpful

# import scraperwiki
# import lxml.html
#
# # Read in a page
# html = scraperwiki.scrape("http://foo.com")
#
# # Find something on the page using css selectors
# root = lxml.html.fromstring(html)
# root.cssselect("div[align='left']")
#
# # Write out to the sqlite database using scraperwiki library
# scraperwiki.sqlite.save(unique_keys=['name'], data={"name": "susan", "occupation": "software developer"})
#
# # An arbitrary query against the database
# scraperwiki.sql.select("* from data where 'name'='peter'")

# You don't have to do things with the ScraperWiki and lxml libraries.
# You can use whatever libraries you want: https://morph.io/documentation/python
# All that matters is that your final data is written to an SQLite database
# called "data.sqlite" in the current working directory which has at least a table
# called "data".

# coding: utf-8

# In[1]:
import scraperwiki
import lxml.html
import requests

# In[2]:

sdf = pd.read_csv('../Data/AU.txt', sep="\t", 
                 names=['Country','Postcode','Suburb','StName','State','City','B1','B22','B3','Lat','Long','Other'])


# In[3]:

property_types = [
    '2bdHouse','3bdHouse','4bdHouse','1bdUnit','2bdUnit','3bdUnit'
]


# In[16]:

df = sdf.copy()


# In[ ]:

df['Suburb'] = df['Suburb'].astype(str).str.strip().str.replace(' ','-')
df['Postcode'] = df['Postcode'].astype(str).str.zfill(4)
success_record = {}

adf = df.copy()#[df['State']=='VIC']

with open('../Outputs/success_record.txt','w') as outfile:

    for sub,st,pc in list(zip(adf['Suburb'].astype(str).str.strip().str.replace(' ','-'),
                              adf['State'].astype(str),
                              adf['Postcode'].astype(str).str.zfill(4))):
        r = requests.get('http://www.domain.com.au/suburb-profile/'+"-".join([sub,st,pc]))
        try:
            r.raise_for_status()
            df.loc[
                (df['Suburb'].astype(str)==sub)&
                (df['State'].astype(str)==st)&
                (df['Postcode'].astype(str)==pc),
                'Rental Percentage'
            ] = r.text.split('data-label="renting">')[1].split("<")[0]
            df.loc[
                (df['Suburb'].astype(str)==sub)&
                (df['State'].astype(str)==st)&
                (df['Postcode'].astype(str)==pc),
                'Purchasing Percentage'
            ] = r.text.split('data-label="purchasing">')[1].split("<")[0]
            try:
                for property_type in property_types:
                    nullcount = 0
                    a = r.text.split(property_type)[1].split("mediansoldprice\":")[1].split(",")[0]
                    if a.lower() == "null":
                        nullcount += 1
                    df.loc[
                        (df['Suburb'].astype(str)==sub)&
                        (df['State'].astype(str)==st)&
                        (df['Postcode'].astype(str)==pc),
                        property_type
                    ] = a
                print('-'.join([sub,st,pc])+' was successful! Inserted to table. There were '+
                      str(nullcount)+' null prices')
                outfile.write('-'.join([sub,st,pc,'successful','yes_prices']))
            except:
                print("Couldn't find property info for "+'-'.join([sub,st,pc]))
                outfile.write('-'.join([sub,st,pc,'successful','no_prices']))
                None
            try:
                df.loc[
                        (df['Suburb'].astype(str)==sub)&
                        (df['State'].astype(str)==st)&
                        (df['Postcode'].astype(str)==pc),
                        'historical_prices'
                    ] = r.text.split('data: [')[1].split(']')[0]
            except:
                print('No historical prices for '+'-'.join([sub,st,pc]))
                None
            try:
                df.loc[
                            (df['Suburb'].astype(str)==sub)&
                            (df['State'].astype(str)==st)&
                            (df['Postcode'].astype(str)==pc),
                            'unit_historical_prices'
                        ] = r.text.split('data: [')[2].split(']')[0]
            except:
                print(sub+"-"+st+"-"+pc,"units hp failed")
        except:
            print('-'.join([sub,st,pc])+' threw an error')
            outfile.write('-'.join([sub,st,pc,'failed','completely']))
            continue


# In[26]:

df[pd.notnull(df['Rental Percentage'])].drop(['B1','B22','B3'],axis=1
                                            ).to_csv('../Outputs/complete_table_2.csv', index=False)




