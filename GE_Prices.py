import requests
import os
import pandas as pd
from dotenv import load_dotenv
import pyodbc
import statistics

load_dotenv()

GetItemIds = os.getenv('itemID_source')
GetApiEndpoints = os.getenv('api_endpoint')
headers = {'User-Agent': 'casual price checker - @dadundies7 on Discord'}

# Establish Sql Server Connection
sqlDriver = os.getenv('DRIVER')
sqlServer = os.getenv('SERVER')
sqlDatabase = os.getenv('DATABASE')

cxnx = "DRIVER={" + sqlDriver + "};SERVER=" + sqlServer + ";DATABASE=" + sqlDatabase
conn = pyodbc.connect(cxnx)
cursor = conn.cursor()

# Input the list of items you want to look up. Do not do more than 20 at a time!!
ListOfItems = []
ListOfInvestmentAmts = []
queryItems = "select Item_Name from osrs_data.dbo.GearInvestments"
for row in cursor.execute(queryItems).fetchall():
    ListOfItems.append(row.Item_Name)
queryInvestmentAmts = "select Amt_Invested from osrs_data.dbo.GearInvestments"
for row in cursor.execute(queryInvestmentAmts).fetchall():
    ListOfInvestmentAmts.append(row.Amt_Invested)

# Get ItemID manifest
itemID_request = requests.request("GET", GetItemIds, headers=headers)
ItemIDs = itemID_request.json()

# Create empty lists to zip together in a dataframe
ListOfBuyPrices = []
ListOfSellPrices = []
ListOfMidPrices = []

for i in range(0, len(ListOfItems)):
    ChooseItem = ListOfItems[i]
    ItemID = ItemIDs[ChooseItem]

    request_url = GetApiEndpoints + 'latest?id=' + str(ItemID)
    price_request = requests.request("GET", request_url, headers=headers)
    json_prices = price_request.json()
    prices = json_prices['data'][str(ItemID)]
    ListOfBuyPrices.append(prices['high'])
    ListOfSellPrices.append(prices['low'])
    ListOfMidPrices.append(statistics.median([prices['high'],prices['low']]))

df = pd.DataFrame(list(zip(ListOfItems,ListOfInvestmentAmts,ListOfBuyPrices,ListOfSellPrices,ListOfMidPrices)), columns=['Item_Name','Amt_Invested','Curr_Buy_Price','Curr_Sell_Price','Curr_Mid_Price'])
df = df.astype({'Amt_Invested': int, 'Curr_Buy_Price': int, 'Curr_Sell_Price': int, 'Curr_Mid_Price': int})
df['As_of'] = pd.Timestamp.today().strftime('%m-%d-%Y')

def sqlInsertData():
    queryTxt_Tracking = "insert into osrs_data.dbo.InvestmentTracking (Item_Name,Amt_Invested,Curr_Buy_Price,Curr_Sell_Price,Curr_Mid_Price) values(?,?,?,?,?)"
    for index, row in df.iterrows():
        cursor.execute(queryTxt_Tracking, row.Item_Name, row.Amt_Invested, row.Curr_Buy_Price, row.Curr_Sell_Price, row.Curr_Mid_Price)


def sqlUpload():
    sqlInsertData()
    conn.commit()
    cursor.close()

sqlUpload()

