import requests
import pandas as pd
import os
from dotenv import load_dotenv
import pyodbc

load_dotenv()

"""
'boss_starting_pos' is the index value of the first boss in the list..
This will change over time as things are added to the game. 
This is used to separate the lists into activities and bosses.
 """
boss_starting_pos = 18

GetUser = os.getenv('rsn')
GetEndpoint = os.getenv('activities_endpoint')
headers = {'User-Agent': 'casual price checker - @dadundies7 on Discord'}

# Query the API
request_url = GetEndpoint + str(GetUser)
request = requests.request("GET", request_url, headers=headers)
jsonRequest = request.json()
jsonActivities = jsonRequest['activities']

# Create empty lists and extract json data into lists then consolidate into one df
ListActivityNames = []
ListActivityRank = []
ListActivityCounts = []
ListBossNames = []
ListBossRank = []
ListBossCounts = []

# Activities
for i in range(0,(boss_starting_pos-1)):
    ListActivityNames.append(jsonActivities[i]['name'])
    ListActivityRank.append(jsonActivities[i]['rank'])
    ListActivityCounts.append(jsonActivities[i]['score'])

ListActivityRank = list(map(lambda x: 0 if x == -1 else x, ListActivityRank))
ListActivityCounts = list(map(lambda x: 0 if x == -1 else x, ListActivityCounts))

# Bosses
for n in range(boss_starting_pos,len(jsonActivities)):
    ListBossNames.append(jsonActivities[n]['name'])
    ListBossRank.append(jsonActivities[n]['rank'])
    ListBossCounts.append(jsonActivities[n]['score'])

ListBossRank = list(map(lambda x: 0 if x == -1 else x, ListBossRank))
ListBossCounts = list(map(lambda x: 0 if x == -1 else x, ListBossCounts))

# Activities
df_activities = pd.DataFrame(list(zip(ListActivityNames,ListActivityCounts,ListActivityRank)), columns=['Activity','KC','Rank'])
df_activities = df_activities.astype({'KC': int, 'Rank': int})
df_activities['As_of'] = pd.Timestamp.today().strftime('%m-%d-%Y')

# Bosses
df_bosses = pd.DataFrame(list(zip(ListBossNames,ListBossCounts,ListBossRank)), columns=['Boss','KC','Rank'])
df_bosses = df_bosses.astype({'KC': int, 'Rank': int})
df_bosses['As_of'] = pd.Timestamp.today().strftime('%m-%d-%Y')

# Establish Sql Server Connection
sqlDriver = os.getenv('DRIVER')
sqlServer = os.getenv('SERVER')
sqlDatabase = os.getenv('DATABASE')

cxnx = "DRIVER={" + sqlDriver + "};SERVER=" + sqlServer + ";DATABASE=" + sqlDatabase
conn = pyodbc.connect(cxnx)
cursor = conn.cursor()

def sqlInsertActivityData():
    queryTxt_Activities = "insert into osrs_data.dbo.ActivityLog (Activity,KC,Rank) (?,?,?)"
    for index, row in df_activities.iterrows():
        cursor.execute(queryTxt_Activities, row.Activity, row.KC, row.Rank)

def sqlInsertBossData():
    queryTxt_Boss = "insert into osrs_data.dbo.BossLog (Boss,KC,Rank) (?,?,?)"
    for index, row in df_bosses.iterrows():
        cursor.execute(queryTxt_Boss, row.Boss, row.KC, row.Rank)

def sqlUpload():
    sqlInsertActivityData()
    sqlInsertBossData()
    conn.commit()
    cursor.close()

sqlUpload()