from osrs_api.const import AccountType
from osrs_api import Hiscores
import pandas as pd
import os
from dotenv import load_dotenv
import pyodbc

load_dotenv()

GetUser = os.getenv('rsn')
user = Hiscores(GetUser)

user_skills = user.skills
user_tot_level = "{0:,.0f}".format(user.total_level)
xp_needed_for_99 = int(13034431)

list_of_skills = list(user_skills.keys())
hiscore_rank = []
skill_level = []
experience = []
xp_till_max = []

for i in list_of_skills:
    rank = user.skills[i].rank
    lvl = user.skills[i].level
    xp = user.skills[i].xp
    if (xp_needed_for_99 - user.skills[i].xp) > 0:
        xptm = xp_needed_for_99 - user.skills[i].xp
    else:
        xptm = 0

    hiscore_rank.append(rank)
    skill_level.append(lvl)
    experience.append(xp)
    xp_till_max.append(xptm)


df = pd.DataFrame(list(zip(list_of_skills, skill_level, experience, hiscore_rank, xp_till_max)), columns=['Skill', 'Level', 'XP', 'Rank', 'XP_till_Max'])
df = df.astype({'Level': int, 'XP': int, 'Rank': int, 'XP_till_Max': int})

df['As_of'] = pd.Timestamp.today().strftime('%m-%d-%Y')

sqlDT = pd.Timestamp.today().strftime('%Y%m%d')
sqlNewTableName = "skillExport_" + sqlDT

sqlDriver = os.getenv('DRIVER')
sqlServer = os.getenv('SERVER')
sqlDatabase = os.getenv('DATABASE')

cxnx = "DRIVER={" + sqlDriver + "};SERVER=" + sqlServer + ";DATABASE=" + sqlDatabase
conn = pyodbc.connect(cxnx)
cursor = conn.cursor()


def sqlInsertData():
    queryTxt_Hist = "insert into osrs_data.dbo.HistoricalSkills (Skill,Level,XP,Rank,As_of,XP_till_Max) values(?,?,?,?,?,?)"
    for index, row in df.iterrows():
        cursor.execute(queryTxt_Hist, row.Skill, row.Level, row.XP, row.Rank, row.As_of, row.XP_till_Max)

    print("Check that the appropriate data has been inserted into osrs_data.dbo.HistoricalSkills")


def sqlUpload():
    sqlInsertData()
    conn.commit()
    cursor.close()


sqlUpload()


