from dotenv import dotenv_values
import os

isExist_logs = os.path.exists('logs')
isExist_databse = os.path.exists('database')

if not isExist_databse:
    os.makedirs('database')
if not isExist_logs:
    os.makedirs('logs')

config = dotenv_values(".env")
if "" in config['TIMEOUT']:
    config['TIMEOUT'] = 50
if "" in config['ELASTIC_DURATION']:
     config['ELASTIC_DURATION']=20
if "" in config['TIME_SLEEP']:
     config['TIME_SLEEP'] = 1
    

