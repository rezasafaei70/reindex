from dotenv import dotenv_values
import os

isExist_logs = os.path.exists('logs')
isExist_databse = os.path.exists('database')

if not isExist_databse:
    os.makedirs('database')
if not isExist_logs:
    os.makedirs('logs')

config = dotenv_values(".env")
if not config['TIMEOUT']:
    config['TIMEOUT'] = 50
if not config['ELASTIC_DURATION']:
     config['ELASTIC_DURATION']=10
if not config['TIME_SLEEP']:
     config['TIME_SLEEP'] = 1
if not config['SIZE']:
     config['SIZE'] = 100
     
config['VERSION']="v2.2.9"
    

