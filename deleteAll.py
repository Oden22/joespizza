from pymongo import MongoClient
from utils import SQLDataAccess

# Get these values from the Azure portal page for your cosmos db account. Change
cosmos_database_name = "Pizza"
uri = "mongodb://ict320-obmongo:UkVH3PR4G5AwII87B9A9NVvklCnYzIbXJ9jV7B4xWccrtO0XCB5uK4rsBUSZghFbW6Btvaln2wvlACDbpWVkzw==@ict320-obmongo.mongo.cosmos.azure.com:10255/?ssl=true&retrywrites=false&replicaSet=globaldb&maxIdleTimeMS=120000&appName=@ict320-obmongo@"
mongo_client = MongoClient(uri)
my_db = mongo_client[cosmos_database_name]

def delete(col):
    my_col = my_db[col]
    my_col.delete_many({})

clientServ = SQLDataAccess(
    server = 'ict320-ob-pizaa-sqlserv.database.windows.net',
    database = 'ICT320-PIZZA-SQL',
    username = 'obadmin ',
    password = 'Adminadmin$!'
)

clientServ.commit_data("delete from dbo.DailySummary")
clientServ.commit_data("delete from pizza.summary")
delete("Docket")
delete("Orders")

