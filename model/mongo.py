from pymongo import MongoClient

host='192.168.0.204'
# host='192.168.0.104'
client = MongoClient(f'mongodb://{host}:27017/')
# 获得db
db = client.game_info
collection = db.gameinfo
