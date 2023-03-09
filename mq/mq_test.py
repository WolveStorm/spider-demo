from pymongo import MongoClient
from mongo_mq import MongoMQ
from msg import SpiderMsgData
def work(data):
    print(data['data'])
    print(type(data))
    print(data)

if __name__ == '__main__':
    host = '192.168.0.204'
    # host='192.168.0.104'
    client = MongoClient(f'mongodb://{host}:27017/')
    # 获得db
    db = client.game_info
    mq = MongoMQ(db,'urls_queue',3)
    # mq.pub_to_mongo(SpiderMsgData("WAITING",'https://pymongo.readthedocs.io/en/stable/api/pymongo/cursor.html'))
    f = mq.sub_from_mongo()
    while True:
        work(next(f))