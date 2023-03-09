from mq.mongo_mq import MongoMQ,SpiderMsgData
from model.mongo import db
from model.redis_m import rclient
from spider import fetch_google_urls
def start_producer():
    mq = MongoMQ(db, 'urls_queue', 3)
    for url in fetch_google_urls():
        i = 0
        while i < 3:
            try:
                mq.pub_to_mongo(SpiderMsgData("WAITING", url))
                break
            except Exception as e:
                i += 1

    rclient.delete("kv:game_list:")

if __name__ == '__main__':
    start_producer()
