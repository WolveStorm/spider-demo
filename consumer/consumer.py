import re
from spider import consumer_func
from mq.mongo_mq import MongoMQ
from model.mongo import db
from model.redis_m import rclient

def work(data,game_mongo_list,game_post_list):
    try:
        url = data['data']
        pkg_name = re.findall("id=(.*?)$", url)[0]
        # 去重
        if rclient.sadd("pkg_set", pkg_name) != 1:
            return
        print(f"开始爬取,url:{url} ,apk包名:{pkg_name}")
        consumer_func(url, pkg_name, game_mongo_list, game_post_list)
    except Exception as e:
        print(e)

def start_consumer():
    mq = MongoMQ(db, 'urls_queue', 3)
    mq.sub_from_mongo(work)

if __name__ == '__main__':
    start_consumer()