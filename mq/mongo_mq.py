import time
from pymongo.cursor import CursorType
from .msg import SpiderMsgData,status_map
from datetime import datetime
from model.postgres import GameInfo
from model.mongo import collection
game_mongo_list = []
game_post_list = []
class MongoMQ:
    def __init__(self, db, q_name):
        self.db = db
        self.q_name = q_name
        if not self._exists():
            self._create_queue_collection()
        self.q = db[q_name]
        if not self._valid_queue_collection():
            raise Exception("please make sure collection is capped collection")

    def _exists(self):
        return self.q_name in self.db.list_collection_names()

    def _create_queue_collection(self):
        try:
            self.db.create_collection(self.q_name,
                                      capped=True, max=100000,
                                      size=100000, autoIndexId=True)
        except:
            raise Exception(f'Collection {self.q_name} already created')

    def _valid_queue_collection(self):
        opts = self.db[self.q_name].options()
        if opts.get('capped', False):
            return True
        return False

    def pub_to_mongo(self, msg: SpiderMsgData):
        if msg.status not in status_map or status_map[msg.status] != 0:
            raise Exception('invalid msg status please check field status')
        doc = {
            "data": msg.data,
            "status": status_map[msg.status],
            'metadata': {
                'create_time': datetime.now(),
                'start_time': datetime.now(),
                'complete_time': datetime.now()
            }
        }
        try:
            self.q.insert_one(doc)
        except:
            raise Exception('pub msg to mongodb error')

    def sub_from_mongo(self,work_func):
        global game_mongo_list
        global game_post_list
        cursor = self.q.find({'status': status_map['WAITING']}, cursor_type=CursorType.TAILABLE)  # 采用Tailable Cursors
        cursor = cursor.hint([('$natural', 1)])  # 依照mongodb建议，采用磁盘顺序进行查询，不使用索引
        while True:
            try:
                row = cursor.next()
                try:
                    # 修改当前任务为正在运行状态
                    self.q.find_one_and_update(
                        {
                            '_id': row['_id'],
                            'status': status_map['WAITING']
                        },
                        {
                            '$set': {
                                'status': status_map['PROCESSING'],
                                'metadata.start_time': datetime.now()
                            }
                        }
                    )
                except Exception as e:
                    print(e)
                    print("update status from WAITING TO PROCESSING error")
                work_func(row,game_mongo_list,game_post_list)
                # 这里任务已然执行完成
                self.q.find_one_and_update(
                    {
                        '_id': row['_id'],
                        'status': status_map['PROCESSING']
                    },
                    {
                        '$set': {
                            'status': status_map['COMPLETE'],
                            'metadata.complete_time': datetime.now()
                        }
                    }
                )
            except StopIteration as e:
                if len(game_mongo_list) != 0 and len(game_post_list) != 0:
                    collection.insert_many(game_mongo_list)
                    GameInfo.insert_many(game_post_list,
                                         fields=[GameInfo.name, GameInfo.apk_url, GameInfo.avatar_url, GameInfo.score,
                                                 GameInfo.company, GameInfo.description,
                                                 GameInfo.download_times]).execute()
                    game_mongo_list = []
                    game_post_list = []
                time.sleep(1)
            except Exception as e:
                print(e)
                # next没有元素会报错，此时由于find是Tailable的，cursor始终指向最后一个文档
                # 我们此时等待新的文档即可。
                time.sleep(1)