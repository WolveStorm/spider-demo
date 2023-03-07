import redis    # 导入redis 模块
host='192.168.0.204'
# host='192.168.0.104'

rclient = redis.Redis(host=host, port=6379, decode_responses=True)
