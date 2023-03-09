status_map = {
    'WAITING': 0,
    'PROCESSING': 1,
    'COMPLETE': 2,
}

class SpiderMsgData:
    def __init__(self, status, url):
        self.status = status
        self.data = url
