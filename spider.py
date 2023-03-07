import os.path
import threading
from model.redis_m import rclient
from model.postgres import GameInfo
from model.mongo import collection
from apk_fetch.apk_fetch import get_apk_url


import requests
import re
from bs4 import BeautifulSoup
import psycopg2

# 伪装header
header = {'User-Agent': 'Mozilla/5.0 (compatible; Baiduspider-render/2.0; +http://www.baidu.com/search/spider.html)'}
# google商店前缀
google_base_url = "https://play.google.com"
# 下载apk包的url
download_apk_url = "https://apkcombo.com/de-de/apk-downloader/?device=&arches=&sdkInt=28&sa=1&lang=en&dpi=480&q="
# 爬虫计数
count = 0
# apk下载的路径
apk_path = "/tmp/pycharm_project_176/apk"

# 下载n个apk
download_times = 0
# 下载线程列表
download_threads = []
# 游戏列表
game_mongo_list = []
game_post_list = []

def do_google_spider():
    global download_threads
    print("开始爬虫")
    url = "https://play.google.com/store/games"
    # 伪装header
    r = requests.get(url, headers=header)
    # 游戏页的响应
    index_resp = r.text
    # 解析
    soup1 = BeautifulSoup(index_resp, "html.parser")
    game_infos = soup1.find_all(name="a", attrs={"class": "Si6A0c Gy4nib"})
    for info in game_infos:
        # 得到游戏详情页的url
        game_detail_url = google_base_url + info['href']
        pkg_name = re.findall("id=(.*?)$", game_detail_url)[0]
        # 去重
        if rclient.sadd("pkg_set",pkg_name) != 1:
            continue
        print(f"开始爬取,url:{game_detail_url} ,apk包名:{pkg_name}")
        game_spider(game_detail_url, pkg_name)
    collection.insert_many(game_mongo_list)
    GameInfo.insert_many(game_post_list, fields=[GameInfo.name, GameInfo.apk_url, GameInfo.avatar_url, GameInfo.score, GameInfo.company, GameInfo.description, GameInfo.download_times]).execute()
    for t in download_threads:
        t.join()
    print(f"爬虫结束,总共爬取{count}条数据,下载了{len(download_threads)}个apk包")


def game_spider(game_url, pkg_name):
    global count
    global download_times
    global download_threads
    global game_mongo_list
    try:
        r = requests.get(game_url, headers=header)
        # 游戏详情的响应
        detail_resp = r.text
        # 解析
        soup1 = BeautifulSoup(detail_resp, "html.parser")
        # g_info = GameInfo()
        games_map = {}
        post_games_map = {}
        # 解析游戏名称
        name_soup = soup1.find_all(name='h1', attrs={"itemprop": "name"})
        game_name = re.findall("<span>(.*?)</span>", str(name_soup[0]))[0]
        games_map["name"] = game_name
        post_games_map["name"] = game_name
        # g_info.name = game_name
        # 解析游戏图片
        avatar_soup = soup1.find_all(name='img', attrs={"class": "T75of cN0oRe fFmL2e"})[0]
        post_games_map["avatar_url"] = avatar_soup['src']
        games_map["avatar"] = avatar_soup['src']
        # g_info.avatar_url = avatar_soup['src']
        # 解析游戏出版公司
        company_soup = soup1.find_all(name='div', attrs={"class": "Vbfug auoIOc"})
        games_map["company"] = re.findall("<span>(.*?)</span>", str(company_soup[0]))[0]
        post_games_map["company"] = re.findall("<span>(.*?)</span>", str(company_soup[0]))[0]
        # g_info.company = re.findall("<span>(.*?)</span>", str(company_soup[0]))[0]
        # 解析游戏评分
        score_soup = soup1.find_all(name='div', attrs={"class": "TT9eCd"})
        games_map["score"] = score_soup[0]['aria-label']
        post_games_map["score"] = score_soup[0]['aria-label']
        # g_info.score = score_soup[0]['aria-label']
        # 解析下载次数
        download_soup = soup1.find_all(name='div', attrs={"class": "ClM7O"})
        games_map["download_times"] = re.findall(">(.*?)<", str(download_soup[1]))[0]
        post_games_map["download_times"] = re.findall(">(.*?)<", str(download_soup[1]))[0]
        # g_info.download_times = re.findall(">(.*?)<", str(download_soup[1]))[0]
        # 解析游戏简介
        desc_soup = soup1.find_all(name='div', attrs={"class": "bARER"})[0]
        games_map["description"] = desc_soup.text
        post_games_map["description"] = desc_soup.text
        # g_info.description = desc_soup.text
        # 解析并下载apk包
        apk_url = get_apk_url(pkg_name)
        games_map["apk_url"] = apk_url
        post_games_map["apk_url"] = apk_url
        # g_info.apk_url = apk_url
        if download_times > 0:
            t = threading.Thread(target=download_apk,args=(apk_url,game_name))
            t.start()
            download_threads.append(t)
            download_times -= 1
            # download_apk(apk_url,game_name)
        # 存储到数据库
        game_mongo_list.append(games_map)
        game_post_list.append(post_games_map)
        # collection.insert_one(games_map)
        # g_info.save()
        count += 1
    except Exception as e:
        print(e)
        print("爬取出现错误")

def download_apk(download_url,apk_name):
    download_header = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/605.1.15      (KHTML, like Gecko) Version/13.1 Safari/605.1.15',
    }
    r = requests.get(url=download_url, headers=download_header)

    f_path = os.path.join(apk_path,apk_name+".apk")

    with open(f_path, 'wb') as writer:
        writer.write(r.content)

if __name__ == '__main__':
    do_google_spider()
