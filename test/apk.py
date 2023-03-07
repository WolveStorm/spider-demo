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




def game_spider(game_url, pkg_name):
    global count
    try:
        r = requests.get(game_url, headers=header)
        # 游戏详情的响应
        detail_resp = r.text
        # 解析
        soup1 = BeautifulSoup(detail_resp, "html.parser")
        g_info = GameInfo()
        games_map = {}
        # 解析游戏名称
        name_soup = soup1.find_all(name='h1', attrs={"itemprop": "name"})
        game_name = re.findall("<span>(.*?)</span>", str(name_soup[0]))[0]
        games_map["name"] = game_name
        g_info.name = game_name
        # 解析游戏图片
        avatar_soup = soup1.find_all(name='img', attrs={"class": "T75of cN0oRe fFmL2e"})[0]
        games_map["avatar"] = avatar_soup['src']
        g_info.avatar_url = avatar_soup['src']
        # 解析游戏出版公司
        company_soup = soup1.find_all(name='div', attrs={"class": "Vbfug auoIOc"})
        games_map["company"] = re.findall("<span>(.*?)</span>", str(company_soup[0]))[0]
        g_info.company = re.findall("<span>(.*?)</span>", str(company_soup[0]))[0]
        # 解析游戏评分
        score_soup = soup1.find_all(name='div', attrs={"class": "TT9eCd"})
        games_map["score"] = score_soup[0]['aria-label']
        g_info.score = score_soup[0]['aria-label']
        # 解析下载次数
        download_soup = soup1.find_all(name='div', attrs={"class": "ClM7O"})
        games_map["download_times"] = re.findall(">(.*?)<", str(download_soup[1]))[0]
        g_info.download_times = re.findall(">(.*?)<", str(download_soup[1]))[0]
        # 解析并下载apk包
        apk_url = get_apk_url(pkg_name)
        games_map["apk_url"] = apk_url
        g_info.apk_url = apk_url
        print(apk_url)
        # 存储到数据库
        # collection.insert_one(games_map)
        # g_info.save()
        count += 1
    except Exception as e:
        print(e)
        print("爬取出现错误")


if __name__ == '__main__':
    game_spider("https://play.google.com/store/apps/details?id=com.king.farmheroessaga","com.king.farmheroessaga")
