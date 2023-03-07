import requests
from bs4 import BeautifulSoup

base_url = "https://d.apkpure.com/b/APK/"

# 观察出来了url规律，应该没啥问题
def get_apk_url(pkg_name):
    return base_url + pkg_name + "?version=latest"