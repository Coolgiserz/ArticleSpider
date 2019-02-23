#coding:utf-8
__author__ = 'coolcats'
from scrapy.selector import Selector
import requests
import MySQLdb
from fake_useragent import UserAgent
HOST = "localhost"
USER = "root"
PASSWD = "zhugemysql"
DB = "scrapyspider"
conn = MySQLdb.connect(host=HOST,user=USER,passwd=PASSWD,db=DB,charset="utf8")
cursor = conn.cursor()

class GetIP(object):

    def insert_ip(self,infodict):
        insert_sql = """
            insert into ip_proxy(ip,port,ip_type,speed) value ('{0}','{1}','{2}',{3})
        """.format(infodict["ip_addr"],infodict["port"],infodict["ip_type"],infodict["speed"])
        cursor.execute(insert_sql)
        conn.commit()
        pass

    def get_random_ip(self):
        pass

    def delete_ip(self):
        pass

ua = UserAgent()
get_ip = GetIP()
def get_random_ua():
    return getattr(ua, "random")
def crawl_ips():
    '''
    爬取西刺代理ip
    :return:
    '''
    headers = {'User-Agent':get_random_ua()}

    for index in range(1,3000):
        re = requests.get("https://www.xicidaili.com/nn/{0}".format(index), headers=headers)
        selector = Selector(text=re.text)
        ip_table = selector.css("#ip_list tr")
        for ip in ip_table[1:]:
            speed = ip.css(".country .bar::attr(title)").extract_first().split('秒')[0]
            if(int(speed) > 2):
                continue
            ipinfo = {
                "speed" : speed,
                "ip_addr" : ip.css("td")[1].css("::text").extract_first(),
                "port" :ip.css("td")[2].css("::text").extract_first(),
                "ip_type" : ip.css("td")[5].css("::text").extract_first(),
            }
            get_ip.insert_ip(ipinfo)
    pass



if __name__=="__main__":
    crawl_ips()