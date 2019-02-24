# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy.pipelines.images import ImagesPipeline
import codecs
import json
from scrapy.exporters import JsonItemExporter
from twisted.enterprise import adbapi
import MySQLdb
import MySQLdb.cursors
class ArticlespiderPipeline(object):
    def process_item(self, item, spider):
        return item

class JsonExporterPipeline(object):
    '''
    使用scrapy自带的Json export导出json文件
    '''
    def __init__(self):
        self.file = open('articleexport.json','wb')#二进制方式打开
        self.exporter = JsonItemExporter(self.file,encoding='utf-8',ensure_ascii=False)
        self.exporter.start_exporting()
    def close_spider(self,spider):
        self.exporter.finish_exporting()
        self.file.close()
    def process_item(self,item,spider):
        self.exporter.export_item(item)
        return item

class JsonWithEncodingPipeline(object):
    '''codecs'''
    def __init__(self):
        self.file = codecs.open('article.json','w',encoding='utf-8')
    def process_item(self,item,spider):
        line = json.dumps(dict(item),ensure_ascii=False)+"\n"
        self.file.write(line)
        return item
    def spider_close(self,spider):
        self.file.close()

class MysqlPipline(object):
    #采用同步机制写入MySQL
    def __init__(self):
        self.conn = MySQLdb.connect('localhost','root','zhugemysql','scrapyspider',charset='utf8',use_unicode=True)
        self.cursor = self.conn.cursor()
    def process_item(self,item,spider):
        insert_sql = """
            insert into jobbole_spider(title,tags,postdate,praise_nums)
            values(%s,%s,%s,%s)
        """
        self.cursor.execute(insert_sql,(item["title"],item["tags"],item["postdate"],item["praise_nums"]))
        self.conn.commit()

class MysqlTwistedPipline(object):
    '''MySQL写入异步化'''
    def __init__(self,dbpool):
        self.dbpool = dbpool
    @classmethod
    def from_settings(cls,settings):

        dbparms = dict(
        host = settings["MYSQL_HOST"],
        db = settings["MYSQL_DBNAME"],
        user = settings["MYSQL_USER"],
        passwd = settings["MYSQL_PASSWORD"],
        charset='utf8',
        cursorclass = MySQLdb.cursors.DictCursor,
        use_unicode = True,
        )
        dbpool = adbapi.ConnectionPool("MySQLdb",**dbparms)
        return cls(dbpool)
    def process_item(self,item,spider):
        #使用teisted将mysql插入变成异步执行
        query = self.dbpool.runInteraction(self.do_insert,item)
        query.addErrback(self.handle_error)# 异步处理异常
    def handle_error(self,failure):
        #处理异步插入的异常
        print(failure)
    def do_insert(self,cursor,item):
        #数据库插入
        # insert_sql = """
        #          insert into jobbole_spider_copy2(title,tags,postdate,praise_nums,comment_nums,fav_nums,front_image_url,front_image_path,url,url_object_id,content)
        #          values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE url_object_id=9;
        #      """
        print('prepare to insert data')
        insert_sql = """
                 insert into jobbole_spider_copy1(title,tags,postdate,praise_nums,comment_nums,fav_nums,front_image_url,front_image_path,url,url_object_id,content)
                 values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE praise_nums=VALUES(praise_nums),comment_nums=VALUES(comment_nums),fav_nums=VALUES(fav_nums);
        """

        cursor.execute(insert_sql,(item["title"],item["tags"],item["postdate"],item["praise_nums"],item["comment_nums"],item["fav_nums"],item["front_image_url"],item["front_image_path"],item["url"],item["url_object_id"],item["content"]))
        print('insert data sessfully')

class ArticleImagePipeline(ImagesPipeline):
    '''
    定制pipeline，处理封面图
    '''

    def item_completed(self, results, item, info):
        for ok,value in results:
            image_file_path = value['path']
            item["front_image_path"] = image_file_path
            return item
        # pass