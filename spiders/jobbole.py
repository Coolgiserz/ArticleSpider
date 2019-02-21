# -*- coding: utf-8 -*-
import scrapy
import re
from scrapy.http import Request
from urllib import parse
from ArticleSpider.items import JobboleArticleItem
from ArticleSpider.utils.common import get_md5
import datetime
class JobboleSpider(scrapy.Spider):
    name = 'jobbole'
    allowed_domains = ['blog.jobbole.com']
    start_urls = ['http://blog.jobbole.com/all-posts/']
    # start_urls = ['http://blog.jobbole.com/category/career/']

    def parse(self, response):
        '''
        获取文章列表页中的url并交给scrapy下载解析
        获取下一页url交给url下载，下载后交给parse
        :param response:
        :return:
        '''
        post_nodes =  response.css("#archive .floated-thumb .post-thumb a")

        for post_node in post_nodes:
            image_url = post_node.css("img::attr(src)").extract_first("")
            post_url = post_node.css("::attr(href)").extract_first("")
            yield Request(url=parse.urljoin(response.url,post_url),meta={"front_image_url":image_url},callback=self.parse_detail)
        # 提取下一页链接
        next_url = response.css(".next.page-numbers::attr(href)").extract_first()
        if next_url:
            yield Request(url=parse.urljoin(response.url,next_url),callback=self.parse)
        pass
    def parse_detail(self,response):
        jobbole = JobboleArticleItem()
        front_image_url = response.meta.get("front_image_url","")
        title = response.xpath('//*[@class="entry-header"]/h1/text()').extract_first()  # 博客标题
        content = response.xpath('//div[@class="entry"]').extract_first()  # 博客内容
        postdate = response.xpath('//*[@class="entry-meta-hide-on-mobile"]/text()').extract_first().replace('·',
                                                                                                            '').strip()  # 发布日期
        favorite = response.xpath(
            '//*[@class=" btn-bluet-bigger href-style bookmark-btn  register-user-only "]/text()').extract_first()
        # 过滤收藏数
        match_obj = re.match('.*(\d+).*', favorite)
        # 过滤评论数
        if match_obj:
            favorite = int(match_obj.group(1))
        else:
            favorite = 0
        praise = response.xpath('//h10[@id]/text()').extract_first()  # 点赞数
        comment = response.xpath('//*[@class="btn-bluet-bigger href-style hide-on-480"]/text()').extract_first()  # 评论数
        match_obj = re.match('.*(\d+).*', comment)
        # 过滤评论数
        if match_obj:
            comment = int(match_obj.group(1))
        else:
            comment = 0
        tag_lists = response.xpath('//*[@class="entry-meta-hide-on-mobile"]/a/text()').extract()
        tag_lists = [i for i in tag_lists if not i.strip().endswith('评论')]
        tags = ",".join(tag_lists)

        jobbole["title"] = title
        jobbole["url"] = response.url
        try:
            postdate = datetime.datetime.strftime(postdate,'%Y/%m/%d').date()
        except Exception as e:
            postdate = datetime.datetime.now().date()

        jobbole["postdate"] = postdate
        jobbole["praise_nums"] = praise
        jobbole["comment_nums"] = comment
        jobbole["fav_nums"] = favorite
        jobbole["front_image_url"] = [front_image_url]
        jobbole["tags"] = tags
        jobbole["content"] = content
        jobbole["url_object_id"] = get_md5(response.url)
        # jobbole["url_object_id"] = front_image_url
        yield  jobbole # 传递到pipeline
        pass