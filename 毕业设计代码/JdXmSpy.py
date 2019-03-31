# -*- coding: utf-8 -*-
"""
Created on Fri Sep 21 11:36:31 2018

@author: dflx
"""

import json
import requests
import pymssql
import threading
import time
import csv
ip_dict = {   #ip代理池
    0: {'http': 'http://yx827w:yx827w@123.249.47.6:888', 'https': 'http://yx827w:yx827w@123.249.47.6:888'},
    1: {'http': 'http://yx827w:yx827w@123.249.47.7:888', 'https': 'http://yx827w:yx827w@123.249.47.7:888'},
    2: {'http': 'http://yx827w:yx827w@123.249.47.8:888', 'https': 'http://yx827w:yx827w@123.249.47.8:888'},
    3: {'http': 'http://yx827w:yx827w@123.249.47.9:888', 'https': 'http://yx827w:yx827w@123.249.47.9:888'},
    4: {'http': 'http://yx827w:yx827w@123.249.47.10:888', 'https': 'http://yx827w:yx827w@123.249.47.10:888'},
    5: {'http': 'http://yx827w:yx827w@123.249.47.35:888', 'https': 'http://yx827w:yx827w@123.249.47.35:888'},
    6: {'http': 'http://yx827w:yx827w@123.249.47.34:888', 'https': 'http://yx827w:yx827w@123.249.47.34:888'},
    7: {'http': 'http://yx827w:yx827w@123.249.47.33:888', 'https': 'http://yx827w:yx827w@123.249.47.33:888'},
    8: {'http': 'http://yx827w:yx827w@123.249.47.32:888', 'https': 'http://yx827w:yx827w@123.249.47.32:888'},
    9: {'http': 'http://yx827w:yx827w@123.249.47.31:888', 'https': 'http://yx827w:yx827w@123.249.47.31:888'},
    10: {'http': 'http://yx827w:yx827w@123.249.47.30:888', 'https': 'http://yx827w:yx827w@123.249.47.30:888'},
    11: {'http': 'http://yx827w:yx827w@123.249.47.29:888', 'https': 'http://yx827w:yx827w@123.249.47.29:888'},
    12: {'http': 'http://yx827w:yx827w@123.249.47.28:888', 'https': 'http://yx827w:yx827w@123.249.47.28:888'},
    13: {'http': 'http://yx827w:yx827w@123.249.47.27:888', 'https': 'http://yx827w:yx827w@123.249.47.27:888'},
    14: {'http': 'http://yx827w:yx827w@123.249.47.26:888', 'https': 'http://yx827w:yx827w@123.249.47.26:888'},
    15: {'http': 'http://yx827w:yx827w@123.249.47.25:888', 'https': 'http://yx827w:yx827w@123.249.47.25:888'},
    16: {'http': 'http://yx827w:yx827w@123.249.47.24:888', 'https': 'http://yx827w:yx827w@123.249.47.24:888'},
    17: {'http': 'http://yx827w:yx827w@123.249.47.23:888', 'https': 'http://yx827w:yx827w@123.249.47.23:888'},
    18: {'http': 'http://yx827w:yx827w@123.249.47.22:888', 'https': 'http://yx827w:yx827w@123.249.47.22:888'},
    19: {'http': 'http://yx827w:yx827w@123.249.47.21:888', 'https': 'http://yx827w:yx827w@123.249.47.21:888'},
    20: {'http': 'http://yx827w:yx827w@123.249.47.1:888', 'https': 'http://yx827w:yx827w@123.249.47.1:888'},
    21: {'http': 'http://yx827w:yx827w@123.249.47.2:888', 'https': 'http://yx827w:yx827w@123.249.47.2:888'},
    22: {'http': 'http://yx827w:yx827w@123.249.47.3:888', 'https': 'http://yx827w:yx827w@123.249.47.3:888'},
    23: {'http': 'http://yx827w:yx827w@123.249.47.4:888', 'https': 'http://yx827w:yx827w@123.249.47.4:888'},
    24: {'http': 'http://yx827w:yx827w@123.249.47.5:888', 'https': 'http://yx827w:yx827w@123.249.47.5:888'},
    25: {'http': 'http://yx827w:yx827w@123.249.47.11:888', 'https': 'http://yx827w:yx827w@123.249.47.11:888'},
    26: {'http': 'http://yx827w:yx827w@123.249.47.12:888', 'https': 'http://yx827w:yx827w@123.249.47.12:888'}
}

database = {   #数据库链接
    "host": "127.0.0.1",
    "user": "",
    "pwd": "",
    "db": "xmdb"
}


class TbXmSpy(threading.Thread):
    """
    继承  threading.Thread模块，需要重写run方法，开启多线程
    爬虫爬取京东商品，每一个商品最多爬取100页，每一页10条评论
    """
    def __init__(self, name, urls, ip):
        threading.Thread.__init__(self)
        print("构造函数执行")
        # 插入计数器
        self.name = name
        self.urls = urls
        self.ip = ip
        self.count = 1




    def makeUrl(self,proid,n):  # 构造json的链接

        base = "https://sclub.jd.com/comment/productPageComments.action?productId="
        urllist = []
        for pid in proid:
            head = base + pid + '&score=0&sortType=5&page='
            for num in range(0, n+1):
                url = head + str(num) + "&pageSize=10&isShadowSku=0&rid=0&fold=1"
                print(url)
                urllist.append(url)
        return urllist

    def get_html(self, url, ip):  # 请求json数据
        header = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'zh-CN,zh;q=0.3',
            'Referer': 'https://detail.tmall.com/item.htm?spm=a1z10.3-b-s.w4011-14756119800.87.55e448feu8EO6T&id=570133905140&rn=05cb7b31bce4076308e809bfd85540b9&abbucket=13&sku_properties=10004:1617715035;5919063:6536025&redirectURL=https://detail.tmall.com/item.htm?spm=a1z10.3-b-s.w4011-14756119800.87.55e448feu8EO6T&id=570133905140&rn=05cb7b31bce4076308e809bfd85540b9&abbucket=13&sku_properties=10004:1617715035;5919063:6536025',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
            'Connection': 'keep-alive',
        }

        response = requests.get(url, headers=header, proxies=ip, verify=False)
        print(url ,"----------------------------")
        if response.status_code != 200:
            print("请求有问题，重新请求")
            for i in range(3):
                time.sleep(2)
                response = requests.get(url, headers=header, proxies=ip, verify=False)
                if response.status_code == 200:
                    break
        html = response.text
        print(html)
        return html

    def getJson(self, url, ip):
        data = self.get_html(url, ip).strip().strip("(").strip(")")  # 变成标准的json格式。
        # print(data)

        datalist = []  # 所有评论的数据
        try:
            d = json.loads(data)
            b = d["comments"]  # 得到comments后面的[]即列表类型，[]里有多个{}信息，每个{}代表一个用户的评论信息
            print(b[0])  # 列表[]里的每一个元素又是一个字典类型的数据
            # 下面遍历列表元素，取出每一个列表元素中字典里的特定信息：content、nick、date
            # print(type(b))
            inf = ()  # 一条评论的数据
            for elem in b:
                elem_list = []
                try:
                    comt = elem['content']  # 评论
                    if not str(comt).strip():
                        continue  # 没有评论跳过
                    user = elem["nickname"]  # 用户名
                    id = elem['id']  # 用户id
                    date = elem['creationTime']  # 日期
                    xh=elem["referenceName"]
                    score=elem["score"]
                    elem_list.append(user)
                    elem_list.append(id)
                    elem_list.append(date)
                    elem_list.append(xh)
                    elem_list.append(comt)
                    elem_list.append(score)
                    elem_list.append(0) #默认感情分析
                    # self.write(elem_list)  # 写csv
                    inf = tuple(elem_list)
                    print(inf)
                    datalist.append(inf)
                except:
                    print("josn有为空")
                    continue
        except Exception as e:
            print("json解析错误", e)
            # if url:  # 插入失败设定为-1
            self.cursor.execute('update urls set flag=-2 where  url=%s', url)

        return datalist

    # 记得修改数据库的名字，用户名，密码，等等，以及插入，创建等语句
    def dbProc(self):
        # 代理Ip的字典
        self.proxies = ip_dict
        # 数据库用户
        self.host = database["host"]
        self.user = database["user"]
        self.pwd = database["pwd"]
        self.db = database["db"]
        # 链接数据库
        db=[]
        try:
            # 链接数据库
            self.conn = pymssql.connect(host=self.host, user=self.user, password=self.pwd, database=self.db,
                                        charset='utf8')
            print("数据库链接成功")
            # 创建游标
            self.cursor = self.conn.cursor()
            db.append(self.conn)
            db.append(self.cursor)
            return db #还回链接 和 游标
        except Exception as e:
            print("链接数据库发生异常", e)

    def createTable(self, sql):  # 创建表格
        self.dbProc()
        try:
            self.cursor.execute(sql)
            self.conn.commit()
            print("创建表格成功")
            self.cursor.close()
            self.conn.close()
        except Exception as e:
            print("创建表格失败", e)

    def insertData(self, url, sql, data):
        try:
            self.cursor.execute(sql, data)
            print("插入", self.count, "成功")
            if url:  # 插入成功设定为1
                self.cursor.execute('update urls set flag=1 where  url=%s', url)
            self.count += 1
            self.conn.commit()
            self.write(list(data))

        except Exception as e:
            print("插入数据发生问题", e)
            if url:  # 插入失败设定为-1
                self.cursor.execute('update urls set flag=-1 where  url=%s', url)
            self.conn.rollback()
            # 在url中进行标记

    def selectUrl(self, sql, cond=None):
        if cond:
            self.cursor.execute(sql, cond)
        else:
            self.cursor.execute(sql)
        result = self.cursor.fetchall()
        return result

    def run(self):
        print("Starting " + self.name)
        insertsql = 'insert phone values(%s,%s,%s,%s,%s,%s,%s)'

        self.dbProc()
        for url in self.urls:
            try:
                marks = self.getJson(url, self.ip)
                print(len(marks))
                if len(marks)<1: #小于1跳出
                    continue
                for mark in marks:
                    self.insertData(url, insertsql, mark)
                    # self.write(list(mark))
            except Exception as e:
                print("出现问题", e)
                continue  # 跳过这次循环


    def write(self,data):
        path="D:\\xmdb.csv"
        with open(path,'a') as f: #追加写话
            write=csv.writer(f)
            write.writerow(data)
            f.close()


if __name__ == '__main__':
    xm = TbXmSpy("main", None, None)
    createsql = 'create table urls(url varchar(2056) ,flag int)'
    xm.dbProc()
    # xm.createTable(createsql)
    xm.dbProc()
    creatMark = 'create table phone(users char(64),id char(32) primary key,data char(32),xh char(256),comment char(5012),score char(2),mark int)'
    # xm.createTable(creatMark)
    insertsql = 'insert urls values(%s,%s);'

    prodid=['100000400010','29338471197','28917571899','29710811196','33276636523','28779745128','8797459342', '1563312675','29408360397','28445462536','28858935461','27931138997','28758228022','29387236242','21057535331','21580859449']
    pd=['33357859207','33365335494','42816824891','28775946305'] #30
    urls = xm.makeUrl(prodid,100)
    url2=xm.makeUrl(pd,30)
    allurl=urls+url2
    xm.dbProc()
    for url in allurl:  # 插入url
        urldata = (url, 0)
        # xm.insertData(None,insertsql,urldata)

    selesql = 'select url from urls where flag=0 '  # 初始状态
    result = xm.selectUrl(selesql)
    thrurl = []
    url1 = []
    url2 = []
    url3 = []
    for row in result:  # 链接分成3个部分，采用3线程
        thrurl.append(row[0])
        print(row[0])
        lh = len(thrurl)
        if (lh % 3 == 0):
            url1.append(thrurl[lh - 3])
            url2.append(thrurl[lh - 2])
            url3.append(thrurl[lh - 1])
    thread1 = TbXmSpy("thread1", url1, ip_dict[3])  # 创建新线程
    thread2 = TbXmSpy("thread2", url2,ip_dict[4] )
    thread3 = TbXmSpy("thread3", url3, ip_dict[5])
    thread1.start()  # 开启线程
    thread2.start()
    thread3.start()



