# -*- coding:utf8 -*-
import requests
import re
import time
import mysql.connector
from mysql.connector import errorcode
from multiprocessing.dummy import Pool as ThreadPool
from functools import reduce


class Crawler:
    def __init__(self, user, password, db_name):
        # 连接MySQL
        try:
            self.cnx = mysql.connector.connect(user=user, password=password)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Wrong username or password")
            else:
                print(err)
                exit(1)
        self.DB_NAME = db_name
        self.TABLES = dict()
        self.TABLES['communities'] = (
            "CREATE TABLE `communities` ("
            " `community_name` char(32) NOT NULL,"
            " `cid` mediumint NOT NULL,"
            " `location` text NOT NULL,"
            " `cur_price` mediumint NULL,"
            " `price201212` mediumint NULL,"
            " `price201306` mediumint NULL,"
            " `price201312` mediumint NULL,"
            " `price201406` mediumint NULL,"
            " `price201412` mediumint NULL,"
            " PRIMARY KEY (`cid`)"
            ") ENGINE=InnoDB")
        # 进入数据库
        self.cursor = self.cnx.cursor()
        try:
            self.cnx.database = self.DB_NAME
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_BAD_DB_ERROR:
                self.create_database(self.cursor)  # 数据库不存在则新建
                self.cnx.database = self.DB_NAME
            else:
                print(err)
                exit(1)
        self.create_tables(self.TABLES)
        self.retry_times = 0
        self.lines = []

    def __del__(self):
        try:
            self.cursor.close()
            self.cnx.close()
        except ReferenceError:
            pass

    def create_database(self, cursor):
        """创建数据库"""
        try:
            cursor.execute(
                "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(self.DB_NAME))
        except mysql.connector.Error as err:
            print("Failed creating database: {}".format(err))
            exit(1)

    def create_tables(self, tables):
        """创建数据表"""
        for name, ddl in tables.items():
            try:
                # print("Creating table {}: ".format(name))
                self.cursor.execute(ddl)
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                    pass
                else:
                    print(err.msg)

    def parse_citylist(self, page):
        """爬取小区列表页面，page为页码
           返回数据表中的行
        """
        html = 'http://shanghai.anjuke.com/community/W0QQpZ'+str(page)
        try:
            t1 = time.time()
            r = requests.get(html)
            cids = [int(i) for i in re.findall(r'"list_item"><ahref=".*?(\d+)"title', ''.join(r.text.split()))]
            community_name = re.findall(r'<aclass="t"id="comm_name.*?>(.*?)<', ''.join(r.text.split()))
            location = re.findall(r'<p>.*?](.*?)</p>', r.text)
            price = [int(i.replace('-', '0')) for i in re.findall(r'<spanclass="price">(?:<spanclass="sp1">)?(\d+|-).*?<',
                                                ''.join(r.text.split()))]
            history = [self.comm_history(cid=CID) for CID in cids]

            def one_date(date, l=history):
                res = []
                for h in l:
                    if isinstance(h, int):
                        res.append(0)
                    else:
                        try:
                            res.append(h[str(date)])
                        except KeyError:
                            res.append(0)
                return res

            res = tuple(zip(community_name, cids, location, price, one_date(201212),
                            one_date(201306), one_date(201312),
                            one_date(201406), one_date(201412)))
            print('page %d' % page, '\tused %.2fs' % (time.time()-t1))
            return res
        except TimeoutError:
            time.sleep(5)
            self.parse_citylist(page)
            print('Failed on page {}'.format(page))
            exit(1)

    def geocoding(self, address, city=None):
        """获取经纬度"""
        headers = {'Referer': 'http://developer.baidu.com'}
        payload = {'address': address, 'output': 'json', 'ak': '5prliyM2Fydg24f3OhitqRvg'}
        if city is not None:
            payload['city'] = city
        try:
            r = requests.get('http://api.map.baidu.com/geocoder/v2/', params=payload, headers=headers)
        except:
            time.sleep(5)
            self.geocoding(address, city)
        if r.json()['status'] == 0:
            location = r.json()['result']['location']
            return location['lat'], location['lng']
        else:
            return 0, 0

    def comm_history(self, cid, date=None):
        """ 获取历史房价
            cid为小区编号， year为要获取的时间点（格式：201406），不填默认返回所有
        """
        payload = {'cid': cid}
        try:
            r = requests.get('http://shanghai.anjuke.com/ajax/pricetrend/comm', params=payload)
            assert r.json()["status"] == "ok"
        except TimeoutError as e:
            print(cid, e)
            time.sleep(3)
            self.comm_history(cid, date)
        except AssertionError as e:
            return 0
        else:
            d = dict([tuple(d.items())[0] for d in r.json()["comm"]])
            if date is None:
                return d
            else:
                try:
                    return d[str(date)]
                except KeyError:
                    return 0

    def crawl(self, max_page=2702):
        pool = ThreadPool(4)
        self.lines = pool.map(self.parse_citylist, range(1, max_page+1))
        pool.close()
        pool.join()
        self.lines=reduce(lambda x,y: x+y, self.lines)
        command = "INSERT INTO communities VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"

        for line in self.lines:
            self.cursor.execute(command, line)
        self.cnx.commit()

if __name__ == '__main__':
    c = Crawler(user='root', password='root', db_name='real_estate')
    t1 = time.time()
    c.crawl()
    print('total time: ', (time.time()-t1)/60, 'minutes')