import requests
import time
import json
import logging
import yaml
import os
import pymysql
import datetime
from kafka import KafkaProducer
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask
from flask_cors import *
from flask import jsonify

app = Flask(__name__)
CORS(app, supports_credentials=True)

# 日志设置
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s (filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='c2c.log',
                    filemode='a')

# 解析yaml
cur_path = os.path.dirname(os.path.realpath(__file__))
x = yaml.load(open('%s/config.yml' % cur_path, encoding='UTF-8'))
# 数据库
host = x['DATADB']['MYSQL']['HOST']
username = x['DATADB']['MYSQL']['UNAME']
pwd = x['DATADB']['MYSQL']['PWD']
database = x['DATADB']['MYSQL']['DNAME']
kafka_con = x['QUEUES']['KAFKA']['HOST']
kafka_topic = x['QUEUES']['KAFKA']['TOPIC']


# 数据库连接
def connect_db():
    logging.info('start to connect mysql')
    db = pymysql.connect('{}'.format(host), '{}'.format(username), '{}'.format(pwd), '{}'.format(database))
    logging.info('connect success')
    return db


# 插入
def insert_db(db, okexPrice, huobiPrice, createTime):
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    # SQL 插入语句
    sql = "INSERT INTO c2c_price(okexPrice, huobiPrice, createTime) VALUES (%s, %s, %s)"
    par = (okexPrice, huobiPrice, createTime)
    try:
        # 执行sql语句
        cursor.execute(sql, par)
        # 提交到数据库执行
        db.commit()
        logging.info("insert success")
    except Exception as e:
        # Rollback in case there is any error
        logging.error(e)
        db.rollback()


# 减少折线统计图的密度
def line(ls, num):
    ls_details = []
    for _ in ls:
        if ls.index(_) % num == 0:
            ls_details.append(_)
    return ls_details


# 查询语句
def query_db(db,):
    ls1 = []
    ls2 = []
    ls3 = []

    # 使用cursor()方法获取操作游标
    cursor = db.cursor()

    # 1天
    sql = "SELECT * FROM c2c_price WHERE createTime >=  NOW() - interval 24 hour"
    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 获取所有记录列表
        results = cursor.fetchall()
    except Exception as e:
        logging.error(e)
        ls1 = None
    for row in results:
        dic1 = {
            'okexPrice': None,
            'huobiPrice': float(row[2]),
            'timestamp': str(row[3])
        }
        ls1.append(dic1)
    line1 = line(ls1, 6)
    logging.info('1day line1 ok')

    # 3天
    sql = "SELECT * FROM c2c_price WHERE TO_DAYS( NOW( ) ) - TO_DAYS(createTime) <= 3"
    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 获取所有记录列表
        results = cursor.fetchall()
    except Exception as e:
        logging.error(e)
        ls2 = None
    for row in results:
        dic2 = {
            'okexPrice': None,
            'huobiPrice': float(row[2]),
            'timestamp': str(row[3])
        }
        ls2.append(dic2)
    line2 = line(ls2, 18)
    logging.info('3day line2 ok')

    # 7天
    sql = "SELECT * FROM c2c_price WHERE DATE_SUB(CURDATE(), INTERVAL 7 DAY) <= date(createTime)"
    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 获取所有记录列表
        results = cursor.fetchall()
    except Exception as e:
        logging.error(e)
        ls3 = None
    for row in results:
        dic3 = {
            'okexPrice': None,
            'huobiPrice': float(row[2]),
            'timestamp': str(row[3])
        }
        ls3.append(dic3)
    line3 = line(ls3, 42)
    logging.info('7day line3 ok')
    dic = {
        "1day": line1,
        "3day": line2,
        "7day": line3
    }
    return dic


# 关闭数据库
def close_db(db):
    db.close()


def crawl():
    head = {
        "authorization": "eyJhbGciOiJIUzUxMiJ9.eyJqdGkiOiJhMjQxNWUyZi0wYzRjLTQ5MGYtYmY4NS1hYjdiZWQyYzczOWFFVHBtIiwidWlkIjoiUUM2UmgzbEhUWkFYV0NYTzZVRysxQT09Iiwic3ViIjoiMTc1KioqMDkwNSIsInN0YSI6MCwibWlkIjowLCJpYXQiOjE1MzM4NjU0OTgsImV4cCI6MTUzNDQ3MDI5OCwiYmlkIjowLCJkb20iOiJ3d3cub2tleC5jb20iLCJpc3MiOiJva2NvaW4ifQ.VMVt5ehclJiyBv-_o_6nHMUhndyPZBnbiS18j4itmZYJloZks6AtYBm-CxypKm6JnxEaKITWoW4XUHrF5rA6zQ"
    }
    try:
        res = requests.get(
            'https://www.okex.com/v2/c2c-open/tradingOrders/group?digitalCurrencySymbol=usdt&legalCurrencySymbol=cny&best=1&\
            exchangeRateLevel=0&paySupport=0', headers=head
        )
        details = json.loads(res.text)['data']
        okexPrice = details["sellTradingOrders"][24]['exchangeRate']
    except Exception as e:
        logging.error(e)
        okexPrice = None
    # 得到okex的c2c价格
    # okexPrice = details["sellTradingOrders"][24]['exchangeRate']
    logging.info('okexPrice-----%s' % okexPrice)
    try:
        res2 = requests.get(
            'https://otc-api.huobi.com/v1/data/trade-market?country=37&currency=1&payMethod=0&currPage=1&coinId=2&tradeType=sell&blockType=general&online=1'
        )
        details2 = json.loads(res2.text)['data']
    except Exception as e:
        logging.error(e)
    # 得到火币的价格
    huobiPrice = details2[0]['price']
    logging.info('huobiPrice-----%s' % huobiPrice)
    ts = time.time()
    createTime = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    logging.info('createTime-----%s' % createTime)
    db = connect_db()   # 连接mysql数据库
    insert_db(db, okexPrice, huobiPrice, createTime)
    logging.info('insert to database success!!!')
    close_db(db)

    # 增加kafka发送给王楷
    producer = KafkaProducer(bootstrap_servers=kafka_con, api_version=(0, 10, 1),
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    dic = {
        "price": huobiPrice,
        "key": "USDT,CNY",
        "timestamp": createTime,
    }
    producer.send(kafka_topic, [dic])
    logging.info('send to kafka success!!!')
    producer.flush()
    producer.close()


# 从数据库查询
@app.route('/row/all/', methods=["GET"])
def select_msg():
    # 使用cursor()方法获取操作游标
    db = connect_db()
    dic = query_db(db)
    close_db(db)
    return jsonify(dic)

# 非阻塞
SCHEDULER = BackgroundScheduler()
if __name__ == '__main__':
    SCHEDULER.add_job(func=crawl, trigger='interval', minutes=5)
    SCHEDULER.start()
    app.run(
        host='0.0.0.0',
        port=5000, debug=True,
        use_reloader=False,
    )
    # # crawl()
