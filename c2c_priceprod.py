import requests
import time
import json
import logging
import yaml
import os
import pymysql
import datetime
from kafka import KafkaProducer
from apscheduler.schedulers.blocking import BlockingScheduler
from flask import jsonify


# 日志设置
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s (filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='c2c_priceprod.log',
                    filemode='a')

# 解析yaml
cur_path = os.path.dirname(os.path.realpath(__file__))
x = yaml.load(open('%s/config.yml' % cur_path, encoding='UTF-8'))
kafka_con = x['QUEUES']['KAFKA']['HOST']
kafka_topic = x['QUEUES']['KAFKA']['TOPIC']


def crawl():
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


# 阻塞
SCHEDULER = BlockingScheduler()
if __name__ == '__main__':
    # SCHEDULER.add_job(func=crawl, trigger='interval', minutes=5)
    # SCHEDULER.start()
    crawl()
