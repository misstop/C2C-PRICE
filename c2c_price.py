import requests
import time
import json
import logging
from apscheduler.schedulers.blocking import BlockingScheduler
from kafka import KafkaProducer


# 日志设置
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s (filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='c2c.log',
                    filemode='a')

head = {
    "authorization": "eyJhbGciOiJIUzUxMiJ9.eyJqdGkiOiJhMjQxNWUyZi0wYzRjLTQ5MGYtYmY4NS1hYjdiZWQyYzczOWFFVHBtIiwidWlkIjoiUUM2UmgzbEhUWkFYV0NYTzZVRysxQT09Iiwic3ViIjoiMTc1KioqMDkwNSIsInN0YSI6MCwibWlkIjowLCJpYXQiOjE1MzM4NjU0OTgsImV4cCI6MTUzNDQ3MDI5OCwiYmlkIjowLCJkb20iOiJ3d3cub2tleC5jb20iLCJpc3MiOiJva2NvaW4ifQ.VMVt5ehclJiyBv-_o_6nHMUhndyPZBnbiS18j4itmZYJloZks6AtYBm-CxypKm6JnxEaKITWoW4XUHrF5rA6zQ"
}


def run():
    producer = KafkaProducer(bootstrap_servers='47.75.116.175:9092', api_version=(0, 10, 1),
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    try:
        res = requests.get(
            'https://www.okex.com/v2/c2c-open/tradingOrders/group?digitalCurrencySymbol=usdt&legalCurrencySymbol=cny&best=1&\
            exchangeRateLevel=0&paySupport=0', headers=head
        )
    except Exception as e:
        logging.error(e)
    details = json.loads(res.text)['data']
    price = details["sellTradingOrders"][24]['exchangeRate']
    dic = {
        "price": price
    }
    producer.send('c2c-test', dic)
    producer.flush()
    producer.close()

SCHEDULER = BlockingScheduler()
if __name__ == '__main__':
    SCHEDULER.add_job(func=run, trigger='interval', minutes=5)
    SCHEDULER.start()
    # run()



