import requests
import json
from kafka import KafkaProducer

head = {
    'hb-pro-token': 'vkOHn_Wib6MtUl-ijvnnwS-8ONAOWNpKOmfaN03YPXsY-uOP2m0-gvjE57ad1qDF',
}
try:
    res = requests.get(
                'https://www.huobi.com/-/x/pro/v1/order/orders/?r=30ioio3pvty&states=partial-canceled,filled,canceled&start-date=2018-02-18&end-date=2018-08-17',
                headers=head
                    )
    details = json.loads(res.text)['data']
except Exception as e:
    print(e)
# 增加kafka发送给王楷
producer = KafkaProducer(bootstrap_servers='47.75.116.175:9092', api_version=(0, 10, 1),
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
for d in details:
    if d['state'] == 'canceled':
        continue
    else:
        dic = {
            'finished-at': d['finished-at'],
            'exchangeType': '币币交易',
            'symbol': d['symbol'],
            'direction': d['type'],
            'price': d['price'],
            'amount': d['amount'],
            'field-amount': d['field-amount'],
            'state': d['state']
        }
        producer.send('actual-quotation', [dic])
        producer.flush()
producer.close()