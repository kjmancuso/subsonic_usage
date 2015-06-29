import sys

import requests

from bs4 import BeautifulSoup
from configobj import ConfigObj
from influxdb import InfluxDBClient

config = ConfigObj(sys.path[0] + '/config.ini')
URL = config['subsonic']['url'] + '/db.view'
USER = config['subsonic']['user']
PASS = config['subsonic']['pass']
IFLX_HOST = config['influx']['host']
IFLX_USER = config['influx']['user']
IFLX_PASS = config['influx']['pass']
IFLX_DB = config['influx']['database']


def get_html(url=URL, username=USER, password=PASS):
    requests.packages.urllib3.disable_warnings()
    payload = {'query': 'select * from user'}
    r = requests.post(url, data=payload, auth=(username, password),
                      verify=False)

    return r


def parse_html(doc):
    users = {}
    soup = BeautifulSoup(doc, 'html.parser')
    table = soup.table
    rows = table.find_all('tr')
    for row in rows[1:]:
        r = row.find_all('td')
        user = r[0].string
        bytes_streamed = int(r[2].string)
        bytes_downloaded = int(r[3].string)
        users[user] = {'user': user,
                       'bytes_streamed': bytes_streamed,
                       'bytes_downloaded': bytes_downloaded}

    return users


def convert_influx(users):
    l = []
    for k, v in users.items():
        tmp = {'measurement': 'bytes_streamed',
               'tags': {'person': k},
               'fields': {'value': v['bytes_streamed']}}
        l.append(tmp)
        tmp = {'measurement': 'bytes_downloaded',
               'tags': {'person': k},
               'fields': {'value': v['bytes_downloaded']}}
        l.append(tmp)

    return l


def send_to_influx(metrics, host=IFLX_HOST, port=8086, user=IFLX_USER,
                   pwd=IFLX_PASS, db=IFLX_DB):
    client = InfluxDBClient(host, port, user, pwd, db)
    client.write_points(metrics)


if __name__ == '__main__':
    test = get_html()
    users = parse_html(test.text)
    send = convert_influx(users)
    send_to_influx(send)
