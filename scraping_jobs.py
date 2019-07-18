# ERCOT imports

import json
import time
import logging
from bs4 import BeautifulSoup
import requests
import boto
from apscheduler.schedulers.blocking import BlockingScheduler

# Non-overlapping MISO imports, overlapping commented out

# import json
import datetime
# import time
# import logging
# import requests
# import boto
# from apscheduler.schedulers.blocking import BlockingScheduler

# Weather.gov imports

from collections import OrderedDict
import re

# ERCOT URLS and boto/S3 config

ercot_urls = {}
ercot_domain = 'http://www.ercot.com'
e_content_path = '/content/cdr/html/'
e_resources = ['rt_conditions', 'as_capacity']
ercot_resources = ['ercot_' + resource for resource in e_resources]
e_paths = ['real_time_system_conditions.html', 
          'as_capacity_monitor.html']
e_url_paths = [ercot_domain + e_content_path + path for path in e_paths]
num_e_res = len(e_resources)
ercot_urls = { ercot_resources[x]: e_url_paths[x] for x in range(num_e_res) }

ercot_bucket = '################'
ercot_key = '########'

# National Weather Service URLS

weather_domain = 'http://w1.weather.gov'
w_content_path = '/obhistory/'
weather_zones = OrderedDict({'North': {'Wichita Falls': 'KSPS.html'},
  'Coast': {'Houston, Sugar Land Muni': 'KSGR.html'},                        \
  'East': {'Tyler': 'KTYR.html'},                                            \
  'South': {'Corpus Christi Naval Air Station': 'KNGP.html'}, 'West':        \
  {'Abilene Regional Airport': 'KABI.html'}, 'Far West': {'Midland Airpark': \
  'KMDD.html'}, 'North Central': {'Dallas Love Field': 'KDAL.html'},         \
  'South Central': {'Austin-Bergstrom International Airport': 'KAUS.html'}})

weather_bucket = '#################'
weather_key = '#################'


# MISO URLS and boto/S3 config

miso_urls = {}
miso_domain = 'https://www.misoenergy.org'
m_resources = ['load', 'wind', 'wind_forecast']
miso_resources = ['miso_' + resource for resource in m_resources]
m_paths = ['ptpTotalLoad.aspx?format=xml', 
         'windgenResponse.aspx?format=xml',
          'WindGenDayAhead.aspx']
m_url_paths = [miso_domain + '/ria/' + path for path in m_paths]
num_m_res = len(m_resources)
miso_urls = { miso_resources[x]: m_url_paths[x] for x in range(num_m_res) }

miso_s3_bucket = '#################'
miso_key = '#############'

connect_timeout = 10
read_timeout = 10

def connect_to_s3_bucket(s3_bucket, key):
    conn = boto.connect_s3()
    bucket = conn.get_bucket(s3_bucket)
    k = boto.s3.key.Key(bucket)
    k.key = key
    return k

def bucket_to_dict(key_object):
    raw_json = key_object.get_contents_as_string()
    str_json = raw_json.decode('utf-8')
    return json.loads(str_json)

def raw_text(url):
    session = requests.Session()
    session.mount("http://", requests.adapters.HTTPAdapter(max_retries=5))
    session.mount("https://", requests.adapters.HTTPAdapter(max_retries=5))
    for attempt in range(5):
        try:
            response = session.get(url=url, timeout = (connect_timeout, read_timeout))
            response.raise_for_status()
        except TimeoutError:
            print('Timed out on try ', attempt + 1)
        except requests.exceptions.HTTPError as e:
            print("Got an HTTPError:", e.message)
        else:
            return response.text

def write_json_to_bucket(data, key_object):
    key_object.set_contents_from_string(json.dumps(data))


def tags_from_html(raw_html, tag_type):
    soup = BeautifulSoup(raw_html, "lxml")
    return soup.find_all(tag_type)

def tag_strings(tags):
    data_vals = []
    for tag in tags:
        s = tag.string
        data_vals.append(s)
    return data_vals

def timestamp_from_ercot_realtime(tag):
    return tag[13:]

def labeled_values(content):
    labels = []
    values = []
    for i, value in enumerate(content):
        if value[-1].isnumeric():
            labels.append(content[i - 1])
            values.append(value)
    return dict(zip(labels, values))

def scrape_ercot_realtime(url):
    data = tags_from_html(raw_text(url), 'span')
    tag_values = tag_strings(data)
    time_label = tag_values[0]
    timestamp = timestamp_from_ercot_realtime(time_label)
    return { timestamp: labeled_values(tag_values) }

def update_ercot_bucket(urls, s3_bucket, key):
    k = connect_to_s3_bucket(s3_bucket, key)
    bucket_data = {}
    bucket_data = bucket_to_dict(k)
    for resource, url in urls.items():
        if resource not in bucket_data:
            bucket_data[resource] = {}
        conditions_data = scrape_ercot_realtime(url)
        bucket_data[resource].update(conditions_data)
        time.sleep(6)
    write_json_to_bucket(bucket_data, k)
    print('Finished ERCOT job for {}'.format(
        list(conditions_data.keys())[0]))

# MISO only functions

def datekey():
    todaysdate = datetime.date.today()
    return todaysdate.strftime('%m-%d-%Y')

def update_miso_bucket(urls, s3_bucket, key):
    k = connect_to_s3_bucket(s3_bucket, key)
    bucket_data = {}
    bucket_data = bucket_to_dict(k)
    today = datekey()
    if today not in bucket_data:
        bucket_data[today] = {}
        for resource, url in urls.items():
            bucket_data[today][resource] = raw_text(url)
            time.sleep(31)
        write_json_to_bucket(bucket_data, k)
    print('Finished MISO load job for {}'.format(today))

# Scrape weather data

def timestamped_weather_data(html):
    table_data = {}
    rows = extract_table_rows(html, {'cellspacing': '3'})
    now = datetime.datetime.now()
    today = (now.month, now.day, now.year)
    for i in range(3, len(rows)):
        cols = rows[i].find_all('td')
        if cols and len(cols) == 18:
            data = {}
            if populate_row_dict(data, cols):
                datetime_stamp = datetime_of_row(cols, today)
                table_data[datetime_stamp] = data
        else:
            break
    return table_data


def extract_table_rows(html, attributes):
    soup = BeautifulSoup(html, "lxml")
    full_table = soup.find('table', attrs = attributes)
    return full_table.find_all('tr')

def datetime_of_row(cols, today):
    day = int(cols[0].string) 
    time = cols[1].string
    (today_month, today_day, today_year) = today
    if day <= today_day:
        month = today_month
        year = today_year
    else:
        if today_month > 1:
            month = today_month - 1
            year = today_year
        else:
            month = 12
            year = today_year - 1
    date_stamp = datetime.datetime(year, month, day).strftime('%Y-%m-%d')
    return ' '.join([date_stamp, time])

def broken_tags(text):
    if re.search('<', text):
        return True
    elif re.search('>', text):
        return True
    elif re.search('td', text):
        return True

def populate_row_dict(data, cols):
    num_cols = len(cols)
    col_strings = []
    for i in range(num_cols):
        if cols[i].string:
            col_strings.append(cols[i].string)
    row_strings = ''.join(col_strings)
    if broken_tags(row_strings):
        return None
    data.update(wind = cols[2].string, sky = cols[5].string, 
                temp_F = int(cols[6].string), dwpt = int(cols[7].string), 
                humidity = cols[10].string)
    if cols[11].string != 'NA':
        data.update(wind_chill_F = int(cols[11].string))
    if cols[12].string != 'NA':
        data.update(heat_index_F = int(cols[12].string))
    if cols[15].string:
        data.update(precip_1_hr = float(cols[15].string))
    return True

def update_weather_bucket(weather_zones, domain, content_path, bucket, key):
    k = connect_to_s3_bucket(bucket, key)
    bucket_data = {}
    bucket_data = bucket_to_dict(k)
    weather_urls = {}
    for zone, resource in weather_zones.items():
        for file_name in resource.values(): 
            zone_url = ''.join([domain, content_path, file_name])
            weather_page = raw_text(zone_url)
            weather_data = timestamped_weather_data(weather_page)
            for datetime_stamp, data in weather_data.items():
                if datetime_stamp not in bucket_data[zone]:
                    bucket_data[zone][datetime_stamp] = data
        time.sleep(5)
    write_json_to_bucket(bucket_data, k)
    print('Finished ERCOT weather job for {}'.format(datetime_stamp))

# Job instantiation and scheduling

logging.basicConfig()

sched = BlockingScheduler()


@sched.scheduled_job('interval', minutes = 5)
def timed_job():
    print('ERCOT realtime job is run every five minutes.')
    update_ercot_bucket(ercot_urls, ercot_bucket, ercot_key)


@sched.scheduled_job('cron', hour = 23, minute = 58)
def scheduled_job():
    print('MISO load job is run at 23:58 pm Central Time.')
    update_miso_bucket(miso_urls, miso_s3_bucket, miso_key)
    update_weather_bucket(weather_zones, weather_domain, w_content_path, 
                          weather_bucket, weather_key)

sched.start()
