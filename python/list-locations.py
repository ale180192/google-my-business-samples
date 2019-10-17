import sys
import os
import json
import time
from datetime import datetime
from datetime import timedelta

from googleapiclient import sample_tools
from googleapiclient.http import build_http
from pytz import timezone
from dotenv import load_dotenv
import psycopg2

load_dotenv()
discovery_doc = "gmb_discovery.json"
account_name = 'accounts/109124004879525640748'
DAYS_HISTORY = 7
DB_NAME = os.getenv('DB_NAME', None)
DB_USER = os.getenv('DB_USER', None)
DB_PASSWORD = os.getenv('DB_PASSWORD', None)


def get_connection_db():
    try:
        return psycopg2.connect("dbname={db_name} user={db_user} password={db_password}".format_map(
            {'db_name': DB_NAME,
            'db_user': DB_USER,
            'db_password': DB_PASSWORD})
        ) 
    except Exception as e:
        print('error to connect')
        print(e)
        raise e


def get_data_location_datetime_range(account, service, locations_name, datetime_start=None, datetime_end=None):
    '''
        return a dic where the keys are each of the date
    '''
    data_dict = {}
    while datetime_start < datetime_end:
        data = get_data_location(account=account, service=service, locations_name=locations_name, datetime_str=datetime_start)
        data_dict[datetime_start.strftime('%Y-%m-%dT%H:%M:%S%z')] = data
        datetime_start = datetime_start + timedelta(days=1)
    return data_dict

def get_data_location(account, service, locations_name, datetime_str ):
    '''
        return a list where each item conatins the metrics of one location
    '''
    datetime_start = datetime_str
    datetime_end= datetime_start + timedelta(days=1)
    datetime_start = datetime.strftime(datetime_start, '%Y-%m-%dT%H:%M:%S%z')
    datetime_end = datetime.strftime(datetime_end, '%Y-%m-%dT%H:%M:%S%z')
    datetime_start = datetime_start[:-2] + ':' + datetime_start[len(datetime_start)-2:]
    datetime_end = datetime_end[:-2] + ':' + datetime_end[len(datetime_end)-2:]
    datetime_start = '2019-10-09T00:00:00Z'
    datetime_end = '2019-10-10T00:00:00Z'
    requestBody = {
      "locationNames": locations_name,
      "basicRequest": {
        "metricRequests": {
            "metric": "ALL"
        },
        "timeRange": {
          "startTime": datetime_start,
          "endTime": datetime_end
        }
      }
    }
    locationinsight_report = service.accounts().locations().reportInsights(name=account, body=requestBody).execute()
    return locationinsight_report


def group_items(data_list, length_group):
    '''
        return a list of listes where the length each list is indicated by length_group
    '''
    if data_list == 0 or length_group == 0:
        raise ValueError('the length of array or the value of length_group are equal to 0')
    length_data = len(data_list)
    if length_data > length_group:
        data_response = []
        div = int(length_data/length_group)
        for index in range(0, div, 1):
            print('item ', index+1)
            start_index = index*(length_group)
            end_index = (index*(length_group) + length_group)
            data_index = data_list[start_index:end_index]
            print('data add length is ', len(data_index))
            if not data_index:
                print('here'*200)
                print('to index ', start_index, ':', end_index)
            data_response.append(data_index)
        if length_data%length_group == 0:
            print('residuo is zero')
            return data_response
        else:
            print('residuo not equal to zero')
            start_index = (div-1)*(length_group)
            end_index = (  (div-1)*(length_group)  ) + length_group
            res = data_list[div*(length_group):length_data]
            if not data_index:
                print('here'*200)
                print('to index ', start_index, ':', end_index)
            data_response.append(res)
            return data_response
    else:
        return [data_list, ]


def get_records_prepared(data, date):
    '''
        return record with the metrics such as 

        {
            location_name: 'accounts/109124004879525640748/locations/3767858523905244827'
            date: "2019-10-09T00:00:00-0500",
            QUERIES_DIRECT: 5,
            QUERIES_INDIRECT: 3,
            .
            . <whole the metrics>
            .
            .
        }

    '''
    for location in data:
        record = {}
        record['location_name'] = location['locationName']
        record['date'] = date
        metrics_dict = {}
        for metric in location['metricValues']:
            if 'metric' not in metric:
                print('no key metric for location {location_name} on date {date}'.format_map({'location_name': location['locationName'], 'date': date}))
                continue
            metrics_dict[metric['metric'].lower()] = metric['totalValue']['value']
        record.update(metrics_dict)
    return record


def prepare_records_to_insert(data):
    '''
        receive a list of dicts:
            [
                {
                    '2019-10-09T00:00:00-05': {
                        'locationMetrics': [
                            {
                                'locationName': 'accounts/109124004879525640748/locations/3767858523905244827'
                                'metricValues': {
                                    "metric": "QUERIES_DIRECT",
                                    "totalValue": {
                                    "metricOption": "AGGREGATED_TOTAL",
                                    "timeDimension": {
                                        "timeRange": {
                                        "startTime": "2019-10-09T00:00:00Z",
                                        "endTime": "2019-10-10T00:00:00Z"
                                        }
                                    },
                                    "value": "24"
                                    }
                                },
                            },
                            {
                                ......
                            }
                        ]
                    }
                },
                {

                } ....
            ]

        return a list of records prepared to insert to db. see get_records_prepared to view format
    '''
    records_list = []
    for item in data:
        for date, value in item.items():
            if not value:
                print('value empty')
                continue
            records = get_records_prepared(value['locationMetrics'], date)
            records_list.append(records)
    return records_list

def insert_data_to_db(data):
    try:
        # TODO: make insertion of data into db
        print('insert data to db')
        print(data[:10])
        connection = get_connection_db()
        cur = connection.cursor()
        # TODO: check aggregated_total field
        query = '''
            INSERT INTO local.gmb_gmbstats(
                location_id, date, queries_direct, queries_indirect, queries_chain, 
                views_maps, views_search, actions_website, actions_phone, actions_driving_directions, 
                photos_views_merchant, photos_views_customers, photos_count_merchant, 
                photos_count_customers, local_post_views_search)
            VALUES ( %(location_name)s, %(date)s, %(queries_direct)s, %(queries_indirect)s, %(queries_chain)s, %(views_maps)s, 
                    %(views_search)s, %(actions_website)s, %(actions_phone)s, %(actions_driving_directions)s, %(photos_views_merchant)s, 
                    %(photos_views_customers)s, %(photos_count_merchant)s, %(photos_count_customers)s, 
                    %(local_post_views_search)s);
        '''
        cur.executemany(query, data)
        connection.commit()
        connection.close()
        cur.close()
        print('ok insert')
    except Exception as e:
        print('Error to insert into db')
        print(e)
    



def process_locations(locations_list):
    date_now = datetime.now(tz=timezone('America/Mexico_City'))
    date_now = datetime(  year=date_now.year,
                            month=date_now.month,
                            day=date_now.day,
                            hour=0,
                            minute=0,
                            second=0,
                            microsecond=0,
                            tzinfo=date_now.tzinfo)
    date_start = date_now - timedelta(days=DAYS_HISTORY)
    locations = group_items(data_list=locations_list, length_group=10)
    # we make one request to google api by each 10 locations
    data_list = []
    print('length not agroup: ', len(locations_list))
    print('length agroup ', len(locations))
    for item in locations:
        print('*'*100)
        location_name_list = [_item['name'] for _item in item]
        print('request data for the locations :', location_name_list)
        data = get_data_location_datetime_range(account=account_name,
                                            service=service,
                                            locations_name=location_name_list, 
                                            datetime_start=date_start, 
                                            datetime_end=date_now)
        print('data received from 10 locations: ')
        print(data)
        data_list.append(data)
    data_before_insert = []
    for item in data_list:
        for date, locations_data in item.items():
            data_before_insert.append(locations_data)
    print('data before insert: ', len(data_before_insert))
    data_prepared = prepare_records_to_insert(data_list)
    print('data after prepare: ', len(data_prepared))    
    insert_data_to_db(data_prepared)


def main(argv):
    global service
    start_script = time.time()
    print("List of locations for account: ", account_name)
    # Get the list of locations for the first account in the list
    service, flags = sample_tools.init(argv, "mybusiness", "v4", __doc__, __file__, scope="https://www.googleapis.com/auth/business.manage", discovery_filename=discovery_doc)
    resp_locations = service.accounts().locations().list(parent=account_name).execute()
    print('total locations is: ', len(resp_locations['locations']))
    count = 0
    process_locations(resp_locations['locations'])
    while 'nextPageToken' in resp_locations:
        print('token is', resp_locations['nextPageToken']) 
        resp_locations = service.accounts().locations().list(parent=account_name, pageToken=resp_locations['nextPageToken']).execute()
        print('#'*100)
        print('total locations is: ', len(resp_locations['locations']))
        process_locations(resp_locations['locations'])
        count += 1
    end_script = time.time()
    seconds_script = end_script - start_script
    print('time of the script in seconds is: ', seconds_script) 

if __name__ == "__main__":
    # TODO: register the locations that arent responding data and its date
    try:
        main(sys.argv)
    except Exception as e:
        print('Error in main function')
        print(e)

