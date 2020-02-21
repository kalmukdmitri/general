import datetime
import json
import requests
import pandas as pd
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="CarWeGo-Analytics-b48469538956.json"

import pd_gbq
from pd_gbq import *

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

with open('mango') as json_file:
    headers = json.load(json_file)
    
print(headers)

def get_moths(start=datetime.date(2019,1,1)):
    end_list = []
    today = datetime.date.today()
    y = 2019
    m = 1
    end_mnth = datetime.date(y,m,31)
    while start < today:
        
        if m == 12:
            
            end_mnth = datetime.date(y,m,31)
            start = datetime.date(y,m,1)
            end_list.append([start,end_mnth])
            m = 1 
            y += 1

        else:
        
            start = datetime.date(y,m,1)
            end_mnth  = datetime.date(y,m+1,1)- datetime.timedelta(days=1) if m<11 else datetime.date(y,11,30)
            end_list.append([start,end_mnth])
            m+=1
    return end_list

def get_API(quer):
    '''Get json data from data '''
    url_org = quer
    resul =requests.request('get', url_org, headers=headers)
    results = json.loads(resul.text)
    return results

def get_all_mango(start=datetime.date(2019,1,1)):
    dates  = get_moths(start)
    results = []
    for i in dates:
        mnth_result=get_API(f'https://widgets-api.mango-office.ru/v1/calltracking/18750/calls?dateStart={str(i[0])}T00:00Z&dateEnd={str(i[1])}T23:59Z')
        results.extend(mnth_result)
    return results
    


mango_frame= pd.DataFrame(get_all_mango())
mango_frame = mango_frame.drop(columns=['hash','tags','customParam'])
mango_frame['dateEnd'] = pd.to_datetime(mango_frame['dateEnd'])
mango_frame['dateStart'] = pd.to_datetime(mango_frame['dateStart'])
ga_cooks_gbq = gbq_pd('mango_calls', datasetId = 'i_cap_full_analytics') 



def initialize_analyticsreporting():

    #INPUT: list Initializes an Analytics Reporting API V4 service object.

    #OUTPUT: An authorized Analytics Reporting API V4 service object.

    credentials = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE_LOCATION, SCOPES)

    # Build the service object.
    analytics = build('analyticsreporting', 'v4', credentials=credentials)

    return analytics

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = 'CarWeGo-Analytics-b48469538956.json'
VIEW_ID = '202103298'

analytics = initialize_analyticsreporting()


def ga_load_procc(rows, headers):
    dim_colums  = headers['dimensions'] 
    metr_colums = [i['name'] for i in headers['metricHeader']['metricHeaderEntries']]
    columns = dim_colums+metr_colums
    columns = [i.replace(':','_') for i in columns]
    flat_report = []
    for i in rows:
        dims = i['dimensions']
        metrics = [int(j) for j in i['metrics'][0]['values']]
        flat_report.append(dims+metrics)
    return pd.DataFrame(flat_report ,columns = columns)

def get_report(date_start, dims, metrics = ['ga:sessions'],endDate='today', nextPageToken= '0'):
  #INPUT: analytics: An authorized Analytics Reporting API V4 service object.
  #OUTPUT: The Analytics Reporting API V4 response.
    metrics = [{'expression': i}  for i in metrics]
    body = {
            'reportRequests': [
            { "samplingLevel": "LARGE",
              'viewId': VIEW_ID,
              'dateRanges': {'startDate': date_start, 'endDate': 'today'},
              'metrics':metrics,
              'dimensions': dims,
              'pageSize': '10000',
             'pageToken' : nextPageToken
            }]
          }
         
    return analytics.reports().batchGet( body=body).execute()
def get_all_mult_ga(date_start, dims, metrics = 'ga:sessions',endDate='today', extra_process = False):
    """Export data of Moysklad Entity to memory.
    If there if more then limit of row of data func loads by biggest possible batches
    Intup: str of entity
    Optinal params: 
    expand - expend certain fields of data, if not False limit must be set to 100
    extra_process - processing func to cleanup the results
    Output: list of JSON responses"""
    limit = 10000
    result1 = get_report( **reqv_params )
    rowCount = int(result1['reports'][0]['data']['rowCount'])
    final = result1['reports'][0]['data']['rows']
    headers = result1['reports'][0]['columnHeader']
    if rowCount > limit:
        len_extra_requests = (int(rowCount/limit)+1)
        rquests_list = []
        for i in range(1,len_extra_requests):
            print(i*limit)
            prms = reqv_params.copy()
            prms['nextPageToken']= str(i*limit)
            rquests_list.append(prms)
        row_req_list = [get_report(**i) for i in rquests_list]
        [final.extend(i['reports'][0]['data']['rows']) for i in row_req_list]
    print('Входные данные обработаны')
    if extra_process:
        final = extra_process(final,headers)
        
        return final
    else:
        return final
    
ga_cooks_gbq = gbq_pd('cookidata', datasetId = 'i_cap_full_analytics') 
ga_cooks_dates= ga_cooks_gbq.df_query('SELECT ga_date FROM `carwego-analytics.i_cap_full_analytics.cookidata`')
len_cooks = len(ga_cooks_dates['ga_date'])
max_date = max(list(ga_cooks_dates['ga_date']))
print(f'Длинна до {len_cooks}')
last_changed_date = max_date.date() - datetime.timedelta(days=1)
print(f'Меняем данные от {str(last_changed_date)}')
clear_uncertain_query =(f"""Select count(ga_date) FROM {ga_cooks_gbq.datasetId}.{ga_cooks_gbq.table_name} 
WHERE ga_date >= '{str(last_changed_date)}'""")
len_predeleted = ga_cooks_gbq.df_query(clear_uncertain_query).iat[0,0]
print(f'Удаляемых данныx от {str(last_changed_date)} - {str(len_predeleted)}')

reqv_params = {
    'date_start': str(last_changed_date),
    'dims' : [{'name': 'ga:date'},
               {'name': 'ga:source'},
               {'name': 'ga:medium'},
               {'name': 'ga:campaign'},
               {'name': 'ga:keyword'},
               {'name': 'ga:dimension3'}],
    'metrics': ['ga:sessions', 'ga:users'],
    'endDate' : 'today'
}

ga_cooks = get_all_mult_ga(**reqv_params, extra_process= ga_load_procc)
ga_cooks['ga_date'] = pd.to_datetime(ga_cooks['ga_date'])
print(f'Получено новых данныx от {str(last_changed_date)} - {str(len(ga_cooks))}')
if len(ga_cooks) > len_predeleted:
    clear_uncertain_query =(f"""DELETE FROM {ga_cooks_gbq.datasetId}.{ga_cooks_gbq.table_name} 
    WHERE ga_date >= '{str(last_changed_date)}'""")
    ga_cooks_gbq.df_query(clear_uncertain_query)
    ga_cooks_gbq.add(ga_cooks, if_exists = 'append')
    print(f'В обновлённой таблице {len(ga_cooks)-len_predeleted+len_cooks}')