import pandas as pd
import requests
import os
import datetime
import calendar
import time
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="CarWeGo-Analytics-b48469538956.json"

import MS_connect
from MS_connect import *

import pd_gbq
from pd_gbq import *

from MS_json_procc import *


def payment_proccess(row):
    row_id = row['id']
    row_name = row['name']
    row_date = datetime.datetime.strptime(row['moment'][:10], "%Y-%m-%d" ).date()
    row_type = row['meta']['type']
    type_rus = "Входящий платёж" if row_type == 'paymentin' else "Исходящий платёж"
    row_sum = row['sum']
    row_organization = universal_dict['organization'][row['organization']['meta']['href'].split('/')[-1]]
    currency_name = universal_dict['currency'][row['rate']['currency']['meta']['href'].split('/')[-1]][0]
    currency_rate_now = universal_dict['currency'][row['rate']['currency']['meta']['href'].split('/')[-1]][1]
    currency_rate_payday = row['rate']['value'] if 'value' in row['rate'] else 1.0
    
    atributes = {i['name']:i['value']['name'] for i in row['attributes'] if 'name' in i['value']}
    type_pay = "None"
    if "Вид платежа" in atributes:
        type_pay = atributes['Вид платежа']
    
    paycomment = "None"
    if "paymentPurpose" in row:
        paycomment = sanitize(row['paymentPurpose'])
    
    row_counter_agent = universal_dict[row['agent']['meta']['type']][row['agent']['meta']['href'].split('/')[-1]]
    if row_type == 'paymentin':
          row_item = atributes['Основание платежа'] if 'Основание платежа' in atributes else 'None'
    else:
          row_item = universal_dict['expenseitem'][row['expenseItem']['meta']['href'].split('/')[-1]]
    
    return [
        row_id,
        row_name,
        row_date,
        row_type,
        type_rus,
        row_sum,
        row_organization,
        currency_name,
        currency_rate_now,
        currency_rate_payday,
        type_pay,
        paycomment,
        row_counter_agent,
        row_item
    ]

def sanitize(cell):
        
    punctuation = list('!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~')
    whitespace = list('\t\n\r\x0b\x0c')
    forbid= punctuation+whitespace
    for j in forbid:
        cell = cell.replace(j,"")
    return cell

paymentin = get_all_entity_mult('paymentin')
paymentout = get_all_entity_mult('paymentout')
universal_dict = {'organization' :  get_all_entity_mult('organization',extra_process=dicti),
                   
                   'employee'    :  get_all_entity_mult('employee',extra_process=dicti),
                   
                   'counterparty':  get_all_entity_mult('counterparty',extra_process=dicti),
                   'currency'    :  get_all_entity_mult('currency',extra_process=dicti_rate),
                  'expenseitem' : get_all_entity_mult('expenseitem',extra_process=dicti)
                  }

all_payments= paymentin + paymentout

processed_payments = [payment_proccess(i) for i in all_payments]

all_payments_gbq = gbq_pd('all_payments', datasetId = 'normalised_tables')

processed_payments_df = pd.DataFrame(processed_payments,columns= all_payments_gbq.columns)
processed_payments_df['row_date'] = pd.to_datetime(processed_payments_df['row_date'])
all_payments_gbq.add(processed_payments_df, if_exists = 'replace')

