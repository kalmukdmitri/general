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

strt = time.time()
otruzka_frer = gbq_pd('otgruzki', datasetId = 'normalised_tables') 
otgruzki_dates_query =(f"""SELECT otruzka_moment FROM `carwego-analytics.normalised_tables.otgruzki` """)
otgruzki_dates = otruzka_frer.df_query(otgruzki_dates_query)
otgruzki_dates = list(set([i[:-4] for i in otgruzki_dates['otruzka_moment']]))
cost_dates = otruzka_frer.df_query('SELECT date FROM `carwego-analytics.normalised_tables.cost`')
len_costs = len(cost_dates['date'])

max_date = max(list(cost_dates['date']))
last_changed_date = max_date.date() - datetime.timedelta(days=1)
all_date_changes = get_all_anything_mult('audit', limit = 25, filtr=f'entityType=supply;eventType=update;moment>={str(last_changed_date)} 00:00:00')
all_changes = [i['events']['meta']['href'] for i  in all_date_changes]
exact_chenges = multiprocess_func_execute(get_mskld_API, all_changes)
dates_chages = []

print(str(last_changed_date))

for i in exact_chenges:
    dates_chages.extend([j for j in i['rows']])
    
if dates_chages != []:
    last_changed_date = min([datetime.date.fromisoformat(i['additionalInfo'][3:13]) for i in dates_chages])
    
print(str(last_changed_date))
changeable_date = []
for i in otgruzki_dates:
    otgr_datetime = datetime.date.fromisoformat(i[:10])
    if otgr_datetime >= last_changed_date:
        changeable_date.append(i)
        
print(len(changeable_date))
multi_prices = multiprocess_func_execute(mult_data_otgruzki_iter, changeable_date, procc=4)
multiprocess_prices_positions = []
extrapross=[multiprocess_prices_positions.extend(i) for i in multi_prices]
print(len(multiprocess_prices_positions))

cost_gbq = gbq_pd('cost', datasetId = 'normalised_tables') 
clear_uncertain_query =(f"""Select count(db_id) FROM {cost_gbq.datasetId}.{cost_gbq.table_name} 
WHERE date < '{str(last_changed_date)}'""")
len_predeleted = cost_gbq.df_query(clear_uncertain_query).iat[0,0]

cost_gbq = gbq_pd('cost', datasetId = 'normalised_tables') 
clear_uncertain_query =(f"""Select count(db_id) FROM {cost_gbq.datasetId}.{cost_gbq.table_name} 
WHERE date < '{str(last_changed_date)}'""")
len_predeleted = cost_gbq.df_query(clear_uncertain_query).iat[0,0]

if int(len_predeleted) + len(multiprocess_prices_positions) >= len_costs:
    clear_uncertain_query =(f"""DELETE FROM {cost_gbq.datasetId}.{cost_gbq.table_name} 
    WHERE date >= '{str(last_changed_date)}'""")
    cost_gbq.df_query(clear_uncertain_query)
    cost_gbq.add(multiprocess_prices_positions, if_exists = 'append')

print(time.time() - strt)