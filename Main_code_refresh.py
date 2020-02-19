import pandas as pd
import requests
import os
import datetime
import calendar
import time
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="CarWeGo-Analytics-b48469538956.json"

from GBQ2 import BQ_table
import MS_connect
from MS_connect import *

import pd_gbq
from pd_gbq import *

from MS_json_procc import *
print('h')
strt = time.time()
otgruzki = get_all_entity_mult('demand', expand='positions', limit=100)
time.sleep(5)
universal_dict = {'organization' :  get_all_entity_mult('organization',extra_process=dicti),

                   'employee'    :  get_all_entity_mult('employee',extra_process=dicti),

                   'counterparty':  get_all_entity_mult('counterparty',extra_process=dicti),
                   'currency'    :  get_all_entity_mult('currency',extra_process=dicti_rate),
                   'product'     :  get_all_entity_mult('product',extra_process=dicti),
                   'variant'     :  get_all_entity_mult('variant',extra_process=dicti),
                   'service'     :  get_all_entity_mult('service',extra_process=dicti),
                   'customerorder' : get_all_entity_mult('customerorder'),
                   'assortment'  :  get_all_entity_mult('assortment'),
                   'resp_otgruzka' : get_responsibles_orders(otgruzki)
                  }
universal_dict['resp_zakaz'] = get_responsibles_orders(universal_dict['customerorder'])
loss = get_all_entity_mult('loss')

otgruzki_table_rows = [otruzka_proccess(i,universal_dict) for i in otgruzki]
positions_table_rows = []
[positions_table_rows.extend(positions_process(i,universal_dict)) for i in otgruzki]
cateroise=assortment_categories_process(universal_dict)
cateroise2 = [[i]+e for i,e in cateroise.items()]
loss_prossesed = [loss_process(i,universal_dict) for i in loss]

loss_gbq =gbq_pd('loss',datasetId = 'normalised_tables')
loss_gbq.add(loss_prossesed, if_exists = 'replace')
categories_gbq =gbq_pd('categories',datasetId = 'normalised_tables')
categories_gbq.add(cateroise2, if_exists = 'replace')
positions_gbq =gbq_pd('positions',datasetId = 'normalised_tables')
positions_gbq.add(positions_table_rows, if_exists = 'replace')
otgruzki_gbq =gbq_pd('otgruzki', datasetId = 'normalised_tables')
otgruzki_gbq.add(otgruzki_table_rows, if_exists = 'replace')


def redifine_row_df(row):
    
    if row['otruzka_urlitco_name'] == 'Транспортный отдел' and row['cat3']== 'Транспортные услуги':
        row['otruzka_urlitco_name'] = 'Income'
    elif row['otruzka_urlitco_name'] == 'ИнтерК' and row['good_name'] in ['Образцы','Курсовая разница']:
        row['cat1'] = 'Прочее'
        
    if  (row['good_name'] == 'ТГФ' or row['good_name'] =='Курсовая разница')  and row['otruzka_urlitco_name'] == 'Income':
        row['cat1'] = row['good_name']
        

    elif  row['cat3'] == 'Колпак В28 "сургуч"':
        row['cat1'] = 'В28'
        
    elif row['cat3'] == 'Термоусадочные колпаки':
        row['cat1'] = 'ТУК'
        
    elif row['cat1'] is None:
        row['cat1'] = row['good_name']

    return row

join_table_gains_query = """
SELECT
  otruzka_name,
  good_name,
  cat1,
  cat2,
  cat3,
  otruzka_urlitco_name,
  otruzka_counter_name,
  otruzka_resposible,
  PARSE_DATE('%F', otruzka_date) as otruzka_date,
  if(units_id = '4ee31997-01e1-11e9-9ff4-31500054e835' or units_id = '4ebe5336-01e1-11e9-9ff4-31500054e802' , positions_quantity*1000,positions_quantity) as quant,
  (otruzka_currency_rate* positions_price* positions_quantity)/100 as Gain,
  (sellCost* positions_quantity)/100 as Costs
FROM
  `carwego-analytics.normalised_tables.positions` AS positions
JOIN
  `carwego-analytics.normalised_tables.otgruzki` AS otgruzki
ON
  positions.otruzka_id = otgruzki.otruzka_id
JOIN
  `carwego-analytics.normalised_tables.categories` AS categories
ON
  positions.position_assort_id = categories.good_id
JOIN
  `carwego-analytics.normalised_tables.cost` AS cost
ON
  positions.cost_id = cost.db_id
  
UNION ALL

SELECT
  'custom1' AS otruzka_name,
  good_name AS good_name,
  cat1 AS cat1,
  'None' AS cat2,
  'None' AS cat3,
  otruzka_urlitco_name AS otruzka_urlitco_name,
  'Доп.позиция'AS otruzka_counter_name,
  'Доп.позиция' AS otruzka_resposible,
  otruzka_date AS otruzka_date,
  1.0 AS quant,
  Gain AS Gain,
  0.0 AS Costs
FROM
  `carwego-analytics.normalised_tables.Custom_gains`
  """

gains_gbq = gbq_pd('gains', datasetId = 'BI_Dataset')
gains_df=gains_gbq.df_query(join_table_gains_query)

gains_df_redif  = pd.DataFrame([redifine_row_df(i[1]) for i in gains_df.iterrows()], columns = gains_df.columns)

gains_gbq.add(gains_df_redif, if_exists = 'replace' )

expendure_query = """SELECT
 row_date  AS month,
  SUM(row_sum * currency_rate_payday)/100 AS out_payed,
  row_item as reason,
  row_organization as urlico,
  row_counter_agent as counert,
  'Исходящий' AS tip,
  type_pay as nalbeznal,
  -SUM(row_sum * currency_rate_payday)/100 AS money
  
FROM
  `carwego-analytics.icapbi.all_payments` AS pmnts
WHERE
  row_type = 'paymentout'
GROUP BY
  1,
  row_item,
  row_organization,
  row_counter_agent,
  type_pay
  
union ALL 

(SELECT
  date(otruzka_date) AS month,
  SUM(Costs) AS out_payed,
  'Себестоимость по отгрузам' as reason,
  otruzka_urlitco_name as urlico,
  otruzka_counter_name as counert,
  'Исходящий' AS tip,
  'Не указано' as nalbeznal,
  -SUM(Costs) AS money
FROM 
  `carwego-analytics.BI_Dataset.gains`
group by 
  1,3,4,5)
  
union ALL 

SELECT
  date(otruzka_date) AS month,
  SUM(Gain) AS out_payed,
  cat1 as reason,
  otruzka_urlitco_name as urlico,
  otruzka_counter_name as counert,
  'Входящий' AS tip,
  'Не указано' as nalbeznal ,
   SUM(Gain) AS money
FROM 
  `carwego-analytics.BI_Dataset.gains`
group by 
  1,3,4,5
  
union ALL  
  
SELECT
  loss_date AS month,
  SUM( loss_sum /-100) AS out_payed,
  'Списание по инвентаризации' AS reason,
  loss_urlizo_name AS urlico,
  loss_urlizo_name AS counert,
  'Исходящий' AS tip,
  'Не указано' AS nalbezna,
  SUM( loss_sum /100) AS money
FROM
  `carwego-analytics.normalised_tables.loss`
group by 
  1,3,4,5 """


expendure_gbq = gbq_pd('expendure', datasetId = 'BI_Dataset')
expendure_df=expendure_gbq.df_query(expendure_query)
expendure_gbq.add(expendure_df, if_exists = 'replace')

print(time.time() - strt)
