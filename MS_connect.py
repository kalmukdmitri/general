import json
import requests
import datetime
import time

with open('SKLAD') as json_file:
    headers = json.load(json_file)
print(headers)

def cost_process(row, date):
    """Prossess data of profitabilty of given product in a given moment
    Intup: JSON
    Output: list"""
    
    db_id = (date+'_'+row['assortment']['meta']['href'].split('/')[-1]).replace(' ', '_')
    date = datetime.date.fromisoformat(date[:10])
    product_id = row['assortment']['meta']['href'].split('/')[-1]
    units_id = "4ecd65c4-01e1-11e9-9ff4-31500054e819"
    
    if 'uom' in row['assortment']:
        units_id = row['assortment']["uom"]['meta']['href'].split('/')[-1]
        
    sellPrice = row['sellPrice']
    sellCost = row['sellCost']

    
    return [db_id, date, product_id,  units_id, 
           sellPrice, sellCost]

def get_all_entity_mult(entity,expand = False, extra_process = False, limit=1000):
    """Export data of Moysklad Entity to memory.
    If there if more then limit of row of data func loads by biggest possible batches
    Intup: str of entity
    Optinal params: 
    expand - expend certain fields of data, if not False limit must be set to 100
    extra_process - processing func to cleanup the results
    Output: list of JSON responses"""
    if expand:
        limit=100
    else:
        expand=''
    
    main_url = 'https://online.moysklad.ru/api/remap/1.2/entity/'
    API_request = f"{main_url}{entity}?limit={limit}{'&expand='+expand}"
    print(API_request)
    result = get_mskld_API(API_request)
    final = result['rows']
    if result['meta']['size'] > limit:
        len_extra_requests = (int(result['meta']['size']/limit)+1)
        print(f"Потребуется {len_extra_requests} запосов по {entity}") 
        rquests_list = [f'{API_request}&offset={i*limit}' for i in range(1,len_extra_requests)]
        row_req_list = multiprocess_func_execute(get_mskld_API, rquests_list, procc = 5)
        [final.extend(i['rows']) for i in row_req_list]
    if extra_process:
        final = extra_process(final)
        print('Входные данные обработаны')
        return final
    else:
        return final
          
def get_mskld_API(quer):
    '''Get json data from data '''
    url_org = quer
    resul =requests.request('get', url_org, headers=headers)
    results = json.loads(resul.text)
    return results

def get_responsibles_orders(otgruzki):
    """func to retrive custom name of Resposible empoloy from otgruzka"""
    
    resposible_order = {}
    for j in otgruzki:
        resposible_order[j['id']]= "NO"
        if 'attributes' in j:
            for i in j['attributes']:
                if i['name'] == 'Ответственный:' or i['name'] == 'Ответственный':
                    resposible_order[j['id']]= i['value']['name']
                    break


    return resposible_order

def multiprocess_func_execute(func, data, procc=4, post_process = False):

    
    import multiprocessing.dummy as multiprocessing
    
    p = multiprocessing.Pool(processes=procc)

    results = p.map(func,data )
    p.close()
    p.join()
    
    if post_process:
        results = post_process(results)

    return results

def dicti(rows):
    """Extra process fild name extraction on load"""
    return {i['id']:i['name'] for i in rows }
          
def dicti_rate(rows):
    """Extra process fild name and currency extraction on load"""
    return {i['id']:[i['name'],i['rate']] for i in rows }


def mult_data_otgruzki_iter(otgruzki):
    result = []
    day_result = get_mskld_API(f'https://online.moysklad.ru/api/remap/1.2/report/profit/byvariant?momentFrom={otgruzki}&momentTo={otgruzki}')
    result.extend([cost_process(j,otgruzki) for j in day_result['rows']])
    return result

def get_mskld_API(quer):
    '''Get json data from data '''
    url_org = quer
    resul =requests.request('get', url_org, headers=headers)
    results = json.loads(resul.text)
    return results

def get_all_anything_mult(entity , expand = False, extra_process = False, limit=1000, filtr = ''):
    """Export data of Moysklad Entity to memory.
    If there if more then limit of row of data func loads by biggest possible batches
    Intup: str of entity
    Optinal params: 
    expand - expend certain fields of data, if not False limit must be set to 100
    extra_process - processing func to cleanup the results
    Output: list of JSON responses"""
    start = time.time()
    if expand:
        limit=100
    else:
        expand=''
    
    main_url = 'https://online.moysklad.ru/api/remap/1.2/'
    API_request = f"{main_url}{entity}?limit={limit}&filter={filtr}{'&expand='+expand}"
    print(API_request)
    result = get_mskld_API(API_request)
    final = result['rows']
    if result['meta']['size'] > limit:
        len_extra_requests = (int(result['meta']['size']/limit)+1)
        print(f"Потребуется {len_extra_requests} запосов по {entity}") 
        rquests_list = [f'{API_request}&offset={i*limit}' for i in range(1,len_extra_requests)]
        row_req_list = multiprocess_func_execute(get_mskld_API, rquests_list, procc = 5)
        [final.extend(i['rows']) for i in row_req_list]
    print(time.time() - start)
    if extra_process:
        final = extra_process(final)
        print('Входные данные обработаны')
        return final
    else:
        return final
    

