import datetime
import time
def otruzka_proccess(otruzka_row,universal_dict):
    """Main process for full OTGRUZKA processing from json to list of list of positions of otgruzka
    Input: json of OTGRUZKA, dict of of dicts of extra data
    Output: list of lists, explanatory dict
    """


    otruzka_id = otruzka_row['id']
    otruzka_name = otruzka_row['name']
    otruzka_moment = otruzka_row['moment']
    otruzka_date = otruzka_row['moment'][:10]

    cureency_id = otruzka_row['rate']['currency']['meta']['href'].split('/')[-1]  
    otruzka_currency = universal_dict['currency'][cureency_id][0]
    otruzka_currency_rate = otruzka_row['rate']['value'] if 'value' in otruzka_row['rate'] else 1.0

    otruzka_urlitco_id = otruzka_row['organization']['meta']['href'].split('/')[-1]
    otruzka_urlitco_name = universal_dict['organization'][otruzka_urlitco_id]

    otruzka_counter_type = otruzka_row['agent']['meta']['type']
    otruzka_counter_id = otruzka_row['agent']['meta']['href'].split("/")[-1]
    otruzka_counter_name = universal_dict[otruzka_counter_type][otruzka_counter_id]

    otruzka_resposible = universal_dict['resp_otgruzka'][otruzka_id]

    otruzka_sum =  otruzka_row['sum']
    otruzka_payedSum = otruzka_row['payedSum']


    if 'customerOrder' in otruzka_row and otruzka_resposible == 'NO':
        linked_order = otruzka_row['customerOrder']['meta']['href'].split('/')[-1]
        otruzka_resposible = universal_dict['resp_zakaz'][linked_order]

    otgruzka_data = [

        otruzka_id,
        otruzka_name,
        otruzka_moment,
        datetime.date.fromisoformat(otruzka_date),
        cureency_id,
        otruzka_currency_rate,
        otruzka_urlitco_id,
        otruzka_urlitco_name,
        otruzka_counter_id,
        otruzka_counter_name,
        otruzka_resposible,
        otruzka_sum,
        otruzka_payedSum

    ]
    return otgruzka_data

def positions_process(otruzka_row,universal_dict):
    """Main process for full OTGRUZKA processing from json to list of list of positions of otgruzka
    Input: json of OTGRUZKA, dict of of dicts of extra data
    Output: list of lists, explanatory dict
    """

    positions_rows = []

    otruzka_id = otruzka_row['id']

    otruzka_date = otruzka_row['moment'][:19]


    for position in otruzka_row['positions']['rows']:
        position_id = position['id']
        position_assort_id = position['assortment']['meta']['href'].split('/')[-1]
        position_type = position['assortment']['meta']['type']
        positions_quantity = position['quantity']
        positions_price = position['price']

        cost_id = (f"{otruzka_date} {position_assort_id}").replace(' ', "_") 
                    
            
        list_positions = [
            position_id,
            position_assort_id,
            position_type,
            positions_quantity,
            positions_price,
            cost_id,
            otruzka_id
        ]


        positions_rows.append(list_positions)


    return positions_rows

def assortment_categories_process(universal_dict):
    """ Gets category names for all position, including varints"""
    dt_product= {}
    modifs_map = {}
    types = {
        'product' : "Товар",
        'variant' : "Модификация",
        'service' : "Услуга"

    }
    for i in universal_dict['assortment']:
        ids = i['id']
        type_p = types[i['meta']['type']]
        name = i['name'].replace("'",'')
        name = name.replace('"','')
        if type_p in {'Товар', 'Услуга'}:
            products = ids
            pathName = i['pathName'].split('/')[:5]
            pathName= pathName + (['None'] * (5-len(pathName)))
            dt_product[ids] = [type_p,products,name]+pathName

        else:
            products = i['product']['meta']['href'].split('/')[-1]
            modifs_map[ids] = [type_p,products,name]
    for i in modifs_map:
        products = modifs_map[i][-2]
        pathName = dt_product[products][3:]
        dt_product[i] = modifs_map[i] + pathName
    return dt_product

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



def loss_process(row, universal_dict):
    """Parse loss data to be inserted in loss table """

    loss_id = row['id']
    loss_name = row['name']

    loss_date = row['moment'][:10]
    cureency_id = row['rate']['currency']['meta']['href'].split('/')[-1]  
    loss_currency = universal_dict['currency'][cureency_id][0]
    loss_currency_rate = row['rate']['value'] if 'value' in row['rate'] else 1.0

    loss_urlitco_id = row['organization']['meta']['href'].split('/')[-1]
    loss_urlitco_name = universal_dict['organization'][loss_urlitco_id]
    

    loss_resposible = 'Списание'

    loss_sum =  row['sum']

    loss_data = [

        loss_id,
        loss_name,
        loss_date,
        loss_currency,
        loss_currency_rate,
        loss_urlitco_id,
        loss_urlitco_name,
        loss_resposible,
        -loss_sum
    ]
    
    return loss_data

