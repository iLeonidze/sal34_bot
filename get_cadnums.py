from typing import Dict
from urllib.parse import quote_plus
from time import sleep

import pandas as pd
import requests
import urllib3
from pandas import DataFrame

urllib3.disable_warnings()

ROSREESTR_COOKIES = ''
OBJS = DataFrame(columns=['type',
                          'number',
                          'id',
                          'address',
                          'status',
                          'area',
                          'cost',
                          'registered',
                          'updated',
                          'cancelled',
                          'right_type',
                          'right_type_code',
                          'right_number',
                          'right_registered',
                          'encumbrance_type',
                          'encumbrance_type_code',
                          'encumbrance_number',
                          'encumbrance_started'
                          ])
OBJ_PARAMS = {
    "кв": {
        "prefix": "квартира",
        "suffix": "",
        "amount": 608,
    },
    "кл": {
        "prefix": "помещение",
        "suffix": "К",
        "amount": 343,
    },
    "мм": {
        "prefix": "машино-место",
        "suffix": "",
        "amount": 123,
    },
    "нж": {
        "prefix": "помещение",
        "suffix": "Н",
        "amount": 16
    }
}

addresses = [
    'Российская Федерация, город Москва, внутригородская территория поселение Сосенское, квартал № 28, дом 2, корпус 6'
]


def get_cadnum(query: str) -> (str or None, str or None):
    r = requests.get("https://lk.rosreestr.ru/account-back/address/search?term=" + quote_plus(query),
                     verify=False,
                     headers={
                         'Accept': 'application/json, text/plain, */*',
                         'Pragma': 'no-cache',
                         'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15',
                         'Accept-Language': 'ru',
                         'Referer': 'https://lk.rosreestr.ru/eservices/real-estate-objects-online'
                     })
    sleep(1)

    results = r.json()
    if len(results) > 0:
        el = results[0]
        if el['actual'] and el['full_name'] == query:
            return el['cadnum'], el['full_name']

    return None, None


def property_details(cadnum: str) -> Dict or None:
    r = requests.post(
        "https://lk.rosreestr.ru/account-back/on",
        verify=False,
        headers={
            'Content-Type': 'application/json;charset=utf-8',
            'Pragma': 'no-cache',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'ru',
            'Host': 'lk.rosreestr.ru',
            'Origin': 'https://lk.rosreestr.ru',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15',
            'Referer': 'https://lk.rosreestr.ru/eservices/real-estate-objects-online',
            'Cookie': ROSREESTR_COOKIES
        },
        json={
            "filterType": "cadastral",
            "cadNumbers": [cadnum]
        }
    )
    sleep(1)
    results = r.json()['elements']
    if len(results) > 0:
        el = results[0]
        parsed = {
            'status': el['status'],
            'area': el['area'],
            'cost': el['cadCost'],
            'registered': el['regDate'],
            'updated': el['infoUpdateDate'],
            'cancelled': el['cancelDate'],
            'right_type': None,
            'right_type_code': None,
            'right_number': None,
            'right_registered': None,
            'encumbrance_type': None,
            'encumbrance_type_code': None,
            'encumbrance_number': None,
            'encumbrance_started': None
        }
        if el['rights']:
            parsed['right_type'] = el['rights'][0]['rightTypeDesc']
            parsed['right_type_code'] = el['rights'][0]['rightType']
            parsed['right_number'] = el['rights'][0]['rightNumber']
            parsed['right_registered'] = el['rights'][0]['rightRegDate']
        if el['encumbrances']:
            parsed['encumbrance_type'] = el['encumbrances'][0]['typeDesc']
            parsed['encumbrance_type_code'] = el['encumbrances'][0]['type']
            parsed['encumbrance_number'] = el['encumbrances'][0]['encumbranceNumber']
            parsed['encumbrance_started'] = el['encumbrances'][0]['startDate']
        return parsed

    return None


def main():
    global OBJS

    for obj_type in OBJ_PARAMS.keys():
        for obj_n in range(1, OBJ_PARAMS[obj_type]['amount'] + 1):
            print(obj_type + ' ' + str(obj_n))

            obj_prefix = OBJ_PARAMS[obj_type]['prefix']
            obj_suffix = OBJ_PARAMS[obj_type]['suffix']
            found = False
            for address in addresses:
                query = address + f', {obj_prefix} {obj_n}{obj_suffix}'
                property_id, parsed_address = get_cadnum(query)
                if property_id is not None:
                    found = True
                    obj = {
                        'type': obj_type,
                        'number': obj_n,
                        'id': property_id,
                        'address': parsed_address
                    }
                    obj_details = property_details(property_id)
                    if obj_details:
                        obj = {**obj, **obj_details}
                    OBJS = pd.concat([OBJS, pd.DataFrame.from_records([obj])])
                    break

            if not found:
                OBJS = pd.concat([OBJS, pd.DataFrame.from_records([{
                    'type': obj_type,
                    'number': obj_n
                }])])

            OBJS.to_excel("cadnums.xlsx")


if __name__ == '__main__':
    main()
