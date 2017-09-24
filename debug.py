#!/usr/bin/env python3
# -*- coding: utf-8 -*-


__author__ = 'Iván de Paz Centeno'


from requests import get

"""data ={
    'privileges': '2',
    'description': 'lalele',
    "max_dataset_count": 10000,
    "max_dataset_size": 100000,
    'dataset_url_prefixes': [
        "ex/ivan2",
        "ex/ivan"
    ]
}
"""

for x in range(100):
    print(get('http://192.168.0.13:5000/tokens').json())