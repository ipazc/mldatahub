#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = "Iván de Paz Centeno"

from requests import post

data ={
    'privileges': '2',
    'description': 'lalele',
    "max_dataset_count": 10000,
    "max_dataset_size": 100000,
    'dataset_url_prefixes': [
        "ex/ivan2",
        "ex/ivan"
    ]
}

print(post('http://localhost:5000/tokens?_tok=0baac2bce68a4ce08b7854d9eaacf438', json=data).json())