#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import requests

__author__ = 'Iván de Paz Centeno'

token = "260d59be41e14ed2ac28484380314178"

response = requests.get("http://localhost:5000/tokens/{}".format(token), params={'_tok': token})

token_data = response.json()


try:
    response = requests.post("http://localhost:5000/datasets", params={'_tok': token},
                             json={'title': "New dataset22", 'description': "Example dataset", 'tags': ["example", "example2"],
                                   'reference': "none", 'url_prefix':"example2"})


    print(response.json())
    dataset_url = response.json()['url_prefix']
except Exception as ex:
    dataset_url = "ipazc/example2"

response = requests.patch("http://localhost:5000/datasets/{}".format(dataset_url), params={'_tok': token},
                         json={'title': "Modified dataset", 'tags': ["example", "le2"]})

print(response.json())

response = requests.get("http://localhost:5000/datasets", params={'_tok': token})
print(response.json())

response = requests.delete("http://localhost:5000/datasets/{}".format(dataset_url), params={'_tok': token})
print(response.json())

