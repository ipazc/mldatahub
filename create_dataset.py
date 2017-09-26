#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import requests

__author__ = 'Iván de Paz Centeno'

token = "1e0d1a22f8f04e9a9213ed984961e694"

response = requests.get("http://localhost:5000/tokens/{}".format(token), params={'_tok': token})

token_data = response.json()


"""response = requests.post("http://localhost:5000/datasets", params={'_tok': token},
                         json={'title': "New dataset22", 'description': "Example dataset", 'tags': ["example", "example2"],
                               'reference': "none", 'url_prefix':"example2"})
"""

#print(response.json())

response = requests.get("http://localhost:5000/datasets", params={'_tok': token})
dataset_url = response.json()[0]['url_prefix']
print(dataset_url)

response = requests.delete("http://localhost:5000/datasets/{}".format(dataset_url), params={'_tok': token})

print(response)