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

#response = requests.delete("http://localhost:5000/datasets/{}".format(dataset_url), params={'_tok': token})
#print(response.json())

# Let's get elements from the dataset
response = requests.get("http://localhost:5000/datasets/{}".format(dataset_url), params={'_tok': token})
print(response.json())

response = requests.get("http://localhost:5000/datasets/{}/elements".format(dataset_url), params={'_tok': token})
print(response.json())

# Let's add a new element to the dataset:
#response = requests.post("http://localhost:5000/datasets/{}/elements".format(dataset_url), params={'_tok': token},
#                         json={'title': 'element1', 'description':'example desc', 'tags': ['test', 'age: 1']})

#element_id = response.json()
#print("written element: {}".format(element_id))

response = requests.get("http://localhost:5000/datasets/{}/elements".format(dataset_url), params={'_tok': token})
print("Elements:",response.json())

#response = requests.put("http://localhost:5000/datasets/{}/elements/{}".format(dataset_url, "59ca645cb9a7c03c3e5dfa40"), params={'_tok': token}, data=b"hello!!!")
#print(response)
response = requests.get("http://localhost:5000/datasets/{}/elements/{}".format(dataset_url, "59ca645cb9a7c03c3e5dfa40"), params={'_tok': token})
print(response.json())

response = requests.get("http://localhost:5000/datasets/{}/elements/{}/content".format(dataset_url, "59ca645cb9a7c03c3e5dfa40"), params={'_tok': token})
print(response.content)

response = requests.patch("http://localhost:5000/datasets/{}/elements/{}".format(dataset_url, "59ca645cb9a7c03c3e5dfa40"), params={'_tok': token}, json={'title': "hellao"})
print(response)
response = requests.delete("http://localhost:5000/datasets/{}/elements/{}".format(dataset_url, "59ca645cb9a7c03c3e5dfa40"), params={'_tok': token})
print(response)
