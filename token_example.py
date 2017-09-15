#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from mldatahub.config.config import global_config
from mldatahub.odm.dataset import Dataset
from mldatahub.odm.token import Token

__author__ = 'Iván de Paz Centeno'

session = global_config.get_session()

token1 = Token("example_token", 2, 5)
token2 = Token("example_token2", 2, 5)
dataset1 = Dataset("ex/ivan", "example1", "lalala", "none")
dataset2 = Dataset("ex/ivan2", "example2", "lalala", "none")

token1 = token1.link_datasets([dataset1, dataset2])
#token1 = token1.link_dataset(dataset2)
token2 = token2.link_dataset(dataset2)

print(token1.datasets)
print(token2.datasets)

token1 = token1.unlink_dataset(dataset2)
print("\n\n", token1.datasets)
