#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from mldatahub.config.config import global_config
from mldatahub.factory.dataset_factory import DatasetFactory
from mldatahub.factory.token_factory import TokenFactory
from mldatahub.odm.dataset_dao import DatasetDAO
from mldatahub.odm.token_dao import TokenDAO

__author__ = 'Iván de Paz Centeno'

session = global_config.get_session()

token1 = TokenDAO("example_token", 2, 5, "dalap")
token2 = TokenDAO("example_token2", 2, 5, "dalap")
dataset1 = DatasetDAO("ex/ivan", "example1", "lalala", "none")
dataset2 = DatasetDAO("ex/ivan2", "example2", "lalala", "none")

token1 = token1.link_datasets([dataset1, dataset2])
token2 = token2.link_dataset(dataset2)

print(token1.datasets)
print(token2.datasets)

token1 = token1.unlink_dataset(dataset2)
print("\n\n", token1.datasets)

datasetN = DatasetDAO.query.get(url_prefix="ex/ivan")

print(datasetN)
print(datasetN in token1.datasets)