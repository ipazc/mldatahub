#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
from flask import Flask
from flask_restful import Api
from mldatahub.api.dataset import Datasets, Dataset
from mldatahub.api.dataset_element import DatasetElements, DatasetElement, DatasetElementContent

from mldatahub.api.token import Tokens, Token, TokenLinker
from mldatahub.config.config import global_config
from mldatahub.observer.garbage_collector import GarbageCollector

__author__ = "Iván de Paz Centeno"


app = Flask(__name__)
api = Api(app)


if __name__ == '__main__':
    garbage_collector = GarbageCollector()
    api.add_resource(Tokens, '/tokens')
    api.add_resource(Token, '/tokens/<token_id>')
    api.add_resource(TokenLinker, '/tokens/<token_id>/link/<token_prefix>/<dataset_prefix>')
    api.add_resource(Datasets, '/datasets')
    api.add_resource(Dataset, '/datasets/<token_prefix>/<dataset_prefix>')
    api.add_resource(DatasetElements, '/datasets/<token_prefix>/<dataset_prefix>/elements')
    api.add_resource(DatasetElement, '/datasets/<token_prefix>/<dataset_prefix>/elements/<element_id>')
    api.add_resource(DatasetElementContent, '/datasets/<token_prefix>/<dataset_prefix>/elements/<element_id>/content')
    app.run(host="localhost", debug=False, threaded=True)
    garbage_collector.stop()
    global_config.get_local_storage().close()
