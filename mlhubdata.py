#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from flask import Flask
from flask_restful import Api
from mldatahub.api.dataset import Datasets, Dataset

from mldatahub.api.token import Tokens, Token, TokenLinker

__author__ = "Iván de Paz Centeno"


app = Flask(__name__)
api = Api(app)


if __name__ == '__main__':
    api.add_resource(Tokens, '/tokens')
    api.add_resource(Token, '/token/<token_id>')
    api.add_resource(TokenLinker, '/token/<token_id>/link/<token_prefix>/<dataset_prefix>')
    api.add_resource(Datasets, '/datasets')
    api.add_resource(Dataset, '/dataset/<token_prefix>/<dataset_prefix>')
    app.run(debug=True, threaded=True)
