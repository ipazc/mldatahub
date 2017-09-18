#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from flask import Flask
from flask_restful import Api

from mldatahub.api.token import Tokens, Token

__author__ = "Iván de Paz Centeno"


app = Flask(__name__)
api = Api(app)


if __name__ == '__main__':
    api.add_resource(Tokens, '/tokens')
    api.add_resource(Token, '/token/<token_id>')
    app.run(debug=True, threaded=True)
