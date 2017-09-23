#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from io import BytesIO
from flask import Flask, request
from flask_restful import Api, Resource
from werkzeug.datastructures import FileStorage

__author__ = 'Iván de Paz Centeno'


app = Flask(__name__)
api = Api(app)

class Res(Resource):
    def post(self):
        for x in request.files:
            print(x)
            print(request.files[x])

            bytes = BytesIO()
            request.files[x].save(bytes)

            bytes.seek(0)
            print(bytes.read())

        return "Ok"

if __name__ == '__main__':
    api.add_resource(Res, '/post')

    app.run(debug=True, threaded=True)
