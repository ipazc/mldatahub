#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from threading import Lock
from flask import request
from flask_restful import reqparse

__author__ = 'Iván de Paz Centeno'

import datetime

def now():
    return datetime.datetime.now()

from ming import Session, create_datastore, Document, Field, schema

session = Session(create_datastore('mongodb://localhost:27017/tutorial2'))

class Dataset(Document):

    class __mongometa__:
        session = session
        name = 'datasets'

    _id = Field(schema.ObjectId)
    title = Field(schema.String)
    reference = Field(schema.String)
    author = Field(schema.String)
    creation_date = Field(schema.datetime)
    modification_date = Field(schema.datetime)
    images = Field(schema.Array(schema.String))


    def __init__(self, title, reference, author, creation_date=now(), modification_date=now(), images=None):
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"]}
        print(kwargs)
        super().__init__(kwargs)

    @classmethod
    def from_dict(cls, init_dict):
        return cls(**init_dict)

class TokenizedREST(object):
    def get_request(self, allow_public=False):
        input_query = request.get_json()

        token = input_query['tokens']
        del input_query['tokens']

        #author = get_token_author(tokens)

        if author is None and not allow_public:
            raise Exception("Forbidden")

        return token, author, input_query

class DatasetList(TokenizedREST):
    def __init__(self):
        super().__init__()
        self.id_lock = Lock()
        self.post_parser = reqparse.RequestParser()
        self.post_parser.add_argument('task')

    def get(self):
        token, author, input_query = self.get_request(allow_public=True)
        return Dataset.m.find({'author': author})

    def post(self):
        token, author, input_query = self.get_request()

        input_query['author'] = author
        dataset = Dataset(**input_query)
        dataset.m.save()

        return "Done", 201


#datasets = Dataset("test", "http://localhost", ["Iván de Paz Centeno"])
#datasets.m.save()

d = DatasetList()
for x in d.get():
    x.delete()

print(Dataset.m.count())
#Dataset.delete()
#print(result)