#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from threading import Lock
from flask import request
from flask_restful import reqparse
import datetime
from ming import Session, create_datastore, Document, Field, schema


__author__ = 'Iván de Paz Centeno'


def now():
    return datetime.datetime.now()


session = Session(create_datastore('mongodb://localhost:27017/tutorial2'))


class DatasetToken(Document):

    class __mongometa__:
        session = session
        name = 'token'

    _id = Field(schema.ObjectId)
    description = Field(schema.String)
    max_datasets = Field(schema.Int)
    max_size = Field(schema.Int)
    creation_date = Field(schema.datetime)
    modification_date = Field(schema.datetime)
    permission = Field(schema.Int)

