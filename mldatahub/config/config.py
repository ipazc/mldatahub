#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from dateutil.relativedelta import relativedelta
from ming import create_datastore
from ming.odm import ThreadLocalODMSession
import datetime
from mldatahub.storage.local.local_storage import LocalStorage

__author__ = 'Iván de Paz Centeno'



def now():
    return datetime.datetime.now()

def token_future_end():
    return now() + relativedelta(months=+1)


class GlobalConfig(object):

    def __init__(self, config_values=None):
        if config_values is None:
            config_values = {}

        self.config_values = config_values
        self.session = None
        self.local_storage = None

    #def from_file(self):

    def set_session_uri(self, new_uri):
        self.config_values['session_uri'] = new_uri

    def set_local_storage_uri(self, new_uri):
        self.config_values['local_storage_uri'] = new_uri

    def get_session(self):
        if 'uri' not in self.config_values:
            global_config.set_session_uri("mongodb://localhost:27017/mlhubdata")

        if self.session is None:
            self.session = ThreadLocalODMSession(bind=create_datastore(self.config_values['session_uri']))
        return self.session

    def get_local_storage(self):
        if self.local_storage is None:
            self.local_storage = LocalStorage(self.config_values['local_storage_uri'])
        return self.local_storage


global_config = GlobalConfig()
