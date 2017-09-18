#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from dateutil.relativedelta import relativedelta
from ming import create_datastore
from ming.odm import ThreadLocalODMSession
import datetime


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
    #def from_file(self):

    def set_session_uri(self, new_uri):
        self.config_values['uri'] = new_uri

    def get_session(self):
        if 'uri' not in self.config_values:
            global_config.set_session_uri("mongodb://localhost:27017/mlhubdata")

        if self.session is None:
            self.session = ThreadLocalODMSession(bind=create_datastore(self.config_values['uri']))
        return self.session

global_config = GlobalConfig()