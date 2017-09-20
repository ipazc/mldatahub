#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from flask_restful import abort
from mldatahub.config.config import global_config, now
from mldatahub.config.privileges import Privileges
from mldatahub.odm.dataset_dao import DatasetDAO

__author__ = 'Iván de Paz Centeno'


class DatasetElementFactory(object):

    def __init__(self, token, dataset):
        self.token = token
        self.dataset = dataset
        self.session = global_config.get_session()

    def create_element(self, *args, **kwargs):

    def edit_element(self, element_id):

    def get_element_info(self, element_id):

    def get_element_thumbnail(self, element_id):

    def get_element_content(self, element_id):

    def destroy_element(self, element_id):
