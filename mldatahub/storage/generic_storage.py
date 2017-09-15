#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'Iván de Paz Centeno'


class GenericStorage(object):

    def __init__(self, root_key, last_file_id):
        self.root_key = root_key
        self.last_file_id = last_file_id

    def get_file_content(self, file_id):
        pass

    def put_file_content(self, content_bytes, file_id=None):
        pass