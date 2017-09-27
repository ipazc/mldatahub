#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'Iván de Paz Centeno'


class GenericStorage(object):

    def __init__(self, root_key):
        self.root_key = root_key

    def get_file_content(self, file_id):
        pass

    def put_file_content(self, content_bytes, file_id=None):
        pass

    def delete_file_content(self, file_id):
        pass

    def get_files_set(self):
        pass