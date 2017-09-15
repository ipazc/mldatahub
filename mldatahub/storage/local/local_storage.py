#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
from multiprocessing import Lock

import shutil

from mldatahub.storage.generic_storage import GenericStorage

__author__ = 'Iván de Paz Centeno'

class LocalStorage(GenericStorage):
    def __init__(self, root_key, last_file_id=-1):
        super().__init__(root_key, last_file_id)

        self.lock = Lock()

        self.last_file_id = self._validate_file_id(self.last_file_id)

        if not os.path.exists(root_key):
            os.makedirs(root_key)

    def put_file_content(self, content_bytes, file_id=None):
        with self.lock:
            self.last_file_id += 1

            if file_id is None:
                file_id = self.last_file_id
            else:
                file_id = self._validate_file_id(file_id)

        with open(os.path.join(self.root_key, "{}".format(file_id)), "wb") as f:
            f.write(content_bytes)

        return file_id

    def get_file_content(self, file_id):
        file_id = self._validate_file_id(file_id)
        with open(os.path.join(self.root_key, "{}".format(file_id)), "rb") as f:
            content_bytes = f.read()
        
        return content_bytes

    def _validate_file_id(self, file_id):
        try:
            file_id = int(file_id)
        except ValueError as ex:
            file_id = None

        if file_id is None:
            raise Exception("File ID is not valid for local storage, must be an integer.")

        return file_id

    def delete(self):
        if os.path.exists(self.root_key):
            shutil.rmtree(self.root_key)