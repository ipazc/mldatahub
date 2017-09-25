#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
from multiprocessing import Lock

import shutil
import uuid

from mldatahub.storage.generic_storage import GenericStorage

__author__ = 'Iván de Paz Centeno'


class LocalStorage(GenericStorage):
    def __init__(self, root_key):
        super().__init__(root_key)

        self.lock = Lock()

        if not os.path.exists(root_key):
            os.makedirs(root_key)

    def put_file_content(self, content_bytes, file_id=None):
        if file_id is None:
            file_id = self._generate_token()

        with open(os.path.join(self.root_key, file_id), "wb") as f:
            f.write(content_bytes)

        return file_id

    def _generate_token(self):
        id = str(uuid.uuid4().hex)
        while os.path.exists(os.path.join(self.root_key, id)):
            id = str(uuid.uuid4().hex)

        return id

    def get_file_content(self, file_id):

        with open(os.path.join(self.root_key, file_id), "rb") as f:
            content_bytes = f.read()
        
        return content_bytes

    def delete_file_content(self, file_id):
        if os.path.exists(file_id):
            os.remove(os.path.join(self.root_key, file_id))
        else:
            raise FileNotFoundError()

    def delete(self):
        if os.path.exists(self.root_key):
            shutil.rmtree(self.root_key)
