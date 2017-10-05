#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# MLDataHub
# Copyright (C) 2017 Iván de Paz Centeno <ipazc@unileon.es>.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 3
# of the License or (at your option) any later version of
# the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
# MA  02110-1301, USA.

import os
from multiprocessing import Lock
import shutil
import uuid
from mldatahub.config.config import global_config
from mldatahub.odm.file_dao import FileDAO
from mldatahub.storage.generic_storage import GenericStorage

__author__ = 'Iván de Paz Centeno'


SAVE_INTERVAL = global_config.get_save_interval()


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

        with self.lock:
            FileDAO(file_reference=file_id)
            global_config.get_session().flush()

        return file_id

    def _generate_token(self):
        id = str(uuid.uuid4().hex)
        while os.path.exists(os.path.join(self.root_key, id)):
            id = str(uuid.uuid4().hex)

        return id

    def get_file_content(self, file_id):
        try:
            with open(os.path.join(self.root_key, file_id), "rb") as f:
                content_bytes = f.read()
        except Exception as ex:
            content_bytes = b""

        return content_bytes

    def delete_file_content(self, file_id):
        file = FileDAO.query.get(file_reference=file_id)
        if file is not None:
            file.delete()
            global_config.get_session().flush()

        if os.path.exists(os.path.join(self.root_key, file_id)):
            os.remove(os.path.join(self.root_key, file_id))
        else:
            raise FileNotFoundError()

    def __iter__(self):
        files_list = FileDAO.query.find()

        for f in files_list:
            yield f.file_reference

    def __len__(self):
        return FileDAO.query.find().count()

    def delete(self):
        if os.path.exists(self.root_key):
            shutil.rmtree(self.root_key)
        FileDAO.query.remove()
