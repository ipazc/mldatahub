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
from bson import ObjectId

__author__ = 'Iván de Paz Centeno'


class File(object):
    def __init__(self, id:ObjectId, content:bytes, size:int):
        self._id = id
        self._content = content
        self._size = size

    @property
    def id(self):
        return self._id

    @property
    def content(self):
        return self._content

    @property
    def size(self):
        return self._size


class GenericStorage(object):

    def get_file(self, file_id):
        pass

    def get_files(self, file_ids:list):
        pass

    def put_file_content(self, content_bytes):
        pass

    def put_files_content(self, content_bytes):
        pass

    def delete_file(self, file_id):
        pass

    def delete_files(self, file_id):
        pass

    def size(self):
        pass

    def get_files_size(self, files_ids:list):
        pass

    def __iter__(self):
        pass

    def __contains__(self, item):
        pass

    def __len__(self):
        pass