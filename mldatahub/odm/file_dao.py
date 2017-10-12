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

from mldatahub.config.config import global_config
from ming import schema
from ming.odm import MappedClass, FieldProperty

__author__ = "Iván de Paz Centeno"

session = global_config.get_session()


class FileDAO(MappedClass):

    class __mongometa__:
        session = session
        name = 'file'

    _id = FieldProperty(schema.ObjectId)
    size = FieldProperty(schema.Int)
    sha256 = FieldProperty(schema.String)

    @property
    def content(self):
        return FileContentDAO.query.get(_id=self._id).content

    def delete(self):
        FileDAO.query.remove({'_id': self._id})

class FileContentDAO(FileDAO):
    content = FieldProperty(schema.Binary)

from ming.odm import Mapper
Mapper.compile_all()