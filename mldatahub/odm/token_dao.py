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

import uuid

from mldatahub.config.config import global_config, now, token_future_end
from ming import schema
from ming.odm import Mapper, ForeignIdProperty, RelationProperty, MappedClass, FieldProperty
from mldatahub.config.privileges import Privileges

__author__ = 'Iván de Paz Centeno'

session = global_config.get_session()


class TokenDAO(MappedClass):

    class __mongometa__:
        session = session
        name = 'token'

    _id = FieldProperty(schema.ObjectId)
    token_gui = FieldProperty(schema.String)
    description = FieldProperty(schema.String)
    max_dataset_count = FieldProperty(schema.Int)
    max_dataset_size = FieldProperty(schema.Int)
    creation_date = FieldProperty(schema.datetime)
    modification_date = FieldProperty(schema.datetime)
    end_date = FieldProperty(schema.datetime)
    privileges = FieldProperty(schema.Int)
    url_prefix = FieldProperty(schema.String)
    _datasets= ForeignIdProperty('DatasetDAO', uselist=True)
    datasets = RelationProperty('DatasetDAO')

    def __init__(self, description, max_dataset_count, max_dataset_size, url_prefix, token_gui=None,
                 creation_date=now(), modification_date=now(), end_date=token_future_end(),
                 privileges=Privileges.RO_WATCH_DATASET):

        if token_gui is None:
            token_gui = self.generate_token()
        else:
            if self.query.get(token_gui=token_uuid) is not None:
                raise Exception("Specified token GUI already exists.")

        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"]}
        super().__init__(**kwargs)

    def generate_token(self):

        token_uuid = str(uuid.uuid4().hex)
        while self.query.get(token_gui=token_uuid) is not None:
            token_uuid = str(uuid.uuid4().hex)

        return token_uuid

    def unlink_dataset(self, dataset):
        return self.unlink_datasets([dataset])

    def unlink_datasets(self, datasets):
        old_datasets = list(self.datasets)
        for dataset in datasets:
            old_datasets.remove(dataset)

        self.datasets = old_datasets
        session.flush()
        return self.update()

    def link_dataset(self, dataset):
        return self.link_datasets([dataset])

    def link_datasets(self, datasets):
        old_datasets = list(self.datasets)
        old_datasets += datasets
        self.datasets = old_datasets
        session.flush()
        return self.update()

    def has_dataset(self, dataset):
        return dataset._id in self._datasets

    def has_dataset_id(self, dataset_id):
        return dataset_id in self._datasets

    def update(self):
        return session.refresh(self)

    def serialize(self):
        fields = ["token_gui", "url_prefix", "description", "max_dataset_count",
                  "max_dataset_size", "creation_date",
                  "modification_date", "end_date", "privileges"]

        return {f: str(self[f]) for f in fields}

Mapper.compile_all()
