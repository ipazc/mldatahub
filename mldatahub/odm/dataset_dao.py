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

from multiprocessing import Lock
from mldatahub.config.config import global_config, now
from ming import schema
from ming.odm import ForeignIdProperty, RelationProperty, MappedClass, FieldProperty


__author__ = 'Iván de Paz Centeno'


lock = Lock()
session = global_config.get_session()


class DatasetDAO(MappedClass):

    class __mongometa__:
        session = session
        name = 'datasets'

    _id = FieldProperty(schema.ObjectId)
    url_prefix = FieldProperty(schema.String)
    title = FieldProperty(schema.String)
    description = FieldProperty(schema.String)
    reference = FieldProperty(schema.String)
    creation_date = FieldProperty(schema.datetime)
    modification_date = FieldProperty(schema.datetime)
    tags = FieldProperty(schema.Array(schema.String))
    elements = RelationProperty('DatasetElementDAO')
    comments = RelationProperty('DatasetCommentDAO')
    tokens = RelationProperty('TokenDAO')
    fork_count = FieldProperty(schema.Int)
    forked_from_id = ForeignIdProperty('DatasetDAO')
    forked_from = RelationProperty('DatasetDAO')

    def __init__(self, url_prefix, title, description, reference, tags=None, creation_date=now(), modification_date=now(),
                 fork_count=0, forked_from=None, forked_from_id=None):
        with lock:
            previous_dset = DatasetDAO.query.get(url_prefix=url_prefix)
            if previous_dset is not None:
                raise Exception("Url prefix already taken.")

        if forked_from_id is None and forked_from is not None:
            forked_from_id = forked_from._id

        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__"]}
        super().__init__(**kwargs)

    @classmethod
    def from_dict(cls, init_dict):
        return cls(**init_dict)

    def add_comment(self, author_name, author_link, content, addition_date=now()):
        return DatasetCommentDAO(author_name, author_link, content, addition_date, dataset=self)

    def add_element(self, title, description, file_ref_id, http_ref=None, tags=None, addition_date=now(), modification_date=now()):
        return DatasetElementDAO(title, description, file_ref_id, http_ref, tags, addition_date, modification_date, dataset=self)

    def delete(self):
        url_prefix = self.url_prefix
        for dataset_element in self.elements:
            dataset_element.delete()

        DatasetCommentDAO.query.remove({'dataset_id': self._id})
        DatasetElementDAO.query.remove({'dataset_id': self._id})

        DatasetDAO.query.remove({'_id': self._id})

    def update(self):
        return session.refresh(self)

    def serialize(self):

        fields = ["title", "description", "reference",
                  "creation_date", "modification_date",
                  "url_prefix", "fork_count"]

        response = {f: str(self[f]) for f in fields}
        response['comments_count'] = len(self.comments)
        response['elements_count'] = len(self.elements)
        response['tags'] = [t for t in self.tags]
        if self.forked_from is not None:
            response['fork_father'] = self.forked_from.url_prefix
        else:
            response['fork_father'] = None

        return response

    def has_element(self, element):
        return DatasetElementDAO.query.get(_id=element._id, dataset_id=self._id) is not None

    def get_elements(self, options=None):
        query = options
        if query is None:
            query = {}

        query['dataset_id'] = self._id

        return DatasetElementDAO.query.find(query).sort("addition_date", 1)

class DatasetElementDAO(MappedClass):

    class __mongometa__:
        session = session
        name = 'element'

    _id = FieldProperty(schema.ObjectId)
    title = FieldProperty(schema.String)
    description = FieldProperty(schema.String)
    file_ref_id = FieldProperty(schema.String)
    http_ref = FieldProperty(schema.String)
    tags = FieldProperty(schema.Array(schema.String))
    addition_date = FieldProperty(schema.datetime)
    modification_date = FieldProperty(schema.datetime)
    comments = RelationProperty('DatasetElementCommentDAO')
    dataset_id = ForeignIdProperty('DatasetDAO')
    dataset = RelationProperty('DatasetDAO')

    def __init__(self, title, description, file_ref_id, http_ref=None, tags=None, addition_date=now(), modification_date=now(), dataset_id=None, dataset=None):
        if dataset_id is None and dataset is not None:
            dataset_id = dataset._id

        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__", "datasets"]}
        super().__init__(**kwargs)

    @classmethod
    def from_dict(cls, init_dict):
        return cls(**init_dict)

    def add_comment(self, author_name, author_link, content, addition_date=now()):
        return DatasetElementCommentDAO(author_name, author_link, content, addition_date, element=self)

    def delete(self):
        DatasetElementCommentDAO.query.remove({'element_id': self._id})
        DatasetElementDAO.query.remove({'_id': self._id})
        super().delete()

    def update(self):
        return session.refresh(self)

    def serialize(self):
        fields = ["title", "description", "_id",
                  "addition_date", "modification_date",
                  "http_ref"]

        response = {f: str(self[f]) for f in fields}
        response['comments_count'] = len(self.comments)
        response['has_content'] = self.file_ref_id is not None
        response['tags'] = [t for t in self.tags]
        return response


class DatasetCommentDAO(MappedClass):

    class __mongometa__:
        session = session
        name = 'dataset_comment'

    _id = FieldProperty(schema.ObjectId)
    author_name = FieldProperty(schema.String)
    author_link = FieldProperty(schema.String)
    content = FieldProperty(schema.String)
    addition_date = FieldProperty(schema.datetime)
    dataset_id = ForeignIdProperty('DatasetDAO')
    dataset = RelationProperty('DatasetDAO')

    def __init__(self, author_name, author_link, content, addition_date=now(), dataset_id=None, dataset=None):
        if dataset_id is None and dataset is not None:
            dataset_id = dataset._id

        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__", "datasets"]}
        super().__init__(**kwargs)

    @classmethod
    def from_dict(cls, init_dict):
        return cls(**init_dict)

    def update(self):
        return session.refresh(self)

    def delete(self):
        DatasetCommentDAO.query.remove({'_id': self._id})


class DatasetElementCommentDAO(MappedClass):

    class __mongometa__:
        session = session
        name = 'data_element_comment'

    _id = FieldProperty(schema.ObjectId)
    author_name = FieldProperty(schema.String)
    author_link = FieldProperty(schema.String)
    content = FieldProperty(schema.String)
    addition_date = FieldProperty(schema.datetime)
    element_id = ForeignIdProperty('DatasetElementDAO')
    element = RelationProperty('DatasetElementDAO')

    def __init__(self, author_name, author_link, content, addition_date=now(), element_id=None, element=None):
        if element_id is None and element is not None:
            element_id = element._id

        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__", "element"]}
        super().__init__(**kwargs)

    @classmethod
    def from_dict(cls, init_dict):
        return cls(**init_dict)

    def update(self):
        return session.refresh(self)

    def delete(self):
        DatasetElementCommentDAO.query.remove({'_id': self._id})
        MappedClass.delete(self)

from ming.odm import Mapper
Mapper.compile_all()