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
from mldatahub.odm.file_dao import FileDAO
from ming import schema
from ming.odm import ForeignIdProperty, MappedClass, FieldProperty


__author__ = 'Iván de Paz Centeno'


lock = Lock()
session = global_config.get_session()

class GIterator(object):
    def __init__(self, cursor):
        self.cursor = cursor

    def __iter__(self):
        return self.cursor

    def __len__(self):
        return self.cursor.count()

    def __getitem__(self, item):
        return next(self.cursor.skip(item))


class DatasetDAO(MappedClass):

    class __mongometa__:
        session = session
        name = 'dataset'

    _id = FieldProperty(schema.ObjectId)
    url_prefix = FieldProperty(schema.String)
    title = FieldProperty(schema.String)
    description = FieldProperty(schema.String)
    reference = FieldProperty(schema.String)
    creation_date = FieldProperty(schema.datetime)
    modification_date = FieldProperty(schema.datetime)
    tags = FieldProperty(schema.Array(schema.String))
    fork_count = FieldProperty(schema.Int)
    forked_from_id = ForeignIdProperty('DatasetDAO')


    @property
    def comments(self):
        return GIterator(self.get_comments())

    @property
    def elements(self):
        return GIterator(self.get_elements())


    def __init__(self, url_prefix, title, description, reference, tags=None, creation_date=now(), modification_date=now(),
                 fork_count=0, forked_from=None, forked_from_id=None):
        with lock:
            if DatasetDAO.query.get(url_prefix=url_prefix) is not None:
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

    def update(self):
        return session.refresh(self)

    def serialize(self):

        fields = ["title", "description", "reference",
                  "creation_date", "modification_date",
                  "url_prefix", "fork_count"]

        response = {f: str(self[f]) for f in fields}
        response['comments_count'] = self.comments.count()
        response['elements_count'] = self.elements.count()
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

    def get_comments(self, options=None):
        query = options
        if query is None:
            query = {}

        query['dataset_id'] = self._id

        return DatasetCommentDAO.query.find(query).sort("addition_date", 1)

    def delete(self):
        DatasetCommentDAO.query.remove({'dataset_id': self._id})
        DatasetElementDAO.query.remove({'dataset_id': self._id})
        DatasetElementCommentDAO.query.remove({'element_id': {'$in': [e._id for e in self.elements]}})
        DatasetDAO.query.remove({'_id': self._id})


class DatasetElementDAO(MappedClass):

    class __mongometa__:
        session = session
        name = 'element'

    _id = FieldProperty(schema.ObjectId)
    title = FieldProperty(schema.String)
    description = FieldProperty(schema.String)
    file_ref_id = ForeignIdProperty('FileDAO')
    http_ref = FieldProperty(schema.String)
    tags = FieldProperty(schema.Array(schema.String))
    addition_date = FieldProperty(schema.datetime)
    modification_date = FieldProperty(schema.datetime)
    dataset_id = ForeignIdProperty('DatasetDAO')

    @property
    def comments(self):
        return GIterator(self.get_comments())

    def __init__(self, title, description, file_ref_id, http_ref=None, tags=None, addition_date=now(), modification_date=now(), dataset_id=None, dataset=None):
        if dataset_id is None and dataset is not None:
            dataset_id = dataset._id

        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__", "datasets"]}
        super().__init__(**kwargs)

    def get_comments(self, options=None):
        query = options
        if query is None:
            query = {}

        query['element_id'] = self._id

        return DatasetElementCommentDAO.query.find(query).sort("addition_date", 1)

    @classmethod
    def from_dict(cls, init_dict):
        return cls(**init_dict)

    def add_comment(self, author_name, author_link, content, addition_date=now()):
        return DatasetElementCommentDAO(author_name, author_link, content, addition_date, element=self)

    def update(self):
        return session.refresh(self)

    def serialize(self):
        fields = ["title", "description", "_id",
                  "addition_date", "modification_date",
                  "http_ref"]

        response = {f: str(self[f]) for f in fields}
        response['comments_count'] = self.comments.count()
        response['has_content'] = self.file_ref_id is not None
        response['tags'] = [t for t in self.tags]
        return response

    def delete(self):
        DatasetElementCommentDAO.query.remove({'element_id': self._id})
        DatasetElementDAO.query.remove({'_id': self._id})


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

from ming.odm import Mapper
Mapper.compile_all()
