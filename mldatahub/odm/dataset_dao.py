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
from bson import ObjectId
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
    size = FieldProperty(schema.Int)
    tags = FieldProperty(schema.Array(schema.Anything))
    fork_count = FieldProperty(schema.Int)
    forked_from_id = ForeignIdProperty('DatasetDAO')


    @property
    def comments(self):
        return GIterator(self.get_comments())

    @property
    def elements(self):
        return GIterator(self.get_elements())

    @property
    def forked_from(self):
        if self.forked_from_id is None:
            return None

        return DatasetDAO.query.get(_id=self.forked_from_id)

    @forked_from.setter
    def forked_from(self, dataset):
        if dataset is not None:
            self.forked_from_id = dataset._id

    def __init__(self, url_prefix, title, description, reference, tags=None, creation_date=now(), modification_date=now(),
                 fork_count=0, forked_from=None, forked_from_id=None):
        with lock:
            if DatasetDAO.query.get(url_prefix=url_prefix) is not None:
                raise Exception("Url prefix already taken.")

        if forked_from_id is None and forked_from is not None:
            forked_from_id = forked_from._id

        size = 0
        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__", "forked_from"]}
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
                  "url_prefix", "fork_count", "size"]

        response = {f: str(self[f]) for f in fields}
        response['comments_count'] = len(self.comments)
        response['elements_count'] = len(self.elements)
        response['tags'] = list(self.tags)

        if self.forked_from is not None:
            response['fork_father'] =  self.forked_from.url_prefix
        else:
            response['fork_father'] = None

        return response

    def has_element(self, element):
        return DatasetElementDAO.query.get(_id=element._id, dataset_id=self._id) is not None

    def get_elements(self, options=None):
        query = options
        if query is None:
            query = {}

        query['dataset_id'] = {'$in': [self._id]}

        return DatasetElementDAO.query.find(query).sort("addition_date", 1)

    def get_comments(self, options=None):
        query = options
        if query is None:
            query = {}

        query['dataset_id'] = self._id

        return DatasetCommentDAO.query.find(query).sort("addition_date", 1)

    def delete(self):
        DatasetCommentDAO.query.remove({'dataset_id': self._id})
        DatasetElementDAO.query.remove({'dataset_id.0': self._id})
        DatasetElementCommentDAO.query.remove({'element_id': {'$in': [e._id for e in self.elements]}})

        # Now those elements that were linked to this dataset (but not owned by the dataset) must be unlinked
        elements = DatasetElementDAO.query.find({'dataset_id': self._id})
        for element in elements:
            element.unlink_dataset(self)

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
    tags = FieldProperty(schema.Array(schema.Anything))
    addition_date = FieldProperty(schema.datetime)
    modification_date = FieldProperty(schema.datetime)
    dataset_id = ForeignIdProperty('DatasetDAO', uselist=True)

    @property
    def comments(self):
        return GIterator(self.get_comments())

    def __init__(self, title, description, file_ref_id, http_ref=None, tags=None, addition_date=None, modification_date=None, dataset_id=None, dataset=None):
        if addition_date is None:
            addition_date = now()

        if modification_date is None:
            modification_date = now()

        if dataset_id is None and dataset is not None:
            dataset_id = [dataset._id]

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

    def unlink_dataset(self, dataset):
        return self.unlink_datasets([dataset])

    def unlink_datasets(self, datasets):
        if len(datasets) > 0 and isinstance(datasets[0], ObjectId):
            datasets_translated = datasets
        else:
            datasets_translated = [d._id for d in datasets]

        self.dataset_id = [d for d in self.dataset_id if d not in datasets_translated]
        return self

    def link_dataset(self, dataset):
        return self.link_datasets([dataset])

    def link_datasets(self, datasets):
        if len(datasets) > 0 and isinstance(datasets[0], ObjectId):
            datasets_translated = datasets
        else:
            datasets_translated = [d._id for d in datasets]
        self.dataset_id += datasets_translated
        return self

    def add_comment(self, author_name, author_link, content, addition_date=now()):
        return DatasetElementCommentDAO(author_name, author_link, content, addition_date, element=self)

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

    def clone(self, dataset_id=None):
        if dataset_id is None:
            dataset_id = self._id

        return DatasetElementDAO(title=self.title, description=self.description, file_ref_id=self.file_ref_id,
                                 http_ref=self.http_ref, tags=self.tags, addition_date=self.addition_date,
                                 modification_date=self.modification_date, dataset_id=[dataset_id])

    def delete(self, owner_id:ObjectId=None):
        try:
            if owner_id is None:
                owner_id = self.dataset_id[0]

            if self.dataset_id.index(owner_id) > 0:
                self.dataset_id = [d for d in self.dataset_id if d != owner_id]
            else:
                for dataset_id in self.dataset_id[1:]:
                    self.clone(dataset_id)

                DatasetElementCommentDAO.query.remove({'element_id': self._id})
                DatasetElementDAO.query.remove({'_id': self._id})

        except Exception as ex:
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
