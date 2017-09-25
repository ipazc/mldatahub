#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from multiprocessing import Lock
from mldatahub.config.config import global_config, now
from ming import schema
from ming.odm import ForeignIdProperty, RelationProperty, MappedClass, FieldProperty


__author__ = 'Iván de Paz Centeno'


taken_url_prefixes = {}
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

    def __init__(self, url_prefix, title, description, reference, tags=None, creation_date=now(), modification_date=now()):
        with lock:
            previous_dset = DatasetDAO.query.get(url_prefix=url_prefix)
            if previous_dset is not None or url_prefix in taken_url_prefixes:
                raise Exception("Url prefix already taken.")
            else:
                taken_url_prefixes[url_prefix] = 1

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

        with lock:
            if url_prefix in taken_url_prefixes:
                del taken_url_prefixes[url_prefix]

    def update(self):
        return DatasetDAO.query.get(_id=self._id)

    def serialize(self):

        fields = ["title", "description", "reference",
                  "addition_date", "modification_date"]

        response = {f: str(self[f]) for f in fields}
        #response['tags'] = [str(tag) for tag in self.tags]
        response['tags'] = self.tags # If this works, append as a field instead.
        response['comments_count'] = len(self.comments)
        response['elements_count'] = len(self.elements)

    def has_element(self, element):
        return DatasetElementDAO.query.get(_id=element._id, dataset_id=self._id) is not None
        #return element._id in [element._id for element in self.elements]


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
        session.refresh(self)
        return DatasetElementDAO.query.get(_id=self._id)

    def serialize(self):
        fields = ["title", "description", "_id",
                  "addition_date", "modification_date"]

        response = {f: str(self[f]) for f in fields}
        #response['tags'] = [str(tag) for tag in self.tags]
        response['tags'] = self.tags # If this works, append as a field instead.
        response['comments_count'] = len(self.comments)

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
        return DatasetCommentDAO.query.get(_id=self._id)

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
        return DatasetElementCommentDAO.query.get(_id=self._id)

    def delete(self):
        DatasetElementCommentDAO.query.remove({'_id': self._id})
        MappedClass.delete(self)

from ming.odm import Mapper
Mapper.compile_all()