#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from multiprocessing import Lock
from mldatahub.config.config import global_config, now

__author__ = 'Iván de Paz Centeno'

from ming import schema
from ming.odm import ForeignIdProperty, RelationProperty, MappedClass, FieldProperty

taken_url_prefixes = {}
lock = Lock()
session = global_config.get_session()


class Dataset(MappedClass):

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
    elements = RelationProperty('DatasetElement')
    comments = RelationProperty('DatasetComment')
    tokens = RelationProperty('Token')

    def __init__(self, url_prefix, title, description, reference, creation_date=now(), modification_date=now()):
        with lock:
            previous_dset = Dataset.query.get(url_prefix=url_prefix)
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
        return DatasetComment(author_name, author_link, content, addition_date, dataset=self)

    def add_element(self, title, description, file_ref_id, tags=None, addition_date=now(), modification_date=now()):
        return DatasetElement(title, description, file_ref_id, tags, addition_date, modification_date, dataset=self)

    def delete(self):
        url_prefix = self.url_prefix
        for dataset_element in self.elements:
            dataset_element.delete()

        DatasetComment.query.remove({'dataset_id': self._id})
        DatasetElement.query.remove({'dataset_id': self._id})

        Dataset.query.remove({'_id': self._id})

        with lock:
            if url_prefix in taken_url_prefixes:
                del taken_url_prefixes[url_prefix]


class DatasetElement(MappedClass):

    class __mongometa__:
        session = session
        name = 'element'

    _id = FieldProperty(schema.ObjectId)
    title = FieldProperty(schema.String)
    description = FieldProperty(schema.String)
    file_ref_id = FieldProperty(schema.Int)
    tags = FieldProperty(schema.Array(schema.String))
    addition_date = FieldProperty(schema.datetime)
    modification_date = FieldProperty(schema.datetime)
    comments = RelationProperty('DatasetElementComment')
    dataset_id = ForeignIdProperty('Dataset')
    dataset = RelationProperty('Dataset')

    def __init__(self, title, description, file_ref_id, tags=None, addition_date=now(), modification_date=now(), dataset_id=None, dataset=None):
        if dataset_id is None and dataset is not None:
            dataset_id = dataset._id

        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__", "datasets"]}
        super().__init__(**kwargs)

    @classmethod
    def from_dict(cls, init_dict):
        return cls(**init_dict)

    def add_comment(self, author_name, author_link, content, addition_date=now()):
        return DatasetElementComment(author_name, author_link, content, addition_date, element=self)

    def delete(self):
        DatasetElementComment.query.remove({'element_id': self._id})
        DatasetElement.query.remove({'_id': self._id})
        super().delete()

class DatasetComment(MappedClass):

    class __mongometa__:
        session = session
        name = 'dataset_comment'

    _id = FieldProperty(schema.ObjectId)
    author_name = FieldProperty(schema.String)
    author_link = FieldProperty(schema.String)
    content = FieldProperty(schema.String)
    addition_date = FieldProperty(schema.datetime)
    dataset_id = ForeignIdProperty('Dataset')
    dataset = RelationProperty('Dataset')

    def __init__(self, author_name, author_link, content, addition_date=now(), dataset_id=None, dataset=None):
        if dataset_id is None and dataset is not None:
            dataset_id = dataset._id

        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__", "datasets"]}
        super().__init__(**kwargs)

    @classmethod
    def from_dict(cls, init_dict):
        return cls(**init_dict)

    def delete(self):
        DatasetComment.query.remove({'_id': self._id})



class DatasetElementComment(MappedClass):

    class __mongometa__:
        session = session
        name = 'data_element_comment'

    _id = FieldProperty(schema.ObjectId)
    author_name = FieldProperty(schema.String)
    author_link = FieldProperty(schema.String)
    content = FieldProperty(schema.String)
    addition_date = FieldProperty(schema.datetime)
    element_id = ForeignIdProperty('DatasetElement')
    element = RelationProperty('DatasetElement')

    def __init__(self, author_name, author_link, content, addition_date=now(), element_id=None, element=None):
        if element_id is None and element is not None:
            element_id = element._id

        kwargs = {k: v for k, v in locals().items() if k not in ["self", "__class__", "element"]}
        super().__init__(**kwargs)

    @classmethod
    def from_dict(cls, init_dict):
        return cls(**init_dict)

    def delete(self):
        DatasetElementComment.query.remove({'_id': self._id})
        MappedClass.delete(self)

from ming.odm import Mapper
Mapper.compile_all()