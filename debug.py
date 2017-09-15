#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from ming import create_datastore, schema
from ming.odm import ThreadLocalODMSession, MappedClass, FieldProperty, RelationProperty, ForeignIdProperty

__author__ = 'Iván de Paz Centeno'


session = ThreadLocalODMSession(bind=create_datastore("mongodb://localhost:27017/foobar"))


class Foo(MappedClass):
    class __mongometa__:
        session = session
        name = 'foo'

    _id = FieldProperty(schema.ObjectId)
    title = FieldProperty(schema.String)
    _bars= ForeignIdProperty('Bar', uselist=True)
    bars = RelationProperty('Bar')

class Bar(MappedClass):
    class __mongometa__:
        session = session
        name = 'bar'

    _id = FieldProperty(schema.ObjectId)
    title = FieldProperty(schema.String)
    foos = RelationProperty('Foo')


from ming.odm import Mapper
Mapper.compile_all()


foo1 = Foo(title="foo1")
foo2 = Foo(title="foo2")
bar1 = Bar(title="bar1")
bar2 = Bar(title="bar2")

foo1.bars = [bar1, bar2]
foo2.bars = [bar2, bar1]

session.flush()

print(len(foo1.bars))
new_list = list(foo1.bars)
new_list.remove(bar2)
foo1.bars = new_list
session.flush()
session.clear()
foo1 = session.refresh(foo1)
print(len(foo1.bars))

foo1 = Foo.query.get(title="foo1")
foo2 = Foo.query.get(title="foo2")

print(len(foo1.bars))
print(len(foo2.bars))

session.flush()

print(foo1.bars)
print(foo2.bars)

Foo.query.remove()
Bar.query.remove()
