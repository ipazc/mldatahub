===============
mldatahub 0.0.1
===============

`MlDataHub` is a dataset storage hub backend. It is intended to be a standard tool for hosting and managing Machine Learning datasets.

.. image:: https://travis-ci.org/ipazc/mldatahub.svg?branch=master
    :target: https://travis-ci.org/ipazc/mldatahub

.. image:: https://coveralls.io/repos/github/ipazc/mldatahub/badge.svg?branch=master
    :target: https://coveralls.io/github/ipazc/mldatahub?branch=master

This is still a work in progress. It is expected to achieve a Proof Of Concept during December 2017.

=====
NOTES
=====

On mongo, it is required to set some indexes:


.. code:: javascript

    db.element.createIndex({'addition_date': 1})
    db.element.createIndex({'dataset_id': 1})
    db.element.createIndex({'file_ref_id': 1})
    db.element.createIndex({'_previous_id': 1})
    db.file.createIndex({'sha256': 1})
    db.restapi.createIndex({'ip': 1})


=======
LICENSE
=======

It is released under the **GNU GPL v3 or greater** license.
