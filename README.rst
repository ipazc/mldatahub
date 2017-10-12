===============
mldatahub 0.0.1
===============

`MlDataHub` is a dataset storage hub backend. It is intended to be a standard tool for hosting and managing Machine Learning datasets.

.. image:: https://travis-ci.org/ipazc/mldatahub.svg?branch=master
    :target: https://travis-ci.org/ipazc/mldatahub

This is still a work in progress. It is expected to achieve a Proof Of Concept during October 2017.

=====
NOTES
=====

On mongo, it is required to set indexes on `element.addition_date` and on `file.sha256`:


.. code:: javascript

    db.element.create_index({'addition_date': 1})
    db.file.create_index({'sha256': 1})


=======
LICENSE
=======

It is released under the **GNU GPL v3 or greater** license.
