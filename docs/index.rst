.. swee'pea documentation master file, created by
   sphinx-quickstart on Mon Oct 12 21:42:14 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

swee'pea
========

Fast, lightweight Python database toolkit for SQLite, built with Cython.

Features:

* Written in `Cython <http://cython.org/>`_, roughly ~4x faster than Peewee for writes, between 2x and 10x for reads.
* Layered APIs allow you to work as close to the database as you want.
* `Peewee <http://docs.peewee-orm.com/en/latest/>`_-like query building syntax.
* Lightweight ORM layer.

.. image:: http://media.charlesleifer.com/blog/photos/sweepea-fast.png

swee'pea is fast
----------------

swee'pea is written in Cython for speed, and utilizes the lightweight and excellent `APSW <https://rogerbinns.github.io/apsw/>`_ Python SQLite bindings.


.. image:: http://media.charlesleifer.com/blog/photos/sweepea-kicks-ass.png

swee'pea packs a punch
----------------------

swee'pea provides a layered API, allowing you to work as close to the database as you want.


Contents:

.. toctree::
   :maxdepth: 2



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

