.. swee'pea documentation master file, created by
   sphinx-quickstart on Mon Oct 12 21:42:14 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

swee'pea
========

Fast, lightweight Python database toolkit for SQLite, built with Cython.

Like it's cousin `peewee <http://docs.peewee-orm.com/>`_, ``swee'pea`` is
comprised of a database connection abstraction and query-building / execution
APIs. This project is a pet project of mine, so tailor expectations
accordingly.

Features:

* Implemented in Cython for performance and to expose advanced features of the
  SQLite database library.
* Composable and consistent APIs for building queries using Python.
* Layered APIs allow you to work as close to the database as you want.
* No magic.
* No bullshit.

Issue tracker and code are hosted on GitHub: https://github.com/coleifer/sweepea.

Documentation hosted on RT**F**D: https://sweepea.readthedocs.io/

.. image:: http://media.charlesleifer.com/blog/photos/sweepea-fast.png

Contents:

.. toctree::
   :maxdepth: 2
   :glob:

   installation
   api


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

