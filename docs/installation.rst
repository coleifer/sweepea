.. _installation:

Installation
============

Most users will want to simply install the latest version, hosted on PyPI:

.. code-block:: console

    pip install sweepea

You'll need to ensure that `Cython <http://cython.org/>`_ is installed in order
to compile the shared library. Furthermore, your Python will need to be
compiled with support for ``sqlite3`` **or** you'll need to install
``pysqlite``.

Installing with git
-------------------

The project is hosted at https://github.com/coleifer/sweepea and can be
installed using git:

.. code-block:: console

    git clone https://github.com/coleifer/sweepea.git
    cd sweepea
    python setup.py build
    python setup.py install

.. note::
    On some systems you may need to use ``sudo python setup.py install`` to install peewee system-wide.

Running tests
-------------

You can test your installation by running the test suite.

.. code-block:: console

    python setup.py test

    # Or run the test module directly:
    python tests.py
