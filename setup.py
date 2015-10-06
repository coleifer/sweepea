import os

from distutils.core import setup, Extension
try:
    from Cython.Build import cythonize
except ImportError:
    import warnings
    cython_installed = False
    warnings.warn('Cython not installed, using pre-generated C source file.')
else:
    cython_installed = True


if cython_installed:
    python_source = 'sweepea.pyx'
else:
    python_source = 'sweepea.c'
    cythonize = lambda obj: [obj]

library_source = None

extension = Extension(
    'sweepea',
    sources=[python_source])

setup(
    name='sweepea',
    version='0.1.0',
    description='Fast Python bindings for SQLite3',
    author='Charles Leifer',
    author_email='',
    ext_modules=cythonize(extension),
)
