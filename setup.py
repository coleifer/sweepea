import glob
import os

from distutils.core import setup, Extension
try:
    from Cython.Build import cythonize
except ImportError:
    import warnings
    raise RuntimeError('Cython must be installed to build sweepea.')

python_source = 'sweepea.pyx'
extension = Extension(
    'sweepea',
    define_macros=[('MODULE_NAME', '"sweepea"')],
    libraries=['sqlite3'],
    sources=[python_source])

setup(
    name='sweepea',
    version='0.2.1',
    description='',
    url='https://github.com/coleifer/sweepea',
    install_requires=['Cython'],
    author='Charles Leifer',
    author_email='',
    ext_modules=cythonize(extension),
)
