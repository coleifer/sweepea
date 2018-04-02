import glob
import os
import warnings

from distutils.core import setup, Extension
try:
    from Cython.Build import cythonize
except ImportError:
    cython_installed = False
    warnings.warn('Cython not installed, using pre-generated C source file.')
else:
    cython_installed = True

if cython_installed:
    python_source = 'sweepea.pyx'
else:
    python_source = 'sweepea.c'
    cythonize = lambda obj: obj

extension = Extension(
    'sweepea',
    define_macros=[('MODULE_NAME', '"sweepea"')],
    #extra_compile_args=['-g', '-O0'],
    #extra_link_args=['-g'],
    libraries=['sqlite3'],
    sources=[python_source])

setup(
    name='sweepea',
    version='0.3.3',
    description='',
    url='https://github.com/coleifer/sweepea',
    install_requires=['Cython'],
    author='Charles Leifer',
    author_email='',
    ext_modules=cythonize([extension]),
)
