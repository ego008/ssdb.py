"""
ssdb.py
-------

Ssdb Python Client Library.

https://github.com/hit9/ssdb.py
"""

from setuptools import setup


setup(
    name='ssdb.py',
    version='0.1.9',
    author='hit9',
    author_email='nz2324@126.com',
    description="Ssdb Python Client Library",
    long_description=__doc__,
    license='bsd2',
    keywords=['ssdb', 'client', 'library'],
    url='https://github.com/ego008/ssdb.py',
    py_modules=['ssdb'],
    install_requires=['spp>=0.0.5'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
