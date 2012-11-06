#!/usr/bin/env python

from setuptools import setup

import os
import sys

if sys.argv[-1] == 'publish':
    os.system('python setup.py sdist upload')
    sys.exit()

setup(
    name='Simon',
    version='0.1.0',
    description='Simple MongoDB Models',
    long_description=open('README.rst').read(),
    author='Andy Dirnberger',
    author_email='dirn@dirnonline.com',
    url='https://github.com/dirn/Simon',
    packages=['simon'],
    package_data={'': ['LICENSE', 'README.rst']},
    include_package_data=True,
    install_requires=['pymongo>=2.1'],
    tests_require=['coverage', 'mock', 'nose'],
    license=open('LICENSE').read(),
    classifiers=(
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: Implementation :: CPython'
        'Programming Language :: Python :: Implementation :: PyPy'
        'Topic :: Software Development :: Libraries :: Python Modules',
    ),
)
