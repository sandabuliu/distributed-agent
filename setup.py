#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import sys

from setuptools import find_packages, setup


POSIX = os.name == "posix"
WINDOWS = os.name == "nt"
LINUX = sys.platform.startswith("linux")
OSX = sys.platform.startswith("darwin")
FREEBSD = sys.platform.startswith("freebsd")
OPENBSD = sys.platform.startswith("openbsd")
NETBSD = sys.platform.startswith("netbsd")
BSD = FREEBSD or OPENBSD or NETBSD
SUNOS = sys.platform.startswith("sunos") or sys.platform.startswith("solaris")


def text_of(relpath):
    thisdir = os.path.dirname(__file__)
    file_path = os.path.join(thisdir, os.path.normpath(relpath))
    with open(file_path) as f:
        text = f.read()
    return text

version = re.search(
    "__version__ = '([^']+)'", text_of('agent_distributed/__init__.py')
).group(1)


NAME = 'distributed-agent'
VERSION = version
DESCRIPTION = 'The framework to extend `python-agent` to a distributed architecture'
KEYWORDS = 'File CSV Log Structure python-agent distributed'
AUTHOR = 'tong'
AUTHOR_EMAIL = 'g_tongbin@foxmail.com'
URL = 'http://t.navan.cc'
LICENSE = ''
PACKAGES = find_packages(exclude=['tests', 'tests.*'])
PACKAGE_DATA = {}

INSTALL_REQUIRES = ['distributed']

CLASSIFIERS = [
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    'Programming Language :: Python :: 2',
    'Programming Language :: Python :: 2.6',
    'Programming Language :: Python :: 2.7'
    'Topic :: structure :: Agent :: Log :: File :: Csv',
    'Topic :: Software Development :: Libraries'
]

LONG_DESCRIPTION = text_of('README.md')


params = {
    'name':             NAME,
    'version':          VERSION,
    'description':      DESCRIPTION,
    'keywords':         KEYWORDS,
    'long_description': LONG_DESCRIPTION,
    'author':           AUTHOR,
    'author_email':     AUTHOR_EMAIL,
    'url':              URL,
    'license':          LICENSE,
    'packages':         PACKAGES,
    'package_data':     PACKAGE_DATA,
    'install_requires': INSTALL_REQUIRES,
    'classifiers':      CLASSIFIERS,
}

setup(**params)
