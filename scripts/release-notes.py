#!/usr/bin/env python3

# Copyright 2018 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

#
# Show a compact release note summary of a range of Git commits.
#
# Example use: release-notes.py --help
#
# Note: the first commit in the range is excluded!
#
# Requires:
#   - GitPython https://pypi.python.org/pypi/GitPython/
#   - You need to configure your local repo to pull the PR refs from
#     GitHub.  To do this, add a line like:
#       fetch = +refs/pull/*/head:refs/pull/origin/*
#     to the GitHub remote section of .git/config.
#
# Disclaimer: this program is provided without warranties of any kind,
# including suitability for any purpose. The author(s) will not be
# responsible if this script eats your left sock.
#
# Known limitations:
#
# - if different people with the same name contribute, this script
#   will be confused. (it will merge their work under one entry).
# - the list of aliases below must be manually modified when
#   contributors change their git name and/or email address.
#
# Note: there are unit tests in the release-notes subdirectory!
#
# pylint: disable=line-too-long, invalid-name, missing-function-docstring, too-many-branches, redefined-outer-name

import sys
import itertools
import re
import datetime
import time
from gitdb import exc
import subprocess
import os.path

from optparse import OptionParser
from git import Repo
from git.repo.fun import name_to_object
from git.util import Stats
import os.path

#
# Global behavior constants
#

# minimum sha length to disambiguate
shamin = 9

# Basic mailmap functionality using the AUTHORS file.
mmre = re.compile(r'^(?P<name>.*?)\s+<(?P<addr>[^>]*)>(?P<aliases>(?:[^<]*<[^>]*>)*)$')
mmare = re.compile('(?P<alias>[^<]*)<(?P<addr>[^>]*)>')
crdb_folk = set()

class P:
    def __init__(self, name, addr):
        self.name = name
        self.email = addr
        self.aliases = [(name, addr)]
        self.crdb = '@cockroachlabs.com' in addr
        if self.crdb:
            crdb_folk.add(self)
    def __repr__(self):
        return "%s <%s>" % (self.name, self.email)
    def __lt__(self, other):
        return self.name < other.name or (self.name == other.name and self.email < other.email)

mmap_bycanon = {}
mmap_byaddr = {}
mmap_byname = {}

def define_person(name, addr):
    p = P(name, addr)
    canon = (name, addr)
    if canon in mmap_bycanon:
        print('warning: duplicate person %r, ignoring', canon)
        return None
    mmap_bycanon[canon] = p
    byaddr = mmap_byaddr.get(addr, [])
    byaddr.append(p)
    mmap_byaddr[addr] = byaddr
    byname = mmap_byname.get(name, [])
    byname.append(p)
    mmap_byname[name] = byname
    return p

if not os.path.exists('AUTHORS'):
    print('warning: AUTHORS missing in current directory.', file=sys.stderr)
    print('Maybe use "cd" to navigate to the working tree root.', file=sys.stderr)
else:
    with open('AUTHORS', 'r') as f:
        for line in f.readlines():
            if line.strip().startswith('#'):
                continue
            m = mmre.match(line)
            if m is None:
                continue
            p = define_person(m.group('name'), m.group('addr'))
            if p is None:
                continue
            p.crdb = '@cockroachlabs.com' in line
            if p.crdb:
                crdb_folk.add(p)
            aliases = m.group('aliases')
            aliases = mmare.findall(aliases)
            for alias, addr in aliases:
                name = alias.strip()
                byaddr = mmap_byaddr.get(addr, [])
                if p not in byaddr:
                    byaddr.append(p)
                mmap_byaddr[addr] = byaddr
                if name == '':
                    name = p.name
                canon = (name, addr)
                if canon in mmap_bycanon:
                    print('warning: duplicate alias %r, ignoring', canon)