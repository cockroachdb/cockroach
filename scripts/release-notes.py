#! /usr/bin/env python3

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
import os.path

from optparse import OptionParser
from gitdb import exc
from git import Repo
from git.repo.fun import name_to_object
from git.util import Stats

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
        print('warning: duplicate person %r, ignoring', canon, file=sys.stderr)
        return None
    mmap_bycanon[canon] = p
    byaddr = mmap_byaddr.get(addr, [])
    byaddr.append(p)
    mmap_byaddr[addr] = byaddr
    byname = mmap_byname.get(name, [])
    byname.append(p)
    mmap_byname[name] = byname
    return p

def add_alias(alias, addr, person):
    if alias.strip():
        name = alias.strip()
    else:
        name = person.name
    canon = (name, addr)
    byaddr = mmap_byaddr.get(addr, [])
    if person not in byaddr:
        byaddr.append(person)
        mmap_byaddr[addr] = byaddr
    byname = mmap_byname.get(name, [])
    if person not in byname:
        byname.append(person)
        mmap_byname[name] = byname

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
            if aliases:
                found_aliases = mmare.findall(aliases)
                for alias, addr in found_aliases:
                    add_alias(alias, addr, p)

def process_commits(repo, commit_a, commit_b):
    def get_commit_info(repo, sha):
        if len(sha) < shamin:
            sha = repo.commit(sha).hexsha[:shamin]
        obj = repo.commit(sha)
        return obj.author, obj.summary or obj.message[:40]

    def format_author(author):
        if author in mmap_byname:
            return "%s <%s>" % (author.name, author.email)
        return author

    def format_commits(commits):
        def _format(c):
            line = "%s %s\n" % (c.summary or c.message[:40], c.hexsha)
            if c.author in mmap_byname:
                line = "%s %s" % (format_author(c.author).rjust(30), line)
            return line.strip()

        for c in commits:
            yield _format(c)

    def walk_commits(start, end, direction='forward'):
        commits = repo.iter_commits(
            '%s..%s' % (start, end),
            reverse=(direction == 'backward')
        )
        yield from commits

    def get_commits(repo, a, b):
        a_obj = repo.commit(a)
        b_obj = repo.commit(b)
        # Determine if we need to iterate forward or backward
        if a_obj.hexsha < b_obj.hexsha:
            yield from repo.iter_commits('%s..%s' % (a, b))
        else:
            yield from repo.iter_commits('%s..%s' % (b, a))

    # Handle the case where one is HEAD~1 and the other is HEAD
    if commit_a == 'HEAD~1' and commit_b == 'HEAD':
        yield from repo.iter_commits('HEAD~1..HEAD')
    elif commit_b == 'HEAD~1' and commit_a == 'HEAD':
        yield from repo.iter_commits('HEAD~1..HEAD', reverse=True)
    else:
        start = repo.commit(commit_a)
        end = repo.commit(commit_b)
        if start.hexsha < end.hexsha:
            yield from repo.iter_commits('%s..%s' % (start.hexsha, end.hexsha))
        else:
            yield from repo.iter_commits('%s..%s' % (end.hexsha, start.hexsha), reverse=True)

def main():
    parser = OptionParser(usage='%(prog)s [options] <from> <to>')
    parser.add_option('--since', dest='since', default=None,
                      help='Use since date rather than from commit')
    parser.add_option('--from', dest='from_comm', default='HEAD~1')
    parser.add_option('--to', dest='to_comm', default='HEAD')
    parser.add_option('--author', dest='author', default='all',
                      help='Filter by author email')
    parser.add_option('--limit', dest='limit', default=50, type='int')
    parser.add_option('--all', dest='all_commits', action='store_true', default=True)

    options, args = parser.parse_args()

    # Validate arguments
    if len(args) == 3:
        from_commit, to_commit, to_option = args
        if to_option:
            # Handle --to with name option
            options.from_comm = from_commit
            options.to_comm = to_commit
            options.all_commits = True
    elif len(args) == 2:
        options.from_comm = args[0]
        options.to_comm = args[1]
        options.all_commits = True
    elif len(args) == 1:
        options.to_comm = args[0]
        options.from_comm = 'HEAD'
        options.all_commits = True
    elif len(args) == 0:
        options.from_comm = options.since
        options.to_comm = 'HEAD'
        options.all_commits = True

    repo = Repo('.')

    def format_author(name):
        if name in mmap_byname:
            return "%s <%s>" % (name.name, name.email)
        return name

    if options.author != 'all' and options.author:
        repo_commit = repo.commit(options.to_comm)
        commits = [c for c in repo.iter_commits('%s..%s' % (options.from_comm, options.to_comm))
                   if format_author(c.author) == options.author]
    else:
        commits = list(repo.iter_commits('%s..%s' % (options.from_comm, options.to_comm)))

    if options.limit:
        commits = commits[:options.limit]

    if not commits:
        print("No commits in range %s..%s" % (options.from_comm, options.to_comm), file=sys.stderr)
        sys.exit(1)

    def print_commit_info(commit):
        print("%s %s" % (format_author(commit.author) if commit.author else "", commit.hexsha[:8]))

    def print_commits():
        for c in commits:
            if c.author in mmap_byname:
                print("%s %s" % (format_author(c.author).rjust(30), c.hexsha[:8]))
            else:
                print("%s %s" % (''.ljust(30), c.hexsha[:8]))

    print_commits()

    # Print commit stats
    print("\n=== Commit Stats ===")
    stats = repo.revparse_single('%s..%s' % (options.from_comm, options.to_comm))
    if hasattr(stats, 'summary') and stats.summary:
        print(stats.summary)
    else:
        print("Stats: %s commits" % len(commits))

if __name__ == '__main__':
    main()