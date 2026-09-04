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
import subprocess
import os.path
from optparse import OptionParser
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
        print('warning: duplicate person %r, ignoring' % canon, file=sys.stderr)
        return None
    mmap_bycanon[canon] = p
    byaddr = mmap_byaddr.get(addr, [])
    byaddr.append(p)
    mmap_byaddr[addr] = byaddr
    byname = mmap_byname.get(name, [])
    byname.append(p)
    mmap_byname[name] = byname
    return p

if os.path.exists('AUTHORS'):
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
                    print('warning: duplicate alias %r, ignoring' % canon, file=sys.stderr)
                else:
                    mmap_bycanon[canon] = p
                    byname = mmap_byname.get(name, [])
                    byname.append(p)
                    mmap_byname[name] = byname

def show_top_contributors(repo, commit_range, top=10, verbose=False):
    """Display top contributors for the given commit range."""
    commit_a = commit_range[0]
    commit_b = commit_range[1]
    
    stats = Repo(repo).commits(commit_a, commit_b)
    author_counts = {}
    author_total = {}
    
    for commit in stats:
        author = commit.author
        name = author.name
        email = author.email
        key = (name, email)
        if key not in author_counts:
            author_counts[key] = 0
            author_total[key] = 0
        author_counts[key] += 1
        author_total[key] += 1
    
    sorted_authors = sorted(author_counts.items(), key=lambda x: x[1], reverse=True)
    
    for i, ((name, email), count) in enumerate(sorted_authors):
        if verbose:
            print("%d %s <%s>" % (i+1, name, email))
        else:
            print(name)
    
    return sorted_authors

def show_commit_log(repo, commit_range, per_line=False, per_line_width=80):
    """Show a commit log for the given range."""
    commit_a = commit_range[0]
    commit_b = commit_range[1]
    
    for commit in Repo(repo).iter_commits(commit_a, commit_b):
        if per_line:
            print("%s %s %s %s" % (
                commit.hexsha,
                commit.summary,
                commit.author.name,
                commit.committer.when))
        else:
            print(commit.hexsha + ' ' + commit.summary)

def show_commit_log_dates(repo, commit_range):
    """Show commit dates for a given range."""
    commit_a = commit_range[0]
    commit_b = commit_range[1]
    
    for commit in Repo(repo).iter_commits(commit_a, commit_b):
        print("%s - %s" % (commit.hexsha, commit.committer.when))

def show_commit_log_messages(repo, commit_range, lines_per_msg=4):
    """Show commit messages with line wrapping."""
    commit_a = commit_range[0]
    commit_b = commit_range[1]
    
    for commit in Repo(repo).iter_commits(commit_a, commit_b):
        lines = commit.message.split('\n')
        for i, line in enumerate(lines):
            if line.strip():
                width = len(commit.hexsha) + 4
                if i < lines_per_msg or line == lines[-1]:
                    print(" %s %s" % (commit.hexsha, line))

def show_commit_log_stats(repo, commit_range):
    """Show statistics about commits in a range."""
    commit_a = commit_range[0]
    commit_b = commit_range[1]
    
    commits = list(Repo(repo).iter_commits(commit_a, commit_b))
    if not commits:
        return
    
    dates = {}
    authors = {}
    
    for commit in commits:
        author = commit.author
        name = author.name
        if name not in authors:
            authors[name] = 0
        authors[name] += 1
        
        when = commit.committer.when
        if when not in dates:
            dates[when] = 0
        dates[when] += 1
    
    print("Authors:", len(authors))
    print("Commits:", len(commits))
    print("Dates:", len(dates))

def show_commit_log_files(repo, commit_range):
    """Show files touched in commits of a range."""
    commit_a = commit_range[0]
    commit_b = commit_range[1]
    
    files = set()
    for commit in Repo(repo).iter_commits(commit_a, commit_b):
        for file_obj in commit.diff(commit_hexsha=commit.hexsha).files:
            files.add(str(file_obj))
    
    for f in sorted(files):
        print(f)

def show_author_stats(repo, commit_range):
    """Show statistics per author."""
    commit_a = commit_range[0]
    commit_b = commit_range[1]
    
    stats = Repo(repo).commits(commit_a, commit_b)
    
    for author_name, author_email in mmap_byname.keys():
        if author_name in mmap_byname:
            p = mmap_byname[author_name][0]
            name = p.name
            email = p.email
            print("%s %s %s" % (name, email, p.__class__.__name__))

def show_pr_refs(repo):
    """Show available PR refs."""
    refs = Repo(repo).head()
    for ref in refs:
        print("%s %s" % (ref.remote_head, ref.commit))

def main():
    parser = OptionParser()
    parser.add_option("--author", action="store_true", dest="author", default=False,
                      help="Show author-specific statistics")
    parser.add_option("--files", action="store_true", dest="files", default=False,
                      help="Show files touched")
    parser.add_option("--stats", action="store_true", dest="stats", default=False,
                      help="Show commit statistics")
    parser.add_option("--dates", action="store_true", dest="dates", default=False,
                      help="Show commit dates")
    parser.add_option("--messages", action="store_true", dest="messages", default=False,
                      help="Show commit messages")
    parser.add_option("--per-line", action="store_true", dest="per_line", default=False,
                      help="Format commits in a single line")
    parser.add_option("--per-line-width", action="store", dest="line_width", type="int",
                      default=80, help="Width for per-line format")
    parser.add_option("--top", action="store", dest="top_count", type="int", default=10,
                      help="Number of top contributors")
    parser.add_option("--commit-range", action="store", dest="commit_range", nargs=2,
                      default=["HEAD~1", "HEAD"], help="Commit range")
    
    (options, args) = parser.parse_args()
    
    if len(args) >= 1:
        repo_path = args[0]
    else:
        repo_path = "."
    
    if options.author:
        repo = Repo(repo_path)
        top_count = options.top_count
        commit_range = options.commit_range
        
        show_top_contributors(repo, commit_range, top_count)
        return 0
    
    elif options.files:
        repo = Repo(repo_path)
        commit_range = options.commit_range
        show_commit_log_files(repo, commit_range)
        return 0
    
    elif options.stats:
        repo = Repo(repo_path)
        commit_range = options.commit_range
        show_commit_log_stats(repo, commit_range)
        return 0
    
    elif options.dates:
        repo = Repo(repo_path)
        commit_range = options.commit_range
        show_commit_log_dates(repo, commit_range)
        return 0
    
    elif options.messages:
        repo = Repo(repo_path)
        commit_range = options.commit_range
        show_commit_log_messages(repo, commit_range)
        return 0
    
    elif options.per_line:
        repo = Repo(repo_path)
        commit_range = options.commit_range
        show_commit_log(repo, commit_range, options.per_line, options.line_width)
        return 0
    
    else:
        repo = Repo(repo_path)
        commit_range = options.commit_range
        
        for commit in Repo(repo_path).iter_commits(commit_range[0], commit_range[1]):
            print(commit.hexsha + ' ' + commit.summary)
        return 0

if __name__ == "__main__":
    sys.exit(main())