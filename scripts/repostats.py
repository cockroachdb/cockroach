#! /usr/bin/env python3
#
# This script aggregates code change intensity
# per constributor across the entire repository.
#
# This requires the directory server from
# github.com/cockroachlabs/roachers to be running on localhost.
#
# Use with: python3 scripts/repostats.py >stats.py
#

import sys
from git import Repo
from git.repo.fun import name_to_object
from git.util import Stats
import urllib.request
import json
import re
import pprint
import datetime
import time

repo = Repo('.')

firstCommit = '8548987813ff9e1b8a9878023d3abfc6911c16db'
mailcache = {}

def get_user(email):
    if email in mailcache:
        return mailcache[email]
    with urllib.request.urlopen('http://localhost:5000/v1/find/' + email) as response:
        data = json.loads(response.read())
        if len(data) == 0:
            print("\nWARN: cant find email:", email, file=sys.stderr)
            f = "?" + email
            mailcache[email] = f
            return f
        if data[0][1] != 100:
            print("\nWARN: cant find email, partial match:", email, file=sys.stderr)
            f = "?" + email
            mailcache[email] = f
            return f
        mailcache[email] = data[0][0]
        return data[0][0]

#currentyear = int(datetime.date.fromtimestamp(time.time()).isoformat()[:4])
#years = ['%d' % x for x in range(2014, currentyear+1)]
def file_stats(dstat, year, uname, stats):
    ustat = dstat.get(uname, {})
    dstat[uname] = ustat
    ystat = ustat.get(year, 0)
    ystat += stats['lines'] # max(stats['insertions'], stats['deletions'])
    ustat[year] = ystat

ren = re.compile(r'\{[^=]* => ([^}]*)\}')
ren2 = re.compile(r'^[^{]* => ([^{]*)$')

def parents(pfile):
    pfile = ren.sub(r'\1', pfile)
    pfile = ren2.sub(r'\1', pfile)
    pfile = pfile.replace('//', '/')
    if pfile.endswith('/'):
        pfile = pfile[:-1]
    res = []
    parts = pfile.split('/')
    res.append('/'.join(parts[:-1]))
    for i in range(len(parts)-1, 0, -1):
        res.append('/'.join(parts[:i])+'*')
    return res

stats = {}
prevm = ''
count = 0
for x in repo.iter_commits('8548987813ff9e1b8a9878023d3abfc6911c16db...HEAD'):
    if x.author.email in ['bors@cockroachlabs.com', 'craig[bot]@users.noreply.github.com']:
        continue
    ts = datetime.date.fromtimestamp(x.committed_date).isoformat()
    year = ts[:4]
    author_user = get_user(x.author.email)
    if x.committer.email in ['noreply@github.com', 'bors@cockroachlabs.com']:
        commit_user = author_user
    else:
        commit_user = get_user(x.committer.email)
    pts = ts[:7]
    if pts != prevm:
        print('\n' + pts + ' ', file=sys.stderr, end='')
        prevm = pts
    if count % 10 == 0:
        print('.', file=sys.stderr, end='')
        sys.stderr.flush()
    count += 1

    #if count > 1000:
    #    break
    for pfile, fstats in x.stats.files.items():
        for prefix in parents(pfile):
            d = stats.get(prefix, {})
            stats[prefix] = d
            file_stats(d, year, '__all__', fstats)
            file_stats(d, year, author_user, fstats)
            if commit_user != author_user:
                file_stats(d, year, commit_user, fstats)
print(file=sys.stderr)
    # break

pprint.pprint(stats)
