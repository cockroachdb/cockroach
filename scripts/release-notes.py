#! /usr/bin/env python3
#
# Show a compact ChangeLog representation of a range of Git commits.
#
# Example use: changelog.py -f v1.1-alpha.20170817
#
# Requires: GitPython https://pypi.python.org/pypi/GitPython/

import sys
import itertools
import re
import os
import datetime
from git import Repo
from optparse import OptionParser

### Global behavior constants ###

# minimum sha length to disambiguate
shamin = 9

# FIXME(knz): This probably needs to use the .mailmap.
author_aliases = {
    'kena': "Raphael 'kena' Poss",
    'vivekmenezes': "Vivek Menezes",
    'RaduBerinde': "Radu Berinde",
    'marc': "Marc Berhault",
    'a6802739': "Song Hao",
    'Abhemailk abhi.madan01@gmail.com': "Abhishek Madan",
    'rytaft': "Rebecca Taft",
    'songhao': "Song Hao",
    'solongordon': "Solon Gordon",
}

# Section titles for release notes.
relnotetitles = {
    'cli change': "Command-Line Changes",
    'sql change': "SQL Language Changes",
    'admin ui change': "Admin UI Changes",
    'general change': "General Changes",
    'core change': "Core Changes",
    'build change': "Build Changes",
    'enterprise change': "Enterprise Changes",
    'backward-incompatible change': "Backward-Incompatible Changes",
    'performance improvement': "Performance Improvements",
    'bug fix': "Bug Fixes",
}

# Order in which to show the sections.
relnote_sec_order = [
    'backward-incompatible change',
    'general change',
    'enterprise change',
    'sql change',
    'cli change',
    'admin ui change',
    'core change',
    'bug fix',
    'performance improvement',
    'build change',
    ]

# Release note category common misspellings.
cat_misspells = {
    'sql' : 'sql change',
    'general': 'general change',
    'bugfix': 'bug fix',
    'performance change' : 'performance improvement',
    'ui' : 'admin ui change',
    'backwards-incompatible change': 'backward-incompatible change',
    }

## Release note format ##

# The following release note formats have been seen in the wild:
#
# Release note (xxx): yyy    <- canonical
# Release Notes: None
# Release note (xxx): yyy
# Release note (xxx) : yyy
# Release note: (xxx): yyy
# Release note: xxx: yyy
# Release note: (xxx) yyy
# Release note: yyy (no category)
# Release note (xxx, zzz): yyy
norelnote = re.compile(r'^[rR]elease [nN]otes?: *[Nn]one', flags=re.M)
# Captures :? (xxx) ?: yyy
form1 = r':? *\((?P<cat1>[^)]*)\) *:?'
# Captures : xxx: yyy - this must be careful not to capture too much, we just accept one word
form2 = r': *(?P<cat2>[^ ]*) *:'
# Captures : yyy - no category
form3 = r':(?P<cat3>)'
relnote = re.compile(r'(?:^|[\n\r])[rR]elease [nN]otes? *(?:' + form1 + '|' + form2 + '|' + form3 + r') *(?P<note>.*)$', flags=re.S)

### Initialization / option parsing ###

parser = OptionParser()
parser.add_option("-k", "--sort-key", dest="sort_key", default="title",
                  help="sort by KEY (pr, title, insertions, deletions, files, sha; default: title)", metavar="KEY")
parser.add_option("-r", "--reverse", action="store_true", dest="reverse_sort", default=False,
                  help="reverse sort")
parser.add_option("-f", "--from", dest="from_commit",
                  help="list history from COMMIT", metavar="COMMIT")
parser.add_option("-t", "--until", dest="until_commit", default="HEAD",
                  help="list history up and until COMMIT (default: HEAD)", metavar="COMMIT")

(options, args) = parser.parse_args()

sortkey = options.sort_key
revsort = options.reverse_sort

repo = Repo('.')
heads = repo.heads

firstCommit = repo.commit(options.from_commit)
commit = repo.commit(options.until_commit)

if commit == firstCommit:
    print("Commit range is empty!", file=sys.stderr)
    print(parser.get_usage(), file=sys.stderr)
    print("Example use:", file=sys.stderr)
    print("  %s --help" % sys.argv[0], file=sys.stderr)
    print("  %s --from xxx >output.md" % sys.argv[0], file=sys.stderr)
    print("  %s --from xxx --until yyy >output.md" % sys.argv[0], file=sys.stderr)
    exit(0)

### Reading data from repository ###

# Is the first commit reachable from the current one?
check = commit
while check != firstCommit:
    if len(check.parents) == 0:
        print("error: origin commit %s not in history of %s" %( options.from_commit, options.until_commit))
        exit(1)
    check = check.parents[0]

print("Changes %s ... %s" % (firstCommit, commit), file=sys.stderr)

release_notes = {}
missing_release_notes = []

def extract_release_notes(pr, title, commit):
    if norelnote.search(commit.message) is not None:
        # Explicitly no release note. Nothing to do.
        return

    item = {'author': (commit.author.name, commit.author.email),
            'sha': commit.hexsha[:shamin],
            'pr': pr,
            'prtitle': title,
            'note': None}

    m = relnote.search(commit.message)
    if m is None:
        # Missing release note. Keep track for later.
        missing_release_notes.append(item)
        return
    item['note'] = m.group('note').strip()
    if item['note'].lower() == 'none':
        # Someone entered 'Release note (cat): None'.
        return

    cat = m.group('cat1')
    if cat is None:
        cat = m.group('cat2')
    if cat is None:
        cat = 'missing category'
    # Normalize to tolerate various capitalizations.
    cat = cat.lower()
    # If there are multiple categories separated by commas or slashes, use the first as grouping key.
    cat = cat.split(',', 1)[0]
    cat = cat.split('/', 1)[0]
    # If there is any misspell, correct it.
    if cat in cat_misspells:
        cat = cat_misspells[cat]
    # Now collect per category.
    catnotes = release_notes.get(cat, [])
    catnotes.append(item)
    release_notes[cat] = catnotes

# This function groups and counts all the commits that belong to a particular PR.
def collect_commits(pr, title, commits, author):
    ncommits = 0
    otherauthors = set()
    for commit in commits:
        if commit.message.startswith("Merge pull request"):
            continue
        extract_release_notes(pr, title, commit)

        ncommits += 1
        if commit.author.name != 'GitHub' and author_aliases.get(commit.author.name, commit.author.name) != author:
            otherauthors.add(commit.author.name)
        if commit.committer.name != 'GitHub' and author_aliases.get(commit.committer.name, commit.committer.name) != author:
            otherauthors.add(commit.committer.name)

        n, a = collect_commits(pr, title, list(commit.parents), author)
        ncommits += n
        otherauthors.update(a)
    return ncommits, otherauthors

per_author_history = {}

spinner = itertools.cycle(['/', '-', '\\', '|'])
counter = 0
while commit != firstCommit:
    # Display a progress bar
    counter += 1
    if counter % 10 == 0:
        if counter % 100 == 0:
            print("\b..", end='', file=sys.stderr)
        print("\b", end='', file=sys.stderr)
        print(next(spinner),  end='', file=sys.stderr)
        sys.stderr.flush()

    # Analyze the commit
    if commit.message.startswith("Merge pull request"):
        author = (commit.author.name, commit.author.email)
        lines = commit.message.split('\n', 3)
        pr = lines[0].split(' ', 4)[3]
        title = lines[2]
        ncommits, otherauthors = collect_commits(pr, title, list(commit.parents), author_aliases.get(author[0], author[0]))

        stats = commit.stats.total
        item = {
            'title': title,
            'pr': pr,
            'sha': commit.hexsha[:shamin],
            'ncommits': ncommits,
            'otherauthors': otherauthors,
            'insertions': stats['insertions'],
            'deletions': stats['deletions'],
            'files': stats['files'],
            'lines': stats['lines'],
            }
        history = per_author_history.get(author, [])
        history.append(item)
        per_author_history[author] = history

    if len(commit.parents) == 0:
        break
    commit = commit.parents[0]

print("\b\n", file=sys.stderr)
sys.stderr.flush()
allauthors = list(per_author_history.keys())
allauthors.sort(key=lambda x:x[0].lower())

### Presentation of results ###

## Print the release notes.

# Start with known sections.

print("---")
print("title: What&#39;s New in ", end='')
sys.stdout.flush()
os.system('git describe --tags ' + options.until_commit)
print("toc: false")
print("summary: Additions and changes in CockroachDB version ", end='')
sys.stdout.flush()
os.system('git describe --tags ' + options.until_commit)
print("   since version ", end='')
sys.stdout.flush()
os.system('git describe --tags ' + options.from_commit)
print("---")
print()
today = datetime.datetime.today()
print("## %s %d, %d" % (today.strftime("%B"), today.day, today.year))
print()

seenshas = set()
seenprs = set()
for sec in relnote_sec_order:
    r = release_notes.get(sec, None)
    if r is None:
        # No change in this section, nothing to print.
        continue
    sectitle = relnotetitles[sec]
    print("###", sectitle)
    print()

    for item in r:
        print("-", item['note'].replace('\n', '\n  '), '[%s] [%s]' % (item['pr'], item['sha']))
        seenshas.add(item['sha'])
        seenprs.add(item['pr'])

    print()

extrasec = set()
for sec in release_notes:
    if sec in relnote_sec_order:
        # already handled above, don't do anything.
        continue
    extrasec.add(sec)
if len(extrasec) > 0 or len(missing_release_notes) > 0:
    print("### Miscellaneous")
    print()
if len(extrasec) > 0:
    extrasec_sorted = sorted(list(extrasec))
    for extrasec in extrasec_sorted:
        print("#### %s" % extrasec.title())
        print()
        for item in release_notes[extrasec]:
            print("-", item['note'].replace('\n', '\n  '), '[%s] [%s]' % (item['pr'], item['sha']))
            seenshas.add(item['sha'])
            seenprs.add(item['pr'])
        print()

if len(missing_release_notes) > 0:
    print("#### Changes without release note annotation")
    print()
    for item in missing_release_notes:
        author = item['author'][0]
        author = author_aliases.get(author, author)
        print("- [%s] [%s] %s (%s)" % (item['pr'], item['sha'], item['prtitle'], author))
        seenshas.add(item['sha'])
        seenprs.add(item['pr'])
    print()

## Print the per-author contribution list.
print()
print("### Per-author contributions")
print()
for author in allauthors:
    items = per_author_history[author]
    print("- %s:" % author_aliases.get(author[0], author[0]))
    items.sort(key=lambda x:x[sortkey],reverse=not revsort)
    for item in items:
        seenshas.add(item['sha'])
        seenprs.add(item['pr'])
        print("  - [%(pr)-6s] [%(sha)s] (+%(insertions)4d -%(deletions)4d ~%(lines)4d/%(files)2d) %(title)s" % item, end='')
        ncommits, otherauthors = item['ncommits'], item['otherauthors']
        if ncommits > 1 or len(otherauthors) > 0:
            print(" (", end='')
            if ncommits > 1:
                print("%d commits" % ncommits, end='')
            if len(otherauthors)> 0:
                if ncommits > 1:
                    print(" ", end='')
                print("w/", ', '.join(otherauthors), end='')
            print(")", end='')
        print()
    print()
print()

# Link the PRs and SHAs
for pr in sorted(list(seenprs)):
    print("[%s]: https://github.com/cockroachdb/cockroach/pull/%s" %(pr, pr[1:]))
for sha in seenshas:
    print("[%s]: https://github.com/cockroachdb/cockroach/commit/%s" %( sha, sha))
print()
