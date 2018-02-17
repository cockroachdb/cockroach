#! /usr/bin/env python3
#
# Show a compact release note summary of a range of Git commits.
#
# Example use: release-notes.py --help
#
# Note: the first commit in the range is excluded!
#
# Requires: GitPython https://pypi.python.org/pypi/GitPython/
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

import sys
import itertools
import re
import os
import datetime, time
import subprocess
from git import Repo
from optparse import OptionParser

### Global behavior constants ###

# minimum sha length to disambiguate
shamin = 9

# FIXME(knz): This probably needs to use the .mailmap.
author_aliases = {
    'dianasaur323': "Diana Hsieh",
    'kena': "Raphael 'kena' Poss",
    'vivekmenezes': "Vivek Menezes",
    'RaduBerinde': "Radu Berinde",
    'Andy Kimball': "Andrew Kimball",
    'marc': "Marc Berhault",
    'Lauren': "Lauren Hirata",
    'lhirata' : "Lauren Hirata",
    'MBerhault': "Marc Berhault",
    'Nate': "Nathaniel Stewart",
    'a6802739': "Song Hao",
    'Abhemailk abhi.madan01@gmail.com': "Abhishek Madan",
    'rytaft': "Rebecca Taft",
    'songhao': "Song Hao",
    'solongordon': "Solon Gordon",
}

# FIXME(knz): This too.
crdb_folk = set([
    "Abhishek Madan",
    "Alex Robinson",
    "Alfonso Subiotto Marqués",
    "Amruta Ranade",
    "Andrei Matei",
    "Andrew Couch",
    "Andrew Kimball",
    "Andy Woods",
    "Arjun Narayan",
    "Ben Darnell",
    "Bram Gruneir",
    "Daniel Harrison",
    "David Taylor",
    "Diana Hsieh",
    "Jesse Seldess",
    "Jessica Edwards",
    "Joey Pereira",
    "Jordan Lewis",
    "Justin Jaffray",
    "Kuan Luo",
    "Marc Berhault",
    "Masha Schneider",
    "Matt Jibson",
    "Matt Tracy",
    "Nathan VanBenschoten",
    "Nathaniel Stewart",
    "Nikhil Benesch",
    "Paul Bardea",
    "Pete Vilter",
    "Peter Mattis",
    "Radu Berinde",
    "Raphael 'kena' Poss",
    "Rebecca Taft",
    "Richard Wu",
    "Sean Loiselle",
    "Solon Gordon",
    "Spencer Kimball",
    "Tamir Duberstein",
    "Tobias Schottdorf",
    "Vivek Menezes",
])


# Section titles for release notes.
relnotetitles = {
    'cli change': "Command-Line Changes",
    'sql change': "SQL Language Changes",
    'admin ui change': "Admin UI Changes",
    'general change': "General Changes",
    'build change': "Build Changes",
    'enterprise change': "Enterprise Edition Changes",
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
    'bug fix',
    'performance improvement',
    'build change',
    ]

# Release note category common misspellings.
cat_misspells = {
    'sql' : 'sql change',
    'general': 'general change',
    'core change': 'general change',
    'bugfix': 'bug fix',
    'performance change' : 'performance improvement',
    'performance' : 'performance improvement',
    'ui' : 'admin ui change',
    'backwards-incompatible change': 'backward-incompatible change',
    'enterprise': 'enterprise change'
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
                  help="list history from COMMIT. Note: the first commit is excluded.", metavar="COMMIT")
parser.add_option("-t", "--until", dest="until_commit", default="HEAD",
                  help="list history up and until COMMIT (default: HEAD)", metavar="COMMIT")
parser.add_option("--hide-unambiguous-shas", action="store_true", dest="hide_shas", default=False,
                  help="omit commit SHAs from the release notes and per-contributor sections")
parser.add_option("--hide-per-contributor-section", action="store_true", dest="hide_per_contributor", default=False,
                  help="omit the per-contributor section")
parser.add_option("--hide-downloads-section", action="store_true", dest="hide_downloads", default=False,
                  help="omit the email sign-up and downloads section")

(options, args) = parser.parse_args()

sortkey = options.sort_key
revsort = options.reverse_sort
hideshas = options.hide_shas
hidepercontributor = options.hide_per_contributor
hidedownloads = options.hide_downloads

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
    print("Note: the first commit is excluded. Use e.g.: --from <prev-release-tag> --until <new-release-candidate-sha>", file=sys.stderr)
    exit(0)

### Reading data from repository ###

# Is the first commit reachable from the current one?
base = repo.merge_base(firstCommit, commit)
if len(base) == 0:
    print("error: %s and %s have no common ancestor" % (options.from_commit, options.until_commit), file=sys.stderr)
    exit(1)
commonParent = base[0]
if firstCommit != commonParent:
    print("warning: %s is not an ancestor of %s!" % (options.from_commit, options.until_commit), file=sys.stderr)
    print(file=sys.stderr)
    ageindays = int((firstCommit.committed_date - commonParent.committed_date)/86400)
    prevlen = sum((1 for x in repo.iter_commits(commonParent.hexsha + '...' + firstCommit.hexsha)))
    print("The first common ancestor is %s," % commonParent.hexsha, file=sys.stderr)
    print("which is %d commits older than %s and %d days older. Using that as origin." %\
          (prevlen, options.from_commit, ageindays), file=sys.stderr)
    print(file=sys.stderr)
    firstCommit = commonParent
    options.from_commit = commonParent.hexsha

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
        this_commits_author = author_aliases.get(commit.author.name, commit.author.name)
        if commit.author.name != 'GitHub' and this_commits_author != author:
            otherauthors.add(this_commits_author)
        this_commits_committer = author_aliases.get(commit.committer.name, commit.committer.name)
        if commit.committer.name != 'GitHub' and this_commits_committer != author:
            otherauthors.add(this_commits_committer)

        n, a = collect_commits(pr, title, list(commit.parents), author)
        ncommits += n
        otherauthors.update(a)
    return ncommits, otherauthors

per_author_history = {}
individual_authors = set()
allprs = set()

spinner = itertools.cycle(['/', '-', '\\', '|'])
counter = 0

def spin():
    global counter
    # Display a progress bar
    counter += 1
    if counter % 10 == 0:
        if counter % 100 == 0:
            print("\b..", end='', file=sys.stderr)
        print("\b", end='', file=sys.stderr)
        print(next(spinner),  end='', file=sys.stderr)
        sys.stderr.flush()

while commit != firstCommit:
    spin()

    # Analyze the commit
    if commit.message.startswith("Merge pull request"):
        author = (author_aliases.get(commit.author.name, commit.author.name), commit.author.email)
        lines = commit.message.split('\n', 3)
        pr = lines[0].split(' ', 4)[3]
        allprs.add(pr)
        title = lines[2]
        ncommits, otherauthors = collect_commits(pr, title, list(commit.parents), author[0])
        individual_authors.add(author[0])
        individual_authors.update(otherauthors)

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

allauthors = list(per_author_history.keys())
allauthors.sort(key=lambda x:x[0].lower())

ext_contributors = individual_authors - crdb_folk
firsttime_contributors = []
for a in individual_authors:
    # Find all aliases known for this person
    aliases = [a]
    for alias, name in author_aliases.items():
        if name == a:
            aliases.append(alias)
    # Collect the history for every alias
    hist = b''
    for al in aliases:
        spin()
        c = subprocess.run(["git", "log", "--author=%s" % al, options.from_commit, '-n', '1'], stdout=subprocess.PIPE, check=True)
        hist += c.stdout
    if len(hist) == 0:
        # No commit from that author older than the first commit
        # selected, so that's a first-time author.
        firsttime_contributors.append(a)

print("\b\n", file=sys.stderr)
sys.stderr.flush()

### Presentation of results ###

## Print the release notes.

# Start with known sections.

current_version = subprocess.check_output(["git", "describe", "--tags", options.until_commit], universal_newlines=True).strip()
previous_version = subprocess.check_output(["git", "describe", "--tags", options.from_commit], universal_newlines=True).strip()

print("---")
print("title: What&#39;s New in", current_version)
print("toc: false")
print("summary: Additions and changes in CockroachDB version", current_version, "since version", previous_version)
print("---")
print()
print("## " + time.strftime("%B %d, %Y"))
print()

## Print the release notes sign-up and Downloads section.
if not hidedownloads:
    print("""Get future release notes emailed to you:

<div class="hubspot-install-form install-form-1 clearfix">
    <script>
        hbspt.forms.create({
            css: '',
            cssClass: 'install-form',
            portalId: '1753393',
            formId: '39686297-81d2-45e7-a73f-55a596a8d5ff',
            formInstanceId: 1,
            target: '.install-form-1'
        });
    </script>
</div>""")
    print()

    print("""### Downloads

<div id="os-tabs" class="clearfix">
    <a href="https://binaries.cockroachdb.com/cockroach-""" + current_version + """.darwin-10.9-amd64.tgz"><button id="mac" data-eventcategory="mac-binary-release-notes">Mac</button></a>
    <a href="https://binaries.cockroachdb.com/cockroach-""" + current_version + """.linux-amd64.tgz"><button id="linux" data-eventcategory="linux-binary-release-notes">Linux</button></a>
    <a href="https://binaries.cockroachdb.com/cockroach-""" + current_version + """.windows-6.2-amd64.zip"><button id="windows" data-eventcategory="windows-binary-release-notes">Windows</button></a>
    <a href="https://binaries.cockroachdb.com/cockroach-""" + current_version + """.src.tgz"><button id="source" data-eventcategory="source-release-notes">Source</button></a>
</div>""")
    print()

seenshas = set()
seenprs = set()
def renderlinks(item):
    ret = '[%(pr)s][%(pr)s]' % item
    seenprs.add(item['pr'])
    if not hideshas:
        ret += ' [%(sha)s][%(sha)s]' % item
        seenshas.add(item['sha'])
    return ret

for sec in relnote_sec_order:
    r = release_notes.get(sec, None)
    if r is None:
        # No change in this section, nothing to print.
        continue
    sectitle = relnotetitles[sec]
    print("###", sectitle)
    print()

    for item in r:
        print("-", item['note'].replace('\n', '\n  '), renderlinks(item))

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
            print("-", item['note'].replace('\n', '\n  '), renderlinks(item))
        print()

if len(missing_release_notes) > 0:
    print("#### Changes without release note annotation")
    print()
    for item in missing_release_notes:
        author = item['author'][0]
        author = author_aliases.get(author, author)
        print("- [%(pr)s][%(pr)s] [%(sha)s][%(sha)s] %(prtitle)s" % item, "(%s)" % author)
        seenshas.add(item['sha'])
        seenprs.add(item['pr'])
    print()

## Print the Doc Updates section.
print("### Doc Updates")
print()
print("Docs team: Please add these manually.")
print()

## Print the Contributors section.
print("### Contributors")
print()
print("This release includes %d merged PRs by %s author%s." %
      (len(allprs),
       len(individual_authors), (len(individual_authors) != 1 and "s" or ""),
      ), end=' ')

ext_contributors = individual_authors - crdb_folk

if len(ext_contributors) > 0:
    ext_contributors = sorted(ext_contributors)
    # # Note: CRDB folk can be first-time contributors too, so
    # # not part of the if ext_contributors above.
    if len(firsttime_contributors) > 0:
        print("We would like to thank the following contributors from the CockroachDB community, with special thanks to first-time contributors ", end='')
        for i, n in enumerate(firsttime_contributors):
            if i > 0 and i < len(firsttime_contributors)-1:
                print(', ', end='')
            elif i > 0:
                print(', and ', end='')
            print(n, end='')
        print('.')
    else:
        print("We would like to thank the following contributors from the CockroachDB community:")
        print()
    for a in ext_contributors:
        print("-", a)
    print()

## Print the per-author contribution list.
if not hidepercontributor:
    print("### PRs merged by committer")
    print()
    if not hideshas:
        fmt = "  - [%(pr)-6s][%(pr)s] [%(sha)s][%(sha)s] (+%(insertions)4d -%(deletions)4d ~%(lines)4d/%(files)2d) %(title)s"
    else:
        fmt = "  - [%(pr)-6s][%(pr)s] (+%(insertions)4d -%(deletions)4d ~%(lines)4d/%(files)2d) %(title)s"

    for author in allauthors:
        items = per_author_history[author]
        print("- %s:" % author_aliases.get(author[0], author[0]))
        items.sort(key=lambda x:x[sortkey],reverse=not revsort)
        for item in items:
            print(fmt % item, end='')
            if not hideshas:
                seenshas.add(item['sha'])
            seenprs.add(item['pr'])

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
    print("[%s]: https://github.com/cockroachdb/cockroach/pull/%s" % (pr, pr[1:]))
for sha in seenshas:
    print("[%s]: https://github.com/cockroachdb/cockroach/commit/%s" % (sha, sha))
print()
