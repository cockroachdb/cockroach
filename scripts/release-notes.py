#! /usr/bin/env python3
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
from gitdb import exc
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
                    continue
                mmap_bycanon[canon] = p
                p.aliases.append(canon)
                byname = mmap_byname.get(name, [])
                if p not in byname:
                    byname.append(p)
                mmap_byname[name] = byname

# lookup_person retrieves the main identity of a person given one of their
# names or email aliases in the mailmap.
def lookup_person(name, email):
    key = (name, email)
    if key in mmap_bycanon:
        # lucky case.
        return mmap_bycanon[key]
    # Name+email didn't work.
    # Let's see email next.
    if email in mmap_byaddr:
        candidates = mmap_byaddr[email]
        if len(candidates) > 1:
            print('warning: no direct name match for', (name, email),
                  'and addr', email, 'is ambiguous,',
                  'keeping as-is', file=sys.stderr)
            return define_person(name, email)
        return candidates[0]
    # Email didn't work either. That's not great.
    if name in mmap_byname:
        candidates = mmap_byname[name]
        if len(candidates) > 1:
            print('warning: no direct name match for', (name, email),
                  'and name', name, 'is ambiguous,',
                  'keeping as-is', file=sys.stderr)
            return define_person(name, email)
        return candidates[0]
    return define_person(name, email)

# Section titles for release notes.
relnotetitles = {
    'cli change': "Command-line changes",
    'ops change': "Operational changes",
    'sql change': "SQL language changes",
    'api change': "API endpoint changes",
    'ui change': "DB Console changes",
    'general change': "General changes",
    'build change': "Build changes",
    'enterprise change': "Enterprise edition changes",
    'backward-incompatible change': "Backward-incompatible changes",
    'performance improvement': "Performance improvements",
    'bug fix': "Bug fixes",
    'security update': "Security updates",
}

# Order in which to show the sections.
relnote_sec_order = [
    'backward-incompatible change',
    'security update',
    'general change',
    'enterprise change',
    'sql change',
    'ops change',
    'cli change',
    'api change',
    'ui change',
    'bug fix',
    'performance improvement',
    'build change',
]

# Release note category common misspellings.
cat_misspells = {
    'sql': 'sql change',
    'general': 'general change',
    'core change': 'general change',
    'bugfix': 'bug fix',
    'performance change': 'performance improvement',
    'performance': 'performance improvement',
    'ui': 'ui change',
    'operational change': 'ops change',
    'admin ui': 'ui change',
    'api': 'api change',
    'http': 'api change',
    'backwards-incompatible change': 'backward-incompatible change',
    'enterprise': 'enterprise change',
    'security': 'security update',
    'security change': 'security update',
}

#
# Release note format
#

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
# Captures : xxx: yyy - this must be careful not to capture too much, we just accept one or two words
form2 = r': *(?P<cat2>[^ ]+(?: +[^ ]+)?) *:'
# Captures : yyy - no category
form3 = r':(?P<cat3>)'
relnote = re.compile(r'(?:^|[\n\r])[rR]elease [nN]otes? *(?:' + form1 + '|' + form2 + '|' + form3 + r') *(?P<note>.*)$', flags=re.S)

coauthor = re.compile(r'^Co-authored-by: (?P<name>[^<]*) <(?P<email>.*)>', flags=re.M)
fixannot = re.compile(r'^([fF]ix(es|ed)?|[cC]lose(d|s)?) #', flags=re.M)

#
# Merge commit format
#

# The following merge commits have been seen in the wild:
#
# Merge pull request #XXXXX from ...      <- GitHub merges
# .... (#XXXX)                            <- GitHub merges (alt format)
# Merge #XXXXX #XXXXX #XXXXX              <- Bors merges
merge_numbers = re.compile(r'^Merge( pull request)?(?P<numbers>( #[0-9]+)+)')
simple_merge = re.compile(r'.*\((?P<numbers>#[0-9]+)\)$', re.M)

#
# Initialization / option parsing
#

parser = OptionParser()
parser.add_option("-k", "--sort-key", dest="sort_key", default="title",
                  help="sort by KEY (pr, title, insertions, deletions, files, sha, date; default: title)", metavar="KEY")
parser.add_option("-r", "--reverse", action="store_true", dest="reverse_sort", default=False,
                  help="reverse sort")
parser.add_option("-f", "--from", dest="from_commit",
                  help="list history from COMMIT. Note: the first commit is excluded.", metavar="COMMIT")
parser.add_option("-t", "--until", dest="until_commit", default="HEAD",
                  help="list history up and until COMMIT (default: HEAD)", metavar="COMMIT")
parser.add_option("-p", "--pull-ref", dest="pull_ref_prefix", default="refs/pull/origin",
                  help="prefix for pull request refs (default: refs/pull/origin)", metavar="PREFIX")
parser.add_option("--hide-unambiguous-shas", action="store_true", dest="hide_shas", default=False,
                  help="omit commit SHAs from the release notes and per-contributor sections")
parser.add_option("--hide-per-contributor-section", action="store_true", dest="hide_per_contributor", default=False,
                  help="omit the per-contributor section")
parser.add_option("--hide-crdb-folk", dest="hide_crdb_folk", action="store_true", default=False,
                  help="don't show crdb folk in the per-contributor section")
parser.add_option("--hide-downloads-section", action="store_true", dest="hide_downloads", default=False,
                  help="omit the email sign-up and downloads section")
parser.add_option("--hide-header", action="store_true", dest="hide_header", default=False,
                  help="omit the title and date header")
parser.add_option("--exclude-from", dest="exclude_from_commit",
                  help="exclude history starting after COMMIT. Note: COMMIT itself is excluded.", metavar="COMMIT")
parser.add_option("--exclude-until", dest="exclude_until_commit",
                  help="exclude history ending at COMMIT", metavar="COMMIT")
parser.add_option("--one-line", dest="one_line", action="store_true", default=False,
                  help="unwrap release notes on a single line")
parser.add_option("--prod-release", dest="prod_release", action="store_true", default=False,
                  help="identify release as production (e.g., v20.2.x) and omit '-unstable' from docker pull command")

(options, args) = parser.parse_args()

sortkey = options.sort_key
revsort = options.reverse_sort
pull_ref_prefix = options.pull_ref_prefix
hideshas = options.hide_shas
hidepercontributor = options.hide_per_contributor
hidedownloads = options.hide_downloads
hideheader = options.hide_header
hidecrdbfolk = options.hide_crdb_folk

repo = Repo('.')
heads = repo.heads


def reformat_note(note_lines):
    sep = '\n'
    if options.one_line:
        sep = ' '
    return sep.join(note_lines).strip()


# Check that pull_ref_prefix is valid
testrefname = "%s/1" % pull_ref_prefix

try:
    repo.commit(testrefname)
except exc.ODBError:
    print("Unable to find pull request refs at %s." % pull_ref_prefix, file=sys.stderr)
    print("Is your repo set up to fetch them?  Try adding", file=sys.stderr)
    print("  fetch = +refs/pull/*/head:%s/*" % pull_ref_prefix, file=sys.stderr)
    print("to the GitHub remote section of .git/config.", file=sys.stderr)
    sys.exit(1)



def find_commits(from_commit_ref, until_commit_ref):
    try:
        firstCommit = repo.commit(from_commit_ref)
    except exc.ODBError:
        print("Unable to find the first commit of the range.", file=sys.stderr)
        print("No ref named %s." % from_commit_ref, file=sys.stderr)
        sys.exit(1)

    try:
        finalCommit = repo.commit(until_commit_ref)
    except exc.ODBError:
        print("Unable to find the last commit of the range.", file=sys.stderr)
        print("No ref named %s." % until_commit_ref, file=sys.stderr)
        sys.exit(1)

    return firstCommit, finalCommit


if not options.until_commit:
    print("no value specified with --until, try --until=xxxxx (without space after =)", file=sys.stderr)
    sys.exit(1)
if not options.from_commit:
    print("no value specified with --from, try --from=xxxx (without space after =)", file=sys.stderr)
    sys.exit(1)

firstCommit, commit = find_commits(options.from_commit, options.until_commit)
if commit == firstCommit:
    print("Commit range is empty!", file=sys.stderr)
    print(parser.get_usage(), file=sys.stderr)
    print("Example use:", file=sys.stderr)
    print("  %s --help" % sys.argv[0], file=sys.stderr)
    print("  %s --from xxx >output.md" % sys.argv[0], file=sys.stderr)
    print("  %s --from xxx --until yyy >output.md" % sys.argv[0], file=sys.stderr)
    print("Note: the first commit is excluded. Use e.g.: --from <prev-release-tag> --until <new-release-candidate-sha>", file=sys.stderr)
    sys.exit(0)

excludedFirst, excludedLast = None, None
if options.exclude_from_commit or options.exclude_until_commit:
    if not options.exclude_from_commit or not options.exclude_until_commit:
        print("Both -xf and -xt must be specified, or not at all.")
        sys.exit(1)
    excludedFirst, excludedLast = find_commits(options.exclude_from_commit, options.exclude_until_commit)

#
# Reading data from repository
#


def identify_commit(c):
    return '%s ("%s", %s)' % (
        c.hexsha, c.message.split('\n', 1)[0],
        datetime.datetime.fromtimestamp(c.committed_date).ctime())


def check_reachability(start, end):
    # Is the first commit reachable from the current one?
    base = repo.merge_base(start, end)
    if len(base) == 0:
        print("error: %s:%s\nand %s:%s\nhave no common ancestor" % (
            options.from_commit, identify_commit(start),
            options.until_commit, identify_commit(end)), file=sys.stderr)
        sys.exit(1)
    commonParent = base[0]
    if start != commonParent:
        print("warning: %s:%s\nis not an ancestor of %s:%s!" % (
            options.from_commit, identify_commit(start),
            options.until_commit, identify_commit(end)), file=sys.stderr)
        print(file=sys.stderr)
        ageindays = int((start.committed_date - commonParent.committed_date) / 86400)
        prevlen = sum((1 for x in repo.iter_commits(commonParent.hexsha + '...' + start.hexsha)))
        print("The first common ancestor is %s" % identify_commit(commonParent), file=sys.stderr)
        print("which is %d commits older than %s:%s\nand %d days older. Using that as origin." %\
              (prevlen, options.from_commit, identify_commit(start), ageindays), file=sys.stderr)
        print(file=sys.stderr)
        start = commonParent
    return start, end


firstCommit, commit = check_reachability(firstCommit, commit)
options.from_commit = firstCommit.hexsha


def extract_release_notes(currentCommit):
    msglines = currentCommit.message.split('\n')
    curnote = []
    innote = False
    foundnote = False
    cat = None
    notes = []
    for line in msglines:
        m = coauthor.search(line)
        if m is not None:
            # A Co-authored-line finishes the parsing of the commit message,
            # because it's included at the end only.
            break

        m = fixannot.search(line)
        if m is not None:
            # Fix/Close etc. Ignore.
            continue

        m = norelnote.search(line)
        if m is not None:
            # Release note: None
            #
            # Remember we found a note (so the commit is not marked as "missing
            # a release note"), but we won't collect it.
            foundnote = True
            continue

        m = relnote.search(line)
        if m is None:
            # Current line does not contain a release note separator.
            # If we were already collecting a note, continue collecting it.
            if innote:
                curnote.append(line)
            continue

        # We have a release note boundary. If we were collecting a
        # note already, complete it.
        if innote:
            notes.append((cat, reformat_note(curnote)))
            curnote = []
            innote = False

        # Start a new release note.

        firstline = m.group('note').strip()
        if firstline.lower() == 'none':
            # Release note: none - there's no note yet.
            continue
        foundnote = True
        innote = True

        # Capitalize the first line.
        if firstline != "":
            firstline = firstline[0].upper() + firstline[1:]

        curnote = [firstline]
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

    if innote:
        notes.append((cat, reformat_note(curnote)))

    return foundnote, notes


spinner = itertools.cycle(['/', '-', '\\', '|'])
spin_counter = 0


def spin():
    global spin_counter
    # Display a progress bar
    spin_counter += 1
    if spin_counter % 10 == 0:
        if spin_counter % 100 == 0:
            print("\b..", end='', file=sys.stderr)
        print("\b", end='', file=sys.stderr)
        print(next(spinner), end='', file=sys.stderr)
        sys.stderr.flush()


def get_direct_history(startCommit, lastCommit):
    history = []
    for c in repo.iter_commits(startCommit.hexsha + '..' + lastCommit.hexsha, first_parent=True):
        history.append(c)
    return history


excluded_notes = set()
if excludedFirst is not None:
    #
    # Collect all the notes to exclude during collection below.
    #
    print("Collecting EXCLUDED release notes from\n%s\nuntil\n%s" %
          (identify_commit(excludedFirst), identify_commit(excludedLast)), file=sys.stderr)

    # First ensure that the loop below will terminate.
    excludedFirst, excludedLast = check_reachability(excludedFirst, excludedLast)
    # Collect all the merge points, so we can measure progress.
    mergepoints = get_direct_history(excludedFirst, excludedLast)

    # Now collect all commits.
    print("Collecting EXCLUDED release notes...", file=sys.stderr)
    i = 0
    progress = 0
    lastTime = time.time()
    for c in repo.iter_commits(excludedFirst.hexsha + '..' + excludedLast.hexsha):
        progress = int(100. * float(i) / len(mergepoints))
        newTime = time.time()
        if newTime >= lastTime + 5:
            print("\b%d%%.." % progress, file=sys.stderr, end='')
            lastTime = newTime
        i += 1

        spin()
        # Collect the release notes in that commit.
        _, notes = extract_release_notes(c)
        for cat, note in notes:
            excluded_notes.add((cat, note))

    print("\b100%\n", file=sys.stderr)

print("Collecting release notes from\n%s\nuntil\n%s" % (identify_commit(firstCommit), identify_commit(commit)), file=sys.stderr)

release_notes = {}
missing_release_notes = []


def collect_authors(commit):
    authors = set()
    author = lookup_person(commit.author.name, commit.author.email)
    if author.name != 'GitHub':
        authors.add(author)
    author = lookup_person(commit.committer.name, commit.committer.email)
    if author.name != 'GitHub':
        authors.add(author)
    for m in coauthor.finditer(commit.message):
        aname = m.group('name').strip()
        amail = m.group('email').strip()
        author = lookup_person(aname, amail)
        authors.add(author)
    return authors


def process_release_notes(pr, title, commit):
    authors = collect_authors(commit)

    foundnote, notes = extract_release_notes(commit)

    # At the end the notes will be presented in reverse order, because
    # we explore the commits in reverse order. However within 1 commit
    # the notes are in the correct order. So reverse them upfront here,
    # so that the 2nd reverse gets them in the right order again.
    for cat, note in reversed(notes):
        if (cat, note) not in excluded_notes:
            completenote(commit, cat, note, authors, pr, title)

    missing_item = None
    if not foundnote:
        # Missing release note. Keep track for later.
        missing_item = makeitem(pr, title, commit.hexsha[:shamin], authors)
    return missing_item, authors


def makeitem(pr, prtitle, sha, authors):
    return {'authors': authors,
            'sha': sha,
            'pr': pr,
            'title': prtitle,
            'note': None}


def completenote(commit, cat, notemsg, authors, pr, title):
    item = makeitem(pr, title, commit.hexsha[:shamin], authors)
    item['note'] = notemsg

    # Now collect per category.
    catnotes = release_notes.get(cat, [])
    catnotes.append(item)
    release_notes[cat] = catnotes


per_group_history = {}
individual_authors = set()
allprs = set()


# This function groups and counts all the commits that belong to a particular PR.
# Some description is in order regarding the logic here: it should visit all
# commits that are on the PR and only on the PR. If there's some secondary
# branch merge included on the PR, as long as those commits don't otherwise end
# up reachable from the target branch, they'll be included.  If there's a back-
# merge from the target branch, that should be excluded.
#
# Examples:
#
# ### secondary branch merged into PR
#
# Dev branched off of K, made a commit J, made a commit G while someone else
# committed H, merged H from the secondary branch to the topic branch in E,
# made a final commit in C, then merged to master in A.
#
#     A <-- master
#     |\
#     | \
#     B  C <-- PR tip
#     |  |
#     |  |
#     D  E <-- secondary merge
#     |  |\
#     |  | \
#     F  G  H <-- secondary branch
#     |  | /
#     |  |/
#     I  J
#     | /
#     |/
#     K <-- merge base
#
# C, E, G, H, and J will each be checked.  None of them are ancestors of B,
# so they will all be visited. E will be not be counted because the message
# starts with "Merge", so in the end C, G, H, and J will be included.
#
# ### back-merge from target branch
#
# Dev branched off H, made one commit G, merged the latest F from master in E,
# made one final commit in C, then merged the PR.
#
#     A <-- master
#     |\
#     | \
#     B  C <-- PR tip
#     |  |
#     |  |
#     D  E <-- back-merge
#     | /|
#     |/ |
#     F  G
#     | /
#     |/
#     H <-- merge base
#
# C, E, F, and G will each be checked. F is an ancestor of B, so it will be
# excluded. E starts with "Merge", so it will not be counted. Only C and G will
# have statistics included.
def analyze_pr(merge, pr, parent_idx):
    allprs.add(pr)


    noteexpr = re.compile("^%s: (?P<message>.*) r=.* a=.*" % pr[1:], flags=re.M)
    m = noteexpr.search(merge.message)
    title = ''
    if m is None:
        # GitHub merge
        title = merge.message.split('\n', 3)[2]
    else:
        # Bors merge
        title = m.group('message')
    title = title.strip()

    try:
        refname = pull_ref_prefix + "/" + pr[1:]
        tip = name_to_object(repo, refname)
    except exc.BadName:
        # Oddly, we have at least one PR (#47761) which does not have a tip
        # at /refs/pull/47761, although it's valid and merged.
        # As of 2020-06-08 it's the only PR missing a branch tip there.
        print("\nuh-oh!  can't find PR head in repo", pr, file=sys.stderr)
        # We deal with it here assuming that the order of the parents
        # of the merge commit is the same as reported by the
        # "Merge ..." string in the merge commit's message.
        # This happens to be true of the missing PR above as well
        # as for several other merge commits with more than two parents.
        tip = merge.parents[parent_idx]
        print("check at https://github.com/cockroachdb/cockroach/pull/%s that the last commit is %s" % (pr[1:], tip.hexsha), file=sys.stderr)
        # TODO(knz): If this method is reliable, this means we don't
        # need the pull tips at /refs/pull *at all* which could
        # streamline the whole experience.
        # This should be investigated further.

    merge_base_result = repo.merge_base(merge.parents[0], tip)
    if len(merge_base_result) == 0:
        print("uh-oh!  can't find merge base!  pr", pr, file=sys.stderr)
        sys.exit(-1)

    merge_base = merge_base_result[0]

    seen_commits = set()

    missing_items = []
    authors = set()
    ncommits = 0
    for commit in repo.iter_commits(merge_base.hexsha + '..' + tip.hexsha):
        spin()

        if commit in seen_commits:
            # We may be seeing the same commit twice if a feature branch has
            # been forked in sub-branches. Just skip over what we've seen
            # already.
            continue
        seen_commits.add(commit)

        if not commit.message.startswith("Merge"):
            missing_item, prauthors = process_release_notes(pr, title, commit)
            authors.update(prauthors)
            ncommits += 1
            if missing_item is not None:
                missing_items.append(missing_item)

    if ncommits == len(missing_items):
        # None of the commits found had a release note. List them.
        for item in missing_items:
            missing_release_notes.append(item)

    text = repo.git.diff(merge_base.hexsha, tip.hexsha, '--', numstat=True)
    stats = Stats._list_from_string(repo, text)

    collect_item(pr, title, merge.hexsha[:shamin], ncommits, authors, stats.total, merge.committed_date)



def collect_item(pr, prtitle, sha, ncommits, authors, stats, prts):
    individual_authors.update(authors)
    if len(authors) == 0:
        authors.add("Unknown Author")
    item = makeitem(pr, prtitle, sha, authors)
    item.update({'ncommits': ncommits,
                 'insertions': stats['insertions'],
                 'deletions': stats['deletions'],
                 'files': stats['files'],
                 'lines': stats['lines'],
                 'date': datetime.date.fromtimestamp(prts).isoformat(),
                 })

    al = item['authors']
    k = str(sorted(al))
    history = per_group_history.get(k, (al, []))
    history[1].append(item)
    per_group_history[k] = history



def analyze_standalone_commit(commit):
    # Some random out-of-branch commit. Let's not forget them.
    authors = collect_authors(commit)
    title = commit.message.split('\n', 1)[0].strip()
    item = makeitem('#unknown', title, commit.hexsha[:shamin], authors)
    missing_release_notes.append(item)
    collect_item('#unknown', title, commit.hexsha[:shamin], 1, authors, commit.stats.total, commit.committed_date)


# Collect all the merge points so we can report progress.
mergepoints = get_direct_history(firstCommit, commit)
i = 0
progress = 0
lastTime = time.time()
for commit in mergepoints:
    progress = int(100. * float(i) / len(mergepoints))
    newTime = time.time()
    if newTime >= lastTime + 5:
        print("\b.%d%%\n." % progress, file=sys.stderr, end='')
        lastTime = newTime
    i += 1
    spin()

    ctime = datetime.datetime.fromtimestamp(commit.committed_date).ctime()
    numbermatch = merge_numbers.search(commit.message)
    if numbermatch is None:
        # Try again with the alternate format.
        firstline = commit.message.split('\n', 1)[0]
        numbermatch = simple_merge.search(firstline)
    # Analyze the commit
    if numbermatch is not None:
        prs = numbermatch.group("numbers").strip().split(" ")
        for idx, pr in enumerate(prs):
            print("                                \r%s (%s) " % (pr, ctime), end='', file=sys.stderr)
            analyze_pr(commit, pr, idx+1)
    else:
        print("                                \r%s (%s) " % (commit.hexsha[:shamin], ctime), end='', file=sys.stderr)
        analyze_standalone_commit(commit)


print("\b\nAnalyzing authors...", file=sys.stderr)
sys.stderr.flush()

allgroups = list(per_group_history.keys())
allgroups.sort(key=lambda x: x.lower())

print("\b\nComputing first-time contributors...", end='', file=sys.stderr)

ext_contributors = individual_authors - crdb_folk
firsttime_contributors = []
for a in individual_authors:
    # Find all aliases known for this person
    aliases = a.aliases
    # Collect the history for every alias
    hist = b''
    for al in aliases:
        spin()
        cmd = subprocess.run(["git", "log", "--author=%s <%s>" % al, options.from_commit, '-n', '1'], stdout=subprocess.PIPE, check=True)
        hist += cmd.stdout
    if len(hist) == 0:
        # No commit from that author older than the first commit
        # selected, so that's a first-time author.
        firsttime_contributors.append(a)

print("\b\n", file=sys.stderr)
sys.stderr.flush()

#
# Presentation of results.
#

# Print the release notes.

# Start with known sections.

current_version = subprocess.check_output(["git", "describe", "--tags", "--match=v[0-9]*", options.until_commit], universal_newlines=True).strip()
previous_version = subprocess.check_output(["git", "describe", "--tags", "--match=v[0-9]*", options.from_commit], universal_newlines=True).strip()

if not hideheader:
    print("---")
    print("title: What&#39;s New in", current_version)
    print("toc: true")
    print("summary: Additions and changes in CockroachDB version", current_version, "since version", previous_version)
    print("---")
    print()
    print("## " + time.strftime("%B %-d, %Y"))
    print()

# Print the release notes sign-up and Downloads section.

if options.prod_release:
    print("""DOCS WRITER: PLEASE UPDATE THE VERSIONS AND LINKS IN THIS INTRO: This page lists additions and changes in <current release> since <previous_version>.

- For a comprehensive summary of features in v20.2, see the [v20.2 GA release notes](v20.2.0.html).
- To upgrade to v20.2, see [Upgrade to CockroachDB v20.2](../v20.2/upgrade-cockroach-version.html).
""")

if not hidedownloads:
    print("""Get future release notes emailed to you:

{% include_cached marketo.html %}
""")
    print()

    print("""### Downloads

<div id="os-tabs" class="filters clearfix">
    <a href="https://binaries.cockroachdb.com/cockroach-""" + current_version + """.linux-amd64.tgz"><button id="linux" class="filter-button" data-scope="linux" data-eventcategory="linux-binary-release-notes">Linux</button></a>
    <a href="https://binaries.cockroachdb.com/cockroach-""" + current_version + """.darwin-10.9-amd64.tgz"><button id="mac" class="filter-button" data-scope="mac" data-eventcategory="mac-binary-release-notes">Mac</button></a>
    <a href="https://binaries.cockroachdb.com/cockroach-""" + current_version + """.windows-6.2-amd64.zip"><button id="windows" class="filter-button" data-scope="windows" data-eventcategory="windows-binary-release-notes">Windows</button></a>
    <a target="_blank" href="https://github.com/cockroachdb/cockroach/releases/tag/""" + current_version + '"' + """><button id="source" class="filter-button" data-scope="source" data-eventcategory="source-release-notes">Source</button></a>
</div>

<section class="filter-content" data-scope="windows">
{% include_cached windows_warning.md %}
</section>
""")

    print("""### Docker image

{% include_cached copy-clipboard.html %}
~~~shell
$ docker pull cockroachdb/cockroach""" + (":" if options.prod_release else "-unstable:") + current_version + """
~~~
""")
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

    for item in reversed(r):
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
        print("#### %s" % extrasec.capitalize())
        print()
        for item in release_notes[extrasec]:
            print("-", item['note'].replace('\n', '\n  '), renderlinks(item))
        print()

if len(missing_release_notes) > 0:
    print("#### Changes without release note annotation")
    print()
    for item in missing_release_notes:
        authors = ', '.join(str(x) for x in sorted(item['authors']))
        print("- [%(pr)s][%(pr)s] [%(sha)s][%(sha)s] %(title)s" % item, "(%s)" % authors)
        seenshas.add(item['sha'])
        seenprs.add(item['pr'])
    print()

# Print the Doc Updates section.
print("### Doc updates")
print()
print("{% comment %}Docs team: Please add these manually.{% endcomment %}")
print()

# Print the Contributors section.
print("### Contributors")
print()
print("This release includes %d merged PR%s by %s author%s." %
      (len(allprs), len(allprs) != 1 and "s" or "",
       len(individual_authors), (len(individual_authors) != 1 and "s" or "")))

ext_contributors = individual_authors - crdb_folk

notified_authors = sorted(set(ext_contributors) | set(firsttime_contributors))
if len(notified_authors) > 0:
    print("We would like to thank the following contributors from the CockroachDB community:")
    print()
    for person in notified_authors:
        print("-", person.name, end='')
        if person in firsttime_contributors:
            annot = ""
            if person.crdb:
                annot = ", CockroachDB team member"
            print(" (first-time contributor%s)" % annot, end='')
        print()
print()

# Print the per-author contribution list.
if not hidepercontributor:
    print("### PRs merged by contributors")
    print()
    if not hideshas:
        fmt = "  - %(date)s [%(pr)-6s][%(pr)-6s] [%(sha)s][%(sha)s] (+%(insertions)4d -%(deletions)4d ~%(lines)4d/%(files)2d) %(title)s"
    else:
        fmt = "  - %(date)s [%(pr)-6s][%(pr)-6s] (+%(insertions)4d -%(deletions)4d ~%(lines)4d/%(files)2d) %(title)s"

    for group in allgroups:
        al, items = per_group_history[group]
        if hidecrdbfolk and all(map(lambda x: x in crdb_folk, al)):
            continue
        items.sort(key=lambda x: x[sortkey], reverse=not revsort)
        print("- %s:" % ', '.join(a.name for a in sorted(al)))
        for item in items:
            print(fmt % item, end='')
            if not hideshas:
                seenshas.add(item['sha'])
            seenprs.add(item['pr'])

            ncommits = item['ncommits']
            if ncommits > 1:
                print(" (", end='')
                print("%d commits" % ncommits, end='')
                print(")", end='')
            print()
        print()
    print()

# Link the PRs and SHAs
for pr in sorted(seenprs):
    print("[%s]: https://github.com/cockroachdb/cockroach/pull/%s" % (pr, pr[1:]))
for sha in sorted(seenshas):
    print("[%s]: https://github.com/cockroachdb/cockroach/commit/%s" % (sha, sha))
print()
