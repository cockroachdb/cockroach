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

import sys
import itertools
import re
import os
import datetime, time
import subprocess
from git import Repo
from optparse import OptionParser
from git.repo.fun import name_to_object
from git.util import Stats

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
    'Emmanuel': "Emmanuel Sales",
    'MBerhault': "Marc Berhault",
    'Nate': "Nathaniel Stewart",
    'a6802739': "Song Hao",
    'Abhemailk abhi.madan01@gmail.com': "Abhishek Madan",
    'rytaft': "Rebecca Taft",
    'songhao': "Song Hao",
    'solongordon': "Solon Gordon",
    'tim-o': "Tim O'Brien",
    'Amruta': "Amruta Ranade",
    'yuzefovich': "Yahor Yuzefovich",
    'madhavsuresh': "Madhav Suresh",
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
    "Bob Vawter",
    "Bram Gruneir",
    "Daniel Harrison",
    "David Taylor",
    "Diana Hsieh",
    "Emmanuel Sales",
    "Jesse Seldess",
    "Jessica Edwards",
    "Joseph Lowinske",
    "Joey Pereira",
    "Jordan Lewis",
    "Justin Jaffray",
    "Kuan Luo",
    "Lauren Hirata",
    "Madhav Suresh",
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
    "Rich Loveland",
    "Richard Wu",
    "Ridwan Sharif",
    "Sean Loiselle",
    "Solon Gordon",
    "Spencer Kimball",
    "Tamir Duberstein",
    "Tim O'Brien",
    "Tobias Schottdorf",
    "Victor Chen",
    "Vivek Menezes",
    "Yahor Yuzefovich",
])


# Section titles for release notes.
relnotetitles = {
    'cli change': "Command line changes",
    'sql change': "SQL language changes",
    'admin ui change': "Admin UI changes",
    'general change': "General changes",
    'build change': "Build changes",
    'enterprise change': "Enterprise edition changes",
    'backward-incompatible change': "Backward-incompatible changes",
    'performance improvement': "Performance improvements",
    'bug fix': "Bug fixes",
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
# Captures : xxx: yyy - this must be careful not to capture too much, we just accept one or two words
form2 = r': *(?P<cat2>[^ ]+(?: +[^ ]+)?) *:'
# Captures : yyy - no category
form3 = r':(?P<cat3>)'
relnote = re.compile(r'(?:^|[\n\r])[rR]elease [nN]otes? *(?:' + form1 + '|' + form2 + '|' + form3 + r') *(?P<note>.*)$', flags=re.S)

coauthor = re.compile(r'^Co-authored-by: (?P<name>[^<]*) <(?P<email>.*)>', flags=re.M)
fixannot = re.compile(r'^([fF]ix(es|ed)?|[cC]lose(d|s)?) #', flags=re.M)

## Merge commit format ##

# The following merge commits have been seen in the wild:
#
# Merge pull request #XXXXX from ...      <- GitHub merges
# Merge #XXXXX #XXXXX #XXXXX              <- Bors merges
merge_numbers = re.compile(r'^Merge( pull request)?(?P<numbers>( #[0-9]+)+)')

### Initialization / option parsing ###

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
parser.add_option("--hide-downloads-section", action="store_true", dest="hide_downloads", default=False,
                  help="omit the email sign-up and downloads section")
parser.add_option("--hide-header", action="store_true", dest="hide_header", default=False,
                  help="omit the title and date header")

(options, args) = parser.parse_args()

sortkey = options.sort_key
revsort = options.reverse_sort
pull_ref_prefix = options.pull_ref_prefix
hideshas = options.hide_shas
hidepercontributor = options.hide_per_contributor
hidedownloads = options.hide_downloads
hideheader = options.hide_header

repo = Repo('.')
heads = repo.heads

try:
    firstCommit = repo.commit(options.from_commit)
except:
    print("Unable to find the first commit of the range.", file=sys.stderr)
    print("No ref named %s." % options.from_commit, file=sys.stderr)
    exit(0)

try:
    commit = repo.commit(options.until_commit)
except:
    print("Unable to find the last commit of the range.", file=sys.stderr)
    print("No ref named %s." % options.until_commit, file=sys.stderr)
    exit(0)

if commit == firstCommit:
    print("Commit range is empty!", file=sys.stderr)
    print(parser.get_usage(), file=sys.stderr)
    print("Example use:", file=sys.stderr)
    print("  %s --help" % sys.argv[0], file=sys.stderr)
    print("  %s --from xxx >output.md" % sys.argv[0], file=sys.stderr)
    print("  %s --from xxx --until yyy >output.md" % sys.argv[0], file=sys.stderr)
    print("Note: the first commit is excluded. Use e.g.: --from <prev-release-tag> --until <new-release-candidate-sha>", file=sys.stderr)
    exit(0)

# Check that pull_ref_prefix is valid
testrefname = "%s/1" % pull_ref_prefix
try:
    repo.commit(testrefname)
except:
    print("Unable to find pull request refs at %s." % pull_ref_prefix, file=sys.stderr)
    print("Is your repo set up to fetch them?  Try adding", file=sys.stderr)
    print("  fetch = +refs/pull/*/head:%s/*" % pull_ref_prefix, file=sys.stderr)
    print("to the GitHub remote section of .git/config.", file=sys.stderr)
    exit(0)

### Reading data from repository ###

def identify_commit(commit):
    return '%s ("%s", %s)' % (
        commit.hexsha, commit.message.split('\n',1)[0],
        datetime.datetime.fromtimestamp(commit.committed_date).ctime())

# Is the first commit reachable from the current one?
base = repo.merge_base(firstCommit, commit)
if len(base) == 0:
    print("error: %s:%s\nand %s:%s\nhave no common ancestor" % (
        options.from_commit, identify_commit(firstCommit),
        options.until_commit, identify_commit(commit)), file=sys.stderr)
    exit(1)
commonParent = base[0]
if firstCommit != commonParent:
    print("warning: %s:%s\nis not an ancestor of %s:%s!" % (
        options.from_commit, identify_commit(firstCommit),
        options.until_commit, identify_commit(commit)), file=sys.stderr)
    print(file=sys.stderr)
    ageindays = int((firstCommit.committed_date - commonParent.committed_date)/86400)
    prevlen = sum((1 for x in repo.iter_commits(commonParent.hexsha + '...' + firstCommit.hexsha)))
    print("The first common ancestor is %s" % identify_commit(commonParent), file=sys.stderr)
    print("which is %d commits older than %s:%s\nand %d days older. Using that as origin." %\
          (prevlen, options.from_commit, identify_commit(firstCommit), ageindays), file=sys.stderr)
    print(file=sys.stderr)
    firstCommit = commonParent
    options.from_commit = commonParent.hexsha

print("Changes from\n%s\nuntil\n%s" % (identify_commit(firstCommit), identify_commit(commit)), file=sys.stderr)

release_notes = {}
missing_release_notes = []

def collect_authors(commit):
    authors = set()
    author = author_aliases.get(commit.author.name, commit.author.name)
    if author != 'GitHub':
        authors.add(author)
    author = author_aliases.get(commit.committer.name, commit.committer.name)
    if author != 'GitHub':
        authors.add(author)
    for m in coauthor.finditer(commit.message):
        aname = m.group('name').strip()
        author = author_aliases.get(aname, aname)
        authors.add(author)
    return authors


def extract_release_notes(pr, title, commit):
    authors = collect_authors(commit)

    msglines = commit.message.split('\n')
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
            notes.append((cat, curnote))
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
        notes.append((cat, curnote))

    # At the end the notes will be presented in reverse order, because
    # we explore the commits in reverse order. However within 1 commit
    # the notes are in the correct order. So reverse them upfront here,
    # so that the 2nd reverse gets them in the right order again.
    for cat, note in reversed(notes):
        completenote(commit, cat, note, authors, pr, title)

    missing_item = None
    if not foundnote:
        # Missing release note. Keep track for later.
        missing_item = makeitem(pr, title, commit.hexsha[:shamin], authors)
    return missing_item, authors

def makeitem(pr, prtitle, sha, authors):
    return {'authors': ', '.join(sorted(authors)),
            'sha': sha,
            'pr': pr,
            'title': prtitle,
            'note': None}

def completenote(commit, cat, curnote, authors, pr, title):
    notemsg = '\n'.join(curnote).strip()
    item = makeitem(pr, title, commit.hexsha[:shamin], authors)
    item['note'] = notemsg

    # Now collect per category.
    catnotes = release_notes.get(cat, [])
    catnotes.append(item)
    release_notes[cat] = catnotes

per_group_history = {}
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
def analyze_pr(merge, pr):
    allprs.add(pr)

    refname = pull_ref_prefix + "/" + pr[1:]
    tip = name_to_object(repo, refname)

    noteexpr = re.compile("^%s: (?P<message>.*) r=.* a=.*" % pr[1:], flags=re.M)
    m = noteexpr.search(merge.message)
    note = ''
    if m is None:
        # GitHub merge
        note = merge.message.split('\n',3)[2]
    else:
        # Bors merge
        note = m.group('message')
    note = note.strip()

    merge_base_result = repo.merge_base(merge.parents[0], tip)
    if len(merge_base_result) == 0:
        print("uh-oh!  can't find merge base!  pr", pr, file=sys.stderr)
        exit(-1)

    merge_base = merge_base_result[0]

    commits_to_analyze = [tip]
    seen_commits = set()

    missing_items = []
    authors = set()
    ncommits = 0
    while len(commits_to_analyze) > 0:
        spin()

        commit = commits_to_analyze.pop(0)
        if commit in seen_commits:
            # We may be seeing the same commit twice if a feature branch has
            # been forked in sub-branches. Just skip over what we've seen
            # already.
            continue
        seen_commits.add(commit)

        if not commit.message.startswith("Merge"):
            missing_item, prauthors = extract_release_notes(pr, note, commit)
            authors.update(prauthors)
            ncommits += 1
            if missing_item is not None:
                missing_items.append(missing_item)

        for parent in commit.parents:
            if not repo.is_ancestor(parent, merge.parents[0]):
                # We're not yet back on the main branch. Just continue digging.
                commits_to_analyze.append(parent)
            else:
                # The parent is on the main branch. We're done digging.
                # print("found merge parent, stopping. final authors", authors)
                pass

    if ncommits == len(missing_items):
        # None of the commits found had a release note. List them.
        for item in missing_items:
            missing_release_notes.append(item)

    text = repo.git.diff(merge_base.hexsha, tip.hexsha, '--', numstat=True)
    stats = Stats._list_from_string(repo, text)

    collect_item(pr, note, merge.hexsha[:shamin], ncommits, authors, stats.total, merge.committed_date)

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

    history = per_group_history.get(item['authors'], [])
    history.append(item)
    per_group_history[item['authors']] = history

def analyze_standalone_commit(commit):
    # Some random out-of-branch commit. Let's not forget them.
    authors = collect_authors(commit)
    title = commit.message.split('\n',1)[0].strip()
    item = makeitem('#unknown', title, commit.hexsha[:shamin], authors)
    missing_release_notes.append(item)
    collect_item('#unknown', title, commit.hexsha[:shamin], 1, authors, commit.stats.total, commit.committed_date)

while commit != firstCommit:
    spin()

    ctime = datetime.datetime.fromtimestamp(commit.committed_date).ctime()
    numbermatch = merge_numbers.search(commit.message)
    # Analyze the commit
    if numbermatch is not None:
        prs = numbermatch.group("numbers").strip().split(" ")
        for pr in prs:
            print("                                \r%s (%s) " % (pr, ctime), end='', file=sys.stderr)
            analyze_pr(commit, pr)
    else:
        print("                                \r%s (%s) " % (commit.hexsha[:shamin], ctime), end='', file=sys.stderr)
        analyze_standalone_commit(commit)

    if len(commit.parents) == 0:
        break
    commit = commit.parents[0]

allgroups = list(per_group_history.keys())
allgroups.sort(key=lambda x:x.lower())

print("\b\nComputing first-time contributors...", end='', file=sys.stderr)

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

if not hideheader:
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
</div>
""")

    print("""### Docker image

~~~shell
docker pull cockroachdb/cockroach:""" + current_version + """
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
        authors = item['authors']
        print("- [%(pr)s][%(pr)s] [%(sha)s][%(sha)s] %(title)s" % item, "(%s)" % authors)
        seenshas.add(item['sha'])
        seenprs.add(item['pr'])
    print()

## Print the Doc Updates section.
print("### Doc updates")
print()
print("Docs team: Please add these manually.")
print()

## Print the Contributors section.
print("### Contributors")
print()
print("This release includes %d merged PR%s by %s author%s." %
      (len(allprs), len(allprs) != 1 and "s" or "",
       len(individual_authors), (len(individual_authors) != 1 and "s" or ""),
      ))

ext_contributors = individual_authors - crdb_folk

notified_authors = sorted(set(ext_contributors) | set(firsttime_contributors))
if len(notified_authors) > 0:
        print("We would like to thank the following contributors from the CockroachDB community:")
        print()
        for person in notified_authors:
            print("-", person, end='')
            if person in firsttime_contributors:
                annot = ""
                if person in crdb_folk:
                    annot = ", CockroachDB team member"
                print(" (first-time contributor%s)" % annot, end='')
            print()
print()

## Print the per-author contribution list.
if not hidepercontributor:
    print("### PRs merged by contributors")
    print()
    if not hideshas:
        fmt = "  - %(date)s [%(pr)-6s][%(pr)-6s] [%(sha)s][%(sha)s] (+%(insertions)4d -%(deletions)4d ~%(lines)4d/%(files)2d) %(title)s"
    else:
        fmt = "  - %(date)s [%(pr)-6s][%(pr)-6s] (+%(insertions)4d -%(deletions)4d ~%(lines)4d/%(files)2d) %(title)s"

    for group in allgroups:
        items = per_group_history[group]
        print("- %s:" % group)
        items.sort(key=lambda x:x[sortkey],reverse=not revsort)
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
