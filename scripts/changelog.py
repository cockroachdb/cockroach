#! /usr/bin/env python3
#
# Show a compact ChangeLog representation of a range of Git commits.
#
# Example use: changelog.py -f v1.1-alpha.20170817
#
# Requires: GitPython https://pypi.python.org/pypi/GitPython/

import sys
import itertools
from git import Repo
from optparse import OptionParser

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
    print("Commit range is empty!")
    print(parser.get_usage())
    exit(0)

# Is the first commit reachable from the current one?
check = commit
while check != firstCommit:
    if len(check.parents) == 0:
        print("error: origin commit %s not in history of %s" %( options.from_commit, options.until_commit))
        exit(1)
    check = check.parents[0]

print("Changes %s ... %s" % (firstCommit, commit))

author_aliases = {
    'kena': "Raphael 'kena' Poss",
    'vivekmenezes': "Vivek Menezes",
    'RaduBerinde': "Radu Berinde",
    'marc': "Marc Berhault",
    'a6802739': "Song Hao",
}

def collect_commits(commits, author):
    ncommits = 0
    otherauthors = set()
    for commit in commits:
        if commit.message.startswith("Merge pull request"):
            continue
        ncommits += 1
        if commit.author.name != 'GitHub' and author_aliases.get(commit.author.name, commit.author.name) != author:
            otherauthors.add(commit.author.name)
        if commit.committer.name != 'GitHub' and author_aliases.get(commit.committer.name, commit.committer.name) != author:
            otherauthors.add(commit.committer.name)

        n, a = collect_commits(list(commit.parents), author)
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
            print("\b..", end='')
        print("\b", end='')
        print(next(spinner),  end='')
        sys.stdout.flush()

    # Analyze the commit
    if commit.message.startswith("Merge pull request"):
        author = (commit.author.name, commit.author.email)
        lines = commit.message.split('\n', 3)
        pr = lines[0].split(' ', 4)[3]
        title = lines[2]

        ncommits, otherauthors = collect_commits(list(commit.parents), author_aliases.get(author[0], author[0]))

        stats = commit.stats.total
        item = {
            'title': title,
            'pr': pr,
            'sha': commit.hexsha[:5],
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

print("\b\n")
allauthors = list(per_author_history.keys())
allauthors.sort(key=lambda x:x[0].lower())

for author in allauthors:
    items = per_author_history[author]
    print("%s:" % author_aliases.get(author[0], author[0]))
    items.sort(key=lambda x:x[sortkey],reverse=not revsort)
    for item in items:
        print("\t%(pr)-6s %(sha)s (+%(insertions)4d -%(deletions)4d ~%(lines)4d/%(files)2d) %(title)s" % item, end='')
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
