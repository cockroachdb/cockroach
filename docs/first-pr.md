# Submitting your first PR

*Original author: Nikhil Benesch <<benesch@cockroachlabs.com>>*

This document is a long-winded guide to preparing and submitting your first
contribution to CockroachDB. It's primarily intended as required reading for new
Cockroach Labs engineers, but may prove useful to external contributors too.

**Table of contents**
+ [The development cycle](#the-development-cycle)
  + [First things first](#first-things-first)
  + [Repository layout](#repository-layout)
  + [Other important repositories](#other-important-repositories)
  + [Internal documentation](#internal-documentation)
  + [Start coding!](#start-coding)
  + [Is there a Go debugger?](#is-there-a-go-debugger)
  + [A note on adding a new file](#a-note-on-adding-a-new-file)
+ [When to submit](#when-to-submit)
  + [Production-ready code](#production-ready-code)
+ [How to submit](#how-to-submit)
  + [Fix your style violations](#fix-your-style-violations)
  + [Where to push](#where-to-push)
  + [Split your change into logical chunks](#split-your-change-into-logical-chunks)
  + [Who to request a review from](#who-to-request-a-review-from)
  + [Write your PR description](#write-your-pr-description)
  + [TeamCity](#teamcity)
+ [The impending code review](#the-impending-code-review)
  + [Reviewable](#reviewable)
  + [Addressing feedback](#addressing-feedback)
  + [CR Jargon](#cr-jargon)
  + [Merging](#merging)
+ [First PR checklist](#first-pr-checklist)

## The development cycle

At the time of this writing, CockroachDB is several hundred thousand lines of Go
code, plus a smattering of C++ and TypeScript. Especially if you've never worked
with Go, this can be daunting. This section is a whirlwind tour of what lives
where, but you shouldn't expect all this information to permanently sink in
until well after your first month.

### First things first

Since CockroachDB is open source, most of the instructions for hacking on
CockroachDB live in this repo, [cockroachdb/cockroach]. You should start by
reading [CONTRIBUTING.md] in full. This file covers how to set up a new
development machine to build CockroachDB from source and the basics of the code
review workflow.

Then, look at [STYLE.md] and the [Google Go Code
Review](https://code.google.com/p/go-wiki/wiki/CodeReviewComments) guide it
links to. These are linked to from [CONTRIBUTING.md], but they're easy to miss.
If you haven't written any Go before CockroachDB, you may want to hold off on
reviewing the style guide until you've written your first few functions in go.

### Repository layout

Here's what's in each top-level directory in this repository:

  + **`build/`**
    Build support scripts. You'll likely only need to
    interact with [build/builder.sh]. See ["Building on Linux"] below.

  + **`c-deps/`**
    Glue to convince our build system to build non-Go dependencies. At the time
    of writing, "non-Go dependencies" means C or C++ dependencies. The most
    important of these is [RocksDB], our underlying persistent key-value store.

  + **`cloud/kubernetes/`**
    Kubernetes configuration to auto-launch CockroachDB clusters.

  + **`docs/`**
    Documentation for CockroachDB developers. See ["Internal
    documentation"] below.

  + **`monitoring/`**
    Configuration to integrate monitoring frameworks, namely Prometheus and
    Grafana, with CockroachDB. This configuration powers our internal monitoring
    dashboard as well.

  + **`pkg/`**
    First-party Go code. See ["Internal documentation"] below for details.

  + **`scripts/`**
    Handy shell scripts that aren't part of the build process. You'll likely
    interact with [scripts/azworker.sh] most, which spins up a personal Linux VM
    for you to develop on in the Azure cloud.

  + **`vendor/`**
    A Git submodule that contains the right version of our dependent libraries.
    For many years, the Go answer to dependency management was "write
    backwards-compatible code." That strategy is as dangerous as it sounds, so
    since v1.6, Go will automatically load packages in a subdirectory named
    `vendor`, if it exists. See [build/README.md] for details on how we maintain
    this folder.

### Other important repositories

Besides [cockroachdb/cockroach], the [cockroachdb] GitHub organization is
home to several other important open-source components of Cockroach:

* **[cockroachdb/docs]**, which houses the code behind our user-facing
  documentation at <https://cockroachlabs.com/docs>. At the time of writing, our
  stellar docs team handles essentially all documentation.

* **[cockroachdb/examples-go]**, which contains small, self-contained Go
  programs that exercise CockroachDB via the PGWire protocol. You're likely to
  hear most about [block_writer], which writes uniformly random values into a
  table, and [photos], which simulates a more-realistic workload of a
  photo-sharing site, where some photos and users are orders of magnitude more
  popular. The other example programs are of a similar scope and purpose, but
  block_writer and photos are deemed important enough to run constantly against
  our production clusters.

* **[cockroachdb/loadgen]**, which contains the next generation of CockroachDB
  load testing. These include the industry-standard TPC-H and YCSB benchmarks
  (Google for details), as well as kv, version of block_writer that can target
  databases other than CockroachDB.

* **[cockroachdb/examples-orms]**, which showcases ORMs a toy API that uses an
  ORM to prepare its responses in several different languages.

Most of the remaining repositories under the [cockroachdb] organization are forks
of existing Go libraries with some small, Cockroach-specific patches.

### Internal documentation

Documentation on the first-party Go packages that make up CockroachDB is, as of
this writing, essentially nonexistent. This is par for the course with code
that's evolving as quickly as Cockroach, but it's something we're hoping to
improve over time, especially as internal packages stabilize.

The internal documentation that we *do* have lives in
[cockroachdb/cockroach/docs](https://github.com/cockroachdb/cockroach/tree/master/docs).
At the time of writing, most of this documentation covers the high-level
architecture of the system. Only a few documents hone in on specifics, and even
those only cover the features that were found to cause significant developer
frustration. For most first-party packages, you'll need to read the source for
usage instructions.

> **Protip:** If you prefer Go-generated HTML documentation to reading the
source directly, take a look at
<https://godoc.org/github.com/cockroachdb/cockroach>.

For our internal docs, I recommend the following reading order.

First, browse through the [design document], which describes the architecture of
the entire system at the highest possible level. You'll likely find there's too
much information here to digest in one sitting: you should instead strive to
remember what topics are covered, so you can refer to it later with more
specific questions in mind.

Then, look through the [docs/tech-notes] folder and determine if any of the tech
notes are relevant to your starter project. Again, you'll likely find that the
tech notes contain too much information to process, so instead try to identify
the sections that are likely to be useful as you make progress on your starter
project.

The one exception to this rule is [docs/tech-notes/contexts.md]. It's worth
learning why so many of our function signatures take a context as their first
parameter, like so:

```
func doOperation(ctx context.Context, ...)
```

Otherwise, plumbing contexts everywhere will feel like a chore with no upsides.

Finally, I feel obligated to reproduce this disclaimer from the tech notes README:

> Standard disclaimer: each document contains parts from one or more author.
> Each part was authored to reflect its author's perspective on the project at
> the time it was written. This understanding is necessarily subjective: its
> context is both the state of the project and the authors', and their
> reviewers', experience around that particular date. In case of doubt, consult
> your local historian and your repository's timeline.

In short, our documentation is not authoritative. Trust your reading of the code.

### Start coding!

It's time to fire up your editor and get to work! The [CONTRIBUTING.md] document
does a good job of describing the basic development commands you'll need. In
short, you'll use `make` to drive builds.

To produce a `cockroach` binary:

```bash
$ make build
```

To run all the tests in `./pkg/rest/of/path`:

```bash
$ make test PKG=./pkg/rest/of/path
```

### Is there a Go debugger?

Not a good one, sadly. Most of us debug single-cluster issues by littering
`log.Infof`s throughout relevant files.

On Linux, GDB is supposedly reasonably functional; on macOS, building LLDB from
source also yields a reasonably-functional debugger. Come talk to Nikhil if you
think you'd find this helpful, and we can work on codifying the steps.

If you're debugging an issue that spans several nodes, you'll want to look into
"distributed tracing." Ask your Roachmate to either walk you through
OpenTracing/LightStep or point you at someone who can.

### A note on adding a new file

You'll notice that all source files in this repository have a license
notification at the top. Be sure to copy this license into any files that you
create.

## When to submit

Code at Cockroach Labs is not about perfection. It's too easy to misinterpret
"build it right" as "build it perfectly," but striving for perfection can lead
to over-engineered code that took longer than necessary to write. Especially as
a startup that moves quickly, building it right often means building the
simplest workable solution, then iterating as necessary. What we try to avoid at
Cockroach Labs is the quick, dirty, gross hack—but even that can be acceptable
to plug an urgent leak.

So, you should get feedback early and often, while being cognizant of your
reviewer's time. You don't want to ask for a detailed review on a rough draft,
but you don't want to spend a week heading down the wrong path, either.

You have a few options for choosing when to submit:

1. You can open a PR with an initial prototype labeled `[DO NOT MERGE]`. You
   should also assign the `do-not-merge` GitHub label to your PR. This is useful
   when you're not entirely sure who should review your code.

2. Especially on your first PR, you can do everything short of opening a GitHub
   PR by sending someone a link to your local branch. Alternatively, open a PR
   against your fork. This won't send an email notification to anyone watching
   this repository, so it's a good way to let your Roachmate (or the lead
   reviewer on your project). This works well when you have a reviewer in mind
   and you want to avoid the onslaught of

3. For small changes where the approach seems obvious, you can open a PR with
   what you believe to be production-ready or near-production-ready code. As you
   get more experience with how we develop code, you'll find that larger and
   larger changesets will begin falling into this category.

PRs are assumed to be production-ready (option 3) unless you say otherwise in
the PR description. Be sure to note if your PR is a WIP to save your reviewer
time.

If you find yourself with a PR (`git diff --stat`) that exceeds roughly 500 line
of code, it's time to consider splitting the PR in two, or at least introducing
multiple commits. This is impossible for some PRs—especially refactors—but "the
feature is only partially complete" should never be a reason to balloon a PR.

One common approach used here is to build features up incrementally, even if
that means exposing a partially-complete feature in a release. For example,
suppose you were tasked with implementing support for a new SQL feature, like
supporting subqueries in UPDATE statements. You might reasonably submit four
separate PRs. First, you could land a change that adjusts the SQL grammar and
links it to a dummy implementation that simply outputs an error like "subqueries
in UPDATE not supported" instead of "syntax error." Then, you might land a
change to implement a naive version of the feature, with tests to verify its
correctness. A third PR might introduce some performance optimizations, and a
fourth PR some refactors that you didn't think of until a few weeks later. This
approach is totally encouraged! It's no problem if the first PR (the one that
simply prints "not implemented") lands in an unstable release, as long as a
change isn't backing you into a corner or causing server panics. Code produced
from an incremental approach is *much* easier to review, which means better,
more robust code.

### Production-ready code

In general, production-ready code:

* Is free of syntax errors and typos

* Omits accidental code movement, stray whitespace, and stray debugging statements

* Documents all exported structs and functions

* Documents internal functions that are not immediately obvious

* Has a well-considered design

That said, don't be embarrassed when your review points out syntax errors, stray
whitespace, typos, and missing docstrings! That's why we have reviews. These
properties are meant to guide you in your final scan.

## How to submit

So, you've written your code and written your tests. It's time to send your code
for ~slaughter~ review.

### Fix your style violations

First, read [STYLE.md] again, looking for any style violations. It's easier to
remember a style rule once you've violated it.

Then, run our suite of linters:

```bash
$ make lint
```

This is not a fast command. On my machine, at the time of writing, it takes
about a full minute to run. You can instead run

```bash
$ make lintshort
```

which clocks in at about 30s by omitting the slowest linters.

### Where to push

In the interest of branch tidiness, we ask that even contributors with the
commit bit avoid pushing directly to this repository. That deserves to be
properly called out:

> **Protip:** don't push to cockroachdb/cockroach directly!

Instead, create yourself a fork on GitHub. This will give you a semi-private
personal workspace at github.com/YOUR-HANDLE/cockroach. Code you push to your
personal fork is assumed to be a work-in-progress (WIP), dangerously broken, and
otherwise unfit for consumption. You can view @bdarnell's WIP code, for example, by
browsing the branches on his fork, <https://github.com/bdarnell/cockroach/>.

Then, you'll need to settle on a name for your branch, if you haven't already.
This isn't a hard-and-fast rule, but Cockroach Labs convention is to
hyphenate-your-feature. These branch identifiers serve primarily to identify *to
you*.

To give you a sense, here's a few feature branches that had been merged in
recently when this document was written, and their associated commit summaries:

* knz/default-column: `sql: ensure that DEFAULT exprs are re-evaluated during backfill.`

* dt/fk: `sql: show only constrained cols in fk`

It's far more important to give the commit message a descriptive
title and message than getting the branch name right.

### Split your change into logical chunks

Be kind to other developers, including your future self, by splitting large
commits. Each commit should represent one logical change to the codebase with a
descriptive message that explains the rationale for the change.
[CONTRIBUTING.md] has some advice on writing good commit messages: on your first
PR, it's worth re-reading that section to ensure your commit messages follow the
guidelines.

Most importantly, all commits which touch this repository should have the format
`pkg: message`, where `pkg` is the name of the package that was most affected by
the change. A few engineers still "watch" this repository, which means they
receive emails about every issue and PR. Having the affected package at the
front of every commit message makes it easier for these brave souls to filter
out irrelevant emails.

> **Protip:** often, you'll realize after a bout of hacking that you've actually
> made *n* separate changes, but you've got just one big diff since you didn't
> commit any intermediate work. Explaining how to fix this is out of scope for
> this guide, but either Git "patch mode" (Google it) or a graphical Git client
> will take care of it.

See also the ["When to submit"] section above for advice on when multiple
commits should be further split into multiple PRs.

### Who to request a review from

Deciding who should review a PR requires context that you just won't have in
your first month. For your starter project, your Roachmate is the obvious
choice. For little bugs that you fix along the way, ask your Roachmate or your
neighbor if there's someone who's obviously maintaining a particular area of the
codebase. (E.g., if you had a question about the code that interfaces with
RocksDB, I would immediately direct you to [@petermattis].)

If you're unable to get a reviewer recommendation, the "Author" listed at the
top of the file may be your best bet. You should check that the hardcoded author
agrees with the Git history of the file, as the hardcoded author often gets
stale. Use `git log FILE` to see all commits that have touched a particular file
or directory sorted by most recent first, or use `git blame FILE` to see the
last commit that touched a particular line. The GitHub web UI or a Git plugin
for your text editor of choice can also get the job done. You might also try
`scripts/authors.sh FILE`, which will show you a list of contributors who have
touched the file, sorted by the number of commits by that author that touched
the file. If there's a clear winner, ask them for a review; if they're not the
right reviewer, they'll suggest someone else who is. In cases that are a bit
less clear, you may want to note in a comment on GitHub that you're not
confident they're the right reviewer and ask if they can suggest someone better.

If you're still unsure, just ask in chat! You'll usually get a response in under
a minute.

It's important to have a tough reviewer. In the two months I've been here, I've
seen a few PRs land that absolutely *shouldn't* have landed, each time because
the person who understood why the PR was a bad idea wasn't looped into the
review. There's no shame in this—it happens! I say this merely as a reminder
that a tough review is a net positive, as it can save a lot of pain down the
road.

Also, note that GitHub allows for both "assignees" and "reviewers" on a pull
request. It's not entirely clear what the distinction between these fields is,
and we're currently split at Cockroach Labs on what we prefer. Choose the field
you like most and move on. Ever since GitHub began auto-suggesting reviewers,
there seems to be a slight preference for using the reviewer field instead of
the assignee field.

### Write your PR description

Nearly everything you're tempted to put in a PR description belongs in a commit
message. (As a corollary, nearly everything you put in a commit message belongs
in a code comment.)

You should absolutely if you're looking for a non-complete review—i.e., if your
code is a WIP. Otherwise, something simple like "see commit messages for
details" is good enough. On PRs with just one commit, GitHub will automatically
include that one commit's message in the description; it's totally fine to leave
that in the PR description.

### TeamCity

We run our own continuous integration server called TeamCity, which runs unit
tests and acceptance tests against all open pull requests.

GitHub displays the status of the latest TeamCity run at the bottom of every
pull request. You can click the "Details" link to get insight into a failed
build or view real-time status of an in-progress build. Occasionally, a build
will fail because of flaky tests. You can verify by running a new build and
seeing if the problem disappears; just hit the "Run" button on the top right of
the page GitHub links to queue a new build. Less frequently, TeamCity will
entirely fail to notice a PR, and GitHub will display "waiting for status to be
reported" forever. The ["TeamCity Continuous Integration" wiki page] describes
how to fix this.

Unfortunately, a full tutorial on TeamCity is outside the scope of this
document. Ask your Roachmate for an overview if you're confused, or post
specific questions in #teamcity.

That's it! You're done. Sit back and get ready for the impending code review.

## The impending code review

We take code reviews very seriously at Cockroach Labs. Please don't be deterred
if you feel like you've received some hefty feedback. That's completely normal
and expected—and, if you're an external contributor, we very much appreciate
your contribution!

In this repository in particular, merging a PR means accepting responsibility
for maintaining that code for, quite possibly, the lifetime of CockroachDB. To
take on that reponsibility, we need to ensure that meets our strict standards
for [production-ready code].

No one is expected to write perfect code on the first try. That's why we have
code reviews in the first place!

## By humans for humans

Probably everyone has at some time had an unpleasant interaction during a code
review, and with code reviews being [by humans for humans], one or both of these
humans could be having a bad day. As if that weren't enough already, text-based
communication is fraught with miscommunication. Consider being a [minimally nice maintainer]),
reflect upon mistakes that will inevitably be made, and take inspiration from the
constructive and friendly reviews that are the daily staple of our repository.

### Reviewable

Except for the smallest of PRs, we eschew GitHub reviews in favor of a
third-party app called [Reviewable]. Reviewable has an incredibly dense UI: you
could probably master a new musical instrument faster than you could master
Reviewable.

Still, Reviewable is far better than GitHub for large changes that undergo
multiple rounds of feedback. The basic model is the same: a diff of your change
where your reviewers can leave inline notes. In Reviewable, unlike GitHub, every
comment spawns a new thread of discussion, and the UI will highlight any threads
that you haven't acknowledged/responded to.

Reviewable also takes great pains to handle force pushes. When you push a new
revision, Reviewable will do its best to match up comment threads to the
logically-equivalent location in the new diff. Even if this algorithm fails,
Reviewable records each force push as a "revision," and will allow you and your
reviewers to track how your PR has evolved as you address review feedback.

You won't need to do anything special to enable Reviewable for your PR; one of
our bots will be along shortly after the PR is opened to post a link to the
review interface.

### Addressing feedback

If someone leaves line comments on your PR without leaving a top-level "looks
good to me" (LGTM), it means they feel you should address their line comments
before merging. That is, without an LGTM, all comments are roughly in the
discussing disposition, even if the Reviewable disposition marker in the bottom
right corner hasn't been set as such. If you agree with their feedback, you
should make the requested change; if you disagree, you must leave a comment with
your counterargument and wait to hear back.

If someone leaves a top-level LGTM but also leaves line comments, you can assume
all line comments are in the satisfied disposition, even if the Reviewable
toggle hasn't been set as such. If you disagree with any of their feedback, you
should still let them know (e.g., "actually, I think my way is better because
reasons"), but you don't need to wait to hear back from them to merge.

You may see comments labeled as "optional" to indicate a one-off satisfied
disposition when the comment default is discussing (i.e., you haven't yet
received an LGTM).

Similarly, you may see an "LGTM modulo the comments" to indicate that you're
free to merge provided that you make the requested changes. If you do disagree
with the feedback, then you need to voice your counterargument and wait for the
reviewer to respond.

Rarely, someone will express a sentiment like "I feel very strongly that we
shouldn't merge this." Disagreements like these are often easier to resolve
outside of Reviewable, via an in-person discussion or via chat.

As you gain confidence in Go, you'll find that some of the nitpicky style
feedback you get does not make for obviously better code. Don't be afraid to
stick to your guns and push back. Much of coding style is subjective. As I was
learning Go, though, I found it helpful to try all style suggestions. For the
first month, essentially every style suggestion I received made for far more
idiomatic Go.

Once you've accumulated a set of changes you need to make to address review
feedback, it's time to force-push a new revision. Be sure to amend your prior
commits—you don't want to tack on a bunch of fixup commits to address review
feedback. If you've split your PR into multiple commits, it can be tricky to
ensure your review feedback changes make it into the right commit. A tutorial on
how to do so is outside the scope of this document, but the magic keywords to
Google for are "git interactive rebase."

You should respond to all unresolved comments whenever you push a new revision
or before you merge, even if it's just to say "Done."

> **Protip:** Wait to publish any "Done" comments until you've verified that
> your changes have been force-pushed and show up properly in Reviewable. It's
> very easy to preemptively draft a "Done" comment as you scroll through
> feedback, but then forget to actually make the change.

### CR Jargon

These are the acronyms you'll frequently encounter during code reviews.

* **CR**, "code review"
* **PR**, "pull request"—you probably knew that one.
* **PTAL**, "please take another look"
* **RFAL**, "ready for another look"—as in, I made some changes, PTAL.
* **LGTM**, "looks good to me"—i.e., ship it!
* 👩‍🔬🐶 , "science dog"—i.e., I have no idea what this code does.
* **TF[YT]R**, "thanks for your/the review"

A list of even more Cockroach jargon lives on [this repository's wiki].

### Merging

*External contributors: you don't need to worry about this section. We'll merge
your PR as soon as you've addressed all review feedback!*

To merge code into any CockroachDB repository, you need at least one LGTM. Some
LGTMs matter more than others; you want to make sure you solicit an LGTM from
whoever "owns" the code you're touching.

Occasionally, someone will jump into a review with some minor style nits and
leave an LGTM once they see you've addressed the nits; you should still wait for
the primary reviewer you selected to LGTM the merits of the design. To confuse
matters, sometimes you'll get a drive-by review from someone who was as
qualified or more qualified than the primary reviewer. In that case, their LGTM
is merge-worthy. Usually it's clear which situation you're dealing with, but if
in doubt, ask!

Once you've gotten an LGTM from who you think is the right person, don't be
afraid to merge. The git revert command exists for a reason. You can expect to
revert at least one PR that you land in your first three months.

When your PR is ready to go, request a merge from our build bot Craig. Craig is
a [Bors merge bot], whose only job is to be gatekeeper to the repository. Add a
comment on the PR of the form `bors r=<reviewer>`, replacing `<reviewer>` with
the GitHub username of the person who gave you the LGTM. Once approved, your PR
will be batched up with other PRs approved around the same time. Craig will try
to build them as though they were merged to the target branch, and if
successful, will merge them. This limits your exposure by ensuring you don't
accidentally merge a commit with an obviously broken build or merge skew.

We call merging locally and pushing to master the "nuclear option" or "god
merging." It's not disabled, but you shouldn't do it unless the repo is on fire.

Congratulations on landing your first PR! It only gets easier from here.

# First PR checklist

Here's a checklist of action items to keep you sane:

+ [ ] Developing
    + [ ] Create a GitHub fork of cockroachdb/cockroach
    + [ ] Read through [CONTRIBUTING.md]
+ [ ] Submitting your first PR
    + [ ] Push to a feature branch on your personal fork
    + [ ] Verify you've followed [CONTRIBUTING.md]
    + [ ] Verify you've followed [STYLE.md]
    + [ ] Ensure files you've added, if any, have a license block
    + [ ] Run make check
    + [ ] Split your change into logical commits with good messages
    + [ ] Avoid writing a PR description
+ [ ] Addressing feedback
    + [ ] Amend the appropriate commits and force-push
    + [ ] Respond to all feedback with "Done" or a counterargument
+ [ ] Merging
    + [ ] Comment `bors r=reviewer` to ask Craig to merge

["Building on Linux"]: #building-on-linux
["Internal documentation"]: #internal-documentation
["TeamCity Continuous Integration" wiki page]: https://github.com/cockroachdb/cockroach/wiki/TeamCity-Continuous-Integration
["When to submit"]: #when-to-submit
[@bdarnell]: https://github.com/bdarnell
[@petermattis]: https://github.com/petermattis
[block_writer]: https://github.com/cockroachdb/examples-go/tree/master/block_writer
[build/README.md]: /build/README.md
[cockroachdb]: https://github.com/cockroachdb
[cockroachdb/cockroach]: https://github.com/cockroachdb/cockroach
[cockroachdb/docs]: https://github.com/cockroachdb/docs
[cockroachdb/examples-go]: https://github.com/cockroachdb/examples-go
[cockroachdb/examples-orms]: https://github.com/cockroachdb/examples-orms
[cockroachdb/loadgen]: https://github.com/cockroachdb/loadgen
[CONTRIBUTING.md]: /CONTRIBUTING.md
[design document]: /docs/design.md
[docs/tech-notes]: /docs/tech-notes
[docs/tech-notes/contexts.md]: /docs/tech-notes/contexts.md
[photos]: https://github.com/cockroachdb/examples-go/tree/master/photos
[production-ready code]: #production-ready-code
[Reviewable]: https://reviewable.io
[RocksDB]: http://rocksdb.org
[STYLE.md]: /STYLE.md
[this repository's wiki]: https://github.com/cockroachdb/cockroach/wiki/Jargon
[by humans for humans]: https://mtlynch.io/human-code-reviews-1/
[minimally nice maintainer]: https://brson.github.io/2017/04/05/minimally-nice-maintainer
[Bors merge bot]: https://github.com/cockroachdb/cockroach/wiki/Bors-merge-bot
