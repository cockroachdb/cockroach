# About This Directory

This directory contains RFCs (design documents) that describe proposed
major changes to CockroachDB.

# The Why of RFCs

An RFC provides a high-level description of a major change or
enhancement to Cockroach. The high-level description allows a reviewer
to critique and poke holes in a design without getting lost in the
particulars of code.

An RFC is a form of communication aimed at both spreading and
gathering knowledge, though it is not the sole means of accomplishing
either task. Prototypes, tech notes, github issues, comments in code,
commit messages and in-person discussions are valid alternatives
depending on the situation.

At its best, an RFC clearly and concisely describes the high-level
architecture of a project giving confidence to all involved. At its
worst, an RFC focuses on unimportant details, fosters discussion that
stymies progress, or demoralizes the author with the complexity of
their undertaking.

# The When and How of RFCs

When to write an RFC is nuanced and there are no firm rules. General
guidance is to write an RFC before embarking on a significant or
complex project that will be spread over multiple pull requests (PRs),
and when multiple alternatives need to be considered and there is no
obvious best approach. A project involving multiple people is a good
signal an RFC is warranted. (Similarly, a project worthy of an RFC
often requires multiple engineers worth of effort). Note that this
guidance is intentionally vague. Many complex projects spread over
multiple PRs do not require an RFC, though they do require adequate
communication and discussion.<sup>[1](#sql-syntax)</sup>

It is encouraged to develop a [prototype](PROTOTYPING.md) concurrently
with writing the RFC. One of the significant benefits of an RFC is
that it forces bigger picture thinking which reviewers can then
disect. In contrast, a prototype forces the details to be considered,
shedding light on the unknown unknowns and helping to ensure that the
RFC focuses on the important design considerations.

An RFC should be a high-level description which does not require
formal correctness. There is utility in conciseness. Do not
overspecify the details in the RFC as doing so can bury the reviewer
in minutiae.<sup>[1](#sql-syntax)</sup> If you've never written an RFC
before consider partnering with a more experienced engineer for
guidance and to help shepherd your RFC through the process.

# RFC Process

1. Every RFC should have a dedicated reviewer familiar with the RFC's
   subject area.

2. Copy `00000000_template.md` to a new file and fill in the
   details. Commit this version in your own fork of the repository or
   a branch. Your commit message (and corresponding pull request)
   should include the prefix `rfc`. Eg: `rfc: edit RFC template`

   If you are a creative person, you may prefer to start with this blank
   slate, write your prose and then later check that all necessary topics
   have been covered.

   If you feel intimidated by a blank template, you can instead peruse
   the list of requested topics and use the questions in there as
   writing prompt.

   The list of topics and questions that can serve as writing guide
   can be found in the separate file [GUIDE.md](GUIDE.md).

3. Submit a pull request (PR) to add your new file to the main
   repository. Each RFC should get its own pull request; do not
   combine RFCs with other files.

   Note: you can send a PR before the RFC is complete in order to
   solicit input about what to write in the RFC. In this case, include
   the term "[WIP]" (work in progress) in the PR title and use the
   label `do-not-merge`, until you are confident the RFC is complete
   and can be reviewed.

4. Go through the PR review, iterating on the RFC to answer questions
   and concerns from the reviewer(s). The duration of this process
   should be related to the complexity of the project. If you or the
   reviewers seem to be at an impasse, consider in-person discussions
   or a prototype. There is no minimum time required to leave an RFC
   open for review. There is also no prohibition about halting or
   reversing work on an accepted RFC if a problem is discovered during
   implementation.

   Reviewers should be conscious of their own limitations and ask for
   other engineers to look at specific areas if necessary.

5. Once discussion has settled and the RFC has received an LGTM from
   the reviewer(s):

   - change the `Status` field of the document to `in-progress`;
   - rename the RFC document to prefix it with the current date (`YYYYMMDD_`);
   - update the `RFC PR` field;
   - and merge the PR.

6. Once the changes in the RFC have been implemented and merged,
   change the `Status` field of the document from `in-progress` to
   `completed`. If subsequent developments render an RFC obsolete,
   change its status to `obsolete`. When you mark a RFC as obsolete,
   ensure that its text references the other RFCs or PRs that make it
   obsolete.

# Footnotes

<a name="sql-syntax">1<a>: An exception to the general guidance on
when an RFC is warranted is that anything more than trivial changes
and extensions to SQL syntax require discussion within an
RFC. Additionally, SQL syntax is a detail that an RFC should mention.
