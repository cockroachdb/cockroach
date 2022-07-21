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

An RFC can help you get buy-in and alignment from necessary stakeholders
on how to best approach your project. Such alignment helps ensure that
by the time the team starts the implementation phase, your design &
approach are sound & have been properly vetted.

A few good questions to ask yourself before writing an RFC are:
1. Does my project introduce new/modify existing architecture that is
   significant to the operations of the database?
1. Will my project have a broad impact on sensitive parts of the product,
   where mistakes or poor design decisions could have a large negative
   impact?

If the answer to either of these questions is a resounding "yes", an RFC
may very well be appropriate. As mentioned before, whether you should
write an RFC is a nuanced topic. When appropriate, an RFC can be a very
effective tool. However, the process can sometimes be slow & come with a
good amount of toil. People need time to respond, edits need to be made,
prototypes need to be modified, and this cycle may repeat multiple times.
Therefore, it's worth giving this question of whether or not to write an
RFC the proper consideration. If it is indeed appropriate, then the RFC
process will be a great tool for strengthening the design and concepts
behind your project.

It is encouraged to develop a [prototype](#prototyping) concurrently
with writing the RFC. One of the significant benefits of an RFC is
that it forces bigger picture thinking which reviewers can then
dissect. In contrast, a prototype forces the details to be considered,
shedding light on the unknown unknowns and helping to ensure that the
RFC focuses on the important design considerations.

If you've never written an RFC before, consider partnering with a more 
experienced engineer for guidance and to help shepherd your RFC through 
the process.

# Keep It Concise

An RFC should be a high-level description which does not require
formal correctness. There is utility in conciseness. Do not
overspecify the details in the RFC as doing so can bury the reviewer
in minutiae.<sup>[1](#sql-syntax)</sup> A [prototype](#prototyping) 
can be of assistance here as it can help highlight the tricky areas 
that deserve mention in a high-level description. If the details are 
relevant, the RFC can include a link to the prototype (which may 
necessitate cleaning up some of the prototype code at the reviewer's 
request). Note that one of the significant benefits of an RFC is that
it forces bigger picture thinking which readers can then dissect. In 
this respect an RFC is complimentary to a prototype which forces details 
to be considered.

# Present a Narrative

When writing your RFC, you should always keep the prospective reader
in mind. We already mentioned above that one should do their best to
[keep things concise](#keep-it-concise), but what more can you do to
make the life of your reader easier?

As engineers & scientists, it can be tempting to use a large pile of
facts to state your case. After all, the facts don't lie, and if we
present enough of them in support of our cause, it helps strengthen
our argument. But is the reader going to enjoy reading a long laundry
list of facts? Probably not. However, if you are able to weave these
facts and technical designs together with a strong, singular narrative,
your writing is more likely to resonate with the reader.

Therefore, when writing your RFC, try to present your ideas with an
overarching narrative. Be clear about the problem(s) you're trying to
solve. Consider presenting some real-world examples to help make things
more relatable. Do your best to structure your writing in an eloquent way. 
Taking such steps will help keep the reader engaged. This will likely
lead to your RFC being better received and gaining more attention, which 
means you'll receive more feedback (a good thing!).

# Prototyping

There is a puzzle at the heart of writing an RFC: if you understand a
project well enough to write an RFC, then an RFC may not be
necessary. Certainly you may write an RFC to communicate your
knowledge to others, but how does an RFC get written by an individual
who doesn't understand the problem fully?

### Iterative Writing

The idealistic view of writing an RFC involves an engineer starting
with a blank document and filling in the pieces from start to
finish. For anyone who has written an RFC, this fairy tale depiction
is clearly not accurate. The writing is iterative. You might first
write down a few bullet points and then start thinking about the
details of one area, fleshing out the document incrementally and in an
unordered fashion. There are very likely many (many) iterations of the
document before anyone else takes a look at it.

The iterative writing of an RFC described above is often insufficient
because simply thinking deeply about a problem is frequently not the
most efficient path to fully understanding the problem. At the very
least you will be consulting the existing code base to understand how
the proposed change fits. And you might discuss the problem in front
of a whiteboard with a colleague. It is very likely you should be
prototyping.

### The Role of Prototyping

Prototyping involves exploring a problem space in order to better
understand it. The primary value of a prototype is in the learning it
provides, not the code. As such, the emphasis during prototyping
should be on speed of learning. The prototype code does not need to
meet any particular quality metric. Error handling? Ignore it while
you're trying to learn. Comments? Only if you, the prototype author,
need them. The real focus of the prototype is exploring the unseen
corners of the problem space in order to reveal where the dragons are
lurking. The goal is to de-risk the problem space by thoroughly
understanding the hardest problems.

### Write, Prototype, Repeat 

The acts of prototyping and writing an RFC should be iterative. Many
engineers want to write down their thoughts before coding, and it is
common to want to discuss an approach before embarking on it. Both
approaches are useful, yet neither should be confused for an RFC. It
is useful to write down a few bullet points for areas to be covered in
a prototype. Then work on the code. Then add more bullet points. At
some point, the prototype will be fleshed out enough that you feel
ready to write the RFC. Alternately, it is very common for the
prototype to run into a significant stumbling block that you can't
overcome yourself. The benefit of the prototype truly shines when this
happens and you'll have a deep understanding of the problem which you
can use to have a focused discussion with other engineers. Share your
failed prototypes. The failure implies learning and that learning
deserves to be shared, both in words and in code. Your failed
prototype might be close to success with the aid of another engineer's
experience. At the very least, a failed prototype indicates to other
engineers that an idea has been explored.

### Consider if the Prototype is Sufficient

Writing an RFC is not a necessary outcome for a successful
prototype. If the prototype is simple enough, don't be afraid to translate
it directly into a PR or series of PRs along with explanatory comments
and commit messages.

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
