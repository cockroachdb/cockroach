# Prototyping and RFCs

There is a puzzle at the heart of writing an RFC: if you understand a
project well enough to write an RFC, then an RFC may not be
necessary. Certainly you may write an RFC to communicate your
knowledge to others, but how does an RFC get written by an individual
who doesn't understand the problem fully?

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

The acts of prototyping and writing an RFC should be iterative. Many
engineers want to write down their thoughts before coding and it is
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

An RFC is a high-level description which does not require formal
correctness. There is utility in conciseness. Do not overspecify the
details in the RFC as doing so can bury the reader in minutiae. The
prototype can be of assistance here as it can help highlight the
tricky areas that deserve mention in a high-level description. If the
details are relevant, the RFC can include a link to the prototype
(which may necessitate cleaning up some of the prototype code at the
reviewer's request). Note that one of the significant benefits of an
RFC is that it forces bigger picture thinking which readers can then
disect. In this respect an RFC is complimentary to a prototype which
forces details to be considered.

Writing an RFC is not a necessary outcome for a successful
prototype. If the prototype is simple enough, it can be translated
directly into a PR or series of PRs along with explanatory comments
and commit messages.
