- Feature Name: Debug Snapshots
- Status: draft
- Start Date: 2017-09-27
- Authors: Andrew Dona-Couch
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issues: #16584, #14584

# Summary

Debugging issues in the field is a significant and growing part of the ongoing
development of CockroachDB.  This is supported by the `debug zip` command, which
provides a quick export of much debugging information from a cluster, allowing
us to investigate and diagnose issues without needing direct access.  This RFC
proposes an expansion of the data available in this export, as well as the
development of a tool based on the admin UI to help our team quickly navigate
the data exported.

# Motivation

We regularly ask users to run `cockroach debug zip` and send us the output, and
separately to take screenshots of the various time series graphs in the admin UI.
While this process has done okay till now, there are a few drawbacks that we'd
like to avoid:

- Not every debug page is exported.
- The screenshotting is entirely manual.
- It requires either knowing in advance which time series we care about or lots
  of scrolling and clicking to get all the screenshots.
- Users don't always capture the right screenshots, adding costly round trips.
- The result is a static view that isn't browsable.
- We don't get the underlying data, just a low-fidelity view of it.

By extending the existing `debug zip` command to export the time series data,
we can roll up all of the user's manual effort into a single command invocation,
and also make sure we get the data we need to solve their problem.

# Guide-level explanation

There are two main components in this debug system: the export command of the
Cockroach CLI, and the new browsing UI for the output of this command.

An export of useful debugging information can be made by running
`cockroach debug zip [filename]` (with the appropriate connection and
authentication flags for the cluster).  After a little while this will produce
the requested file which can be sent along to an engineer on support duty for
further debugging.  This should be the primary way a user gives us insight into
their cluster; we might even think about requesting it from the start in the
GitHub issue template (though it would need to be submitted separately).

The new browsing UI will provide an interface largely the same as the built-in
admin UI.  Rather than showing the metrics and configuration of a running
cluster, shows the data from the debug export.  This should be as simple as
specifying the source data file and then browsing the otherwise-familiar admin
UI pages.

# Reference-level explanation

Enhancements to the `debug zip` command come in two basic flavors:

- exporting more of the debug endpoints (e.g. problem ranges) and
- exporting time series data.

Adding additional debug endpoints to the zip file is a no-brainer.  It is a
straightforward continuation of the existing tooling, and so could probably be
done as a starter project or by an external contributor without much guidance.

Some candidate debug pages include:

- problem ranges,
- allocator,
- certs,
- raft debug,
- profiling pages:
  - heap dumps
  - gorouting dumps
- etc.

Exporting the time series data will require more thoughtful design, so that is
where the focus of the rest of this RFC will be.  The main questions are about
the scope of data exported and the format we write it to disk.  It seems
reasonable to write the time series data in the raw protobuf format that we use
to transfer it over the wire to the admin UI: this is an already-established
serialization format, and may make the UI changes easier.

The amount of data we want to export is an open question.  In the long-term, we
may want some amount of configurability, since some problems are not noticed as
soon as they occur.  However, most issues that we debug are noticed relatively
quickly, so for a first pass we could pick a time limit and just use that.

The other side of this project is building a tool to browse the exported data.
This should be as close to the experience of using the admin UI as possible - it
should feel to the support engineer as though they are looking at the admin UI
of the cluster in question as of the time the issue was identified.  For good
software engineering reasons, we should try to use the admin UI codebase as well.

Since this tool is primarily intended to be used by Cockroach Labs engineers, it
doesn't need to be built into the `cockroach` binary.  Of course it could be,
which would have a certain elegance, but if it would be easier to build as a
separate tool we should feel justified in going down that route.

A few specific changes need to be made to the admin UI code to support this:

- disabling ongoing data refresh
- changing the time period selection to work well with the recorded time period
- other changes as needed

The backend of this tool can be built in one of two ways.  Either we just spin
up a one-node cluster and import the zipped data, or we write a bespoke server
for this purpose.  There are reasons to think the either of these choices will
be faster than the other, but the evidence seems to lean towards writing a
bespoke tool for this purpose.  It wouldn't require us to add a bunch of
conditional code paths for the debugging endpoints, which could be fraught.  The
new tool would just implement the various gRPC endpoints, but backed by the
debug zip rather than the running cluster, as well as serving the admin UI code.

## Detailed design

TODO: detailed design

Outline both "how it works" and "what needs to be changed and in which order to get there."

Describe the overview of the design, and then explain each part of the
implementation in enough detail that reviewers will be able to
identify any missing pieces. Make sure to call out interactions with
other active RFCs.

## Drawbacks

This proposal will take a non-zero amount of developer effort.  However, one can
argue that it will, in the long run, save much more developer effort.

There will be some changes necessary to the main `cockroach` binary (more if we
build the browsing tool into the binary).  But, these won't be extensive.

Some users will be hesitant to send us a zip file of detailed information about
their running production cluster, and would be more comfortable with the current,
screenshot-driven system.  In that case, there's nothing stopping us from
continuing to use screenshots, though if it ends up being a significant fraction
of our users the development time will have been wasted.

## Rationale and Alternatives

### Automating screenshots

Using something like [this POC](https://github.com/tschottdorf/roacheteer), we
could automate the screenshot process, relieving the burden on the users.
However, we still only get a low-fidelity view of the data, giving us less
useful information for the debugging process.

### A new "everything" view + print-to-PDF

Making an "everything" view and using print-to-pdf instead of screenshots could
also be an alternative. Still low-fidelity (although we could customize the
display for print purposes, e.g. by including legends on the graphs instead of
only showing them on hover), but would be easier for users than a
headless-browser-based screenshot tool.

Low fidelity can be a good thing if users (especially those in regulated
industries) feel more comfortable sending us a PDF full of graphs than logs that
may contain sensitive information. This will matter more as these customers move
from the proof of concept phase into production.

### Just add more of the debug pages

We'll get some benefit by just expanding the number of exported debug pages.
The problem ranges page, in particular, would be very helpful.  This would get
us part of the way there, but more and more the debug pages are getting fairly
dynamic, and the time series data is also really useful.

### Business as usual

We continue expecting users to take screenshots of time series we need.  This
requires more back-and-forth with our engineers on support, making the debug
process take longer as well as making everyone's experience more frustrating.

## Unresolved questions

- How much data is too much?  How much is enough?
- Build in to the binary or a separate tool?
- What's the best way to receive the zip files?  Until now we've just had users
  send them to us over e-mail.  A future improvement would be to create some
  kind of "black hole" drop box for users to send us the debug files.
- What's the debug story for wedged clusters?  We're necessarily using this tool
  when something's gone wrong, so it needs to work when some things are broken.
  We might allow the debug export to fall back to an inconsistent read if we
  can't successfully get what we're looking for consistently.
