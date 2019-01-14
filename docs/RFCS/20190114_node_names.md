- Feature Name: Node Names
- Status: draft
- Start Date: 2019-01-14
- Authors: Andrew Dona-Couch
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: (one or more # from the issue tracker)

# Summary

Identify nodes by a user-configurable name.  Prefer this name in all
program output, rather than using node id or host name.  In user-facing
contexts, it may be helpful to add host name as well, but we should
avoid using the cluster-assigned id for that purpose.  Set the name
with a flag to the `node start` command.

# Motivation

The cluster-assigned id is a small sequential number.  On it's own, it
is a natural identifier for a node.  However, cockroach nodes are often
run on hardware provisioned by computers.  This hardware frequently uses
small sequential numbers for identifiers.  In the happy case, these are
the same.  In any other case, it is a persistent source of confusion.

Some users don't want to use small sequential numbers to identify their
machines, they'd rather refer to them by hostname.  This is a natural
thing, since it is after all a host name.  Or maybe they have some
other identifying scheme to use.  No matter what it is, we want to get
out of their way.

When a user sees identifying information about a node, it should be the
identifier the user themselves would use to refer to that node.

c.f. issues [#31409][31409], [#33542][33542], presumably many others

[31409]: https://github.com/cockroachdb/cockroach/issues/31409
[33542]: https://github.com/cockroachdb/cockroach/issues/33542

# Guide-level explanation

Set the node name by adding the `--name` flag to the `start` command.
You can set the name to any value you wish, but it is recommended that
it be short and unique within the cluster.

Any node started without `--name` flag will be assigned a default name,
a short, readable and random phrase like `uncertain-ocelot-1337`.

# Reference-level explanation

## Detailed design

- A command-line flag for the `start` command.
- Update CLI info and logging statements to use name for node identification.
- Update node protobuf & web UI to use name for node identification.

The only part of the design with real alternatives is the choice of
default name.  Since most users will probably not set names, this will
have a big impact on the experience of most users.

Let's consider the ideal characteristics of default node names:

- easy to remember for at least few minutes at a time
- unlikely to collide with other identifiers in the vicinity

There are a few potential candidates:

- node id (minimum possible change)
- advertise-addr
- advertise-addr, but remove the port number
- some random short name (e.g. `uncertain-ocelot-1337`)
- ?? (suggestions welcome!)

Since one of the prime motivations of this change is to move away from
small sequential numbers, it would seem unwise to use node id.  Though
usually nodes are run on different hosts using just one port per host,
configurations where the port number matters are common enough that we
can eliminate the third option, too.

It would be really nice to use `advertise-addr` as the node name, but if
we implemented it only as the default name it would be broken from the
start: any later change to the node's address would not be reflected in
the name.

We're left with the option of some random, but short and user-readable
name.  A quick search finds [three][0] [Go][1] [libraries][2] that will do
the job, but the algorithm used is trivial, just pick from the words in
two word lists.

Since we have the known-unique node id already, we can turn that into a
name on-demand, with one caveat: we want `node 1` from two different
clusters to have two different names.  We can acheive this by first rotating
the word lists by an amount based on the cluster's id.

The specific form of the id needs to be determined.  In the general sense,
we need to be able to support up to 64-bits of information.  But real
cluster sizes are only in the hundreds as yet, and will (likely) always be
exponentially distributed.  This suggests a prefix encoding to reduce the
length of names in the common case.

[0]: https://github.com/yelinaung/go-haikunator
[1]: https://github.com/gjohnson/haikunator
[2]: https://github.com/taion809/haikunator

## Drawbacks

The biggest risk is that the change would increase confusion rather than 
reduce it.  One way to mitigate the confusion would be to assign default
names that match the current node identifiers, however, that strategy
would prevent us from getting all the advantages of the new way.
There will be a period of time for users and developers to get accustomed
to the new naming system.

Otherwise,
- A little more complexity in some code?
- ??

## Alternatives

Of course one alternative is the status quo, which is not that bad,
though it is persistently problematic.

Another alternative would be to try to identify a few more specific
naming schemes and provided specific support for them.  This is more
cumbersome to implement, but for those users with common schemes, the
experience would be seamless.  Fortunately, that can be implemented
on top of the explicit name system recommended here.

One other mechanism that would be useful is a uniqueness check.  By
preventing users from connecting a node to the cluster with a conflicting
name we can make sure to avoid confusion.  This might also catch a
particular class of user error, where a user tries to "restart" a dead
node without providing that node's store directory.

## Unresolved questions

- The main unresolved question is the correct default value, as
  discussed above.
