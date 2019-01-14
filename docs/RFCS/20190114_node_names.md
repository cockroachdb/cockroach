- Feature Name: Node Names
- Status: draft
- Start Date: 2019-01-14
- Authors: Andrew Dona-Couch
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: (one or more # from the issue tracker)

# Summary

Identify nodes in a way appropriate for the particular use.  For human
users, provide a short, unique, and memorable text name.  For computer users,
use a numeric id.  Store the numeric id, and derive the short, unique, and
memorable one on demand.

# Motivation

The cluster-assigned id is a small sequential number.  On its own, it
is a natural identifier for a node.  However, cockroach nodes are often
run on hardware provisioned by computers.  This hardware frequently uses
small sequential numbers for identifiers.  In the happy case, these are
the same.  In any other case, it is a persistent source of user confusion.

On my recent support rotation, for instance, I witnessed several
person-hours wasted on exactly this issue.  One node in particular was
acting up, but the logs we were examining looked fine.  They had been
pulled from the node with the hostname that matched our node id.

c.f. issues [#31409][31409], [#33542][33542], presumably many others

[31409]: https://github.com/cockroachdb/cockroach/issues/31409
[33542]: https://github.com/cockroachdb/cockroach/issues/33542

# Guide-level explanation

## For human users:

Nodes are automatically assigned a short, unique, and memorable name,
which can be used whenever you need to refer to that node.

You can see the node's name on the last line of the startup message:

```
...
clusterID:           6d101589-20fe-4089-8aac-930beae044c2
nodeID:              1
nodeName:            uncertain-ocelot
```

If you need to specify a node to a CLI command, you can use the name:

```
> cockroach node status uncertain-ocelot
> cockroach node decommission pungent-rhinocerous
```

Any time the node appears in the web UI the name is listed prominently:

```
Live Nodes
Name                    Address	                          Replicas
uncertain-ocelot        cockroach-adriatic-0001:26257     273
tranquil-tapir          cockroach-adriatic-0002:26257     274
pungent-rhinocerous     cockroach-adriatic-0003:26257     274
```

TODO: is the below a good idea?

If you need to get a node's numeric id (maybe you're building an integration),
you can convert it with a SQL built-in:

```
> SELECT node_name_to_id('uncertain-ocelot');
  ?column?
+----------+
         1
(1 row)

Time: 354µs
```

Or the reverse:

```sql
> SELECT node_id_to_name(1);
      ?column?
+------------------+
  uncertain-ocelot
(1 row)

Time: 413µs
```

Note that this must be done in the right cluster, since node names are
cluster-specific, but doesn't need to be made against the specific node.

## For computer users:

Continue to use a numeric node id for node identification, it works for you.

# Reference-level explanation

## Detailed design

We want a deterministic algorithm to encode a node id as a short, unique, and
memorable phrase.  Node ids are 32-bit integers, but the vast majority of
nodes will have ids less than 16, and almost all ids will be less than 512.
We need to be able to uniquely encode 32 bits of numbers, but after six or
eight bits we don't care if the names start to get ridiculous in one way or
another.

To get a bit more specific about our criteria, we want the phrase to be:

- short
A short phrase makes it quick to use.  The longer the phrase, the more human
overhead required.
- unique
Node 1 from two different clusters should, to a high probability, have distinct
names.  Two names from the same cluster should look and sound different.
- memorable
A user should be able to switch contexts between several tools and keep the
name in their head.  This is partly a function of the first two criteria, but
also is helped by using words that the user is familiar with.

TODO: finish

## Drawbacks

The biggest risk is that the change would increase confusion rather than 
reduce it.  One way to mitigate the confusion would be to assign default
names that match the current node identifiers, however, that strategy
doesn't really address the core concerns of this RFC.

There will be a period of time for users and developers to get accustomed
to the new naming system, but it should be minimal.

## Alternatives

Of course one alternative is the status quo, which is not that bad,
though it is persistently problematic.

One alternative considered was to support a user-defined name.  This
could potentially be useful for some users, but introduces a number
of hairy problems we don't want to have to get into here.  The immediate
concern this RFC wishes to solve is the potential for confusion when
humans see similar, but different lists of small numbers.  There is
nothing in this proposal that would stop us from adding manual names
in the future.

Rather than basing our names on the node id, we could potentially
use some user-defined setting.  One apparently strong candidate is
the `advertise-addr` setting, since hostname is already the identifier
many users use for their nodes.  But we need the name to be unique and
stable in the face of configuration changes, to prevent confusion.

## Unresolved questions

- The specifics of the name generator.
