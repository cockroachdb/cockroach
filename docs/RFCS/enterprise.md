- Feature Name: Enterprise Registration
- Status: draft
- Start Date: 2017-01-17
- Authors: Daniel Harrison and David Taylor
- RFC PR: (PR # after acceptance of initial draft)

# Summary

CockroachDB's [business model][] describes a set of "enterprise features", which
(after an evaluation period) must be purchased to be used by commercial
entities. This document describes the technical mechanisms involved.

# Detailed design

An enterprise license is represented as a protobuf. It contains the user
organization's name, a valid_until date (for an N-day evaluation license), a
cluster_id, and whatever other information is necessary. The protobuf is
serialized and base64 encoded.

A new system table, `licenses`, contains each license and the timestamp of when
it was added. It is located in the system config keyspace and so is gossiped to
every node. When a SQL statement using an enterprise feature is executed, the
latest entry in this table is selected and both the valid_until date and the
cluster_id are checked. If the feature is not enabled, a SQL error message is
returned.

```
CREATE TABLE system.licenses (
    applied TIMESTAMP PRIMARY KEY,
    descriptor BYTES
)
```

To acquire a license, a user visits the registration page on
https://cockroachlabs.com. Further details are out of scope.

The user applies a license to a cluster in either of two ways: via a page on the
admin ui or a supplied SQL command run as the `root` user. The registration
status is immediately surfaced in the admin ui and available via a new `SHOW
LICENSE` SQL command.

# Drawbacks

`BACKUP` and `RESTORE` are a natural fit for this model, but `PARTITION` is more
difficult as a table may be created using an evaluation license and used after
it expires.

# Alternatives

## Command-line Flag

An alternative to the system table is a command line flag specifying the path of
a file with the serialized license data. This doesn't require gossiping or a new
system table.

Unfortunately, there is potential for disagreements between nodes, which will
lead to confusing behavior. Plus licensing (or re-licensing) a cluster is more
difficult operationally and SQL is a more natural interface, especially as
licenses get more complicated (e.g with rights management).

[business model]: https://www.cockroachlabs.com/blog/how-were-building-a-business-to-last/
