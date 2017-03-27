- Feature Name: Enterprise Registration
- Status: draft
- Start Date: 2017-01-17
- Authors: Daniel Harrison and David Taylor
- RFC PR: [#14114]

# Summary

CockroachDB's [business model] describes a set of "enterprise features", which
(after an evaluation period) must be purchased to be used by commercial
entities. This document describes the technical mechanisms involved.

# Detailed design

An enterprise license is represented as a CockroachLicense protobuf. It contains
the user organization's name, a valid_until date (for an N-day evaluation
license), a cluster_id, and whatever other information is necessary. The
protobuf is serialized and base64 encoded.

```
message CockroachLicense {
    bytes cluster_id = 1 [(gogoproto.nullable) = false,
        (gogoproto.customname) = "ClusterID",
        (gogoproto.customtype) = "github.com/cockroachdb/cockroach/pkg/util/uuid.UUID"];
    string organization_name = 2;
    int64 valid_until_nanos = 3;
}
```

The latest license will be stored in the [settings table] (TODO(dan): replace
with a link to the final rfc when merged). Once viewing historical settings is
supported, it can be used to show historical licensing information. The settings
table is in the system config keyspace and so is gossiped to every node. Upon
receiving a gossiped value, the node will decode the protobuf and check both the
valid_until date and the cluster_id. The result of this check is cached until
the value of the gossiped licence setting changes. When a SQL statement using an
enterprise feature is executed, this cached value is consulted.

The settings table is reused here to avoid taking another table ID in the system
config range, of which we have a very limited number. However, licenses are
different enough from other settings that we're likely to want to treat them
differently in the future, so we'll create a separate table when/if we need it.

To acquire a license, a user visits the registration page on
https://cockroachlabs.com. Further details are out of scope.

The user applies a license to a cluster in either of two ways: via a page on the
admin ui or a supplied SQL command run as the `root` user. The registration
status is immediately surfaced in the admin ui and available via a new `SHOW
LICENSE` SQL command. This is also recorded as an event in the eventlog.

# Drawbacks

`BACKUP` and `RESTORE` are a natural fit for this model, but `PARTITION` is more
difficult as a table may be created using an evaluation license and used after
it expires. We'll require that a valid license is present to create or edit a
partition, but not to use a table with a partition.

# Alternatives

## Command-line Flag

An alternative to the system table is a command line flag specifying the path of
a file with the serialized license data. This doesn't require gossiping or a new
system table.

Unfortunately, there is potential for disagreements between nodes, which will
lead to confusing behavior. Plus licensing (or re-licensing) a cluster is more
difficult operationally and SQL is a more natural interface, especially as
licenses get more complicated (e.g with rights management).

# Unresolved questions

When a user's license is expired (or near expired), they need to be notified.
Assuming that not every customer will regularly use the admin ui, how should we
do these sorts of notifications? Some candidates are during node start-up (next
to where we report candidate updates for the binary), periodically in the logs,
and/or in an email to the cluster administrator (address provided when they
register for the license).

[#14114]: https://github.com/cockroachdb/cockroach/pull/14114
[business model]: https://www.cockroachlabs.com/blog/how-were-building-a-business-to-last/
[settings table]: https://github.com/cockroachdb/cockroach/pull/14230
