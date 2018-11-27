- Feature Name: Enterprise Registration
- Status: completed
- Start Date: 2017-01-17
- Authors: Daniel Harrison and David Taylor
- RFC PR: [#14114]

# Summary

CockroachDB's [business model] describes a set of "enterprise features", which
(after an evaluation period) must be purchased to be used by commercial
entities. This document describes the technical mechanisms involved.

# Context

The goal of the mechanisms described here is to prevent usage of "enterprise
features" without a valid license. However the source code of both the Open
Source and Enterprise portions of CockroachDB is publicly available, according
to its respective licensing terms, in our published Github repository. Thus
there is a limited extent to which it makes sense to add increasingly complex
technical barriers to circumvention, e.g. signing keys, obfuscation, etc.

In short: our goal is to put a lock on the front door, not to build Fort Knox.

# Detailed design

An enterprise license is represented as a `License` protobuf. It contains
the user organization's name, a valid_until date (for an N-day evaluation
license), a list of cluster_ids, and whatever other information is necessary.
The protobuf is serialized and base64 encoded.

```
message License {
    repeated bytes cluster_id = 1 [(gogoproto.nullable) = false,
        (gogoproto.customname) = "ClusterID",
        (gogoproto.customtype) = "github.com/cockroachdb/cockroach/pkg/util/uuid.UUID"];
    int64 valid_until_unix_sec = 2;

    enum Type {
      NonCommercial = 0;
      Evaluation = 1;
      Enterprise = 2;
    }

    Type type = 3;

    string organization_name = 4;
}

```

The latest license will be stored as a gossiped [cluster
setting](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20170317_settings_table.md).
Once viewing historical settings is supported, it can be used to show historical
licensing information. When nodes observe the setting change, the value is
decoded to update an in-memory copy of the `License` struct.

 When a SQL statement using an enterprise feature is executed, the time and
 executing clusterID are checked against the current license to determine if it
 is valid (the result of this check could potentially be cached until the
 expiration or next change if or when it becomes a performance concern).

To acquire a license, a user visits the registration page on
https://cockroachlabs.com. Further details are out of scope.

The user applies a license to a cluster in either of two ways: via a page on the
admin ui or `SET CLUSTER LICENSE` SQL command (run as the `root` user). Both
methods must immediately return an error if the license is invalid (malformed or
not currently valid for the executing cluster), rather than updating the setting
and potentially leading to subsequent SQL commands failing (e.g. a nightly
unattended `BACKUP`).

The current registration status is surfaced in the admin ui and can be inspected
via a new `SHOW CLUSTER LICENSE` SQL command.

## Grace Period

Suddenly breaking existing workflows can be very disruptive, and would create a
poor experience for paying customers. Thus Enterprise licenses may remain usable
for some grace period beyond their expiration time.

## Licenses Without Expirations

In some situations, we may want to issue licenses that are not time-bound.
Leaving the expiration unset (0) signifies a license has no expiration.

## Cluster-specific vs Site Licenses

Including the ClusterID(s) for which a license is valid is intended to help
prevent reuse of a license string except by the licensee to which it was issued,
e.g. if someone accidentally included their `SET LICENSE abcdef` string in a
tutorial or question online.

In some situations, we may want to issue licenses that are not tied to specific
clusterIDs -- e.g. if a team is creating clusters for testing or evaluation,
having to acquire a new license from Cockroach Labs every time a testing cluster
is started would become very tedious.

As specified above, Licenses include the organization to which they are issued.
Licenses that do not specify individual clusterIDs for which they are valid are
treated as valid if their organization field matches a new "Organization"
cluster attribute -- likely stored as a cluster setting, set during explicit
`init` or  via the UI or a setting after initialization.

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

## A Separate Table
The settings table is used above to avoid taking another table ID in the system
config range, of which we have a very limited number. However, licenses are
different enough from other settings that we're likely to want to treat them
differently in the future, so we'll create a separate table when/if we need it.

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
