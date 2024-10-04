// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

/*
Package upgrade contains infrastructure for migrating a cluster from a version
to another. It contains "upgrades" associated to different cluster versions that
are run when the cluster is upgraded: the upgrade for version v is run when the
cluster version is bumped from a version < v to one >= v. The code for the
upgrade corresponding to version v can be deleted when compatibility with
version v is no longer needed.

There is also a special category of upgrades called "permanent upgrades".
Permanent upgrades are associated with a cluster version (just like the
non-permanent ones), but, in addition to running when the cluster version is
bumped above their version, also run when the cluster is created at a version >=
v. They are called "permanent" because these upgrades cannot be deleted as soon
as compatibility with version v is no longer needed. Permanent upgrades can be
used to perform setup needed by the cluster, but that cannot be included in the
cluster's bootstrap image. For example, let's say that we have upgrades
associated to the following cluster versions:

v0.0.2  - permanent
v0.0.3  - permanent
v23.1.1
v23.1.5 - permanent
v23.1.7
v23.2.0
v23.2.1 - permanent
v23.2.2
v23.2.3
v24.1.0

and let's futher say that we're creating the cluster at version v23.2.0. On
cluster creation, we'll run all permanent upgrades <= v23.2.0, and none of the
non-permanent ones. Then, let's say that all the nodes are upgraded to
BinaryVersion=v24.1.0 binaries (and automatic version upgrades are turned off);
the cluster logical version remains v23.2.0. No upgrades are run. Then automatic
upgrades are turned on, and `SET CLUSTER VERSION 24.1.0` is run in the
background. At this point, all permanent and non-permanent upgrades > 23.2.0 and
<= 24.1.0 are run, in order.

Upgrades are run inside jobs, with one job per upgrade. A single job is ever
created for a particular upgrade, across the cluster - i.e. nodes coordinate
with one another when creating these jobs. Once an upgrade finishes, a row is
left in system.migrations.

Upgrades need to be idempotent: they might be run multiple times as the jobs
error or nodes crash in the middle of running one of the jobs. However, if an
upgrade has been run successfully by a binary with BinaryVersion=b, it is not
run again by a binary with a different BinaryVersion. This is a useful guarantee
for permanent upgrades, as it allows the code for an upgrade to change between
versions (for example in response to updated bootstrap schema), without needing
to worry about making the upgrade work for cluster being upgraded. Consider the
following example: say v23.1 adds column "foo" to table system.users. This is
done by adding the column to the bootstrap schema, and by creating an upgrade
corresponding to version 23.1.<foo> doing the `ALTER TABLE` for old cluster, and
probably a backfill for the table rows. Say that a permanent upgrade like
`addRootUser()` has code inserting into system.users. If it were not for this
guarantee, the `addRootUser()` code could not be changed to do:

	INSERT INTO system.users (..., foo) ....

If this query was run in a cluster being upgraded from 22.2 to 23.1, it would
fail because the column doesn't exist yet (as the upgrade adding it has not yet
run). This guarantee says that, if addRootUser() runs, it can expect the
bootstrap schema that has "foo".
*/
package upgrade
