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

Upgrades are run inside jobs, with one job per upgrade. A single job is ever
created for a particular upgrade, across the cluster - i.e. nodes coordinate
with one another when creating these jobs. Once an upgrade finishes, a row is
left in system.migrations.

Upgrades need to be idempotent: they might be run multiple times as the jobs
error or nodes crash in the middle of running one of the jobs. However, if an
upgrade has been run successfully by a binary with LatestVersion=b, it is not
run again by a binary with a different LatestVersion. This is a useful guarantee
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
