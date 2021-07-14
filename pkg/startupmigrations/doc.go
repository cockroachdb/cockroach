// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package startupmigrations provides a toolkit for running migrations upon
// startup.
//
// These migrations may be associated with a clusterversion which includes the
// relevant change in bootstrap or they may be permanent. Migrations
// associated with bootstrap state are said to be "baked in" to a certain
// version and thus can be deleted in the subsequent major release.
//
//
// Differences from migration package
//
// This package overlaps in functionality with the migration subsystem. The
// major differences are that the "long running" migrations in pkg/migration
// run only after all instances have been upgraded to run the new code as
// opposed to startup migrations which run upon the first instance to run
// the new code. Another difference is that startup migrations run before
// the instance will serve any traffic whereas the long-running migrations
// run only after startup.
//
// A key differentiator between the two migration frameworks is the
// possibility of "permanent" migrations. The long-running migrations
// are always anchored to a version and thus are always assumed to be
// "baked in" at that version. This is not the case for startupmigrations.
// An advantage to "permanent" migrations over baking some data into the
// cluster at bootstrap is that it may be easier to write given that you
// can execute sql and exercise code on a running cluster.
//
// In a future world, it may make sense to unify these two frameworks.
package startupmigrations
