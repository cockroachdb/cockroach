// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package kvstorage houses the logic that manages the on-disk state for the
// Replicas housed on a Store. Replicas store data in two storage.Engine
// instances, which are optionally (and at the time of writing, always)
// identical. One Engine, the "log storage", stores Raft-related state (such as
// the raft log), while the other engine stores the replicated keyspace.
//
// The ability to separate log and state machine opens up performance
// improvements, but results in a more complex lifecycle where operations that
// need to update both the state machine and the raft state require more complex
// recovery in the event of an ill-timed crash. Encapsulating and testing this
// logic is the raison d'être for this package.
package kvstorage
