// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package mmasnappb defines the proto schema for snapshots of the in-memory
// state maintained by the multi-metric allocator
// (pkg/kv/kvserver/allocator/mmaprototype). Snapshots are produced by
// clusterState.Snapshot and consumed by diagnostics tooling. The schema only
// re-declares messages for types owned by mmaprototype; shared types from
// roachpb and similar are imported and used directly.
package mmasnappb
