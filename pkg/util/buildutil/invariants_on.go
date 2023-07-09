// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build invariants || race
// +build invariants race

package buildutil

// Invariants is enabled when built with the invariants or race build tags. It
// enables expensive invariant checks in Pebble, and must.Expensive assertions
// in CRDB. This matches the Pebble build tags.
const Invariants = true
