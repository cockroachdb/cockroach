// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package schemachangestatus contains strings used for schema change job
// running status.
package schemachangestatus

const (
	// WaitingForMVCCGC is used for the GC job when it has cleared
	// the data but is waiting for MVCC GC to remove the data.
	WaitingForMVCCGC = "waiting for MVCC GC"
	// DeletingData is used for the GC job when it is about
	// to clear the data.
	DeletingData = "deleting data"
	// WaitingGC is for jobs that are currently in progress and
	// are waiting for the GC interval to expire
	WaitingGC = "waiting for GC TTL"
	// DeleteOnly is for jobs that are currently waiting on
	// the cluster to converge to seeing the schema element in the DELETE_ONLY
	// state.
	DeleteOnly = "waiting in DELETE-ONLY"
	// WriteOnly is for jobs that are currently waiting on
	// the cluster to converge to seeing the schema element in the
	// WRITE_ONLY state.
	WriteOnly = "waiting in WRITE_ONLY"
	// Merging is for jobs that are currently waiting on
	// the cluster to converge to seeing the schema element in the
	// MERGING state.
	Merging = "waiting in MERGING"
	// Backfill is for jobs that are currently running a backfill
	// for a schema element.
	Backfill = "populating schema"
	// Validation is for jobs that are currently validating
	// a schema element.
	Validation = "validating schema"
)
