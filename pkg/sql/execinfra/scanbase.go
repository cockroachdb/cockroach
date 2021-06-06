// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execinfra

import "github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"

// Prettier aliases for execinfrapb.ScanVisibility values.
const (
	ScanVisibilityPublic             = execinfrapb.ScanVisibility_PUBLIC
	ScanVisibilityPublicAndNotPublic = execinfrapb.ScanVisibility_PUBLIC_AND_NOT_PUBLIC
)

// ScanProgressFrequency determines how often the scan operators should emit
// the metadata about how many rows they have read - once the operators have
// read at least this number of rows, then the new progress metadata should be
// produced.
var ScanProgressFrequency int64 = 5000

// TestingSetScanProgressFrequency changes the frequency at which row-scanned
// progress metadata is emitted by the scan operators.
func TestingSetScanProgressFrequency(val int64) func() {
	oldVal := ScanProgressFrequency
	ScanProgressFrequency = val
	return func() { ScanProgressFrequency = oldVal }
}
