// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migration

import "testing"

func TestXXX(t *testing.T) {
	// XXX: Things I want to test.
	//
	// ValidateClusterVersion
	// cluster version gate has been pushed out on all nodes
	// migration registry
	// flushing all engines, gc-ing all replicas

	// migration status updates (migration.Run), progress, recording in
	// system.migrations.
	// testing that migrate has been run on all nodes
	// Test individual migrations
	//
	// Will want to use test cluster for the whole thing.
}
