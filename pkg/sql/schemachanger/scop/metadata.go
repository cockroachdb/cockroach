// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scop

// StatementForDropJob is a statement used to build a description for a
// drop job. The set of statements associated with the drop job will
// be accumulated for the description.
type StatementForDropJob struct {

	// Statement is the statement which lead to this drop.
	Statement string

	// StatementID is the order of the statement in the transaction. It is used
	// to synthesize the appropriate description.
	StatementID uint32

	// Rollback should be marked true if the schema change job is currently
	// rolling back. This is needed to build the correct description for the
	// job.
	Rollback bool
}
