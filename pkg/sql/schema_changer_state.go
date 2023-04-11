// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

// SchemaChangerState is state associated with the new schema changer.
// It is used to capture the state of an ongoing schema changer in the
// transaction inside extraTnState field of a `conn_executor` (or an
// `internal_executor`).
type SchemaChangerState struct {
	mode  sessiondatapb.NewSchemaChangerMode
	state scpb.CurrentState
	// jobID contains the ID of the schema changer job, if it is to be created.
	jobID jobspb.JobID
	// stmts contains the SQL statements involved in the schema change. This is
	// the bare minimum of statement information we need for testing, but in the
	// future we may want sql.Statement or something.
	stmts []string
	// memAcc tracks memory usage of this schema changer state.
	memAcc mon.BoundAccount
}
