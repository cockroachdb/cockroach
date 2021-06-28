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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

// SchemaChangerState is state associated with the new schema changer.
type SchemaChangerState struct {
	mode  sessiondata.NewSchemaChangerMode
	state scpb.State
	// stmts contains the SQL statements involved in the schema change. This is
	// the bare minimum of statement information we need for testing, but in the
	// future we may want sql.Statement or something.
	stmts []string
}
