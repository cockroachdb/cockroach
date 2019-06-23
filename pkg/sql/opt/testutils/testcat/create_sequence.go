// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testcat

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

// CreateSequence creates a test sequence from a parsed DDL statement and adds it
// the catalog. This is intended for testing, and is not a complete (and
// probably not fully correct) implementation. It just has to be "good enough".
func (tc *Catalog) CreateSequence(stmt *tree.CreateSequence) *Sequence {
	tc.qualifyTableName(&stmt.Name)

	seq := &Sequence{
		SeqID:   tc.nextStableID(),
		SeqName: stmt.Name,
		Catalog: tc,
	}

	tc.AddSequence(seq)
	return seq
}
