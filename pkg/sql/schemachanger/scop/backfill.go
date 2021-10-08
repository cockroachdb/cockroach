// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scop

import "github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"

//go:generate go run ./generate_visitor.go scop Backfill backfill.go backfill_visitor_generated.go

// Make sure baseOp is used for linter.
type backfillOp struct{ baseOp }

// Type implements the Op interface.
func (backfillOp) Type() Type { return BackfillType }

// BackfillIndex specifies an index backfill operation.
type BackfillIndex struct {
	backfillOp
	TableID descpb.ID
	IndexID descpb.IndexID
}

// Make sure baseOp is used for linter.
var _ = backfillOp{baseOp: baseOp{}}
