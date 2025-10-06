// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	TableID       descpb.ID
	SourceIndexID descpb.IndexID
	IndexID       descpb.IndexID
}

// MergeIndex specifies an index merge operation.
type MergeIndex struct {
	backfillOp
	TableID           descpb.ID
	TemporaryIndexID  descpb.IndexID
	BackfilledIndexID descpb.IndexID
}

// Make sure baseOp is used for linter.
var _ = backfillOp{baseOp: baseOp{}}
