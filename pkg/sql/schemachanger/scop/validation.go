// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scop

import "github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"

//go:generate go run ./generate_visitor.go scop Validation validation.go validation_visitor_generated.go

type validationOp struct{ baseOp }

func (validationOp) Type() Type { return ValidationType }

// ValidateIndex validates the following on an index
// addition that is in WRITE_ONLY:
//  1. its row count is equal to the current primary index row count;
//  2. check for entry uniqueness if it's a unique index;
type ValidateIndex struct {
	validationOp
	TableID descpb.ID
	IndexID descpb.IndexID
}

// ValidateConstraint validates a check constraint on a table's columns.
type ValidateConstraint struct {
	validationOp
	TableID              descpb.ID
	ConstraintID         descpb.ConstraintID
	IndexIDForValidation descpb.IndexID
}

// ValidateColumnNotNull validates a NOT NULL constraint on a table's column.
type ValidateColumnNotNull struct {
	validationOp
	TableID              descpb.ID
	ColumnID             descpb.ColumnID
	IndexIDForValidation descpb.IndexID
}

// Make sure baseOp is used for linter.
var _ = validationOp{baseOp: baseOp{}}
