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

//go:generate go run ./generate_visitor.go scop Validation validation.go validation_visitor_generated.go

type validationOp struct{ baseOp }

func (validationOp) Type() Type { return ValidationType }

// ValidateUniqueIndex validates uniqueness of entries for a unique index.
type ValidateUniqueIndex struct {
	validationOp
	TableID        descpb.ID
	PrimaryIndexID descpb.IndexID
	IndexID        descpb.IndexID
}

// ValidateCheckConstraint validates a check constraint on a table's columns.
type ValidateCheckConstraint struct {
	validationOp
	TableID descpb.ID
	Name    string
}

// Make sure baseOp is used for linter.
var _ = validationOp{baseOp: baseOp{}}
