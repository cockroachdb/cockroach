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

//go:generate bash ./generate_visitor.sh scop Validation validation.go validation_visitor.go

type validationOp struct{ baseOp }

func (validationOp) Type() Type { return ValidationType }

type UniqueIndexValidation struct {
	validationOp
	TableID        descpb.ID
	PrimaryIndexID descpb.IndexID
	IndexID        descpb.IndexID
}

type ValidateCheckConstraint struct {
	validationOp
	TableID descpb.ID
	Name    string
}
