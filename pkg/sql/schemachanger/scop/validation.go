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
