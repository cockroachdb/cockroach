package ops

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/targets"
)

type Type int

const (
	_ Type = iota
	DescriptorMutationType
	BackfillType
	ValidationType
)

// Op represents an action to be taken on a single descriptor.
type Op interface {
	op()
	Type() Type
}

type baseOp struct{}

func (baseOp) op() {}

type descriptorMutationOp struct{ baseOp }

func (descriptorMutationOp) Type() Type { return DescriptorMutationType }

type backfillOp struct{ baseOp }

func (backfillOp) Type() Type { return BackfillType }

type validationOp struct{ baseOp }

func (validationOp) Type() Type { return ValidationType }

type AddIndexDescriptor struct {
	descriptorMutationOp
	TableID descpb.ID
	Index   descpb.IndexDescriptor
	Primary bool
}

type IndexDescriptorStateChange struct {
	descriptorMutationOp
	TableID   descpb.ID
	IndexID   descpb.IndexID
	NextState targets.State
}

type IndexBackfill struct {
	backfillOp
	TableID descpb.ID
	IndexID descpb.IndexID
}

type UniqueIndexValidation struct {
	validationOp
	TableID        descpb.ID
	PrimaryIndexID descpb.IndexID
	IndexID        descpb.IndexID
}

type AddColumnDescriptor struct {
	descriptorMutationOp
	TableID descpb.ID
	Column  descpb.ColumnDescriptor
}

type ColumnDescriptorStateChange struct {
	descriptorMutationOp
	TableID   descpb.ID
	ColumnID  descpb.ColumnID
	NextState targets.State
}

type AddCheckConstraint struct {
	descriptorMutationOp
	TableID     descpb.ID
	Name        string
	Expr        string
	ColumnIDs   descpb.ColumnIDs
	Unvalidated bool
	Hidden      bool
}

type ValidateCheckConstraint struct {
	validationOp
	TableID descpb.ID
	Name    string
}

type CheckConstraintStateChange struct {
	descriptorMutationOp
	TableID descpb.ID
	Name    string

	NextState targets.State
}
