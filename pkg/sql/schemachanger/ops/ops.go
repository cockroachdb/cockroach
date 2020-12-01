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
	IndexID descpb.IndexID

	ColumnIDs       descpb.ColumnIDs
	ExtraColumnIDs  descpb.ColumnIDs
	StoredColumnIDs descpb.ColumnIDs

	Primary bool
	Unique  bool
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
	TableID  descpb.ID
	ColumnID descpb.ColumnID

	// more metadata
}

type ColumnDescriptorStateChange struct {
	descriptorMutationOp
	TableID   descpb.ID
	ColumnID  descpb.ColumnID
	NextState targets.State
}

type AddCheckConstraint struct {
	descriptorMutationOp
	TableID   descpb.ID
	Name      string
	Expr      string
	ColumnIDs descpb.ColumnIDs
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
