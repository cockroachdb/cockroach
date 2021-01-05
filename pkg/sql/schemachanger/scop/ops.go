package scop

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
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
}

type IndexDescriptorStateChange struct {
	descriptorMutationOp
	TableID   descpb.ID
	IndexID   descpb.IndexID
	State     scpb.State
	NextState scpb.State
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

type MakeAddedPrimaryIndexDeleteOnly struct {
	descriptorMutationOp
	TableID          descpb.ID
	Index            descpb.IndexDescriptor
	StoreColumnIDs   []descpb.ColumnID
	StoreColumnNames []string
}

// Once we split up the secondary index ops into separate types, this can be
// reused.
type MakeAddedIndexDeleteAndWriteOnly struct {
	descriptorMutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
}

type MakeAddedPrimaryIndexPublic struct {
	descriptorMutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
}

type MakeDroppedPrimaryIndexDeleteAndWriteOnly struct {
	descriptorMutationOp
	TableID          descpb.ID
	IndexID          descpb.IndexID
	StoreColumnIDs   []descpb.ColumnID
	StoreColumnNames []string
}

// Once we split up the secondary index ops into separate types, this can be
// reused.
type MakeDroppedIndexDeleteOnly struct {
	descriptorMutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
}

// Once we split up the secondary index ops into separate types, this can be
// reused.
type MakeDroppedIndexAbsent struct {
	descriptorMutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
}

type AddColumnDescriptor struct {
	descriptorMutationOp
	TableID      descpb.ID
	ColumnFamily descpb.FamilyID
	Column       descpb.ColumnDescriptor
}

type ColumnDescriptorStateChange struct {
	descriptorMutationOp
	TableID   descpb.ID
	ColumnID  descpb.ColumnID
	State     scpb.State
	NextState scpb.State
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

	NextState scpb.State
}

type AddColumnFamily struct {
	descriptorMutationOp
	TableID descpb.ID
	Family  descpb.ColumnFamilyDescriptor
}
