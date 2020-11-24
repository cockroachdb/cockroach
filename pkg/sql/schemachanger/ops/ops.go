package ops

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/targets"
)

type Op interface {
	op()
}

type baseOp struct{}

func (baseOp) op() {}

type AddIndexDescriptor struct {
	baseOp
	TableID descpb.ID
	IndexID descpb.IndexID

	ColumnIDs       descpb.ColumnIDs
	ExtraColumnIDs  descpb.ColumnIDs
	StoredColumnIDs descpb.ColumnIDs

	Primary bool
	Unique  bool
}

type IndexDescriptorStateChange struct {
	baseOp
	TableID   descpb.ID
	IndexID   descpb.IndexID
	NextState targets.State
}

type IndexBackfill struct {
	baseOp
	TableID descpb.ID
	IndexID descpb.IndexID
}

type UniqueIndexValidation struct {
	baseOp
	TableID        descpb.ID
	PrimaryIndexID descpb.IndexID
	IndexID        descpb.IndexID
}

type AddColumnDescriptor struct {
	baseOp
	TableID  descpb.ID
	ColumnID descpb.ColumnID

	// more metadata
}

type ColumnDescriptorStateChange struct {
	baseOp
	TableID   descpb.ID
	ColumnID  descpb.ColumnID
	NextState targets.State
}

type AddCheckConstraint struct {
	baseOp
	TableID   descpb.ID
	Name      string
	Expr      string
	ColumnIDs descpb.ColumnIDs
}

type ValidateCheckConstraint struct {
	baseOp
	TableID descpb.ID
	Name    string
}

type CheckConstraintStateChange struct {
	baseOp
	TableID descpb.ID
	Name    string

	NextState targets.State
}
