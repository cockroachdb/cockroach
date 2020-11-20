package ops

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/targets"
)

type Op interface {
}

type AddIndexDescriptor struct {
	TableID descpb.ID
	IndexID descpb.IndexID

	ColumnIDs       descpb.ColumnIDs
	ExtraColumnIDs  descpb.ColumnIDs
	StoredColumnIDs descpb.ColumnIDs

	Primary bool
	Unique  bool
}

type IndexDescriptorStateChange struct {
	TableID   descpb.ID
	IndexID   descpb.IndexID
	NextState targets.State
}

type IndexBackfill struct {
	TableID descpb.ID
	IndexID descpb.IndexID
}

type UniqueIndexValidation struct {
	TableID        descpb.ID
	PrimaryIndexID descpb.ID
	IndexID        descpb.IndexID
}

type AddColumnDescriptor struct {
	TableID  descpb.ID
	ColumnID descpb.ColumnID

	// more metadata
}

type ColumnDescriptorStateChange struct {
	TableID   descpb.ID
	ColumnID  descpb.ColumnID
	NextState targets.State
}

type AddCheckConstraint struct {
	TableID   descpb.ID
	Name      string
	Expr      string
	ColumnIDs descpb.ColumnIDs
}

type ValidateCheckConstraint struct {
	TableID descpb.ID
	Name    string
}

type CheckConstraintStateChange struct {
	TableID descpb.ID
	Name    string

	NextState targets.State
}
