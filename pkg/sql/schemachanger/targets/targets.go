package targets

import "github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"

type TargetState struct {
	Target Target
	State  State
}

type Target interface {
}

type AddIndex struct {
	TableID descpb.ID
	IndexID descpb.IndexID

	ReplacementFor descpb.IndexID

	ColumnIDs       descpb.ColumnIDs
	ExtraColumnIDs  descpb.ColumnIDs
	StoredColumnIDs descpb.ColumnIDs

	Primary bool
	Unique  bool
}

type DropIndex struct {
	TableID descpb.ID
	IndexID descpb.IndexID

	ReplacedBy descpb.IndexID
}

type AddColumn struct {
	TableID  descpb.ID
	ColumnID descpb.ColumnID
}

type DropColumn struct {
	TableID  descpb.ID
	ColumnID descpb.ColumnID
}

type AddUniqueConstraint struct {
	TableID   descpb.ID
	IndexID   descpb.ID
	ColumnIDs descpb.ColumnIDs
}

type DropUniqueConstraint struct {
	TableID   descpb.ID
	IndexID   descpb.ID
	ColumnIDs descpb.ColumnIDs
}

type AddCheckConstraint struct {
	TableID   descpb.ID
	Name      string
	Expr      string
	ColumnIDs descpb.ColumnIDs
}

type DropCheckConstraint struct {
	TableID descpb.ID
	Name    string
}

// TODO: move this to some lower-level package
type State int

const (
	elemDeleteOnly State = iota
	elemDeleteAndWriteOnly
	elemBackfilled
	elemPublic
	elemValidated
	elemAbsent
)
