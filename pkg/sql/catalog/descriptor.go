// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catalog

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// IndexOpts configures the behavior of TableDescriptor.ForeachIndex.
type IndexOpts struct {
	// NonPhysicalPrimaryIndex should be included.
	NonPhysicalPrimaryIndex bool
	// DropMutations should be included.
	DropMutations bool
	// AddMutations should be included.
	AddMutations bool
}

// Descriptor is an interface to be shared by individual descriptor
// types.
type Descriptor interface {
	tree.NameResolutionResult

	GetID() descpb.ID
	GetName() string
	GetParentID() descpb.ID
	GetParentSchemaID() descpb.ID

	// Metadata for descriptor leasing.
	GetVersion() descpb.DescriptorVersion
	GetModificationTime() hlc.Timestamp
	GetDrainingNames() []descpb.NameInfo

	GetPrivileges() *descpb.PrivilegeDescriptor
	TypeName() string
	GetAuditMode() descpb.TableDescriptor_AuditMode

	Adding() bool
	// Note: Implementers of Dropped() should also update the implementation
	// (*descpb.Descriptor).Dropped(). These implementations are not shared
	// behind this interface.
	Dropped() bool
	Offline() bool
	GetOfflineReason() string

	// DescriptorProto prepares this descriptor for serialization.
	DescriptorProto() *descpb.Descriptor
}

// DatabaseDescriptor will eventually be called dbdesc.Descriptor.
// It is implemented by ImmutableDatabaseDescriptor.
type DatabaseDescriptor interface {
	Descriptor

	// Note: Prior to user-defined schemas, databases were the schema meta for
	// objects.
	//
	// TODO(ajwerner): Remove this in the 20.2 cycle as part of user-defined
	// schemas.
	tree.SchemaMeta
	DatabaseDesc() *descpb.DatabaseDescriptor
}

// SchemaDescriptor will eventually be called schemadesc.Descriptor.
// It is implemented by ImmutableSchemaDescriptor.
type SchemaDescriptor interface {
	Descriptor
	SchemaDesc() *descpb.SchemaDescriptor
}

// TableDescriptor is an interface around the table descriptor types.
type TableDescriptor interface {
	Descriptor

	TableDesc() *descpb.TableDescriptor

	GetState() descpb.TableDescriptor_State
	GetSequenceOpts() *descpb.TableDescriptor_SequenceOpts
	GetViewQuery() string
	GetLease() *descpb.TableDescriptor_SchemaChangeLease
	GetDropTime() int64
	GetFormatVersion() descpb.FormatVersion

	GetPrimaryIndexID() descpb.IndexID
	GetPrimaryIndex() *descpb.IndexDescriptor
	PrimaryIndexSpan(codec keys.SQLCodec) roachpb.Span
	GetPublicNonPrimaryIndexes() []descpb.IndexDescriptor
	ForeachIndex(opts IndexOpts, f func(idxDesc *descpb.IndexDescriptor, isPrimary bool) error) error
	AllNonDropIndexes() []*descpb.IndexDescriptor
	ForeachNonDropIndex(f func(idxDesc *descpb.IndexDescriptor) error) error
	IndexSpan(codec keys.SQLCodec, id descpb.IndexID) roachpb.Span
	FindIndexByID(id descpb.IndexID) (*descpb.IndexDescriptor, error)
	FindIndexByName(name string) (_ *descpb.IndexDescriptor, dropped bool, _ error)
	FindIndexesWithPartition(name string) []*descpb.IndexDescriptor
	GetIndexMutationCapabilities(id descpb.IndexID) (isMutation, isWriteOnly bool)
	KeysPerRow(id descpb.IndexID) (int, error)
	PartialIndexOrds() util.FastIntSet
	DeletableIndexes() []descpb.IndexDescriptor

	HasPrimaryKey() bool
	PrimaryKeyString() string

	GetPublicColumns() []descpb.ColumnDescriptor
	ForeachPublicColumn(f func(col *descpb.ColumnDescriptor) error) error
	NamesForColumnIDs(ids descpb.ColumnIDs) ([]string, error)
	FindColumnByName(name tree.Name) (*descpb.ColumnDescriptor, bool, error)
	FindActiveColumnByID(id descpb.ColumnID) (*descpb.ColumnDescriptor, error)
	FindColumnByID(id descpb.ColumnID) (*descpb.ColumnDescriptor, error)
	ColumnIdxMap() map[descpb.ColumnID]int
	GetColumnAtIdx(idx int) *descpb.ColumnDescriptor
	AllNonDropColumns() []descpb.ColumnDescriptor
	VisibleColumns() []descpb.ColumnDescriptor
	ColumnsWithMutations(includeMutations bool) []descpb.ColumnDescriptor
	ColumnIdxMapWithMutations(includeMutations bool) map[descpb.ColumnID]int
	DeletableColumns() []descpb.ColumnDescriptor

	GetFamilies() []descpb.ColumnFamilyDescriptor
	NumFamilies() int
	FindFamilyByID(id descpb.FamilyID) (*descpb.ColumnFamilyDescriptor, error)
	ForeachFamily(f func(family *descpb.ColumnFamilyDescriptor) error) error

	IsTable() bool
	IsView() bool
	IsSequence() bool
	IsTemporary() bool
	IsVirtualTable() bool
	IsPhysicalTable() bool
	IsInterleaved() bool
	MaterializedView() bool

	GetMutationJobs() []descpb.TableDescriptor_MutationJob

	GetReplacementOf() descpb.TableDescriptor_Replacement
	GetAllReferencedTypeIDs(
		getType func(descpb.ID) (TypeDescriptor, error),
	) (descpb.IDs, error)

	Validate(ctx context.Context, protoGetter ProtoGetter, codec keys.SQLCodec) error

	ForeachDependedOnBy(f func(dep *descpb.TableDescriptor_Reference) error) error
	GetDependsOn() []descpb.ID
	GetConstraintInfoWithLookup(fn TableLookupFn) (map[string]descpb.ConstraintDetail, error)
	ForeachOutboundFK(f func(fk *descpb.ForeignKeyConstraint) error) error
	GetChecks() []*descpb.TableDescriptor_CheckConstraint
	AllActiveAndInactiveChecks() []*descpb.TableDescriptor_CheckConstraint
	ActiveChecks() []descpb.TableDescriptor_CheckConstraint
	ForeachInboundFK(f func(fk *descpb.ForeignKeyConstraint) error) error
}

// TypeDescriptor will eventually be called typedesc.Descriptor.
// It is implemented by (Imm|M)utableTypeDescriptor.
type TypeDescriptor interface {
	Descriptor
	TypeDesc() *descpb.TypeDescriptor
	HydrateTypeInfoWithName(ctx context.Context, typ *types.T, name *tree.TypeName, res TypeDescriptorResolver) error
	MakeTypesT(ctx context.Context, name *tree.TypeName, res TypeDescriptorResolver) (*types.T, error)
	HasPendingSchemaChanges() bool
	GetIDClosure() map[descpb.ID]struct{}
}

// TypeDescriptorResolver is an interface used during hydration of type
// metadata in types.T's. It is similar to tree.TypeReferenceResolver, except
// that it has the power to return TypeDescriptor, rather than only a
// types.T. Implementers of tree.TypeReferenceResolver should implement this
// interface as well.
type TypeDescriptorResolver interface {
	// GetTypeDescriptor returns the type descriptor for the input ID.
	GetTypeDescriptor(ctx context.Context, id descpb.ID) (tree.TypeName, TypeDescriptor, error)
}

type inactiveDescriptorError struct {
	cause error
}

// errTableAdding is returned when the descriptor is being added.
//
// Only tables can be in the adding state, and this will be true for the
// foreseeable future, so the error message remains a table-specific version.
var errTableAdding = errors.New("table is being added")

// ErrDescriptorDropped is returned when the descriptor is being dropped.
// TODO (lucy): Make the error message specific to each descriptor type (e.g.,
// "table is being dropped") and add the pgcodes (UndefinedTable, etc.).
var ErrDescriptorDropped = errors.New("descriptor is being dropped")

func (i *inactiveDescriptorError) Error() string { return i.cause.Error() }

func (i *inactiveDescriptorError) Unwrap() error { return i.cause }

// HasAddingTableError returns true if the error contains errTableAdding.
func HasAddingTableError(err error) bool {
	return errors.Is(err, errTableAdding)
}

// HasInactiveDescriptorError returns true if the error contains an
// inactiveDescriptorError.
func HasInactiveDescriptorError(err error) bool {
	return errors.HasType(err, (*inactiveDescriptorError)(nil))
}

// NewInactiveDescriptorError wraps an error in a new inactiveDescriptorError.
func NewInactiveDescriptorError(err error) error {
	return &inactiveDescriptorError{err}
}

// FilterDescriptorState inspects the state of a given descriptor and returns an
// error if the state is anything but public. The error describes the state of
// the descriptor.
func FilterDescriptorState(desc Descriptor) error {
	switch {
	case desc.Dropped():
		return NewInactiveDescriptorError(ErrDescriptorDropped)
	case desc.Offline():
		err := errors.Errorf("%s %q is offline", desc.TypeName(), desc.GetName())
		if desc.GetOfflineReason() != "" {
			err = errors.Errorf("%s %q is offline: %s", desc.TypeName(), desc.GetName(), desc.GetOfflineReason())
		}
		return NewInactiveDescriptorError(err)
	case desc.Adding():
		return errTableAdding
	default:
		return nil
	}
}

// TableLookupFn is used to resolve a table from an ID, particularly when
// getting constraint info.
type TableLookupFn func(descpb.ID) (TableDescriptor, error)

// ProtoGetter is a sub-interface of client.Txn that can fetch protobufs in a
// transaction.
type ProtoGetter interface {
	// GetProtoTs retrieves a protoutil.Message that's stored at key, storing it
	// into the input msg parameter. If the key doesn't exist, the input proto
	// will be reset.
	GetProtoTs(ctx context.Context, key interface{}, msg protoutil.Message) (hlc.Timestamp, error)
}
