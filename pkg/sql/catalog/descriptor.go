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
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
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

	// IsUncommittedVersion returns true if this descriptor represent a version
	// which is not the currently committed version. Implementations may return
	// false negatives here in cases where a descriptor may have crossed a
	// serialization boundary. In particular, this can occur during execution on
	// remote nodes as well as during some scenarios in schema changes.
	IsUncommittedVersion() bool

	// Metadata for descriptor leasing.
	GetVersion() descpb.DescriptorVersion
	GetModificationTime() hlc.Timestamp
	GetDrainingNames() []descpb.NameInfo

	GetPrivileges() *descpb.PrivilegeDescriptor
	TypeName() string
	GetAuditMode() descpb.TableDescriptor_AuditMode

	Public() bool
	Adding() bool
	Dropped() bool
	Offline() bool
	GetOfflineReason() string

	// DescriptorProto prepares this descriptor for serialization.
	DescriptorProto() *descpb.Descriptor
}

// DatabaseDescriptor will eventually be called dbdesc.Descriptor.
// It is implemented by Immutable.
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
// It is implemented by Immutable.
type SchemaDescriptor interface {
	Descriptor
	SchemaDesc() *descpb.SchemaDescriptor
}

// TableDescriptor is an interface around the table descriptor types.
type TableDescriptor interface {
	Descriptor

	TableDesc() *descpb.TableDescriptor

	GetState() descpb.DescriptorState
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
	ForeachNonDropColumn(f func(col *descpb.ColumnDescriptor) error) error
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

	Validate(ctx context.Context, txn DescGetter) error

	ForeachDependedOnBy(f func(dep *descpb.TableDescriptor_Reference) error) error
	GetDependsOn() []descpb.ID
	GetConstraintInfoWithLookup(fn TableLookupFn) (map[string]descpb.ConstraintDetail, error)
	ForeachOutboundFK(f func(fk *descpb.ForeignKeyConstraint) error) error
	GetChecks() []*descpb.TableDescriptor_CheckConstraint
	AllActiveAndInactiveChecks() []*descpb.TableDescriptor_CheckConstraint
	ActiveChecks() []descpb.TableDescriptor_CheckConstraint
	ForeachInboundFK(f func(fk *descpb.ForeignKeyConstraint) error) error
	FindActiveColumnByName(s string) (*descpb.ColumnDescriptor, error)
	WritableColumns() []descpb.ColumnDescriptor
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

// FilterDescriptorState inspects the state of a given descriptor and returns an
// error if the state is anything but public. The error describes the state of
// the descriptor.
func FilterDescriptorState(desc Descriptor, flags tree.CommonLookupFlags) error {
	switch {
	case desc.Dropped() && !flags.IncludeDropped:
		return NewInactiveDescriptorError(ErrDescriptorDropped)
	case desc.Offline() && !flags.IncludeOffline:
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

// Descriptors is a sortable list of Descriptors.
type Descriptors []Descriptor

func (d Descriptors) Len() int           { return len(d) }
func (d Descriptors) Less(i, j int) bool { return d[i].GetID() < d[j].GetID() }
func (d Descriptors) Swap(i, j int)      { d[i], d[j] = d[j], d[i] }

// FormatSafeDescriptorProperties is a shared helper function for writing
// un-redacted, common parts of a descriptor. It writes <prop>: value separated
// by commas to w. These key-value pairs would be valid YAML if wrapped in
// curly braces.
func FormatSafeDescriptorProperties(w *redact.StringBuilder, desc Descriptor) {
	w.Printf("ID: %d, Version: %d", desc.GetID(), desc.GetVersion())
	if desc.IsUncommittedVersion() {
		w.Printf(", IsUncommitted: true")
	}
	w.Printf(", ModificationTime: %q", desc.GetModificationTime())
	if parentID := desc.GetParentID(); parentID != 0 {
		w.Printf(", ParentID: %d", parentID)
	}
	if parentSchemaID := desc.GetParentSchemaID(); parentSchemaID != 0 {
		w.Printf(", ParentSchemaID: %d", parentSchemaID)
	}
	{
		var state descpb.DescriptorState
		switch {
		case desc.Public():
			state = descpb.DescriptorState_PUBLIC
		case desc.Dropped():
			state = descpb.DescriptorState_DROP
		case desc.Adding():
			state = descpb.DescriptorState_ADD
		case desc.Offline():
			state = descpb.DescriptorState_OFFLINE
		}
		w.Printf(", State: %v", state)
		if offlineReason := desc.GetOfflineReason(); state == descpb.DescriptorState_OFFLINE &&
			offlineReason != "" {
			w.Printf(", OfflineReason: %q", redact.Safe(offlineReason))
		}
	}
	if drainingNames := desc.GetDrainingNames(); len(drainingNames) > 0 {
		w.Printf(", NumDrainingNames: %d", len(drainingNames))
	}
}
