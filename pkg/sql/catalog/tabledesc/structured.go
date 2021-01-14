// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tabledesc

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// Mutable is a custom type for TableDescriptors
// going through schema mutations.
type Mutable struct {
	wrapper

	// ClusterVersion represents the version of the table descriptor read from the store.
	ClusterVersion descpb.TableDescriptor
}

const (
	// PrimaryKeyIndexName is the name of the index for the primary key.
	PrimaryKeyIndexName = "primary"
	// SequenceColumnID is the ID of the sole column in a sequence.
	SequenceColumnID = 1
	// SequenceColumnName is the name of the sole column in a sequence.
	SequenceColumnName = "value"
)

// ErrMissingColumns indicates a table with no columns.
var ErrMissingColumns = errors.New("table must contain at least 1 column")

// ErrMissingPrimaryKey indicates a table with no primary key.
var ErrMissingPrimaryKey = errors.New("table must contain a primary key")

// ErrIndexGCMutationsList is returned by FindIndexWithID to signal that the
// index with the given ID does not have a descriptor and is in the garbage
// collected mutations list.
var ErrIndexGCMutationsList = errors.New("index in GC mutations list")

// NewCreatedMutable returns a Mutable from the
// given TableDescriptor with the cluster version being the zero table. This
// is for a table that is created in the transaction.
func NewCreatedMutable(tbl descpb.TableDescriptor) *Mutable {
	return &Mutable{wrapper: wrapper{TableDescriptor: tbl}}
}

// NewExistingMutable returns a Mutable from the
// given TableDescriptor with the cluster version also set to the descriptor.
// This is for an existing table.
func NewExistingMutable(tbl descpb.TableDescriptor) *Mutable {
	m := NewCreatedMutable(tbl)
	m.ClusterVersion = tbl
	return m
}

// NewFilledInExistingMutable will construct a Mutable and potentially perform
// post-serialization upgrades.
//
// If skipFKsWithMissingTable is true, the foreign key representation upgrade
// may not fully complete if the other table cannot be found in the ProtoGetter
// but no error will be returned.
func NewFilledInExistingMutable(
	ctx context.Context,
	dg catalog.DescGetter,
	skipFKsWithMissingTable bool,
	tbl *descpb.TableDescriptor,
) (*Mutable, error) {
	changes, err := maybeFillInDescriptor(ctx, dg, tbl, skipFKsWithMissingTable)
	if err != nil {
		return nil, err
	}
	w := wrapper{TableDescriptor: *tbl, postDeserializationChanges: changes}
	return &Mutable{wrapper: w, ClusterVersion: *tbl}, nil
}

// MakeImmutable returns an Immutable from the given TableDescriptor.
func MakeImmutable(tbl descpb.TableDescriptor) Immutable {
	publicAndNonPublicCols := tbl.Columns

	readableCols := tbl.Columns

	desc := Immutable{wrapper: wrapper{TableDescriptor: tbl, indexCache: newIndexCache(&tbl)}}

	if len(tbl.Mutations) > 0 {
		publicAndNonPublicCols = make([]descpb.ColumnDescriptor, 0, len(tbl.Columns)+len(tbl.Mutations))
		readableCols = make([]descpb.ColumnDescriptor, 0, len(tbl.Columns)+len(tbl.Mutations))

		publicAndNonPublicCols = append(publicAndNonPublicCols, tbl.Columns...)
		readableCols = append(readableCols, tbl.Columns...)

		// Fill up mutations into the column/index lists by placing the writable columns/indexes
		// before the delete only columns/indexes.
		for _, m := range tbl.Mutations {
			switch m.State {
			case descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY:
				if col := m.GetColumn(); col != nil {
					publicAndNonPublicCols = append(publicAndNonPublicCols, *col)
					desc.writeOnlyColCount++
				}
			}
		}

		for _, m := range tbl.Mutations {
			switch m.State {
			case descpb.DescriptorMutation_DELETE_ONLY:
				if col := m.GetColumn(); col != nil {
					publicAndNonPublicCols = append(publicAndNonPublicCols, *col)
				}
			}
		}

		// Iterate through all mutation columns.
		for _, c := range publicAndNonPublicCols[len(tbl.Columns):] {
			// Mutation column may need to be fetched, but may not be completely backfilled
			// and have be null values (even though they may be configured as NOT NULL).
			c.Nullable = true
			readableCols = append(readableCols, c)
		}
	}

	desc.readableColumns = readableCols
	desc.publicAndNonPublicCols = publicAndNonPublicCols

	desc.allChecks = make([]descpb.TableDescriptor_CheckConstraint, len(tbl.Checks))
	for i, c := range tbl.Checks {
		desc.allChecks[i] = *c
	}

	// Remember what columns have user defined types.
	for i := range desc.publicAndNonPublicCols {
		typ := desc.publicAndNonPublicCols[i].Type
		if typ != nil && typ.UserDefined() {
			desc.columnsWithUDTs = append(desc.columnsWithUDTs, i)
		}
	}

	return desc
}

// NewImmutable returns a Immutable from the given TableDescriptor.
// This function assumes that this descriptor has not been modified from the
// version stored in the key-value store.
func NewImmutable(tbl descpb.TableDescriptor) *Immutable {
	return NewImmutableWithIsUncommittedVersion(tbl, false /* isUncommittedVersion */)
}

// NewImmutableWithIsUncommittedVersion returns a Immutable from the given
// TableDescriptor and allows the caller to mark the table as corresponding to
// an uncommitted version. This should be used when constructing a new copy of
// an Immutable from an existing descriptor which may have a new version.
func NewImmutableWithIsUncommittedVersion(
	tbl descpb.TableDescriptor, isUncommittedVersion bool,
) *Immutable {
	desc := MakeImmutable(tbl)
	desc.isUncommittedVersion = isUncommittedVersion
	return &desc
}

// NewFilledInImmutable will construct an Immutable and potentially perform
// post-deserialization upgrades.
func NewFilledInImmutable(
	ctx context.Context, dg catalog.DescGetter, tbl *descpb.TableDescriptor,
) (*Immutable, error) {
	changes, err := maybeFillInDescriptor(ctx, dg, tbl, false /* skipFKsWithNoMatchingTable */)
	if err != nil {
		return nil, err
	}
	desc := MakeImmutable(*tbl)
	desc.postDeserializationChanges = changes
	return &desc, nil
}

// PostDeserializationTableDescriptorChanges are a set of booleans to indicate
// which types of upgrades or fixes occurred when filling in the descriptor
// after deserialization.
type PostDeserializationTableDescriptorChanges struct {
	// UpgradedFormatVersion indicates that the FormatVersion was upgraded.
	UpgradedFormatVersion bool

	// FixedPrivileges indicates that the privileges were fixed.
	//
	// TODO(ajwerner): Determine whether this still needs to exist of can be
	// removed.
	FixedPrivileges bool

	// UpgradedForeignKeyRepresentation indicates that the foreign key
	// representation was upgraded.
	UpgradedForeignKeyRepresentation bool
}

// FindIndexPartitionByName searches this index descriptor for a partition whose name
// is the input and returns it, or nil if no match is found.
func FindIndexPartitionByName(
	desc *descpb.IndexDescriptor, name string,
) *descpb.PartitioningDescriptor {
	return desc.Partitioning.FindPartitionByName(name)
}

// TypeName returns the plain type of this descriptor.
func (desc *wrapper) TypeName() string {
	return "relation"
}

// SetName implements the DescriptorProto interface.
func (desc *Mutable) SetName(name string) {
	desc.Name = name
}

// IsPartitionAllBy returns whether the table has a PARTITION ALL BY clause.
func (desc *wrapper) IsPartitionAllBy() bool {
	return desc.PartitionAllBy
}

// GetParentSchemaID returns the ParentSchemaID if the descriptor has
// one. If the descriptor was created before the field was added, then the
// descriptor belongs to a table under the `public` physical schema. The static
// public schema ID is returned in that case.
func (desc *wrapper) GetParentSchemaID() descpb.ID {
	parentSchemaID := desc.GetUnexposedParentSchemaID()
	if parentSchemaID == descpb.InvalidID {
		parentSchemaID = keys.PublicSchemaID
	}
	return parentSchemaID
}

// KeysPerRow returns the maximum number of keys used to encode a row for the
// given index. If a secondary index doesn't store any columns, then it only
// has one k/v pair, but if it stores some columns, it can return up to one
// k/v pair per family in the table, just like a primary index.
func (desc *wrapper) KeysPerRow(indexID descpb.IndexID) (int, error) {
	if desc.PrimaryIndex.ID == indexID {
		return len(desc.Families), nil
	}
	idx, err := desc.FindIndexWithID(indexID)
	if err != nil {
		return 0, err
	}
	if idx.NumStoredColumns() == 0 {
		return 1, nil
	}
	return len(desc.Families), nil
}

// AllNonDropColumns returns all the columns, including those being added
// in the mutations.
func (desc *wrapper) AllNonDropColumns() []descpb.ColumnDescriptor {
	cols := make([]descpb.ColumnDescriptor, 0, len(desc.Columns)+len(desc.Mutations))
	cols = append(cols, desc.Columns...)
	for _, m := range desc.Mutations {
		if col := m.GetColumn(); col != nil {
			if m.Direction == descpb.DescriptorMutation_ADD {
				cols = append(cols, *col)
			}
		}
	}
	return cols
}

// buildIndexName sets desc.Name to a value that is not EqualName to any
// of tableDesc's indexes. allocateName roughly follows PostgreSQL's
// convention for automatically-named indexes.
func buildIndexName(tableDesc *Mutable, index catalog.Index) string {
	idx := index.IndexDesc()
	segments := make([]string, 0, len(idx.ColumnNames)+2)
	segments = append(segments, tableDesc.Name)
	segments = append(segments, idx.ColumnNames[idx.ExplicitColumnStartIdx():]...)
	if idx.Unique {
		segments = append(segments, "key")
	} else {
		segments = append(segments, "idx")
	}

	baseName := strings.Join(segments, "_")
	name := baseName
	for i := 1; ; i++ {
		foundIndex, _ := tableDesc.FindIndexWithName(name)
		if foundIndex == nil {
			break
		}
		name = fmt.Sprintf("%s%d", baseName, i)
	}

	return name
}

// AllActiveAndInactiveChecks returns all check constraints, including both
// "active" ones on the table descriptor which are being enforced for all
// writes, and "inactive" ones queued in the mutations list.
func (desc *wrapper) AllActiveAndInactiveChecks() []*descpb.TableDescriptor_CheckConstraint {
	// A check constraint could be both on the table descriptor and in the
	// list of mutations while the constraint is validated for existing rows. In
	// that case, the constraint is in the Validating state, and we avoid
	// including it twice. (Note that even though unvalidated check constraints
	// cannot be added as of 19.1, they can still exist if they were created under
	// previous versions.)
	checks := make([]*descpb.TableDescriptor_CheckConstraint, 0, len(desc.Checks))
	for _, c := range desc.Checks {
		// While a constraint is being validated for existing rows or being dropped,
		// the constraint is present both on the table descriptor and in the
		// mutations list in the Validating or Dropping state, so those constraints
		// are excluded here to avoid double-counting.
		if c.Validity != descpb.ConstraintValidity_Validating &&
			c.Validity != descpb.ConstraintValidity_Dropping {
			checks = append(checks, c)
		}
	}
	for _, m := range desc.Mutations {
		if c := m.GetConstraint(); c != nil && c.ConstraintType == descpb.ConstraintToUpdate_CHECK {
			checks = append(checks, &c.Check)
		}
	}
	return checks
}

// GetColumnFamilyForShard returns the column family that a newly added shard column
// should be assigned to, given the set of columns it's computed from.
//
// This is currently the column family of the first column in the set of index columns.
func GetColumnFamilyForShard(desc *Mutable, idxColumns []string) string {
	for _, f := range desc.Families {
		for _, fCol := range f.ColumnNames {
			if fCol == idxColumns[0] {
				return f.Name
			}
		}
	}
	return ""
}

// AllActiveAndInactiveUniqueWithoutIndexConstraints returns all unique
// constraints that are not enforced by an index, including both "active"
// ones on the table descriptor which are being enforced for all writes, and
// "inactive" ones queued in the mutations list.
func (desc *wrapper) AllActiveAndInactiveUniqueWithoutIndexConstraints() []*descpb.UniqueWithoutIndexConstraint {
	ucs := make([]*descpb.UniqueWithoutIndexConstraint, 0, len(desc.UniqueWithoutIndexConstraints))
	for i := range desc.UniqueWithoutIndexConstraints {
		uc := &desc.UniqueWithoutIndexConstraints[i]
		// While a constraint is being validated for existing rows or being dropped,
		// the constraint is present both on the table descriptor and in the
		// mutations list in the Validating or Dropping state, so those constraints
		// are excluded here to avoid double-counting.
		if uc.Validity != descpb.ConstraintValidity_Validating &&
			uc.Validity != descpb.ConstraintValidity_Dropping {
			ucs = append(ucs, uc)
		}
	}
	for i := range desc.Mutations {
		if c := desc.Mutations[i].GetConstraint(); c != nil &&
			c.ConstraintType == descpb.ConstraintToUpdate_UNIQUE_WITHOUT_INDEX {
			ucs = append(ucs, &c.UniqueWithoutIndexConstraint)
		}
	}
	return ucs
}

// AllActiveAndInactiveForeignKeys returns all foreign keys, including both
// "active" ones on the index descriptor which are being enforced for all
// writes, and "inactive" ones queued in the mutations list. An error is
// returned if multiple foreign keys (including mutations) are found for the
// same index.
func (desc *wrapper) AllActiveAndInactiveForeignKeys() []*descpb.ForeignKeyConstraint {
	fks := make([]*descpb.ForeignKeyConstraint, 0, len(desc.OutboundFKs))
	for i := range desc.OutboundFKs {
		fk := &desc.OutboundFKs[i]
		// While a constraint is being validated for existing rows or being dropped,
		// the constraint is present both on the table descriptor and in the
		// mutations list in the Validating or Dropping state, so those constraints
		// are excluded here to avoid double-counting.
		if fk.Validity != descpb.ConstraintValidity_Validating && fk.Validity != descpb.ConstraintValidity_Dropping {
			fks = append(fks, fk)
		}
	}
	for i := range desc.Mutations {
		if c := desc.Mutations[i].GetConstraint(); c != nil && c.ConstraintType == descpb.ConstraintToUpdate_FOREIGN_KEY {
			fks = append(fks, &c.ForeignKey)
		}
	}
	return fks
}

// ForeachPublicColumn runs a function on all public columns.
func (desc *wrapper) ForeachPublicColumn(f func(column *descpb.ColumnDescriptor) error) error {
	for i := range desc.Columns {
		if err := f(&desc.Columns[i]); err != nil {
			return err
		}
	}
	return nil
}

// ForeachNonDropColumn runs a function on all public columns and columns
// currently being added.
func (desc *wrapper) ForeachNonDropColumn(f func(column *descpb.ColumnDescriptor) error) error {
	if err := desc.ForeachPublicColumn(f); err != nil {
		return err
	}

	for i := range desc.Mutations {
		mut := &desc.Mutations[i]
		mutCol := mut.GetColumn()
		if mut.Direction == descpb.DescriptorMutation_ADD && mutCol != nil {
			if err := f(mutCol); err != nil {
				return err
			}
		}
	}
	return nil
}

// ForeachDependedOnBy runs a function on all indexes, including those being
// added in the mutations.
func (desc *wrapper) ForeachDependedOnBy(
	f func(dep *descpb.TableDescriptor_Reference) error,
) error {
	for i := range desc.DependedOnBy {
		if err := f(&desc.DependedOnBy[i]); err != nil {
			return err
		}
	}
	return nil
}

// ForeachOutboundFK calls f for every outbound foreign key in desc until an
// error is returned.
func (desc *wrapper) ForeachOutboundFK(
	f func(constraint *descpb.ForeignKeyConstraint) error,
) error {
	for i := range desc.OutboundFKs {
		if err := f(&desc.OutboundFKs[i]); err != nil {
			return err
		}
	}
	return nil
}

// ForeachInboundFK calls f for every inbound foreign key in desc until an
// error is returned.
func (desc *wrapper) ForeachInboundFK(f func(fk *descpb.ForeignKeyConstraint) error) error {
	for i := range desc.InboundFKs {
		if err := f(&desc.InboundFKs[i]); err != nil {
			return err
		}
	}
	return nil
}

// NumFamilies returns the number of column families in the descriptor.
func (desc *wrapper) NumFamilies() int {
	return len(desc.Families)
}

// ForeachFamily calls f for every column family key in desc until an
// error is returned.
func (desc *wrapper) ForeachFamily(f func(family *descpb.ColumnFamilyDescriptor) error) error {
	for i := range desc.Families {
		if err := f(&desc.Families[i]); err != nil {
			return err
		}
	}
	return nil
}

func generatedFamilyName(familyID descpb.FamilyID, columnNames []string) string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "fam_%d", familyID)
	for _, n := range columnNames {
		buf.WriteString(`_`)
		buf.WriteString(n)
	}
	return buf.String()
}

// maybeFillInDescriptor performs any modifications needed to the table descriptor.
// This includes format upgrades and optional changes that can be handled by all version
// (for example: additional default privileges).
func maybeFillInDescriptor(
	ctx context.Context,
	dg catalog.DescGetter,
	desc *descpb.TableDescriptor,
	skipFKsWithNoMatchingTable bool,
) (changes PostDeserializationTableDescriptorChanges, err error) {
	changes.UpgradedFormatVersion = maybeUpgradeFormatVersion(desc)
	changes.FixedPrivileges = descpb.MaybeFixPrivileges(desc.ID, desc.Privileges)

	if dg != nil {
		changes.UpgradedForeignKeyRepresentation, err = maybeUpgradeForeignKeyRepresentation(
			ctx, dg, skipFKsWithNoMatchingTable /* skipFKsWithNoMatchingTable*/, desc)
	}
	if err != nil {
		return PostDeserializationTableDescriptorChanges{}, err
	}
	return changes, nil
}

func indexHasDeprecatedForeignKeyRepresentation(idx *descpb.IndexDescriptor) bool {
	return idx.ForeignKey.IsSet() || len(idx.ReferencedBy) > 0
}

// TableHasDeprecatedForeignKeyRepresentation returns true if the table is not
// dropped and any of the indexes on the table have deprecated foreign key
// representations.
func TableHasDeprecatedForeignKeyRepresentation(desc *descpb.TableDescriptor) bool {
	if desc.Dropped() {
		return false
	}
	if indexHasDeprecatedForeignKeyRepresentation(&desc.PrimaryIndex) {
		return true
	}
	for i := range desc.Indexes {
		if indexHasDeprecatedForeignKeyRepresentation(&desc.Indexes[i]) {
			return true
		}
	}
	return false
}

// maybeUpgradeForeignKeyRepresentation destructively modifies the input table
// descriptor by replacing all old-style foreign key references (the ForeignKey
// and ReferencedBy fields on IndexDescriptor) with new-style foreign key
// references (the InboundFKs and OutboundFKs fields on TableDescriptor). It
// uses the supplied proto getter to look up the referenced descriptor on
// outgoing FKs and the origin descriptor on incoming FKs. It returns true in
// the first position if the descriptor was upgraded at all (i.e. had old-style
// references on it) and an error if the descriptor was unable to be upgraded
// for some reason.
// If skipFKsWithNoMatchingTable is set to true, if a *table* that's supposed to
// contain the matching forward/back-reference for an FK is not found, the FK
// is dropped from the table and no error is returned.
//
// TODO(lucy): Write tests for when skipFKsWithNoMatchingTable is true.
// TODO(ajwerner): This exists solely for the purpose of front-loading upgrade
// at backup and restore time and occurs in a hacky way. All of that upgrading
// should get reworked but we're leaving this here for now for simplicity.
func maybeUpgradeForeignKeyRepresentation(
	ctx context.Context,
	dg catalog.DescGetter,
	skipFKsWithNoMatchingTable bool,
	desc *descpb.TableDescriptor,
) (bool, error) {
	if desc.Dropped() {
		// If the table has been dropped, it's permitted to have corrupted foreign
		// keys, so we have no chance to properly upgrade it. Just return as-is.
		return false, nil
	}
	otherUnupgradedTables := make(map[descpb.ID]catalog.TableDescriptor)
	changed := false
	// No need to process mutations, since only descriptors written on a 19.2
	// cluster (after finalizing the upgrade) have foreign key mutations.
	for i := range desc.Indexes {
		newChanged, err := maybeUpgradeForeignKeyRepOnIndex(
			ctx, dg, otherUnupgradedTables, desc, &desc.Indexes[i], skipFKsWithNoMatchingTable,
		)
		if err != nil {
			return false, err
		}
		changed = changed || newChanged
	}
	newChanged, err := maybeUpgradeForeignKeyRepOnIndex(
		ctx, dg, otherUnupgradedTables, desc, &desc.PrimaryIndex, skipFKsWithNoMatchingTable,
	)
	if err != nil {
		return false, err
	}
	changed = changed || newChanged

	return changed, nil
}

// maybeUpgradeForeignKeyRepOnIndex is the meat of the previous function - it
// tries to upgrade a particular index's foreign key representation.
func maybeUpgradeForeignKeyRepOnIndex(
	ctx context.Context,
	dg catalog.DescGetter,
	otherUnupgradedTables map[descpb.ID]catalog.TableDescriptor,
	desc *descpb.TableDescriptor,
	idx *descpb.IndexDescriptor,
	skipFKsWithNoMatchingTable bool,
) (bool, error) {
	var changed bool
	if idx.ForeignKey.IsSet() {
		ref := &idx.ForeignKey
		if _, ok := otherUnupgradedTables[ref.Table]; !ok {
			tbl, err := catalog.GetTableDescFromID(ctx, dg, ref.Table)
			if err != nil {
				if errors.Is(err, catalog.ErrDescriptorNotFound) && skipFKsWithNoMatchingTable {
					// Ignore this FK and keep going.
				} else {
					return false, err
				}
			} else {
				otherUnupgradedTables[ref.Table] = tbl
			}
		}
		if tbl, ok := otherUnupgradedTables[ref.Table]; ok {
			referencedIndex, err := tbl.FindIndexWithID(ref.Index)
			if err != nil {
				return false, err
			}
			numCols := ref.SharedPrefixLen
			outFK := descpb.ForeignKeyConstraint{
				OriginTableID:       desc.ID,
				OriginColumnIDs:     idx.ColumnIDs[:numCols],
				ReferencedTableID:   ref.Table,
				ReferencedColumnIDs: referencedIndex.IndexDesc().ColumnIDs[:numCols],
				Name:                ref.Name,
				Validity:            ref.Validity,
				OnDelete:            ref.OnDelete,
				OnUpdate:            ref.OnUpdate,
				Match:               ref.Match,
			}
			desc.OutboundFKs = append(desc.OutboundFKs, outFK)
		}
		changed = true
		idx.ForeignKey = descpb.ForeignKeyReference{}
	}

	for refIdx := range idx.ReferencedBy {
		ref := &(idx.ReferencedBy[refIdx])
		if _, ok := otherUnupgradedTables[ref.Table]; !ok {
			tbl, err := catalog.GetTableDescFromID(ctx, dg, ref.Table)
			if err != nil {
				if errors.Is(err, catalog.ErrDescriptorNotFound) && skipFKsWithNoMatchingTable {
					// Ignore this FK and keep going.
				} else {
					return false, err
				}
			} else {
				otherUnupgradedTables[ref.Table] = tbl
			}
		}

		if otherTable, ok := otherUnupgradedTables[ref.Table]; ok {
			originIndexI, err := otherTable.FindIndexWithID(ref.Index)
			if err != nil {
				return false, err
			}
			originIndex := originIndexI.IndexDesc()
			// There are two cases. Either the other table is old (not upgraded yet),
			// or it's new (already upgraded).
			var inFK descpb.ForeignKeyConstraint
			if !originIndex.ForeignKey.IsSet() {
				// The other table has either no foreign key, indicating a corrupt
				// reference, or the other table was upgraded. Assume the second for now.
				// If we also find no matching reference in the new-style foreign keys,
				// that indicates a corrupt reference.
				var forwardFK *descpb.ForeignKeyConstraint
				_ = otherTable.ForeachOutboundFK(func(otherFK *descpb.ForeignKeyConstraint) error {
					if forwardFK != nil {
						return nil
					}
					// To find a match, we find a foreign key reference that has the same
					// referenced table ID, and that the index we point to is a valid
					// index to satisfy the columns in the foreign key.
					if otherFK.ReferencedTableID == desc.ID &&
						descpb.ColumnIDs(originIndex.ColumnIDs).HasPrefix(otherFK.OriginColumnIDs) {
						// Found a match.
						forwardFK = otherFK
					}
					return nil
				})
				if forwardFK == nil {
					// Corrupted foreign key - there was no forward reference for the back
					// reference.
					return false, errors.AssertionFailedf(
						"error finding foreign key on table %d for backref %+v",
						otherTable.GetID(), ref)
				}
				inFK = descpb.ForeignKeyConstraint{
					OriginTableID:       ref.Table,
					OriginColumnIDs:     forwardFK.OriginColumnIDs,
					ReferencedTableID:   desc.ID,
					ReferencedColumnIDs: forwardFK.ReferencedColumnIDs,
					Name:                forwardFK.Name,
					Validity:            forwardFK.Validity,
					OnDelete:            forwardFK.OnDelete,
					OnUpdate:            forwardFK.OnUpdate,
					Match:               forwardFK.Match,
				}
			} else {
				// We have an old (not upgraded yet) table, with a matching forward
				// foreign key.
				numCols := originIndex.ForeignKey.SharedPrefixLen
				inFK = descpb.ForeignKeyConstraint{
					OriginTableID:       ref.Table,
					OriginColumnIDs:     originIndex.ColumnIDs[:numCols],
					ReferencedTableID:   desc.ID,
					ReferencedColumnIDs: idx.ColumnIDs[:numCols],
					Name:                originIndex.ForeignKey.Name,
					Validity:            originIndex.ForeignKey.Validity,
					OnDelete:            originIndex.ForeignKey.OnDelete,
					OnUpdate:            originIndex.ForeignKey.OnUpdate,
					Match:               originIndex.ForeignKey.Match,
				}
			}
			desc.InboundFKs = append(desc.InboundFKs, inFK)
		}
		changed = true
	}
	idx.ReferencedBy = nil
	return changed, nil
}

// maybeUpgradeFormatVersion transforms the TableDescriptor to the latest
// FormatVersion (if it's not already there) and returns true if any changes
// were made.
// This method should be called through maybeFillInDescriptor, not directly.
func maybeUpgradeFormatVersion(desc *descpb.TableDescriptor) bool {
	if desc.FormatVersion >= descpb.InterleavedFormatVersion {
		return false
	}
	maybeUpgradeToFamilyFormatVersion(desc)
	desc.FormatVersion = descpb.InterleavedFormatVersion
	return true
}

func maybeUpgradeToFamilyFormatVersion(desc *descpb.TableDescriptor) bool {
	if desc.FormatVersion >= descpb.FamilyFormatVersion {
		return false
	}

	var primaryIndexColumnIDs catalog.TableColSet
	for _, colID := range desc.PrimaryIndex.ColumnIDs {
		primaryIndexColumnIDs.Add(colID)
	}

	desc.Families = []descpb.ColumnFamilyDescriptor{
		{ID: 0, Name: "primary"},
	}
	desc.NextFamilyID = desc.Families[0].ID + 1
	addFamilyForCol := func(col *descpb.ColumnDescriptor) {
		if primaryIndexColumnIDs.Contains(col.ID) {
			desc.Families[0].ColumnNames = append(desc.Families[0].ColumnNames, col.Name)
			desc.Families[0].ColumnIDs = append(desc.Families[0].ColumnIDs, col.ID)
			return
		}
		colNames := []string{col.Name}
		family := descpb.ColumnFamilyDescriptor{
			ID:              descpb.FamilyID(col.ID),
			Name:            generatedFamilyName(descpb.FamilyID(col.ID), colNames),
			ColumnNames:     colNames,
			ColumnIDs:       []descpb.ColumnID{col.ID},
			DefaultColumnID: col.ID,
		}
		desc.Families = append(desc.Families, family)
		if family.ID >= desc.NextFamilyID {
			desc.NextFamilyID = family.ID + 1
		}
	}

	for i := range desc.Columns {
		addFamilyForCol(&desc.Columns[i])
	}
	for i := range desc.Mutations {
		m := &desc.Mutations[i]
		if c := m.GetColumn(); c != nil {
			addFamilyForCol(c)
		}
	}

	desc.FormatVersion = descpb.FamilyFormatVersion

	return true
}

// ForEachExprStringInTableDesc runs a closure for each expression string
// within a TableDescriptor. The closure takes in a string pointer so that
// it can mutate the TableDescriptor if desired.
func ForEachExprStringInTableDesc(descI catalog.TableDescriptor, f func(expr *string) error) error {
	var desc *wrapper
	switch descV := descI.(type) {
	case *wrapper:
		desc = descV
	case *Immutable:
		desc = &descV.wrapper
	case *Mutable:
		desc = &descV.wrapper
	default:
		return errors.AssertionFailedf("unexpected type of table %T", descI)
	}
	// Helpers for each schema element type that can contain an expression.
	doCol := func(c *descpb.ColumnDescriptor) error {
		if c.HasDefault() {
			if err := f(c.DefaultExpr); err != nil {
				return err
			}
		}
		if c.IsComputed() {
			if err := f(c.ComputeExpr); err != nil {
				return err
			}
		}
		return nil
	}
	doIndex := func(i catalog.Index) error {
		if i.IsPartial() {
			return f(&i.IndexDesc().Predicate)
		}
		return nil
	}
	doCheck := func(c *descpb.TableDescriptor_CheckConstraint) error {
		return f(&c.Expr)
	}

	// Process columns.
	for i := range desc.Columns {
		if err := doCol(&desc.Columns[i]); err != nil {
			return err
		}
	}

	// Process all indexes.
	if err := catalog.ForEachIndex(descI, catalog.IndexOpts{
		NonPhysicalPrimaryIndex: true,
		DropMutations:           true,
		AddMutations:            true,
	}, doIndex); err != nil {
		return err
	}

	// Process checks.
	for i := range desc.Checks {
		if err := doCheck(desc.Checks[i]); err != nil {
			return err
		}
	}

	// Process all non-index mutations.
	for _, mut := range desc.GetMutations() {
		if c := mut.GetColumn(); c != nil {
			if err := doCol(c); err != nil {
				return err
			}
		}
		if c := mut.GetConstraint(); c != nil &&
			c.ConstraintType == descpb.ConstraintToUpdate_CHECK {
			if err := doCheck(&c.Check); err != nil {
				return err
			}
		}
	}
	return nil
}

// GetAllReferencedTypeIDs returns all user defined type descriptor IDs that
// this table references. It takes in a function that returns the TypeDescriptor
// with the desired ID.
func (desc *wrapper) GetAllReferencedTypeIDs(
	getType func(descpb.ID) (catalog.TypeDescriptor, error),
) (descpb.IDs, error) {
	// All serialized expressions within a table descriptor are serialized
	// with type annotations as ID's, so this visitor will collect them all.
	visitor := &tree.TypeCollectorVisitor{
		OIDs: make(map[oid.Oid]struct{}),
	}

	addOIDsInExpr := func(exprStr *string) error {
		expr, err := parser.ParseExpr(*exprStr)
		if err != nil {
			return err
		}
		tree.WalkExpr(visitor, expr)
		return nil
	}

	if err := ForEachExprStringInTableDesc(desc, addOIDsInExpr); err != nil {
		return nil, err
	}

	// For each of the collected type IDs in the table descriptor expressions,
	// collect the closure of ID's referenced.
	ids := make(map[descpb.ID]struct{})
	for id := range visitor.OIDs {
		typDesc, err := getType(typedesc.UserDefinedTypeOIDToID(id))
		if err != nil {
			return nil, err
		}
		for child := range typDesc.GetIDClosure() {
			ids[child] = struct{}{}
		}
	}

	// Now add all of the column types in the table.
	addIDsInColumn := func(c *descpb.ColumnDescriptor) {
		for id := range typedesc.GetTypeDescriptorClosure(c.Type) {
			ids[id] = struct{}{}
		}
	}
	for i := range desc.Columns {
		addIDsInColumn(&desc.Columns[i])
	}
	for _, mut := range desc.Mutations {
		if c := mut.GetColumn(); c != nil {
			addIDsInColumn(c)
		}
	}

	// Construct the output.
	result := make(descpb.IDs, 0, len(ids))
	for id := range ids {
		result = append(result, id)
	}
	// Sort the output so that the order is deterministic.
	sort.Sort(result)
	return result, nil
}

func (desc *Mutable) initIDs() {
	if desc.NextColumnID == 0 {
		desc.NextColumnID = 1
	}
	if desc.Version == 0 {
		desc.Version = 1
	}
	if desc.NextMutationID == descpb.InvalidMutationID {
		desc.NextMutationID = 1
	}
}

// MaybeFillColumnID assigns a column ID to the given column if the said column has an ID
// of 0.
func (desc *Mutable) MaybeFillColumnID(
	c *descpb.ColumnDescriptor, columnNames map[string]descpb.ColumnID,
) {
	desc.initIDs()

	columnID := c.ID
	if columnID == 0 {
		columnID = desc.NextColumnID
		desc.NextColumnID++
	}
	columnNames[c.Name] = columnID
	c.ID = columnID
}

// AllocateIDs allocates column, family, and index ids for any column, family,
// or index which has an ID of 0.
func (desc *Mutable) AllocateIDs(ctx context.Context) error {
	// Only tables with physical data can have / need a primary key.
	if desc.IsPhysicalTable() {
		if err := desc.ensurePrimaryKey(); err != nil {
			return err
		}
	}

	desc.initIDs()
	columnNames := map[string]descpb.ColumnID{}
	for i := range desc.Columns {
		desc.MaybeFillColumnID(&desc.Columns[i], columnNames)
	}
	for _, m := range desc.Mutations {
		if c := m.GetColumn(); c != nil {
			desc.MaybeFillColumnID(c, columnNames)
		}
	}

	// Only tables and materialized views can have / need indexes and column families.
	if desc.IsTable() || desc.MaterializedView() {
		if err := desc.allocateIndexIDs(columnNames); err != nil {
			return err
		}
		// Virtual tables don't have column families.
		if desc.IsPhysicalTable() {
			desc.allocateColumnFamilyIDs(columnNames)
		}
	}

	// This is sort of ugly. If the descriptor does not have an ID, we hack one in
	// to pass the table ID check. We use a non-reserved ID, reserved ones being set
	// before AllocateIDs.
	savedID := desc.ID
	if desc.ID == 0 {
		desc.ID = keys.MinUserDescID
	}
	err := desc.ValidateTable(ctx)
	desc.ID = savedID
	return err
}

func (desc *Mutable) ensurePrimaryKey() error {
	if len(desc.PrimaryIndex.ColumnNames) == 0 && desc.IsPhysicalTable() {
		// Ensure a Primary Key exists.
		nameExists := func(name string) bool {
			_, _, err := desc.FindColumnByName(tree.Name(name))
			return err == nil
		}
		s := "unique_rowid()"
		col := &descpb.ColumnDescriptor{
			Name:        GenerateUniqueConstraintName("rowid", nameExists),
			Type:        types.Int,
			DefaultExpr: &s,
			Hidden:      true,
			Nullable:    false,
		}
		desc.AddColumn(col)
		idx := descpb.IndexDescriptor{
			Unique:           true,
			ColumnNames:      []string{col.Name},
			ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
		}
		if err := desc.AddIndex(idx, true); err != nil {
			return err
		}
	}
	return nil
}

func (desc *Mutable) allocateIndexIDs(columnNames map[string]descpb.ColumnID) error {
	if desc.NextIndexID == 0 {
		desc.NextIndexID = 1
	}

	// Assign names to unnamed indexes.
	_ = catalog.ForEachDeletableNonPrimaryIndex(desc, func(idx catalog.Index) error {
		if len(idx.GetName()) == 0 {
			idx.IndexDesc().Name = buildIndexName(desc, idx)
		}
		return nil
	})

	var compositeColIDs catalog.TableColSet
	for i := range desc.Columns {
		col := &desc.Columns[i]
		if colinfo.HasCompositeKeyEncoding(col.Type) {
			compositeColIDs.Add(col.ID)
		}
	}

	// Populate IDs.
	for _, idx := range desc.AllIndexes() {
		if idx.GetID() != 0 {
			// This index has already been populated. Nothing to do.
			continue
		}
		index := idx.IndexDesc()
		index.ID = desc.NextIndexID
		desc.NextIndexID++

		for j, colName := range index.ColumnNames {
			if len(index.ColumnIDs) <= j {
				index.ColumnIDs = append(index.ColumnIDs, 0)
			}
			if index.ColumnIDs[j] == 0 {
				index.ColumnIDs[j] = columnNames[colName]
			}
		}

		if !idx.Primary() && index.EncodingType == descpb.SecondaryIndexEncoding {
			indexHasOldStoredColumns := index.HasOldStoredColumns()
			// Need to clear ExtraColumnIDs and StoreColumnIDs because they are used
			// by ContainsColumnID.
			index.ExtraColumnIDs = nil
			index.StoreColumnIDs = nil
			var extraColumnIDs []descpb.ColumnID
			for _, primaryColID := range desc.PrimaryIndex.ColumnIDs {
				if !index.ContainsColumnID(primaryColID) {
					extraColumnIDs = append(extraColumnIDs, primaryColID)
				}
			}
			index.ExtraColumnIDs = extraColumnIDs

			for _, colName := range index.StoreColumnNames {
				col, _, err := desc.FindColumnByName(tree.Name(colName))
				if err != nil {
					return err
				}
				if desc.PrimaryIndex.ContainsColumnID(col.ID) {
					// If the primary index contains a stored column, we don't need to
					// store it - it's already part of the index.
					err = pgerror.Newf(
						pgcode.DuplicateColumn, "index %q already contains column %q", index.Name, col.Name)
					err = errors.WithDetailf(err, "column %q is part of the primary index and therefore implicit in all indexes", col.Name)
					return err
				}
				if index.ContainsColumnID(col.ID) {
					return pgerror.Newf(
						pgcode.DuplicateColumn,
						"index %q already contains column %q", index.Name, col.Name)
				}
				if indexHasOldStoredColumns {
					index.ExtraColumnIDs = append(index.ExtraColumnIDs, col.ID)
				} else {
					index.StoreColumnIDs = append(index.StoreColumnIDs, col.ID)
				}
			}
		}

		index.CompositeColumnIDs = nil
		for _, colID := range index.ColumnIDs {
			if compositeColIDs.Contains(colID) {
				index.CompositeColumnIDs = append(index.CompositeColumnIDs, colID)
			}
		}
		for _, colID := range index.ExtraColumnIDs {
			if compositeColIDs.Contains(colID) {
				index.CompositeColumnIDs = append(index.CompositeColumnIDs, colID)
			}
		}
	}
	return nil
}

func (desc *Mutable) allocateColumnFamilyIDs(columnNames map[string]descpb.ColumnID) {
	if desc.NextFamilyID == 0 {
		if len(desc.Families) == 0 {
			desc.Families = []descpb.ColumnFamilyDescriptor{
				{ID: 0, Name: "primary"},
			}
		}
		desc.NextFamilyID = 1
	}

	var columnsInFamilies catalog.TableColSet
	for i := range desc.Families {
		family := &desc.Families[i]
		if family.ID == 0 && i != 0 {
			family.ID = desc.NextFamilyID
			desc.NextFamilyID++
		}

		for j, colName := range family.ColumnNames {
			if len(family.ColumnIDs) <= j {
				family.ColumnIDs = append(family.ColumnIDs, 0)
			}
			if family.ColumnIDs[j] == 0 {
				family.ColumnIDs[j] = columnNames[colName]
			}
			columnsInFamilies.Add(family.ColumnIDs[j])
		}

		desc.Families[i] = *family
	}

	var primaryIndexColIDs catalog.TableColSet
	for _, colID := range desc.PrimaryIndex.ColumnIDs {
		primaryIndexColIDs.Add(colID)
	}

	ensureColumnInFamily := func(col *descpb.ColumnDescriptor) {
		if col.Virtual {
			// Virtual columns don't need to be part of families.
			return
		}
		if columnsInFamilies.Contains(col.ID) {
			return
		}
		if primaryIndexColIDs.Contains(col.ID) {
			// Primary index columns are required to be assigned to family 0.
			desc.Families[0].ColumnNames = append(desc.Families[0].ColumnNames, col.Name)
			desc.Families[0].ColumnIDs = append(desc.Families[0].ColumnIDs, col.ID)
			return
		}
		var familyID descpb.FamilyID
		if desc.ParentID == keys.SystemDatabaseID {
			// TODO(dan): This assigns families such that the encoding is exactly the
			// same as before column families. It's used for all system tables because
			// reads of them don't go through the normal sql layer, which is where the
			// knowledge of families lives. Fix that and remove this workaround.
			familyID = descpb.FamilyID(col.ID)
			desc.Families = append(desc.Families, descpb.ColumnFamilyDescriptor{
				ID:          familyID,
				ColumnNames: []string{col.Name},
				ColumnIDs:   []descpb.ColumnID{col.ID},
			})
		} else {
			idx, ok := fitColumnToFamily(desc, *col)
			if !ok {
				idx = len(desc.Families)
				desc.Families = append(desc.Families, descpb.ColumnFamilyDescriptor{
					ID:          desc.NextFamilyID,
					ColumnNames: []string{},
					ColumnIDs:   []descpb.ColumnID{},
				})
			}
			familyID = desc.Families[idx].ID
			desc.Families[idx].ColumnNames = append(desc.Families[idx].ColumnNames, col.Name)
			desc.Families[idx].ColumnIDs = append(desc.Families[idx].ColumnIDs, col.ID)
		}
		if familyID >= desc.NextFamilyID {
			desc.NextFamilyID = familyID + 1
		}
	}
	for i := range desc.Columns {
		ensureColumnInFamily(&desc.Columns[i])
	}
	for _, m := range desc.Mutations {
		if c := m.GetColumn(); c != nil {
			ensureColumnInFamily(c)
		}
	}

	for i := range desc.Families {
		family := &desc.Families[i]
		if len(family.Name) == 0 {
			family.Name = generatedFamilyName(family.ID, family.ColumnNames)
		}

		if family.DefaultColumnID == 0 {
			defaultColumnID := descpb.ColumnID(0)
			for _, colID := range family.ColumnIDs {
				if !primaryIndexColIDs.Contains(colID) {
					if defaultColumnID == 0 {
						defaultColumnID = colID
					} else {
						defaultColumnID = descpb.ColumnID(0)
						break
					}
				}
			}
			family.DefaultColumnID = defaultColumnID
		}

		desc.Families[i] = *family
	}
}

// fitColumnToFamily attempts to fit a new column into the existing column
// families. If the heuristics find a fit, true is returned along with the
// index of the selected family. Otherwise, false is returned and the column
// should be put in a new family.
//
// Current heuristics:
//  - Put all columns in family 0.
func fitColumnToFamily(desc *Mutable, col descpb.ColumnDescriptor) (int, bool) {
	// Fewer column families means fewer kv entries, which is generally faster.
	// On the other hand, an update to any column in a family requires that they
	// all are read and rewritten, so large (or numerous) columns that are not
	// updated at the same time as other columns in the family make things
	// slower.
	//
	// The initial heuristic used for family assignment tried to pack
	// fixed-width columns into families up to a certain size and would put any
	// variable-width column into its own family. This was conservative to
	// guarantee that we avoid the worst-case behavior of a very large immutable
	// blob in the same family as frequently updated columns.
	//
	// However, our initial customers have revealed that this is backward.
	// Repeatedly, they have recreated existing schemas without any tuning and
	// found lackluster performance. Each of these has turned out better as a
	// single family (sometimes 100% faster or more), the most aggressive tuning
	// possible.
	//
	// Further, as the WideTable benchmark shows, even the worst-case isn't that
	// bad (33% slower with an immutable 1MB blob, which is the upper limit of
	// what we'd recommend for column size regardless of families). This
	// situation also appears less frequent than we feared.
	//
	// The result is that we put all columns in one family and require the user
	// to manually specify family assignments when this is incorrect.
	return 0, true
}

// MaybeIncrementVersion implements the MutableDescriptor interface.
func (desc *Mutable) MaybeIncrementVersion() {
	// Already incremented, no-op.
	if desc.Version == desc.ClusterVersion.Version+1 || desc.ClusterVersion.Version == 0 {
		return
	}
	desc.Version++

	// Starting in 19.2 we use a zero-valued ModificationTime when incrementing
	// the version, and then, upon reading, use the MVCC timestamp to populate
	// the ModificationTime.
	desc.ModificationTime = hlc.Timestamp{}
}

// OriginalName implements the MutableDescriptor interface.
func (desc *Mutable) OriginalName() string {
	return desc.ClusterVersion.Name
}

// OriginalID implements the MutableDescriptor interface.
func (desc *Mutable) OriginalID() descpb.ID {
	return desc.ClusterVersion.ID
}

// OriginalVersion implements the MutableDescriptor interface.
func (desc *Mutable) OriginalVersion() descpb.DescriptorVersion {
	return desc.ClusterVersion.Version
}

type testingDescriptorValidation bool

// PerformTestingDescriptorValidation can be set as a value on a context to
// ensure testing specific descriptor validation happens.
var PerformTestingDescriptorValidation testingDescriptorValidation = true

// Validate validates that the table descriptor is well formed. Checks include
// both single table and cross table invariants.
func (desc *wrapper) Validate(ctx context.Context, dg catalog.DescGetter) error {
	err := desc.ValidateTable(ctx)
	if err != nil {
		return err
	}
	if desc.Dropped() {
		return nil
	}
	return errors.Wrapf(desc.validateCrossReferences(ctx, dg), "desc %d", desc.GetID())
}

// validateTableIfTesting is similar to validateTable, except it is only invoked
// when the context has the `PerformTestingDescriptorValidation` value set on it
// (dictated by ExecutorTestingKnobs). Any cross descriptor validations that may
// fail in the wild due to known bugs that have now been fixed should be added
// here instead of validateCrossReferences.
func (desc *wrapper) validateTableIfTesting(ctx context.Context) error {
	if isTesting := ctx.Value(PerformTestingDescriptorValidation); isTesting == nil {
		return nil
	}
	// TODO(arul): Fill this with testing only table validation
	return nil
}

// validateCrossReferencesIfTesting is similar to validateCrossReferences,
// except it is only invoked when the context has the
// `PerformTestingDescriptorValidation` value set on it
// (dictated by ExecutorTestingKnobs). Any cross reference descriptor validation
// that may fail in the wild due to known bugs that have now been fixed should
// be added here instead of validateCrossReferences.
func (desc *wrapper) validateCrossReferencesIfTesting(
	ctx context.Context, _ catalog.DescGetter,
) error {
	if isTesting := ctx.Value(PerformTestingDescriptorValidation); isTesting == nil {
		return nil
	}
	// TODO(arul): Fill this with testing only cross reference validation
	return nil
}

// validateCrossReferences validates that each reference to another table is
// resolvable and that the necessary back references exist.
func (desc *wrapper) validateCrossReferences(ctx context.Context, dg catalog.DescGetter) error {
	{
		// Check that parent DB exists.
		dbDesc, err := dg.GetDesc(ctx, desc.ParentID)
		if err != nil {
			return err
		}
		db, isDB := dbDesc.(catalog.DatabaseDescriptor)

		if !isDB {
			return errors.AssertionFailedf("parentID %d does not exist", errors.Safe(desc.ParentID))
		}

		if desc.LocalityConfig != nil {
			if err := ValidateTableLocalityConfig(desc.Name, desc.LocalityConfig, db); err != nil {
				return errors.AssertionFailedf("invalid locality config: %v", errors.Safe(err))
			}
		}
	}

	tablesByID := map[descpb.ID]catalog.TableDescriptor{desc.ID: desc}
	getTable := func(id descpb.ID) (catalog.TableDescriptor, error) {
		if table, ok := tablesByID[id]; ok {
			return table, nil
		}
		table, err := catalog.GetTableDescFromID(ctx, dg, id)
		if err != nil {
			return nil, err
		}
		tablesByID[id] = table
		return table, nil
	}

	findTargetIndex := func(tableID descpb.ID, indexID descpb.IndexID) (catalog.TableDescriptor, *descpb.IndexDescriptor, error) {
		targetTable, err := getTable(tableID)
		if err != nil {
			return nil, nil, errors.Wrapf(err,
				"missing table=%d index=%d", errors.Safe(tableID), errors.Safe(indexID))
		}
		targetIndex, err := targetTable.FindIndexWithID(indexID)
		if err != nil {
			return nil, nil, errors.Wrapf(err,
				"missing table=%s index=%d", targetTable.GetName(), errors.Safe(indexID))
		}
		return targetTable, targetIndex.IndexDesc(), nil
	}

	// Check foreign keys.
	for i := range desc.OutboundFKs {
		fk := &desc.OutboundFKs[i]
		referencedTable, err := getTable(fk.ReferencedTableID)
		if err != nil {
			return errors.Wrapf(err,
				"invalid foreign key: missing table=%d", errors.Safe(fk.ReferencedTableID))
		}
		found := false
		_ = referencedTable.ForeachInboundFK(func(backref *descpb.ForeignKeyConstraint) error {
			if !found && backref.OriginTableID == desc.ID && backref.Name == fk.Name {
				found = true
			}
			return nil
		})
		if found {
			continue
		}
		// In 20.2 we introduced a bug where we fail to upgrade the FK references
		// on the referenced descriptors from their pre-19.2 format when reading
		// them during validation (#57032). So we account for the possibility of
		// un-upgraded foreign key references on the other table. This logic
		// somewhat parallels the logic in maybeUpgradeForeignKeyRepOnIndex.
		unupgradedFKsPresent := false
		if err := catalog.ForEachIndex(referencedTable, catalog.IndexOpts{}, func(referencedIdx catalog.Index) error {
			if found {
				// TODO (lucy): If we ever revisit the tabledesc.Immutable methods, add
				// a way to break out of the index loop.
				return nil
			}
			if len(referencedIdx.IndexDesc().ReferencedBy) > 0 {
				unupgradedFKsPresent = true
			} else {
				return nil
			}
			// Determine whether the index on the other table is a unique index that
			// could support this FK constraint.
			if !referencedIdx.IsValidReferencedUniqueConstraint(fk.ReferencedColumnIDs) {
				return nil
			}
			// Now check the backreferences. Backreferences in ReferencedBy only had
			// Index and Table populated.
			for i := range referencedIdx.IndexDesc().ReferencedBy {
				backref := &referencedIdx.IndexDesc().ReferencedBy[i]
				if backref.Table != desc.ID {
					continue
				}
				// Look up the index that the un-upgraded reference refers to and
				// see if that index could support the foreign key reference. (Note
				// that it shouldn't be possible for this index to not exist. See
				// planner.MaybeUpgradeDependentOldForeignKeyVersionTables, which is
				// called from the drop index implementation.)
				originalOriginIndex, err := desc.FindIndexWithID(backref.Index)
				if err != nil {
					return errors.AssertionFailedf(
						"missing index %d on %q from pre-19.2 foreign key "+
							"backreference %q on %q",
						backref.Index, desc.Name, fk.Name, referencedTable.GetName(),
					)
				}
				if originalOriginIndex.IsValidOriginIndex(fk.OriginColumnIDs) {
					found = true
					break
				}
			}
			return nil
		}); err != nil {
			return err
		}
		if found {
			continue
		}
		if unupgradedFKsPresent {
			return errors.AssertionFailedf("missing fk back reference %q to %q "+
				"from %q (un-upgraded foreign key references present)",
				fk.Name, desc.Name, referencedTable.GetName())
		}
		return errors.AssertionFailedf("missing fk back reference %q to %q from %q",
			fk.Name, desc.Name, referencedTable.GetName())
	}
	for i := range desc.InboundFKs {
		backref := &desc.InboundFKs[i]
		originTable, err := getTable(backref.OriginTableID)
		if err != nil {
			return errors.Wrapf(err,
				"invalid foreign key backreference: missing table=%d", errors.Safe(backref.OriginTableID))
		}
		found := false
		_ = originTable.ForeachOutboundFK(func(fk *descpb.ForeignKeyConstraint) error {
			if !found && fk.ReferencedTableID == desc.ID && fk.Name == backref.Name {
				found = true
			}
			return nil
		})
		if found {
			continue
		}
		// In 20.2 we introduced a bug where we fail to upgrade the FK references
		// on the referenced descriptors from their pre-19.2 format when reading
		// them during validation (#57032). So we account for the possibility of
		// un-upgraded foreign key references on the other table. This logic
		// somewhat parallels the logic in maybeUpgradeForeignKeyRepOnIndex.
		unupgradedFKsPresent := false
		if err := catalog.ForEachIndex(originTable, catalog.IndexOpts{}, func(originIdx catalog.Index) error {
			if found {
				// TODO (lucy): If we ever revisit the tabledesc.Immutable methods, add
				// a way to break out of the index loop.
				return nil
			}
			fk := originIdx.IndexDesc().ForeignKey
			if fk.IsSet() {
				unupgradedFKsPresent = true
			} else {
				return nil
			}
			// Determine whether the index on the other table is a index that could
			// support this FK constraint on the referencing side. Such an index would
			// have been required in earlier versions.
			if !originIdx.IsValidOriginIndex(backref.OriginColumnIDs) {
				return nil
			}
			if fk.Table != desc.ID {
				return nil
			}
			// Look up the index that the un-upgraded reference refers to and
			// see if that index could support the foreign key reference. (Note
			// that it shouldn't be possible for this index to not exist. See
			// planner.MaybeUpgradeDependentOldForeignKeyVersionTables, which is
			// called from the drop index implementation.)
			originalReferencedIndex, err := desc.FindIndexWithID(fk.Index)
			if err != nil {
				return errors.AssertionFailedf(
					"missing index %d on %q from pre-19.2 foreign key forward reference %q on %q",
					fk.Index, desc.Name, backref.Name, originTable.GetName(),
				)
			}
			if originalReferencedIndex.IsValidReferencedUniqueConstraint(backref.ReferencedColumnIDs) {
				found = true
			}
			return nil
		}); err != nil {
			return err
		}
		if found {
			continue
		}
		if unupgradedFKsPresent {
			return errors.AssertionFailedf("missing fk forward reference %q to %q from %q "+
				"(un-upgraded foreign key references present)",
				backref.Name, desc.Name, originTable.GetName())
		}
		return errors.AssertionFailedf("missing fk forward reference %q to %q from %q",
			backref.Name, desc.Name, originTable.GetName())
	}

	for _, indexI := range desc.NonDropIndexes() {
		index := indexI.IndexDesc()
		// Check partitioning is correctly set.
		if desc.PartitionAllBy {
			primaryIndexPartitioning := desc.PrimaryIndex.ColumnIDs[:desc.PrimaryIndex.Partitioning.NumColumns]
			indexPartitioning := index.ColumnIDs[:index.Partitioning.NumColumns]
			matchesPartitioning := false
			if len(primaryIndexPartitioning) == len(indexPartitioning) {
				matchesPartitioning = true
				for i := range primaryIndexPartitioning {
					if primaryIndexPartitioning[i] != indexPartitioning[i] {
						matchesPartitioning = false
						break
					}
				}
			}
			if !matchesPartitioning {
				return errors.AssertionFailedf(
					"table has PARTITION ALL BY defined, but index %s does not have matching PARTITION BY",
					index.Name,
				)
			}
		}
		// Check interleaves.
		if len(index.Interleave.Ancestors) > 0 {
			// Only check the most recent ancestor, the rest of them don't point
			// back.
			ancestor := index.Interleave.Ancestors[len(index.Interleave.Ancestors)-1]
			targetTable, targetIndex, err := findTargetIndex(ancestor.TableID, ancestor.IndexID)
			if err != nil {
				return errors.Wrapf(err, "invalid interleave")
			}
			found := false
			for _, backref := range targetIndex.InterleavedBy {
				if backref.Table == desc.ID && backref.Index == index.ID {
					found = true
					break
				}
			}
			if !found {
				return errors.AssertionFailedf(
					"missing interleave back reference to %q@%q from %q@%q",
					desc.Name, index.Name, targetTable.GetName(), targetIndex.Name)
			}
		}
		interleaveBackrefs := make(map[descpb.ForeignKeyReference]struct{})
		for _, backref := range index.InterleavedBy {
			if _, ok := interleaveBackrefs[backref]; ok {
				return errors.AssertionFailedf("duplicated interleave backreference %+v", backref)
			}
			interleaveBackrefs[backref] = struct{}{}
			targetTable, err := getTable(backref.Table)
			if err != nil {
				return errors.Wrapf(err,
					"invalid interleave backreference table=%d index=%d",
					backref.Table, backref.Index)
			}
			targetIndex, err := targetTable.FindIndexWithID(backref.Index)
			if err != nil {
				return errors.Wrapf(err,
					"invalid interleave backreference table=%s index=%d",
					targetTable.GetName(), backref.Index)
			}
			if targetIndex.NumInterleaveAncestors() == 0 {
				return errors.AssertionFailedf(
					"broken interleave backward reference from %q@%q to %q@%q",
					desc.Name, index.Name, targetTable.GetName(), targetIndex.GetName())
			}
			// The last ancestor is required to be a backreference.
			ancestor := targetIndex.GetInterleaveAncestor(targetIndex.NumInterleaveAncestors() - 1)
			if ancestor.TableID != desc.ID || ancestor.IndexID != index.ID {
				return errors.AssertionFailedf(
					"broken interleave backward reference from %q@%q to %q@%q",
					desc.Name, index.Name, targetTable.GetName(), targetIndex.GetName())
			}
		}
	}
	// TODO(dan): Also validate SharedPrefixLen in the interleaves.

	// Validate the all types present in the descriptor exist. typeMap caches
	// accesses to TypeDescriptors, and is wrapped by getType.
	// TODO(ajwerner): generalize this to a cached implementation of the
	// DescGetter.
	typeMap := make(map[descpb.ID]catalog.TypeDescriptor)
	getType := func(id descpb.ID) (catalog.TypeDescriptor, error) {
		if typeDesc, ok := typeMap[id]; ok {
			return typeDesc, nil
		}
		typeDesc, err := catalog.GetTypeDescFromID(ctx, dg, id)
		if err != nil {
			return nil, errors.Wrapf(err, "type ID %d in descriptor not found", id)
		}
		typeMap[id] = typeDesc
		return typeDesc, nil
	}
	typeIDs, err := desc.GetAllReferencedTypeIDs(getType)
	if err != nil {
		return err
	}
	for _, id := range typeIDs {
		if _, err := getType(id); err != nil {
			return err
		}
	}

	return desc.validateCrossReferencesIfTesting(ctx, dg)
}

// FormatTableLocalityConfig formats the table locality.
func FormatTableLocalityConfig(c *descpb.TableDescriptor_LocalityConfig, f *tree.FmtCtx) error {
	switch v := c.Locality.(type) {
	case *descpb.TableDescriptor_LocalityConfig_Global_:
		f.WriteString("GLOBAL")
	case *descpb.TableDescriptor_LocalityConfig_RegionalByTable_:
		f.WriteString("REGIONAL BY TABLE IN ")
		if v.RegionalByTable.Region != nil {
			region := tree.Name(*v.RegionalByTable.Region)
			f.FormatNode(&region)
		} else {
			f.WriteString("PRIMARY REGION")
		}
	case *descpb.TableDescriptor_LocalityConfig_RegionalByRow_:
		f.WriteString("REGIONAL BY ROW")
	default:
		return errors.Newf("unknown locality: %T", v)
	}
	return nil
}

// ValidateTableLocalityConfig validates whether a given locality config is valid
// under the given database.
func ValidateTableLocalityConfig(
	tblName string,
	localityConfig *descpb.TableDescriptor_LocalityConfig,
	db catalog.DatabaseDescriptor,
) error {
	if !db.IsMultiRegion() {
		s := tree.NewFmtCtx(tree.FmtSimple)
		var locality string
		// This should never happen; if so, the error message is more clear if we
		// return a dummy locality here.
		if err := FormatTableLocalityConfig(localityConfig, s); err != nil {
			locality = "INVALID LOCALITY"
		}
		locality = s.String()
		return pgerror.Newf(
			pgcode.InvalidTableDefinition,
			"database %s is not multi-region enabled, but table %s has locality %s set",
			db.DatabaseDesc().Name,
			tblName,
			locality,
		)
	}
	switch lc := localityConfig.Locality.(type) {
	case *descpb.TableDescriptor_LocalityConfig_Global_, *descpb.TableDescriptor_LocalityConfig_RegionalByRow_:
	case *descpb.TableDescriptor_LocalityConfig_RegionalByTable_:
		if lc.RegionalByTable.Region != nil {
			foundRegion := false
			regions, err := db.Regions()
			if err != nil {
				return err
			}
			for _, r := range regions {
				if *lc.RegionalByTable.Region == r {
					foundRegion = true
					break
				}
			}
			if !foundRegion {
				return errors.WithHintf(
					pgerror.Newf(
						pgcode.InvalidTableDefinition,
						`region "%s" has not been added to database "%s"`,
						*lc.RegionalByTable.Region,
						db.DatabaseDesc().Name,
					),
					"available regions: %s",
					strings.Join(regions.ToStrings(), ", "),
				)
			}
		}
	default:
		return pgerror.Newf(
			pgcode.InvalidTableDefinition,
			"unknown locality level: %T",
			lc,
		)
	}
	return nil
}

// ValidateIndexNameIsUnique validates that the index name does not exist.
func (desc *wrapper) ValidateIndexNameIsUnique(indexName string) error {
	if catalog.FindNonDropIndex(desc, func(idx catalog.Index) bool {
		return idx.GetName() == indexName
	}) != nil {
		return sqlerrors.NewRelationAlreadyExistsError(indexName)
	}
	return nil
}

// ValidateTable validates that the table descriptor is well formed. Checks
// include validating the table, column and index names, verifying that column
// names and index names are unique and verifying that column IDs and index IDs
// are consistent. Use Validate to validate that cross-table references are
// correct.
// If version is supplied, the descriptor is checked for version incompatibilities.
func (desc *wrapper) ValidateTable(ctx context.Context) error {
	if err := catalog.ValidateName(desc.Name, "table"); err != nil {
		return err
	}
	if desc.ID == 0 {
		return errors.AssertionFailedf("invalid table ID %d", errors.Safe(desc.ID))
	}

	// TODO(dt, nathan): virtual descs don't validate (missing privs, PK, etc).
	if desc.IsVirtualTable() {
		return nil
	}

	if desc.IsSequence() {
		return nil
	}

	// ParentID is the ID of the database holding this table.
	// It is often < ID, except when a table gets moved across databases.
	if desc.ParentID == 0 {
		return errors.AssertionFailedf("invalid parent ID %d", errors.Safe(desc.ParentID))
	}

	// We maintain forward compatibility, so if you see this error message with a
	// version older that what this client supports, then there's a
	// maybeFillInDescriptor missing from some codepath.
	if v := desc.GetFormatVersion(); v != descpb.FamilyFormatVersion && v != descpb.InterleavedFormatVersion {
		// TODO(dan): We're currently switching from FamilyFormatVersion to
		// InterleavedFormatVersion. After a beta is released with this dual version
		// support, then:
		// - Upgrade the bidirectional reference version to that beta
		// - Start constructing all TableDescriptors with InterleavedFormatVersion
		// - Change maybeUpgradeFormatVersion to output InterleavedFormatVersion
		// - Change this check to only allow InterleavedFormatVersion
		return errors.AssertionFailedf(
			"table %q is encoded using using version %d, but this client only supports version %d and %d",
			desc.Name, errors.Safe(desc.GetFormatVersion()),
			errors.Safe(descpb.FamilyFormatVersion), errors.Safe(descpb.InterleavedFormatVersion))
	}

	if len(desc.Columns) == 0 {
		return ErrMissingColumns
	}

	if err := desc.CheckUniqueConstraints(); err != nil {
		return err
	}

	columnNames := make(map[string]descpb.ColumnID, len(desc.Columns))
	columnIDs := make(map[descpb.ColumnID]*descpb.ColumnDescriptor, len(desc.Columns))
	if err := desc.validateColumns(columnNames, columnIDs); err != nil {
		return err
	}

	for _, m := range desc.Mutations {
		unSetEnums := m.State == descpb.DescriptorMutation_UNKNOWN || m.Direction == descpb.DescriptorMutation_NONE
		switch desc := m.Descriptor_.(type) {
		case *descpb.DescriptorMutation_Column:
			col := desc.Column
			if unSetEnums {
				return errors.AssertionFailedf(
					"mutation in state %s, direction %s, col %q, id %v",
					errors.Safe(m.State), errors.Safe(m.Direction), col.Name, errors.Safe(col.ID))
			}
			columnIDs[col.ID] = col
		case *descpb.DescriptorMutation_Index:
			if unSetEnums {
				idx := desc.Index
				return errors.AssertionFailedf(
					"mutation in state %s, direction %s, index %s, id %v",
					errors.Safe(m.State), errors.Safe(m.Direction), idx.Name, errors.Safe(idx.ID))
			}
		case *descpb.DescriptorMutation_Constraint:
			if unSetEnums {
				return errors.AssertionFailedf(
					"mutation in state %s, direction %s, constraint %v",
					errors.Safe(m.State), errors.Safe(m.Direction), desc.Constraint.Name)
			}
		case *descpb.DescriptorMutation_PrimaryKeySwap:
			if m.Direction == descpb.DescriptorMutation_NONE {
				return errors.AssertionFailedf(
					"primary key swap mutation in state %s, direction %s", errors.Safe(m.State), errors.Safe(m.Direction))
			}
		case *descpb.DescriptorMutation_ComputedColumnSwap:
			if m.Direction == descpb.DescriptorMutation_NONE {
				return errors.AssertionFailedf(
					"computed column swap mutation in state %s, direction %s", errors.Safe(m.State), errors.Safe(m.Direction))
			}
		case *descpb.DescriptorMutation_MaterializedViewRefresh:
			if m.Direction == descpb.DescriptorMutation_NONE {
				return errors.AssertionFailedf(
					"materialized view refresh mutation in state %s, direction %s", errors.Safe(m.State), errors.Safe(m.Direction))
			}
		default:
			return errors.AssertionFailedf(
				"mutation in state %s, direction %s, and no column/index descriptor",
				errors.Safe(m.State), errors.Safe(m.Direction))
		}
	}

	// TODO(dt): Validate each column only appears at-most-once in any FKs.

	// Only validate column families, constraints, and indexes if this is
	// actually a table, not if it's just a view.
	if desc.IsPhysicalTable() {
		if err := desc.validateColumnFamilies(columnIDs); err != nil {
			return err
		}

		if err := desc.validateCheckConstraints(columnIDs); err != nil {
			return err
		}

		if err := desc.validateUniqueWithoutIndexConstraints(columnIDs); err != nil {
			return err
		}

		if err := desc.validateTableIndexes(columnNames); err != nil {
			return err
		}

		if err := desc.validatePartitioning(); err != nil {
			return err
		}
	}

	// Fill in any incorrect privileges that may have been missed due to mixed-versions.
	// TODO(mberhault): remove this in 2.1 (maybe 2.2) when privilege-fixing migrations have been
	// run again and mixed-version clusters always write "good" descriptors.
	descpb.MaybeFixPrivileges(desc.GetID(), desc.Privileges)

	// Ensure that mutations cannot be queued if a primary key change or
	// an alter column type schema change has either been started in
	// this transaction, or is currently in progress.
	var alterPKMutation descpb.MutationID
	var alterColumnTypeMutation descpb.MutationID
	var foundAlterPK bool
	var foundAlterColumnType bool

	for _, m := range desc.Mutations {
		// If we have seen an alter primary key mutation, then
		// m we are considering right now is invalid.
		if foundAlterPK {
			if alterPKMutation == m.MutationID {
				return unimplemented.NewWithIssue(
					45615,
					"cannot perform other schema changes in the same transaction as a primary key change",
				)
			}
			return unimplemented.NewWithIssue(
				45615,
				"cannot perform a schema change operation while a primary key change is in progress",
			)
		}
		if foundAlterColumnType {
			if alterColumnTypeMutation == m.MutationID {
				return unimplemented.NewWithIssue(
					47137,
					"cannot perform other schema changes in the same transaction as an ALTER COLUMN TYPE schema change",
				)
			}
			return unimplemented.NewWithIssue(
				47137,
				"cannot perform a schema change operation while an ALTER COLUMN TYPE schema change is in progress",
			)
		}
		if m.GetPrimaryKeySwap() != nil {
			foundAlterPK = true
			alterPKMutation = m.MutationID
		}
		if m.GetComputedColumnSwap() != nil {
			foundAlterColumnType = true
			alterColumnTypeMutation = m.MutationID
		}
	}

	if err := desc.validateTableIfTesting(ctx); err != nil {
		return err
	}

	// Validate the privilege descriptor.
	return desc.Privileges.Validate(desc.GetID(), privilege.Table)
}

func (desc *wrapper) validateColumns(
	columnNames map[string]descpb.ColumnID, columnIDs map[descpb.ColumnID]*descpb.ColumnDescriptor,
) error {
	colDescs := desc.AllNonDropColumns()
	for colIdx := range colDescs {
		column := &colDescs[colIdx]

		if err := catalog.ValidateName(column.Name, "column"); err != nil {
			return err
		}
		if column.ID == 0 {
			return errors.AssertionFailedf("invalid column ID %d", errors.Safe(column.ID))
		}

		if _, columnNameExists := columnNames[column.Name]; columnNameExists {
			for i := range desc.Columns {
				if desc.Columns[i].Name == column.Name {
					return pgerror.Newf(pgcode.DuplicateColumn,
						"duplicate column name: %q", column.Name)
				}
			}
			return pgerror.Newf(pgcode.DuplicateColumn,
				"duplicate: column %q in the middle of being added, not yet public", column.Name)
		}
		if colinfo.IsSystemColumnName(column.Name) {
			return pgerror.Newf(pgcode.DuplicateColumn,
				"column name %q conflicts with a system column name", column.Name)
		}
		columnNames[column.Name] = column.ID

		if other, ok := columnIDs[column.ID]; ok {
			return fmt.Errorf("column %q duplicate ID of column %q: %d",
				column.Name, other.Name, column.ID)
		}
		columnIDs[column.ID] = column

		if column.ID >= desc.NextColumnID {
			return errors.AssertionFailedf("column %q invalid ID (%d) >= next column ID (%d)",
				column.Name, errors.Safe(column.ID), errors.Safe(desc.NextColumnID))
		}

		if column.IsComputed() {
			// Verify that the computed column expression is valid.
			expr, err := parser.ParseExpr(*column.ComputeExpr)
			if err != nil {
				return err
			}
			valid, err := schemaexpr.HasValidColumnReferences(desc, expr)
			if err != nil {
				return err
			}
			if !valid {
				return fmt.Errorf("computed column %q refers to unknown columns in expression: %s",
					column.Name, *column.ComputeExpr)
			}
		} else if column.Virtual {
			return fmt.Errorf("virtual column %q is not computed", column.Name)
		}
	}
	return nil
}

func (desc *wrapper) validateColumnFamilies(
	columnIDs map[descpb.ColumnID]*descpb.ColumnDescriptor,
) error {
	if len(desc.Families) < 1 {
		return fmt.Errorf("at least 1 column family must be specified")
	}
	if desc.Families[0].ID != descpb.FamilyID(0) {
		return fmt.Errorf("the 0th family must have ID 0")
	}

	familyNames := map[string]struct{}{}
	familyIDs := map[descpb.FamilyID]string{}
	colIDToFamilyID := map[descpb.ColumnID]descpb.FamilyID{}
	for i := range desc.Families {
		family := &desc.Families[i]
		if err := catalog.ValidateName(family.Name, "family"); err != nil {
			return err
		}

		if i != 0 {
			prevFam := desc.Families[i-1]
			if family.ID < prevFam.ID {
				return errors.Newf(
					"family %s at index %d has id %d less than family %s at index %d with id %d",
					family.Name, i, family.ID, prevFam.Name, i-1, prevFam.ID)
			}
		}

		if _, ok := familyNames[family.Name]; ok {
			return fmt.Errorf("duplicate family name: %q", family.Name)
		}
		familyNames[family.Name] = struct{}{}

		if other, ok := familyIDs[family.ID]; ok {
			return fmt.Errorf("family %q duplicate ID of family %q: %d",
				family.Name, other, family.ID)
		}
		familyIDs[family.ID] = family.Name

		if family.ID >= desc.NextFamilyID {
			return fmt.Errorf("family %q invalid family ID (%d) > next family ID (%d)",
				family.Name, family.ID, desc.NextFamilyID)
		}

		if len(family.ColumnIDs) != len(family.ColumnNames) {
			return fmt.Errorf("mismatched column ID size (%d) and name size (%d)",
				len(family.ColumnIDs), len(family.ColumnNames))
		}

		for i, colID := range family.ColumnIDs {
			col, ok := columnIDs[colID]
			if !ok {
				return fmt.Errorf("family %q contains unknown column \"%d\"", family.Name, colID)
			}
			if col.Name != family.ColumnNames[i] {
				return fmt.Errorf("family %q column %d should have name %q, but found name %q",
					family.Name, colID, col.Name, family.ColumnNames[i])
			}
			if col.Virtual {
				return fmt.Errorf("virtual computed column %q cannot be part of a family", col.Name)
			}
		}

		for _, colID := range family.ColumnIDs {
			if famID, ok := colIDToFamilyID[colID]; ok {
				return fmt.Errorf("column %d is in both family %d and %d", colID, famID, family.ID)
			}
			colIDToFamilyID[colID] = family.ID
		}
	}
	for colID, colDesc := range columnIDs {
		if !colDesc.Virtual {
			if _, ok := colIDToFamilyID[colID]; !ok {
				return fmt.Errorf("column %q is not in any column family", colDesc.Name)
			}
		}
	}
	return nil
}

// validateCheckConstraints validates that check constraints are well formed.
// Checks include validating the column IDs and verifying that check expressions
// do not reference non-existent columns.
func (desc *wrapper) validateCheckConstraints(
	columnIDs map[descpb.ColumnID]*descpb.ColumnDescriptor,
) error {
	for _, chk := range desc.AllActiveAndInactiveChecks() {
		// Verify that the check's column IDs are valid.
		for _, colID := range chk.ColumnIDs {
			_, ok := columnIDs[colID]
			if !ok {
				return fmt.Errorf("check constraint %q contains unknown column \"%d\"", chk.Name, colID)
			}
		}

		// Verify that the check's expression is valid.
		expr, err := parser.ParseExpr(chk.Expr)
		if err != nil {
			return err
		}
		valid, err := schemaexpr.HasValidColumnReferences(desc, expr)
		if err != nil {
			return err
		}
		if !valid {
			return fmt.Errorf("check constraint %q refers to unknown columns in expression: %s",
				chk.Name, chk.Expr)
		}
	}
	return nil
}

// validateUniqueWithoutIndexConstraints validates that unique without index
// constraints are well formed. Checks include validating the column IDs and
// column names.
func (desc *wrapper) validateUniqueWithoutIndexConstraints(
	columnIDs map[descpb.ColumnID]*descpb.ColumnDescriptor,
) error {
	for _, c := range desc.AllActiveAndInactiveUniqueWithoutIndexConstraints() {
		if err := catalog.ValidateName(c.Name, "unique without index constraint"); err != nil {
			return err
		}

		// Verify that the table ID is valid.
		if c.TableID != desc.ID {
			return fmt.Errorf(
				"TableID mismatch for unique without index constraint %q: \"%d\" doesn't match descriptor: \"%d\"",
				c.Name, c.TableID, desc.ID,
			)
		}

		// Verify that the constraint's column IDs are valid and unique.
		var seen util.FastIntSet
		for _, colID := range c.ColumnIDs {
			_, ok := columnIDs[colID]
			if !ok {
				return fmt.Errorf(
					"unique without index constraint %q contains unknown column \"%d\"", c.Name, colID,
				)
			}
			if seen.Contains(int(colID)) {
				return fmt.Errorf(
					"unique without index constraint %q contains duplicate column \"%d\"", c.Name, colID,
				)
			}
			seen.Add(int(colID))
		}
	}

	return nil
}

// validateTableIndexes validates that indexes are well formed. Checks include
// validating the columns involved in the index, verifying the index names and
// IDs are unique, and the family of the primary key is 0. This does not check
// if indexes are unique (i.e. same set of columns, direction, and uniqueness)
// as there are practical uses for them.
func (desc *wrapper) validateTableIndexes(columnNames map[string]descpb.ColumnID) error {
	if len(desc.PrimaryIndex.ColumnIDs) == 0 {
		return ErrMissingPrimaryKey
	}

	var virtualCols catalog.TableColSet
	for i := range desc.Columns {
		if desc.Columns[i].Virtual {
			virtualCols.Add(desc.Columns[i].ID)
		}
	}

	// Verify that the primary index columns are not virtual.
	for i, col := range desc.PrimaryIndex.ColumnIDs {
		if virtualCols.Contains(col) {
			return fmt.Errorf("primary index column %q cannot be virtual", desc.PrimaryIndex.ColumnNames[i])
		}
	}

	indexNames := map[string]struct{}{}
	indexIDs := map[descpb.IndexID]string{}
	for _, indexI := range desc.NonDropIndexes() {
		index := indexI.IndexDesc()
		if err := catalog.ValidateName(index.Name, "index"); err != nil {
			return err
		}
		if index.ID == 0 {
			return fmt.Errorf("invalid index ID %d", index.ID)
		}

		if _, indexNameExists := indexNames[index.Name]; indexNameExists {
			for i := range desc.Indexes {
				if desc.Indexes[i].Name == index.Name {
					// This error should be caught in MakeIndexDescriptor or NewTableDesc.
					return errors.HandleAsAssertionFailure(fmt.Errorf("duplicate index name: %q", index.Name))
				}
			}
			// This error should be caught in MakeIndexDescriptor.
			return errors.HandleAsAssertionFailure(fmt.Errorf(
				"duplicate: index %q in the middle of being added, not yet public", index.Name))
		}
		indexNames[index.Name] = struct{}{}

		if other, ok := indexIDs[index.ID]; ok {
			return fmt.Errorf("index %q duplicate ID of index %q: %d",
				index.Name, other, index.ID)
		}
		indexIDs[index.ID] = index.Name

		if index.ID >= desc.NextIndexID {
			return fmt.Errorf("index %q invalid index ID (%d) > next index ID (%d)",
				index.Name, index.ID, desc.NextIndexID)
		}

		if len(index.ColumnIDs) != len(index.ColumnNames) {
			return fmt.Errorf("mismatched column IDs (%d) and names (%d)",
				len(index.ColumnIDs), len(index.ColumnNames))
		}
		if len(index.ColumnIDs) != len(index.ColumnDirections) {
			return fmt.Errorf("mismatched column IDs (%d) and directions (%d)",
				len(index.ColumnIDs), len(index.ColumnDirections))
		}
		// In the old STORING encoding, stored columns are in ExtraColumnIDs;
		// tolerate a longer list of column names.
		if len(index.StoreColumnIDs) > len(index.StoreColumnNames) {
			return fmt.Errorf("mismatched STORING column IDs (%d) and names (%d)",
				len(index.StoreColumnIDs), len(index.StoreColumnNames))
		}

		if len(index.ColumnIDs) == 0 {
			return fmt.Errorf("index %q must contain at least 1 column", index.Name)
		}

		var validateIndexDup catalog.TableColSet
		for i, name := range index.ColumnNames {
			colID, ok := columnNames[name]
			if !ok {
				return fmt.Errorf("index %q contains unknown column %q", index.Name, name)
			}
			if colID != index.ColumnIDs[i] {
				return fmt.Errorf("index %q column %q should have ID %d, but found ID %d",
					index.Name, name, colID, index.ColumnIDs[i])
			}
			if validateIndexDup.Contains(colID) {
				return fmt.Errorf("index %q contains duplicate column %q", index.Name, name)
			}
			validateIndexDup.Add(colID)
		}
		if index.IsSharded() {
			if err := desc.ensureShardedIndexNotComputed(index); err != nil {
				return err
			}
			if _, exists := columnNames[index.Sharded.Name]; !exists {
				return fmt.Errorf("index %q refers to non-existent shard column %q",
					index.Name, index.Sharded.Name)
			}
		}
		if index.IsPartial() {
			expr, err := parser.ParseExpr(index.Predicate)
			if err != nil {
				return err
			}
			valid, err := schemaexpr.HasValidColumnReferences(desc, expr)
			if err != nil {
				return err
			}
			if !valid {
				return fmt.Errorf("partial index %q refers to unknown columns in predicate: %s",
					index.Name, index.Predicate)
			}
		}
		// Ensure that indexes do not STORE virtual columns.
		for _, col := range index.ExtraColumnIDs {
			if virtualCols.Contains(col) {
				return fmt.Errorf("index %q cannot store virtual column %d", index.Name, col)
			}
		}
		for i, col := range index.StoreColumnIDs {
			if virtualCols.Contains(col) {
				return fmt.Errorf("index %q cannot store virtual column %q", index.Name, index.StoreColumnNames[i])
			}
		}
	}

	return nil
}

// ensureShardedIndexNotComputed ensures that the sharded index is not based on a computed
// column. This is because the sharded index is based on a hidden computed shard column
// under the hood and we don't support transitively computed columns (computed column A
// based on another computed column B).
func (desc *wrapper) ensureShardedIndexNotComputed(index *descpb.IndexDescriptor) error {
	for _, colName := range index.Sharded.ColumnNames {
		col, _, err := desc.FindColumnByName(tree.Name(colName))
		if err != nil {
			return err
		}
		if col.IsComputed() {
			return pgerror.Newf(pgcode.InvalidTableDefinition,
				"cannot create a sharded index on a computed column")
		}
	}
	return nil
}

// PrimaryKeyString returns the pretty-printed primary key declaration for a
// table descriptor.
func (desc *wrapper) PrimaryKeyString() string {
	var primaryKeyString strings.Builder
	primaryKeyString.WriteString("PRIMARY KEY (%s)")
	if desc.PrimaryIndex.IsSharded() {
		fmt.Fprintf(&primaryKeyString, " USING HASH WITH BUCKET_COUNT = %v",
			desc.PrimaryIndex.Sharded.ShardBuckets)
	}
	return fmt.Sprintf(primaryKeyString.String(),
		desc.PrimaryIndex.ColNamesString(),
	)
}

// validatePartitioningDescriptor validates that a PartitioningDescriptor, which
// may represent a subpartition, is well-formed. Checks include validating the
// index-level uniqueness of all partition names, validating that the encoded
// tuples match the corresponding column types, and that range partitions are
// stored sorted by upper bound. colOffset is non-zero for subpartitions and
// indicates how many index columns to skip over.
func (desc *wrapper) validatePartitioningDescriptor(
	a *rowenc.DatumAlloc,
	idxDesc *descpb.IndexDescriptor,
	partDesc *descpb.PartitioningDescriptor,
	colOffset int,
	partitionNames map[string]string,
) error {
	if partDesc.NumImplicitColumns > partDesc.NumColumns {
		return errors.Newf(
			"cannot have implicit partitioning columns (%d) > partitioning columns (%d)",
			partDesc.NumImplicitColumns,
			partDesc.NumColumns,
		)
	}
	if partDesc.NumColumns == 0 {
		return nil
	}

	// TODO(dan): The sqlccl.GenerateSubzoneSpans logic is easier if we disallow
	// setting zone configs on indexes that are interleaved into another index.
	// InterleavedBy is fine, so using the root of the interleave hierarchy will
	// work. It is expected that this is sufficient for real-world use cases.
	// Revisit this restriction if that expectation is wrong.
	if len(idxDesc.Interleave.Ancestors) > 0 {
		return errors.Errorf("cannot set a zone config for interleaved index %s; "+
			"set it on the root of the interleaved hierarchy instead", idxDesc.Name)
	}

	// We don't need real prefixes in the DecodePartitionTuple calls because we're
	// only using it to look for collisions and the prefix would be the same for
	// all of them. Faking them out with DNull allows us to make O(list partition)
	// calls to DecodePartitionTuple instead of O(list partition entry).
	fakePrefixDatums := make([]tree.Datum, colOffset)
	for i := range fakePrefixDatums {
		fakePrefixDatums[i] = tree.DNull
	}

	if len(partDesc.List) == 0 && len(partDesc.Range) == 0 {
		return fmt.Errorf("at least one of LIST or RANGE partitioning must be used")
	}
	if len(partDesc.List) > 0 && len(partDesc.Range) > 0 {
		return fmt.Errorf("only one LIST or RANGE partitioning may used")
	}

	// Do not validate partitions which use unhydrated user-defined types.
	// This should only happen at read time and descriptors should not become
	// invalid at read time, only at write time.
	{
		numColumns := int(partDesc.NumColumns)
		for i := colOffset; i < colOffset+numColumns; i++ {
			// The partitioning descriptor may be invalid and refer to columns
			// not stored in the index. In that case, skip this check as the
			// validation will fail later.
			if i >= len(idxDesc.ColumnIDs) {
				continue
			}
			col, err := desc.FindColumnByID(idxDesc.ColumnIDs[i])
			if err != nil {
				return err
			}
			if col.Type.UserDefined() && !col.Type.IsHydrated() {
				return nil
			}
		}
	}

	checkName := func(name string) error {
		if len(name) == 0 {
			return fmt.Errorf("PARTITION name must be non-empty")
		}
		if indexName, exists := partitionNames[name]; exists {
			if indexName == idxDesc.Name {
				return fmt.Errorf("PARTITION %s: name must be unique (used twice in index %q)",
					name, indexName)
			}
		}
		partitionNames[name] = idxDesc.Name
		return nil
	}

	// Use the system-tenant SQL codec when validating the keys in the partition
	// descriptor. We just want to know how the partitions relate to one another,
	// so it's fine to ignore the tenant ID prefix.
	codec := keys.SystemSQLCodec

	if len(partDesc.List) > 0 {
		listValues := make(map[string]struct{}, len(partDesc.List))
		for _, p := range partDesc.List {
			if err := checkName(p.Name); err != nil {
				return err
			}

			if len(p.Values) == 0 {
				return fmt.Errorf("PARTITION %s: must contain values", p.Name)
			}
			// NB: key encoding is used to check uniqueness because it has
			// to match the behavior of the value when indexed.
			for _, valueEncBuf := range p.Values {
				tuple, keyPrefix, err := rowenc.DecodePartitionTuple(
					a, codec, desc, idxDesc, partDesc, valueEncBuf, fakePrefixDatums)
				if err != nil {
					return fmt.Errorf("PARTITION %s: %v", p.Name, err)
				}
				if _, exists := listValues[string(keyPrefix)]; exists {
					return fmt.Errorf("%s cannot be present in more than one partition", tuple)
				}
				listValues[string(keyPrefix)] = struct{}{}
			}

			newColOffset := colOffset + int(partDesc.NumColumns)
			if err := desc.validatePartitioningDescriptor(
				a, idxDesc, &p.Subpartitioning, newColOffset, partitionNames,
			); err != nil {
				return err
			}
		}
	}

	if len(partDesc.Range) > 0 {
		tree := interval.NewTree(interval.ExclusiveOverlapper)
		for _, p := range partDesc.Range {
			if err := checkName(p.Name); err != nil {
				return err
			}

			// NB: key encoding is used to check uniqueness because it has to match
			// the behavior of the value when indexed.
			fromDatums, fromKey, err := rowenc.DecodePartitionTuple(
				a, codec, desc, idxDesc, partDesc, p.FromInclusive, fakePrefixDatums)
			if err != nil {
				return fmt.Errorf("PARTITION %s: %v", p.Name, err)
			}
			toDatums, toKey, err := rowenc.DecodePartitionTuple(
				a, codec, desc, idxDesc, partDesc, p.ToExclusive, fakePrefixDatums)
			if err != nil {
				return fmt.Errorf("PARTITION %s: %v", p.Name, err)
			}
			pi := partitionInterval{p.Name, fromKey, toKey}
			if overlaps := tree.Get(pi.Range()); len(overlaps) > 0 {
				return fmt.Errorf("partitions %s and %s overlap",
					overlaps[0].(partitionInterval).name, p.Name)
			}
			if err := tree.Insert(pi, false /* fast */); errors.Is(err, interval.ErrEmptyRange) {
				return fmt.Errorf("PARTITION %s: empty range: lower bound %s is equal to upper bound %s",
					p.Name, fromDatums, toDatums)
			} else if errors.Is(err, interval.ErrInvertedRange) {
				return fmt.Errorf("PARTITION %s: empty range: lower bound %s is greater than upper bound %s",
					p.Name, fromDatums, toDatums)
			} else if err != nil {
				return errors.Wrapf(err, "PARTITION %s", p.Name)
			}
		}
	}

	return nil
}

type partitionInterval struct {
	name  string
	start roachpb.Key
	end   roachpb.Key
}

var _ interval.Interface = partitionInterval{}

// ID is part of `interval.Interface` but unused in validatePartitioningDescriptor.
func (ps partitionInterval) ID() uintptr { return 0 }

// Range is part of `interval.Interface`.
func (ps partitionInterval) Range() interval.Range {
	return interval.Range{Start: []byte(ps.start), End: []byte(ps.end)}
}

// validatePartitioning validates that any PartitioningDescriptors contained in
// table indexes are well-formed. See validatePartitioningDesc for details.
func (desc *wrapper) validatePartitioning() error {
	partitionNames := make(map[string]string)

	a := &rowenc.DatumAlloc{}
	return catalog.ForEachNonDropIndex(desc, func(idx catalog.Index) error {
		idxDesc := idx.IndexDesc()
		return desc.validatePartitioningDescriptor(
			a, idxDesc, &idxDesc.Partitioning, 0 /* colOffset */, partitionNames,
		)
	})
}

// FamilyHeuristicTargetBytes is the target total byte size of columns that the
// current heuristic will assign to a family.
const FamilyHeuristicTargetBytes = 256

func notIndexableError(cols []descpb.ColumnDescriptor, inverted bool) error {
	if len(cols) == 0 {
		return nil
	}
	var msg string
	var typInfo string
	if len(cols) == 1 {
		col := &cols[0]
		msg = "column %s is of type %s and thus is not indexable"
		if inverted {
			msg += " with an inverted index"
		}
		typInfo = col.Type.DebugString()
		msg = fmt.Sprintf(msg, col.Name, col.Type.Name())
	} else {
		msg = "the following columns are not indexable due to their type: "
		for i := range cols {
			col := &cols[i]
			msg += fmt.Sprintf("%s (type %s)", col.Name, col.Type.Name())
			typInfo += col.Type.DebugString()
			if i != len(cols)-1 {
				msg += ", "
				typInfo += ","
			}
		}
	}
	return unimplemented.NewWithIssueDetailf(35730, typInfo, "%s", msg)
}

func checkColumnsValidForIndex(tableDesc *Mutable, indexColNames []string) error {
	invalidColumns := make([]descpb.ColumnDescriptor, 0, len(indexColNames))
	for _, indexCol := range indexColNames {
		for _, col := range tableDesc.AllNonDropColumns() {
			if col.Name == indexCol {
				if !colinfo.ColumnTypeIsIndexable(col.Type) {
					invalidColumns = append(invalidColumns, col)
				}
			}
		}
	}
	if len(invalidColumns) > 0 {
		return notIndexableError(invalidColumns, false)
	}
	return nil
}

func checkColumnsValidForInvertedIndex(tableDesc *Mutable, indexColNames []string) error {
	invalidColumns := make([]descpb.ColumnDescriptor, 0, len(indexColNames))
	for i, indexCol := range indexColNames {
		for _, col := range tableDesc.AllNonDropColumns() {
			if col.Name == indexCol {
				lastCol := len(indexColNames) - 1
				if i == lastCol && !colinfo.ColumnTypeIsInvertedIndexable(col.Type) ||
					i < lastCol && !colinfo.ColumnTypeIsIndexable(col.Type) {
					invalidColumns = append(invalidColumns, col)
				}
			}
		}
	}
	if len(invalidColumns) > 0 {
		return notIndexableError(invalidColumns, true)
	}
	return nil
}

// AddColumn adds a column to the table.
func (desc *Mutable) AddColumn(col *descpb.ColumnDescriptor) {
	desc.Columns = append(desc.Columns, *col)
}

// AddFamily adds a family to the table.
func (desc *Mutable) AddFamily(fam descpb.ColumnFamilyDescriptor) {
	desc.Families = append(desc.Families, fam)
}

// AddIndex adds an index to the table.
func (desc *Mutable) AddIndex(idx descpb.IndexDescriptor, primary bool) error {
	if idx.Type == descpb.IndexDescriptor_FORWARD {
		if err := checkColumnsValidForIndex(desc, idx.ColumnNames); err != nil {
			return err
		}

		if primary {
			// PrimaryIndex is unset.
			if desc.PrimaryIndex.Name == "" {
				if idx.Name == "" {
					// Only override the index name if it hasn't been set by the user.
					idx.Name = PrimaryKeyIndexName
				}
				desc.SetPrimaryIndex(idx)
			} else {
				return fmt.Errorf("multiple primary keys for table %q are not allowed", desc.Name)
			}
		} else {
			desc.AddPublicNonPrimaryIndex(idx)
		}

	} else {
		if err := checkColumnsValidForInvertedIndex(desc, idx.ColumnNames); err != nil {
			return err
		}
		desc.AddPublicNonPrimaryIndex(idx)
	}

	return nil
}

// AddColumnToFamilyMaybeCreate adds the specified column to the specified
// family. If it doesn't exist and create is true, creates it. If it does exist
// adds it unless "strict" create (`true` for create but `false` for
// ifNotExists) is specified.
//
// AllocateIDs must be called before the TableDescriptor will be valid.
func (desc *Mutable) AddColumnToFamilyMaybeCreate(
	col string, family string, create bool, ifNotExists bool,
) error {
	idx := int(-1)
	if len(family) > 0 {
		for i := range desc.Families {
			if desc.Families[i].Name == family {
				idx = i
				break
			}
		}
	}

	if idx == -1 {
		if create {
			// NB: When AllocateIDs encounters an empty `Name`, it'll generate one.
			desc.AddFamily(descpb.ColumnFamilyDescriptor{Name: family, ColumnNames: []string{col}})
			return nil
		}
		return fmt.Errorf("unknown family %q", family)
	}

	if create && !ifNotExists {
		return fmt.Errorf("family %q already exists", family)
	}
	desc.Families[idx].ColumnNames = append(desc.Families[idx].ColumnNames, col)
	return nil
}

// RemoveColumnFromFamily removes a colID from the family it's assigned to.
func (desc *Mutable) RemoveColumnFromFamily(colID descpb.ColumnID) {
	for i := range desc.Families {
		for j, c := range desc.Families[i].ColumnIDs {
			if c == colID {
				desc.Families[i].ColumnIDs = append(
					desc.Families[i].ColumnIDs[:j], desc.Families[i].ColumnIDs[j+1:]...)
				desc.Families[i].ColumnNames = append(
					desc.Families[i].ColumnNames[:j], desc.Families[i].ColumnNames[j+1:]...)
				// Due to a complication with allowing primary key columns to not be restricted
				// to family 0, we might end up deleting all the columns from family 0. We will
				// allow empty column families now, but will disallow this in the future.
				// TODO (rohany): remove this once the reliance on sentinel family 0 has been removed.
				if len(desc.Families[i].ColumnIDs) == 0 && desc.Families[i].ID != 0 {
					desc.Families = append(desc.Families[:i], desc.Families[i+1:]...)
				}
				return
			}
		}
	}
}

// RenameColumnDescriptor updates all references to a column name in
// a table descriptor including indexes and families.
func (desc *Mutable) RenameColumnDescriptor(column *descpb.ColumnDescriptor, newColName string) {
	colID := column.ID
	column.Name = newColName

	for i := range desc.Families {
		for j := range desc.Families[i].ColumnIDs {
			if desc.Families[i].ColumnIDs[j] == colID {
				desc.Families[i].ColumnNames[j] = newColName
			}
		}
	}

	for _, idx := range desc.AllIndexes() {
		idxDesc := idx.IndexDesc()
		for i, id := range idxDesc.ColumnIDs {
			if id == colID {
				idxDesc.ColumnNames[i] = newColName
			}
		}
		for i, id := range idxDesc.StoreColumnIDs {
			if id == colID {
				idxDesc.StoreColumnNames[i] = newColName
			}
		}
	}
}

// FindActiveColumnsByNames finds all requested columns (in the requested order)
// or returns an error.
func (desc *wrapper) FindActiveColumnsByNames(
	names tree.NameList,
) ([]descpb.ColumnDescriptor, error) {
	cols := make([]descpb.ColumnDescriptor, len(names))
	for i := range names {
		c, err := desc.FindActiveColumnByName(string(names[i]))
		if err != nil {
			return nil, err
		}
		cols[i] = *c
	}
	return cols, nil
}

// HasColumnWithName finds the column with the specified name. It returns
// nil if there is no such column, and true if the column is being dropped.
func (desc *wrapper) HasColumnWithName(name tree.Name) (*descpb.ColumnDescriptor, bool) {
	for i := range desc.Columns {
		c := &desc.Columns[i]
		if c.Name == string(name) {
			return c, false
		}
	}
	for i := range desc.Mutations {
		m := &desc.Mutations[i]
		if c := m.GetColumn(); c != nil {
			if c.Name == string(name) {
				return c, m.Direction == descpb.DescriptorMutation_DROP
			}
		}
	}
	return nil, false
}

// FindColumnByName finds the column with the specified name. It returns
// an active column or a column from the mutation list. It returns true
// if the column is being dropped.
func (desc *wrapper) FindColumnByName(name tree.Name) (*descpb.ColumnDescriptor, bool, error) {
	ret, ok := desc.HasColumnWithName(name)
	if ret == nil {
		return nil, false, colinfo.NewUndefinedColumnError(string(name))
	}
	return ret, ok, nil
}

// FindActiveOrNewColumnByName finds the column with the specified name.
// It returns either an active column or a column that was added in the
// same transaction that is currently running.
func (desc *Mutable) FindActiveOrNewColumnByName(name tree.Name) (*descpb.ColumnDescriptor, error) {
	for i := range desc.Columns {
		c := &desc.Columns[i]
		if c.Name == string(name) {
			return c, nil
		}
	}
	currentMutationID := desc.ClusterVersion.NextMutationID
	for i := range desc.Mutations {
		mut := &desc.Mutations[i]
		if col := mut.GetColumn(); col != nil &&
			mut.MutationID == currentMutationID &&
			mut.Direction == descpb.DescriptorMutation_ADD {
			return col, nil
		}
	}
	return nil, colinfo.NewUndefinedColumnError(string(name))
}

// FindColumnMutationByName finds the mutation on the specified column.
func (desc *wrapper) FindColumnMutationByName(name tree.Name) *descpb.DescriptorMutation {
	for i := range desc.Mutations {
		m := &desc.Mutations[i]
		if c := m.GetColumn(); c != nil {
			if c.Name == string(name) {
				return m
			}
		}
	}
	return nil
}

// ColumnIdxMap returns a map from Column ID to the ordinal position of that
// column.
func (desc *wrapper) ColumnIdxMap() catalog.TableColMap {
	return desc.ColumnIdxMapWithMutations(false)
}

// ColumnIdxMapWithMutations returns a map from Column ID to the ordinal
// position of that column, optionally including mutation columns if the input
// bool is true.
func (desc *wrapper) ColumnIdxMapWithMutations(mutations bool) catalog.TableColMap {
	var colIdxMap catalog.TableColMap
	for i := range desc.Columns {
		id := desc.Columns[i].ID
		colIdxMap.Set(id, i)
	}
	if mutations {
		idx := len(desc.Columns)
		for i := range desc.Mutations {
			col := desc.Mutations[i].GetColumn()
			if col != nil {
				colIdxMap.Set(col.ID, idx)
				idx++
			}
		}
	}
	return colIdxMap
}

// FindActiveColumnByName finds an active column with the specified name.
func (desc *wrapper) FindActiveColumnByName(name string) (*descpb.ColumnDescriptor, error) {
	for i := range desc.Columns {
		c := &desc.Columns[i]
		if c.Name == name {
			return c, nil
		}
	}
	return nil, colinfo.NewUndefinedColumnError(name)
}

// FindColumnByID finds the column with specified ID.
func (desc *wrapper) FindColumnByID(id descpb.ColumnID) (*descpb.ColumnDescriptor, error) {
	for i := range desc.Columns {
		c := &desc.Columns[i]
		if c.ID == id {
			return c, nil
		}
	}
	for i := range desc.Mutations {
		if c := desc.Mutations[i].GetColumn(); c != nil {
			if c.ID == id {
				return c, nil
			}
		}
	}
	return nil, fmt.Errorf("column-id \"%d\" does not exist", id)
}

// FindActiveColumnByID finds the active column with specified ID.
func (desc *wrapper) FindActiveColumnByID(id descpb.ColumnID) (*descpb.ColumnDescriptor, error) {
	for i := range desc.Columns {
		c := &desc.Columns[i]
		if c.ID == id {
			return c, nil
		}
	}
	return nil, fmt.Errorf("column-id \"%d\" does not exist", id)
}

// ContainsUserDefinedTypes returns whether or not this table descriptor has
// any columns of user defined types.
func (desc *wrapper) ContainsUserDefinedTypes() bool {
	return len(desc.GetColumnOrdinalsWithUserDefinedTypes()) > 0
}

// ContainsUserDefinedTypes returns whether or not this table descriptor has
// any columns of user defined types.
// This method is re-implemented for Immutable only for the purpose of calling
// the correct GetColumnOrdinalsWithUserDefinedTypes() method on desc.
func (desc *Immutable) ContainsUserDefinedTypes() bool {
	return len(desc.GetColumnOrdinalsWithUserDefinedTypes()) > 0
}

// GetColumnOrdinalsWithUserDefinedTypes returns a slice of column ordinals
// of columns that contain user defined types.
func (desc *Immutable) GetColumnOrdinalsWithUserDefinedTypes() []int {
	return desc.columnsWithUDTs
}

// UserDefinedTypeColsHaveSameVersion returns whether this descriptor's columns
// with user defined type metadata have the same versions of metadata as in the
// other descriptor. Note that this function is only valid on two descriptors
// representing the same table at the same version.
func (desc *wrapper) UserDefinedTypeColsHaveSameVersion(otherDesc catalog.TableDescriptor) bool {
	thisCols := desc.DeletableColumns()
	otherCols := otherDesc.DeletableColumns()
	for _, idx := range desc.GetColumnOrdinalsWithUserDefinedTypes() {
		this, other := thisCols[idx].Type, otherCols[idx].Type
		if this.TypeMeta.Version != other.TypeMeta.Version {
			return false
		}
	}
	return true
}

// UserDefinedTypeColsHaveSameVersion returns whether this descriptor's columns
// with user defined type metadata have the same versions of metadata as in the
// other descriptor. Note that this function is only valid on two descriptors
// representing the same table at the same version.
// This method is re-implemented for Immutable only for the purpose of calling
// the correct DeletableColumns() and GetColumnOrdinalsWithUserDefinedTypes()
// methods on desc.
func (desc *Immutable) UserDefinedTypeColsHaveSameVersion(otherDesc catalog.TableDescriptor) bool {
	thisCols := desc.DeletableColumns()
	otherCols := otherDesc.DeletableColumns()
	for _, idx := range desc.GetColumnOrdinalsWithUserDefinedTypes() {
		this, other := thisCols[idx].Type, otherCols[idx].Type
		if this.TypeMeta.Version != other.TypeMeta.Version {
			return false
		}
	}
	return true
}

// FindReadableColumnByID finds the readable column with specified ID. The
// column may be undergoing a schema change and is marked nullable regardless
// of its configuration. It returns true if the column is undergoing a
// schema change.
func (desc *Immutable) FindReadableColumnByID(
	id descpb.ColumnID,
) (*descpb.ColumnDescriptor, bool, error) {
	for i, c := range desc.ReadableColumns() {
		if c.ID == id {
			return &c, i >= len(desc.Columns), nil
		}
	}
	return nil, false, fmt.Errorf("column-id \"%d\" does not exist", id)
}

// FindFamilyByID finds the family with specified ID.
func (desc *wrapper) FindFamilyByID(id descpb.FamilyID) (*descpb.ColumnFamilyDescriptor, error) {
	for i := range desc.Families {
		family := &desc.Families[i]
		if family.ID == id {
			return family, nil
		}
	}
	return nil, fmt.Errorf("family-id \"%d\" does not exist", id)
}

// NamesForColumnIDs returns the names for the given column ids, or an error
// if one or more column ids was missing. Note - this allocates! It's not for
// hot path code.
func (desc *wrapper) NamesForColumnIDs(ids descpb.ColumnIDs) ([]string, error) {
	names := make([]string, len(ids))
	for i, id := range ids {
		col, err := desc.FindColumnByID(id)
		if err != nil {
			return nil, err
		}
		names[i] = col.Name
	}
	return names, nil
}

// RenameIndexDescriptor renames an index descriptor.
func (desc *Mutable) RenameIndexDescriptor(index *descpb.IndexDescriptor, name string) error {
	id := index.ID
	if id == desc.PrimaryIndex.ID {
		idx := desc.PrimaryIndex
		idx.Name = name
		desc.SetPrimaryIndex(idx)
		return nil
	}
	for i, idx := range desc.Indexes {
		if idx.ID == id {
			idx.Name = name
			desc.SetPublicNonPrimaryIndex(i+1, idx)
			return nil
		}
	}
	for _, m := range desc.Mutations {
		if idx := m.GetIndex(); idx != nil && idx.ID == id {
			idx.Name = name
			return nil
		}
	}
	return fmt.Errorf("index with id = %d does not exist", id)
}

// DropConstraint drops a constraint, either by removing it from the table
// descriptor or by queuing a mutation for a schema change.
func (desc *Mutable) DropConstraint(
	ctx context.Context,
	name string,
	detail descpb.ConstraintDetail,
	removeFK func(*Mutable, *descpb.ForeignKeyConstraint) error,
	settings *cluster.Settings,
) error {
	switch detail.Kind {
	case descpb.ConstraintTypePK:
		{
			primaryIndex := desc.PrimaryIndex
			primaryIndex.Disabled = true
			desc.SetPrimaryIndex(primaryIndex)
		}
		return nil

	case descpb.ConstraintTypeUnique:
		if detail.Index != nil {
			return unimplemented.NewWithIssueDetailf(42840, "drop-constraint-unique",
				"cannot drop UNIQUE constraint %q using ALTER TABLE DROP CONSTRAINT, use DROP INDEX CASCADE instead",
				tree.ErrNameStringP(&detail.Index.Name))
		}
		if detail.UniqueWithoutIndexConstraint == nil {
			return errors.AssertionFailedf(
				"Index or UniqueWithoutIndexConstraint must be non-nil for a unique constraint",
			)
		}
		if detail.UniqueWithoutIndexConstraint.Validity == descpb.ConstraintValidity_Validating {
			return unimplemented.NewWithIssueDetailf(42844,
				"drop-constraint-unique-validating",
				"constraint %q in the middle of being added, try again later", name)
		}
		if detail.UniqueWithoutIndexConstraint.Validity == descpb.ConstraintValidity_Dropping {
			return unimplemented.NewWithIssueDetailf(42844,
				"drop-constraint-unique-mutation",
				"constraint %q in the middle of being dropped", name)
		}
		// Search through the descriptor's unique constraints and delete the
		// one that we're supposed to be deleting.
		for i := range desc.UniqueWithoutIndexConstraints {
			ref := &desc.UniqueWithoutIndexConstraints[i]
			if ref.Name == name {
				// If the constraint is unvalidated, there's no assumption that it must
				// hold for all rows, so it can be dropped immediately.
				if detail.UniqueWithoutIndexConstraint.Validity == descpb.ConstraintValidity_Unvalidated {
					desc.UniqueWithoutIndexConstraints = append(
						desc.UniqueWithoutIndexConstraints[:i], desc.UniqueWithoutIndexConstraints[i+1:]...,
					)
					return nil
				}
				// TODO(rytaft): set validity to Dropping and call AddUniqueMutation
				// once supported.
				return pgerror.New(pgcode.FeatureNotSupported,
					"dropping valid unique constraints without an index is not yet supported",
				)
			}
		}
		return errors.AssertionFailedf("constraint %q not found on table %q", name, desc.Name)

	case descpb.ConstraintTypeCheck:
		if detail.CheckConstraint.Validity == descpb.ConstraintValidity_Validating {
			return unimplemented.NewWithIssueDetailf(42844, "drop-constraint-check-mutation",
				"constraint %q in the middle of being added, try again later", name)
		}
		if detail.CheckConstraint.Validity == descpb.ConstraintValidity_Dropping {
			return unimplemented.NewWithIssueDetailf(42844, "drop-constraint-check-mutation",
				"constraint %q in the middle of being dropped", name)
		}
		for i, c := range desc.Checks {
			if c.Name == name {
				// If the constraint is unvalidated, there's no assumption that it must
				// hold for all rows, so it can be dropped immediately.
				// We also drop the constraint immediately instead of queuing a mutation
				// unless the cluster is fully upgraded to 19.2, for backward
				// compatibility.
				if detail.CheckConstraint.Validity == descpb.ConstraintValidity_Unvalidated {
					desc.Checks = append(desc.Checks[:i], desc.Checks[i+1:]...)
					return nil
				}
				c.Validity = descpb.ConstraintValidity_Dropping
				desc.AddCheckMutation(c, descpb.DescriptorMutation_DROP)
				return nil
			}
		}
		return errors.Errorf("constraint %q not found on table %q", name, desc.Name)

	case descpb.ConstraintTypeFK:
		if detail.FK.Validity == descpb.ConstraintValidity_Validating {
			return unimplemented.NewWithIssueDetailf(42844,
				"drop-constraint-fk-validating",
				"constraint %q in the middle of being added, try again later", name)
		}
		if detail.FK.Validity == descpb.ConstraintValidity_Dropping {
			return unimplemented.NewWithIssueDetailf(42844,
				"drop-constraint-fk-mutation",
				"constraint %q in the middle of being dropped", name)
		}
		// Search through the descriptor's foreign key constraints and delete the
		// one that we're supposed to be deleting.
		for i := range desc.OutboundFKs {
			ref := &desc.OutboundFKs[i]
			if ref.Name == name {
				// If the constraint is unvalidated, there's no assumption that it must
				// hold for all rows, so it can be dropped immediately.
				if detail.FK.Validity == descpb.ConstraintValidity_Unvalidated {
					// Remove the backreference.
					if err := removeFK(desc, detail.FK); err != nil {
						return err
					}
					desc.OutboundFKs = append(desc.OutboundFKs[:i], desc.OutboundFKs[i+1:]...)
					return nil
				}
				ref.Validity = descpb.ConstraintValidity_Dropping
				desc.AddForeignKeyMutation(ref, descpb.DescriptorMutation_DROP)
				return nil
			}
		}
		return errors.AssertionFailedf("constraint %q not found on table %q", name, desc.Name)

	default:
		return unimplemented.Newf(fmt.Sprintf("drop-constraint-%s", detail.Kind),
			"constraint %q has unsupported type", tree.ErrNameString(name))
	}

}

// RenameConstraint renames a constraint.
func (desc *Mutable) RenameConstraint(
	detail descpb.ConstraintDetail,
	oldName, newName string,
	dependentViewRenameError func(string, descpb.ID) error,
	renameFK func(*Mutable, *descpb.ForeignKeyConstraint, string) error,
) error {
	switch detail.Kind {
	case descpb.ConstraintTypePK:
		for _, tableRef := range desc.DependedOnBy {
			if tableRef.IndexID != detail.Index.ID {
				continue
			}
			return dependentViewRenameError("index", tableRef.ID)
		}
		return desc.RenameIndexDescriptor(detail.Index, newName)

	case descpb.ConstraintTypeUnique:
		if detail.Index != nil {
			for _, tableRef := range desc.DependedOnBy {
				if tableRef.IndexID != detail.Index.ID {
					continue
				}
				return dependentViewRenameError("index", tableRef.ID)
			}
			if err := desc.RenameIndexDescriptor(detail.Index, newName); err != nil {
				return err
			}
		} else if detail.UniqueWithoutIndexConstraint != nil {
			if detail.UniqueWithoutIndexConstraint.Validity == descpb.ConstraintValidity_Validating {
				return unimplemented.NewWithIssueDetailf(42844,
					"rename-constraint-unique-mutation",
					"constraint %q in the middle of being added, try again later",
					tree.ErrNameStringP(&detail.UniqueWithoutIndexConstraint.Name))
			}
			detail.UniqueWithoutIndexConstraint.Name = newName
		} else {
			return errors.AssertionFailedf(
				"Index or UniqueWithoutIndexConstraint must be non-nil for a unique constraint",
			)
		}
		return nil

	case descpb.ConstraintTypeFK:
		if detail.FK.Validity == descpb.ConstraintValidity_Validating {
			return unimplemented.NewWithIssueDetailf(42844,
				"rename-constraint-fk-mutation",
				"constraint %q in the middle of being added, try again later",
				tree.ErrNameStringP(&detail.FK.Name))
		}
		// Update the name on the referenced table descriptor.
		if err := renameFK(desc, detail.FK, newName); err != nil {
			return err
		}
		// Update the name on this table descriptor.
		fk, err := desc.FindFKByName(detail.FK.Name)
		if err != nil {
			return err
		}
		fk.Name = newName
		return nil

	case descpb.ConstraintTypeCheck:
		if detail.CheckConstraint.Validity == descpb.ConstraintValidity_Validating {
			return unimplemented.NewWithIssueDetailf(42844,
				"rename-constraint-check-mutation",
				"constraint %q in the middle of being added, try again later",
				tree.ErrNameStringP(&detail.CheckConstraint.Name))
		}
		detail.CheckConstraint.Name = newName
		return nil

	default:
		return unimplemented.Newf(fmt.Sprintf("rename-constraint-%s", detail.Kind),
			"constraint %q has unsupported type", tree.ErrNameString(oldName))
	}
}

// FindIndexByIndexIdx returns an active index with the specified
// index's index which has a domain of [0, # of secondary indexes] and whether
// the index is a secondary index.
// The primary index has an index of 0 and the first secondary index
// (if it exists) has an index of 1.
func (desc *wrapper) FindIndexByIndexIdx(
	indexIdx int,
) (index *descpb.IndexDescriptor, isSecondary bool, err error) {
	// indexIdx is 0 for the primary index, or 1 to <num-indexes> for a
	// secondary index.
	if indexIdx < 0 || indexIdx > len(desc.Indexes) {
		return nil, false, errors.Errorf("invalid indexIdx %d", indexIdx)
	}

	if indexIdx > 0 {
		return &desc.Indexes[indexIdx-1], true, nil
	}

	return &desc.PrimaryIndex, false, nil
}

// GetIndexMutationCapabilities returns:
// 1. Whether the index is a mutation
// 2. if so, is it in state DELETE_AND_WRITE_ONLY
func (desc *wrapper) GetIndexMutationCapabilities(id descpb.IndexID) (bool, bool) {
	for _, mutation := range desc.Mutations {
		if mutationIndex := mutation.GetIndex(); mutationIndex != nil {
			if mutationIndex.ID == id {
				return true,
					mutation.State == descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY
			}
		}
	}
	return false, false
}

// FindFKByName returns the FK constraint on the table with the given name.
// Must return a pointer to the FK in the TableDescriptor, so that
// callers can use returned values to modify the TableDesc.
func (desc *wrapper) FindFKByName(name string) (*descpb.ForeignKeyConstraint, error) {
	for i := range desc.OutboundFKs {
		fk := &desc.OutboundFKs[i]
		if fk.Name == name {
			return fk, nil
		}
	}
	return nil, fmt.Errorf("fk %q does not exist", name)
}

// IsInterleaved returns true if any part of this this table is interleaved with
// another table's data.
func (desc *wrapper) IsInterleaved() bool {
	return nil != catalog.FindNonDropIndex(desc, func(idx catalog.Index) bool {
		return idx.IsInterleaved()
	})
}

// IsPrimaryIndexDefaultRowID returns whether or not the table's primary
// index is the default primary key on the hidden rowid column.
func (desc *wrapper) IsPrimaryIndexDefaultRowID() bool {
	if len(desc.PrimaryIndex.ColumnIDs) != 1 {
		return false
	}
	col, err := desc.FindColumnByID(desc.PrimaryIndex.ColumnIDs[0])
	if err != nil {
		// Should never be in this case.
		panic(err)
	}
	return col.Hidden
}

// MakeMutationComplete updates the descriptor upon completion of a mutation.
// There are three Validity types for the mutations:
// Validated   - The constraint has already been added and validated, should
//               never be the case for a validated constraint to enter this
//               method.
// Validating  - The constraint has already been added, and just needs to be
//               marked as validated.
// Unvalidated - The constraint has not yet been added, and needs to be added
//               for the first time.
func (desc *Mutable) MakeMutationComplete(m descpb.DescriptorMutation) error {
	switch m.Direction {
	case descpb.DescriptorMutation_ADD:
		switch t := m.Descriptor_.(type) {
		case *descpb.DescriptorMutation_Column:
			desc.AddColumn(t.Column)

		case *descpb.DescriptorMutation_Index:
			if err := desc.AddIndex(*t.Index, false); err != nil {
				return err
			}

		case *descpb.DescriptorMutation_Constraint:
			switch t.Constraint.ConstraintType {
			case descpb.ConstraintToUpdate_CHECK:
				switch t.Constraint.Check.Validity {
				case descpb.ConstraintValidity_Validating:
					// Constraint already added, just mark it as Validated
					for _, c := range desc.Checks {
						if c.Name == t.Constraint.Name {
							c.Validity = descpb.ConstraintValidity_Validated
							break
						}
					}
				case descpb.ConstraintValidity_Unvalidated:
					// add the constraint to the list of check constraints on the table
					// descriptor
					desc.Checks = append(desc.Checks, &t.Constraint.Check)
				default:
					return errors.AssertionFailedf("invalid constraint validity state: %d", t.Constraint.Check.Validity)
				}
			case descpb.ConstraintToUpdate_FOREIGN_KEY:
				switch t.Constraint.ForeignKey.Validity {
				case descpb.ConstraintValidity_Validating:
					// Constraint already added, just mark it as Validated
					for i := range desc.OutboundFKs {
						fk := &desc.OutboundFKs[i]
						if fk.Name == t.Constraint.Name {
							fk.Validity = descpb.ConstraintValidity_Validated
							break
						}
					}
				case descpb.ConstraintValidity_Unvalidated:
					// Takes care of adding the Foreign Key to the table index. Adding the
					// backreference to the referenced table index must be taken care of
					// in another call.
					// TODO (tyler): Combine both of these tasks in the same place.
					desc.OutboundFKs = append(desc.OutboundFKs, t.Constraint.ForeignKey)
				}
			case descpb.ConstraintToUpdate_NOT_NULL:
				// Remove the dummy check constraint that was in place during validation
				for i, c := range desc.Checks {
					if c.Name == t.Constraint.Check.Name {
						desc.Checks = append(desc.Checks[:i], desc.Checks[i+1:]...)
					}
				}
				col, err := desc.FindColumnByID(t.Constraint.NotNullColumn)
				if err != nil {
					return err
				}
				col.Nullable = false
			default:
				return errors.Errorf("unsupported constraint type: %d", t.Constraint.ConstraintType)
			}
		case *descpb.DescriptorMutation_PrimaryKeySwap:
			args := t.PrimaryKeySwap
			getIndexIdxByID := func(id descpb.IndexID) (int, error) {
				for i, idx := range desc.Indexes {
					if idx.ID == id {
						return i + 1, nil
					}
				}
				return 0, errors.New("index was not in list of indexes")
			}

			// Update the old primary index's descriptor to denote that it uses the primary
			// index encoding and stores all columns. This ensures that it will be properly
			// encoded and decoded when it is accessed after it is no longer the primary key
			// but before it is dropped entirely during the index drop process.
			primaryIndexCopy := protoutil.Clone(&desc.PrimaryIndex).(*descpb.IndexDescriptor)
			primaryIndexCopy.EncodingType = descpb.PrimaryIndexEncoding
			for _, col := range desc.Columns {
				containsCol := false
				for _, colID := range primaryIndexCopy.ColumnIDs {
					if colID == col.ID {
						containsCol = true
						break
					}
				}
				if !containsCol {
					primaryIndexCopy.StoreColumnIDs = append(primaryIndexCopy.StoreColumnIDs, col.ID)
					primaryIndexCopy.StoreColumnNames = append(primaryIndexCopy.StoreColumnNames, col.Name)
				}
			}
			// Move the old primary index from the table descriptor into the mutations queue
			// to schedule it for deletion.
			if err := desc.AddIndexMutation(primaryIndexCopy, descpb.DescriptorMutation_DROP); err != nil {
				return err
			}

			// Promote the new primary index into the primary index position on the descriptor,
			// and remove it from the secondary indexes list.
			newIndex, err := desc.FindIndexWithID(args.NewPrimaryIndexId)
			if err != nil {
				return err
			}

			{
				primaryIndex := newIndex.IndexDescDeepCopy()
				if args.NewPrimaryIndexName == "" {
					primaryIndex.Name = PrimaryKeyIndexName
				} else {
					primaryIndex.Name = args.NewPrimaryIndexName
				}
				// The primary index "implicitly" stores all columns in the table.
				// Explicitly including them in the stored columns list is incorrect.
				primaryIndex.StoreColumnNames, primaryIndex.StoreColumnIDs = nil, nil
				desc.SetPrimaryIndex(primaryIndex)
			}

			idx, err := getIndexIdxByID(newIndex.GetID())
			if err != nil {
				return err
			}
			desc.RemovePublicNonPrimaryIndex(idx)

			// Swap out the old indexes with their rewritten versions.
			for j := range args.OldIndexes {
				oldID := args.OldIndexes[j]
				newID := args.NewIndexes[j]
				// All our new indexes have been inserted into the table descriptor by now, since the primary key swap
				// is the last mutation processed in a group of mutations under the same mutation ID.
				newIndex, err := desc.FindIndexWithID(newID)
				if err != nil {
					return err
				}
				oldIndexIdx, err := getIndexIdxByID(oldID)
				if err != nil {
					return err
				}
				oldIndex, _, err := desc.FindIndexByIndexIdx(oldIndexIdx)
				if err != nil {
					return err
				}
				oldIndexCopy := protoutil.Clone(oldIndex).(*descpb.IndexDescriptor)
				newIndex.IndexDesc().Name = oldIndexCopy.Name
				// Splice out old index from the indexes list.
				desc.RemovePublicNonPrimaryIndex(oldIndexIdx)
				// Add a drop mutation for the old index. The code that calls this function will schedule
				// a schema change job to pick up all of these index drop mutations.
				if err := desc.AddIndexMutation(oldIndexCopy, descpb.DescriptorMutation_DROP); err != nil {
					return err
				}
			}
		case *descpb.DescriptorMutation_ComputedColumnSwap:
			if err := desc.performComputedColumnSwap(t.ComputedColumnSwap); err != nil {
				return err
			}

		case *descpb.DescriptorMutation_MaterializedViewRefresh:
			// Completing a refresh mutation just means overwriting the table's
			// indexes with the new indexes that have been backfilled already.
			desc.SetPrimaryIndex(t.MaterializedViewRefresh.NewPrimaryIndex)
			desc.SetPublicNonPrimaryIndexes(t.MaterializedViewRefresh.NewIndexes)
		}

	case descpb.DescriptorMutation_DROP:
		switch t := m.Descriptor_.(type) {
		// Nothing else to be done. The column/index was already removed from the
		// set of column/index descriptors at mutation creation time.
		// Constraints to be dropped are dropped before column/index backfills.
		case *descpb.DescriptorMutation_Column:
			desc.RemoveColumnFromFamily(t.Column.ID)
		}
	}
	return nil
}

func (desc *Mutable) performComputedColumnSwap(swap *descpb.ComputedColumnSwap) error {
	// Get the old and new columns from the descriptor.
	oldCol, err := desc.FindColumnByID(swap.OldColumnId)
	if err != nil {
		return err
	}
	newCol, err := desc.FindColumnByID(swap.NewColumnId)
	if err != nil {
		return err
	}

	// Mark newCol as no longer a computed column.
	newCol.ComputeExpr = nil

	// Make the oldCol a computed column by setting its computed expression.
	oldCol.ComputeExpr = &swap.InverseExpr

	// Generate unique name for old column.
	nameExists := func(name string) bool {
		_, _, err := desc.FindColumnByName(tree.Name(name))
		return err == nil
	}

	uniqueName := GenerateUniqueConstraintName(newCol.Name, nameExists)

	// Remember the name of oldCol, because newCol will take it.
	oldColName := oldCol.Name

	// Rename old column to this new name, and rename newCol to oldCol's name.
	desc.RenameColumnDescriptor(oldCol, uniqueName)
	desc.RenameColumnDescriptor(newCol, oldColName)

	// Swap Column Family ordering for oldCol and newCol.
	// Both columns must be in the same family since the new column is
	// created explicitly with the same column family as the old column.
	// This preserves the ordering of column families when querying
	// for column families.
	oldColColumnFamily, err := desc.GetFamilyOfColumn(oldCol.ID)
	if err != nil {
		return err
	}
	newColColumnFamily, err := desc.GetFamilyOfColumn(newCol.ID)
	if err != nil {
		return err
	}

	if oldColColumnFamily.ID != newColColumnFamily.ID {
		return errors.Newf("expected the column families of the old and new columns to match,"+
			"oldCol column family: %v, newCol column family: %v",
			oldColColumnFamily.ID, newColColumnFamily.ID)
	}

	for i := range oldColColumnFamily.ColumnIDs {
		if oldColColumnFamily.ColumnIDs[i] == oldCol.ID {
			oldColColumnFamily.ColumnIDs[i] = newCol.ID
			oldColColumnFamily.ColumnNames[i] = newCol.Name
		} else if oldColColumnFamily.ColumnIDs[i] == newCol.ID {
			oldColColumnFamily.ColumnIDs[i] = oldCol.ID
			oldColColumnFamily.ColumnNames[i] = oldCol.Name
		}
	}

	// Set newCol's PGAttributeNum to oldCol's ID. This makes
	// newCol display like oldCol in catalog tables.
	newCol.PGAttributeNum = oldCol.GetPGAttributeNum()
	oldCol.PGAttributeNum = 0

	// Mark oldCol as being the result of an AlterColumnType. This allows us
	// to generate better errors for failing inserts.
	oldCol.AlterColumnTypeInProgress = true

	// Clone oldColDesc so that we can queue it up as a mutation.
	// Use oldColCopy to queue mutation in case oldCol's memory address
	// gets overwritten during mutation.
	oldColCopy := protoutil.Clone(oldCol).(*descpb.ColumnDescriptor)
	newColCopy := protoutil.Clone(newCol).(*descpb.ColumnDescriptor)
	desc.AddColumnMutation(oldColCopy, descpb.DescriptorMutation_DROP)

	// Remove the new column from the TableDescriptor first so we can reinsert
	// it into the position where the old column is.
	for i := range desc.Columns {
		if desc.Columns[i].ID == newCol.ID {
			desc.Columns = append(desc.Columns[:i:i], desc.Columns[i+1:]...)
			break
		}
	}

	// Replace the old column with the new column.
	for i := range desc.Columns {
		if desc.Columns[i].ID == oldCol.ID {
			desc.Columns[i] = *newColCopy
		}
	}

	return nil
}

// AddCheckMutation adds a check constraint mutation to desc.Mutations.
func (desc *Mutable) AddCheckMutation(
	ck *descpb.TableDescriptor_CheckConstraint, direction descpb.DescriptorMutation_Direction,
) {
	m := descpb.DescriptorMutation{
		Descriptor_: &descpb.DescriptorMutation_Constraint{
			Constraint: &descpb.ConstraintToUpdate{
				ConstraintType: descpb.ConstraintToUpdate_CHECK, Name: ck.Name, Check: *ck,
			},
		},
		Direction: direction,
	}
	desc.addMutation(m)
}

// AddForeignKeyMutation adds a foreign key constraint mutation to desc.Mutations.
func (desc *Mutable) AddForeignKeyMutation(
	fk *descpb.ForeignKeyConstraint, direction descpb.DescriptorMutation_Direction,
) {
	m := descpb.DescriptorMutation{
		Descriptor_: &descpb.DescriptorMutation_Constraint{
			Constraint: &descpb.ConstraintToUpdate{
				ConstraintType: descpb.ConstraintToUpdate_FOREIGN_KEY,
				Name:           fk.Name,
				ForeignKey:     *fk,
			},
		},
		Direction: direction,
	}
	desc.addMutation(m)
}

// MakeNotNullCheckConstraint creates a dummy check constraint equivalent to a
// NOT NULL constraint on a column, so that NOT NULL constraints can be added
// and dropped correctly in the schema changer. This function mutates inuseNames
// to add the new constraint name.
// TODO(mgartner): Move this to schemaexpr.CheckConstraintBuilder.
func MakeNotNullCheckConstraint(
	colName string,
	colID descpb.ColumnID,
	inuseNames map[string]struct{},
	validity descpb.ConstraintValidity,
) *descpb.TableDescriptor_CheckConstraint {
	name := fmt.Sprintf("%s_auto_not_null", colName)
	// If generated name isn't unique, attempt to add a number to the end to
	// get a unique name, as in generateNameForCheckConstraint().
	if _, ok := inuseNames[name]; ok {
		i := 1
		for {
			appended := fmt.Sprintf("%s%d", name, i)
			if _, ok := inuseNames[appended]; !ok {
				name = appended
				break
			}
			i++
		}
	}
	if inuseNames != nil {
		inuseNames[name] = struct{}{}
	}

	expr := &tree.IsNotNullExpr{
		Expr: &tree.ColumnItem{ColumnName: tree.Name(colName)},
	}

	return &descpb.TableDescriptor_CheckConstraint{
		Name:                name,
		Expr:                tree.Serialize(expr),
		Validity:            validity,
		ColumnIDs:           []descpb.ColumnID{colID},
		IsNonNullConstraint: true,
	}
}

// AddNotNullMutation adds a not null constraint mutation to desc.Mutations.
// Similarly to other schema elements, adding or dropping a non-null
// constraint requires a multi-state schema change, including a bulk validation
// step, before the Nullable flag can be set to false on the descriptor. This is
// done by adding a dummy check constraint of the form "x IS NOT NULL" that is
// treated like other check constraints being added, until the completion of the
// schema change, at which the check constraint is deleted. This function
// mutates inuseNames to add the new constraint name.
func (desc *Mutable) AddNotNullMutation(
	ck *descpb.TableDescriptor_CheckConstraint, direction descpb.DescriptorMutation_Direction,
) {
	m := descpb.DescriptorMutation{
		Descriptor_: &descpb.DescriptorMutation_Constraint{
			Constraint: &descpb.ConstraintToUpdate{
				ConstraintType: descpb.ConstraintToUpdate_NOT_NULL,
				Name:           ck.Name,
				NotNullColumn:  ck.ColumnIDs[0],
				Check:          *ck,
			},
		},
		Direction: direction,
	}
	desc.addMutation(m)
}

// AddColumnMutation adds a column mutation to desc.Mutations. Callers must take
// care not to further mutate the column descriptor, since this method retains
// a pointer to it.
func (desc *Mutable) AddColumnMutation(
	c *descpb.ColumnDescriptor, direction descpb.DescriptorMutation_Direction,
) {
	m := descpb.DescriptorMutation{
		Descriptor_: &descpb.DescriptorMutation_Column{Column: c},
		Direction:   direction,
	}
	desc.addMutation(m)
}

// AddIndexMutation adds an index mutation to desc.Mutations.
func (desc *Mutable) AddIndexMutation(
	idx *descpb.IndexDescriptor, direction descpb.DescriptorMutation_Direction,
) error {

	switch idx.Type {
	case descpb.IndexDescriptor_FORWARD:
		if err := checkColumnsValidForIndex(desc, idx.ColumnNames); err != nil {
			return err
		}
	case descpb.IndexDescriptor_INVERTED:
		if err := checkColumnsValidForInvertedIndex(desc, idx.ColumnNames); err != nil {
			return err
		}
	}

	m := descpb.DescriptorMutation{
		Descriptor_: &descpb.DescriptorMutation_Index{Index: idx},
		Direction:   direction,
	}
	desc.addMutation(m)
	return nil
}

// AddPrimaryKeySwapMutation adds a PrimaryKeySwap mutation to the table descriptor.
func (desc *Mutable) AddPrimaryKeySwapMutation(swap *descpb.PrimaryKeySwap) {
	m := descpb.DescriptorMutation{
		Descriptor_: &descpb.DescriptorMutation_PrimaryKeySwap{PrimaryKeySwap: swap},
		Direction:   descpb.DescriptorMutation_ADD,
	}
	desc.addMutation(m)
}

// AddMaterializedViewRefreshMutation adds a MaterializedViewRefreshMutation to
// the table descriptor.
func (desc *Mutable) AddMaterializedViewRefreshMutation(refresh *descpb.MaterializedViewRefresh) {
	m := descpb.DescriptorMutation{
		Descriptor_: &descpb.DescriptorMutation_MaterializedViewRefresh{MaterializedViewRefresh: refresh},
		Direction:   descpb.DescriptorMutation_ADD,
	}
	desc.addMutation(m)
}

// AddComputedColumnSwapMutation adds a ComputedColumnSwap mutation to the table descriptor.
func (desc *Mutable) AddComputedColumnSwapMutation(swap *descpb.ComputedColumnSwap) {
	m := descpb.DescriptorMutation{
		Descriptor_: &descpb.DescriptorMutation_ComputedColumnSwap{ComputedColumnSwap: swap},
		Direction:   descpb.DescriptorMutation_ADD,
	}
	desc.addMutation(m)
}

func (desc *Mutable) addMutation(m descpb.DescriptorMutation) {
	switch m.Direction {
	case descpb.DescriptorMutation_ADD:
		m.State = descpb.DescriptorMutation_DELETE_ONLY

	case descpb.DescriptorMutation_DROP:
		m.State = descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY
	}
	// For tables created in the same transaction the next mutation ID will
	// not have been allocated and the added mutation will use an invalid ID.
	// This is fine because the mutation will be processed immediately.
	m.MutationID = desc.ClusterVersion.NextMutationID
	desc.NextMutationID = desc.ClusterVersion.NextMutationID + 1
	desc.Mutations = append(desc.Mutations, m)
}

// IgnoreConstraints is used in MakeFirstMutationPublic to indicate that the
// table descriptor returned should not include newly added constraints, which
// is useful when passing the returned table descriptor to be used in
// validating constraints to be added.
const IgnoreConstraints = false

// IncludeConstraints is used in MakeFirstMutationPublic to indicate that the
// table descriptor returned should include newly added constraints.
const IncludeConstraints = true

// MakeFirstMutationPublic creates a Mutable from the
// Immutable by making the first mutation public.
// This is super valuable when trying to run SQL over data associated
// with a schema mutation that is still not yet public: Data validation,
// error reporting.
func (desc *wrapper) MakeFirstMutationPublic(includeConstraints bool) (*Mutable, error) {
	// Clone the ImmutableTable descriptor because we want to create an ImmutableCopy one.
	table := NewExistingMutable(*protoutil.Clone(desc.TableDesc()).(*descpb.TableDescriptor))
	mutationID := desc.Mutations[0].MutationID
	i := 0
	for _, mutation := range desc.Mutations {
		if mutation.MutationID != mutationID {
			// Mutations are applied in a FIFO order. Only apply the first set
			// of mutations if they have the mutation ID we're looking for.
			break
		}
		if includeConstraints || mutation.GetConstraint() == nil {
			if err := table.MakeMutationComplete(mutation); err != nil {
				return nil, err
			}
		}
		i++
	}
	table.Mutations = table.Mutations[i:]
	table.Version++
	return table, nil
}

// ColumnNeedsBackfill returns true if adding the given column requires a
// backfill (dropping a column always requires a backfill).
func ColumnNeedsBackfill(desc *descpb.ColumnDescriptor) bool {
	if desc.HasNullDefault() {
		return false
	}
	return desc.HasDefault() || !desc.Nullable || desc.IsComputed()
}

// HasPrimaryKey returns true if the table has a primary key.
func (desc *wrapper) HasPrimaryKey() bool {
	return !desc.PrimaryIndex.Disabled
}

// HasColumnBackfillMutation returns whether the table has any queued column
// mutations that require a backfill.
func (desc *wrapper) HasColumnBackfillMutation() bool {
	for _, m := range desc.Mutations {
		col := m.GetColumn()
		if col == nil {
			// Index backfills don't affect changefeeds.
			continue
		}
		// It's unfortunate that there's no one method we can call to check if a
		// mutation will be a backfill or not, but this logic was extracted from
		// backfill.go.
		if m.Direction == descpb.DescriptorMutation_DROP || ColumnNeedsBackfill(col) {
			return true
		}
	}
	return false
}

// IsNew returns true if the table was created in the current
// transaction.
func (desc *Mutable) IsNew() bool {
	return desc.ClusterVersion.ID == descpb.InvalidID
}

// VisibleColumns returns all non hidden columns.
func (desc *wrapper) VisibleColumns() []descpb.ColumnDescriptor {
	var cols []descpb.ColumnDescriptor
	for i := range desc.Columns {
		col := &desc.Columns[i]
		if !col.Hidden {
			cols = append(cols, *col)
		}
	}
	return cols
}

// ColumnTypes returns the types of all columns.
func (desc *wrapper) ColumnTypes() []*types.T {
	return desc.ColumnTypesWithMutations(false)
}

// ColumnsWithMutations returns all column descriptors, optionally including
// mutation columns.
func (desc *wrapper) ColumnsWithMutations(includeMutations bool) []descpb.ColumnDescriptor {
	n := len(desc.Columns)
	columns := desc.Columns[:n:n] // immutable on append
	if includeMutations {
		for i := range desc.Mutations {
			if col := desc.Mutations[i].GetColumn(); col != nil {
				columns = append(columns, *col)
			}
		}
	}
	return columns
}

// ColumnTypesWithMutations returns the types of all columns, optionally
// including mutation columns, which will be returned if the input bool is true.
func (desc *wrapper) ColumnTypesWithMutations(mutations bool) []*types.T {
	columns := desc.ColumnsWithMutations(mutations)
	types := make([]*types.T, len(columns))
	for i := range columns {
		types[i] = columns[i].Type
	}
	return types
}

// ColumnTypesWithMutationsAndVirtualCol returns the types of all columns,
// optionally including mutation columns, which will be returned if the input
// bool is true. If virtualCol is non-nil, substitutes the type of the virtual
// column instead of the table column with the same ID.
func (desc *wrapper) ColumnTypesWithMutationsAndVirtualCol(
	mutations bool, virtualCol *descpb.ColumnDescriptor,
) []*types.T {
	columns := desc.ColumnsWithMutations(mutations)
	types := make([]*types.T, len(columns))
	for i := range columns {
		if virtualCol != nil && columns[i].ID == virtualCol.ID {
			types[i] = virtualCol.Type
		} else {
			types[i] = columns[i].Type
		}
	}
	return types
}

// ColumnsSelectors generates Select expressions for cols.
func ColumnsSelectors(cols []descpb.ColumnDescriptor) tree.SelectExprs {
	exprs := make(tree.SelectExprs, len(cols))
	colItems := make([]tree.ColumnItem, len(cols))
	for i, col := range cols {
		colItems[i].ColumnName = tree.Name(col.Name)
		exprs[i].Expr = &colItems[i]
	}
	return exprs
}

// InvalidateFKConstraints sets all FK constraints to un-validated.
func (desc *wrapper) InvalidateFKConstraints() {
	// We don't use GetConstraintInfo because we want to edit the passed desc.
	for i := range desc.OutboundFKs {
		fk := &desc.OutboundFKs[i]
		fk.Validity = descpb.ConstraintValidity_Unvalidated
	}
}

// AllIndexSpans returns the Spans for each index in the table, including those
// being added in the mutations.
func (desc *wrapper) AllIndexSpans(codec keys.SQLCodec) roachpb.Spans {
	var spans roachpb.Spans
	for _, index := range desc.NonDropIndexes() {
		spans = append(spans, desc.IndexSpan(codec, index.GetID()))
	}
	return spans
}

// PrimaryIndexSpan returns the Span that corresponds to the entire primary
// index; can be used for a full table scan.
func (desc *wrapper) PrimaryIndexSpan(codec keys.SQLCodec) roachpb.Span {
	return desc.IndexSpan(codec, desc.PrimaryIndex.ID)
}

// IndexSpan returns the Span that corresponds to an entire index; can be used
// for a full index scan.
func (desc *wrapper) IndexSpan(codec keys.SQLCodec, indexID descpb.IndexID) roachpb.Span {
	prefix := roachpb.Key(rowenc.MakeIndexKeyPrefix(codec, desc, indexID))
	return roachpb.Span{Key: prefix, EndKey: prefix.PrefixEnd()}
}

// TableSpan returns the Span that corresponds to the entire table.
func (desc *wrapper) TableSpan(codec keys.SQLCodec) roachpb.Span {
	// TODO(jordan): Why does IndexSpan consider interleaves but TableSpan does
	// not? Should it?
	prefix := codec.TablePrefix(uint32(desc.ID))
	return roachpb.Span{Key: prefix, EndKey: prefix.PrefixEnd()}
}

// ColumnsUsed returns the IDs of the columns used in the check constraint's
// expression. v2.0 binaries will populate this during table creation, but older
// binaries will not, in which case this needs to be computed when requested.
//
// TODO(nvanbenschoten): we can remove this in v2.1 and replace it with a sql
// migration to backfill all descpb.TableDescriptor_CheckConstraint.ColumnIDs slices.
// See #22322.
func (desc *wrapper) ColumnsUsed(
	cc *descpb.TableDescriptor_CheckConstraint,
) ([]descpb.ColumnID, error) {
	if len(cc.ColumnIDs) > 0 {
		// Already populated.
		return cc.ColumnIDs, nil
	}

	parsed, err := parser.ParseExpr(cc.Expr)
	if err != nil {
		return nil, pgerror.Wrapf(err, pgcode.Syntax,
			"could not parse check constraint %s", cc.Expr)
	}

	var colIDsUsed catalog.TableColSet
	visitFn := func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		if vBase, ok := expr.(tree.VarName); ok {
			v, err := vBase.NormalizeVarName()
			if err != nil {
				return false, nil, err
			}
			if c, ok := v.(*tree.ColumnItem); ok {
				col, dropped, err := desc.FindColumnByName(c.ColumnName)
				if err != nil || dropped {
					return false, nil, pgerror.Newf(pgcode.UndefinedColumn,
						"column %q not found for constraint %q",
						c.ColumnName, parsed.String())
				}
				colIDsUsed.Add(col.ID)
			}
			return false, v, nil
		}
		return true, expr, nil
	}
	if _, err := tree.SimpleVisit(parsed, visitFn); err != nil {
		return nil, err
	}

	cc.ColumnIDs = make([]descpb.ColumnID, 0, colIDsUsed.Len())
	for colID, ok := colIDsUsed.Next(0); ok; colID, ok = colIDsUsed.Next(colID + 1) {
		cc.ColumnIDs = append(cc.ColumnIDs, colID)
	}
	sort.Sort(descpb.ColumnIDs(cc.ColumnIDs))
	return cc.ColumnIDs, nil
}

// CheckConstraintUsesColumn returns whether the check constraint uses the
// specified column.
func (desc *wrapper) CheckConstraintUsesColumn(
	cc *descpb.TableDescriptor_CheckConstraint, colID descpb.ColumnID,
) (bool, error) {
	colsUsed, err := desc.ColumnsUsed(cc)
	if err != nil {
		return false, err
	}
	i := sort.Search(len(colsUsed), func(i int) bool {
		return colsUsed[i] >= colID
	})
	return i < len(colsUsed) && colsUsed[i] == colID, nil
}

// GetFamilyOfColumn returns the ColumnFamilyDescriptor for the
// the family the column is part of.
func (desc *wrapper) GetFamilyOfColumn(
	colID descpb.ColumnID,
) (*descpb.ColumnFamilyDescriptor, error) {
	for _, fam := range desc.Families {
		for _, id := range fam.ColumnIDs {
			if id == colID {
				return &fam, nil
			}
		}
	}

	return nil, errors.Newf("no column family found for column id %v", colID)
}

// PartitionNames returns a slice containing the name of every partition and
// subpartition in an arbitrary order.
func (desc *wrapper) PartitionNames() []string {
	var names []string
	for _, index := range desc.NonDropIndexes() {
		names = append(names, index.PartitionNames()...)
	}
	return names
}

// SetAuditMode configures the audit mode on the descriptor.
func (desc *Mutable) SetAuditMode(mode tree.AuditMode) (bool, error) {
	prev := desc.AuditMode
	switch mode {
	case tree.AuditModeDisable:
		desc.AuditMode = descpb.TableDescriptor_DISABLED
	case tree.AuditModeReadWrite:
		desc.AuditMode = descpb.TableDescriptor_READWRITE
	default:
		return false, pgerror.Newf(pgcode.InvalidParameterValue,
			"unknown audit mode: %s (%d)", mode, mode)
	}
	return prev != desc.AuditMode, nil
}

// FindAllReferences returns all the references from a table.
func (desc *wrapper) FindAllReferences() (map[descpb.ID]struct{}, error) {
	refs := map[descpb.ID]struct{}{}
	for i := range desc.OutboundFKs {
		fk := &desc.OutboundFKs[i]
		refs[fk.ReferencedTableID] = struct{}{}
	}
	for i := range desc.InboundFKs {
		fk := &desc.InboundFKs[i]
		refs[fk.OriginTableID] = struct{}{}
	}
	for _, index := range desc.NonDropIndexes() {
		for i := 0; i < index.NumInterleaveAncestors(); i++ {
			refs[index.GetInterleaveAncestor(i).TableID] = struct{}{}
		}
		for i := 0; i < index.NumInterleavedBy(); i++ {
			refs[index.GetInterleavedBy(i).Table] = struct{}{}
		}
	}

	for _, c := range desc.AllNonDropColumns() {
		for _, id := range c.UsesSequenceIds {
			refs[id] = struct{}{}
		}
	}

	for _, dest := range desc.DependsOn {
		refs[dest] = struct{}{}
	}

	for _, c := range desc.DependedOnBy {
		refs[c.ID] = struct{}{}
	}
	return refs, nil
}

// ActiveChecks returns a list of all check constraints that should be enforced
// on writes (including constraints being added/validated). The columns
// referenced by the returned checks are writable, but not necessarily public.
func (desc *Immutable) ActiveChecks() []descpb.TableDescriptor_CheckConstraint {
	return desc.allChecks
}

// WritableColumns returns a list of public and write-only mutation columns.
func (desc *Immutable) WritableColumns() []descpb.ColumnDescriptor {
	return desc.publicAndNonPublicCols[:len(desc.Columns)+desc.writeOnlyColCount]
}

// DeletableColumns returns a list of public and non-public columns.
func (desc *Immutable) DeletableColumns() []descpb.ColumnDescriptor {
	return desc.publicAndNonPublicCols
}

// MutationColumns returns a list of mutation columns.
func (desc *Immutable) MutationColumns() []descpb.ColumnDescriptor {
	return desc.publicAndNonPublicCols[len(desc.Columns):]
}

// IsShardColumn returns true if col corresponds to a non-dropped hash sharded
// index. This method assumes that col is currently a member of desc.
func (desc *Mutable) IsShardColumn(col *descpb.ColumnDescriptor) bool {
	return nil != catalog.FindNonDropIndex(desc, func(idx catalog.Index) bool {
		return idx.IsSharded() && idx.GetShardColumnName() == col.Name
	})
}

// TableDesc implements the TableDescriptor interface.
func (desc *wrapper) TableDesc() *descpb.TableDescriptor {
	return &desc.TableDescriptor
}

// GenerateUniqueConstraintName attempts to generate a unique constraint name
// with the given prefix.
// It will first try prefix by itself, then it will subsequently try
// adding numeric digits at the end, starting from 1.
func GenerateUniqueConstraintName(prefix string, nameExistsFunc func(name string) bool) string {
	name := prefix
	for i := 1; nameExistsFunc(name); i++ {
		name = fmt.Sprintf("%s_%d", prefix, i)
	}
	return name
}

// SetParentSchemaID sets the SchemaID of the table.
func (desc *Mutable) SetParentSchemaID(schemaID descpb.ID) {
	desc.UnexposedParentSchemaID = schemaID
}

// AddDrainingName adds a draining name to the TableDescriptor's slice of
// draining names.
func (desc *Mutable) AddDrainingName(name descpb.NameInfo) {
	desc.DrainingNames = append(desc.DrainingNames, name)
}

// SetPublic implements the MutableDescriptor interface.
func (desc *Mutable) SetPublic() {
	desc.State = descpb.DescriptorState_PUBLIC
	desc.OfflineReason = ""
}

// SetDropped implements the MutableDescriptor interface.
func (desc *Mutable) SetDropped() {
	desc.State = descpb.DescriptorState_DROP
	desc.OfflineReason = ""
}

// SetOffline implements the MutableDescriptor interface.
func (desc *Mutable) SetOffline(reason string) {
	desc.State = descpb.DescriptorState_OFFLINE
	desc.OfflineReason = reason
}
