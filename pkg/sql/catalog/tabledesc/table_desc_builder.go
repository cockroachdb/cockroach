// Copyright 2021 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// TableDescriptorBuilder is an extension of catalog.DescriptorBuilder
// for table descriptors.
type TableDescriptorBuilder interface {
	catalog.DescriptorBuilder
	BuildImmutableTable() catalog.TableDescriptor
	BuildExistingMutableTable() *Mutable
	BuildCreatedMutableTable() *Mutable
}

type tableDescriptorBuilder struct {
	original                   *descpb.TableDescriptor
	maybeModified              *descpb.TableDescriptor
	changes                    PostDeserializationTableDescriptorChanges
	skipFKsWithNoMatchingTable bool
	isUncommittedVersion       bool
}

var _ TableDescriptorBuilder = &tableDescriptorBuilder{}

// NewBuilder creates a new catalog.DescriptorBuilder object for building
// table descriptors.
func NewBuilder(desc *descpb.TableDescriptor) TableDescriptorBuilder {
	return newBuilder(desc)
}

// NewBuilderForUncommittedVersion is like NewBuilder but ensures that the
// uncommitted version flag is set in the built descriptor.
// This should be used when constructing a new copy of an immutable from an
// existing descriptor which may have a new version.
func NewBuilderForUncommittedVersion(desc *descpb.TableDescriptor) TableDescriptorBuilder {
	b := newBuilder(desc)
	b.isUncommittedVersion = true
	return b
}

// NewBuilderForFKUpgrade should be used when attempting to upgrade the
// foreign key representation of a table descriptor.
// When skipFKsWithNoMatchingTable is set, the FK upgrade is allowed
// to proceed even in the case where a referenced table cannot be retrieved
// by the DescGetter. Such upgrades are then not fully complete.
func NewBuilderForFKUpgrade(
	desc *descpb.TableDescriptor, skipFKsWithNoMatchingTable bool,
) TableDescriptorBuilder {
	b := newBuilder(desc)
	b.skipFKsWithNoMatchingTable = skipFKsWithNoMatchingTable
	return b
}

// NewUnsafeImmutable should be used as sparingly as possible only in cases
// where deep-copying the descpb.TableDescriptor struct is bad for performance
// and is known to not be necessary for safety. This is typically the case when
// the descpb struct is embedded in another proto message and is never used in
// any way other than to build a catalog.TableDescriptor interface. Currently
// this is the case for the execinfrapb package.
// Deprecated: this should be replaced with a NewBuilder call which is
// implemented in such a way that it can deep-copy the descpb.TableDescriptor
// struct without reflection (which is what protoutil.Clone uses, sadly).
func NewUnsafeImmutable(desc *descpb.TableDescriptor) catalog.TableDescriptor {
	b := tableDescriptorBuilder{original: desc}
	return b.BuildImmutableTable()
}

func newBuilder(desc *descpb.TableDescriptor) *tableDescriptorBuilder {
	return &tableDescriptorBuilder{
		original: protoutil.Clone(desc).(*descpb.TableDescriptor),
	}
}

// DescriptorType implements the catalog.DescriptorBuilder interface.
func (tdb *tableDescriptorBuilder) DescriptorType() catalog.DescriptorType {
	return catalog.Table
}

// RunPostDeserializationChanges implements the catalog.DescriptorBuilder
// interface.
func (tdb *tableDescriptorBuilder) RunPostDeserializationChanges(
	ctx context.Context, dg catalog.DescGetter,
) error {
	var err error
	tdb.maybeModified = protoutil.Clone(tdb.original).(*descpb.TableDescriptor)
	tdb.changes, err = maybeFillInDescriptor(ctx, dg, tdb.maybeModified, tdb.skipFKsWithNoMatchingTable)
	return err
}

// BuildImmutable implements the catalog.DescriptorBuilder interface.
func (tdb *tableDescriptorBuilder) BuildImmutable() catalog.Descriptor {
	return tdb.BuildImmutableTable()
}

// BuildImmutableTable returns an immutable table descriptor.
func (tdb *tableDescriptorBuilder) BuildImmutableTable() catalog.TableDescriptor {
	desc := tdb.maybeModified
	if desc == nil {
		desc = tdb.original
	}
	imm := makeImmutable(desc)
	imm.postDeserializationChanges = tdb.changes
	imm.isUncommittedVersion = tdb.isUncommittedVersion
	return imm
}

// BuildExistingMutable implements the catalog.DescriptorBuilder interface.
func (tdb *tableDescriptorBuilder) BuildExistingMutable() catalog.MutableDescriptor {
	return tdb.BuildExistingMutableTable()
}

// BuildExistingMutableTable returns a mutable descriptor for a table
// which already exists.
func (tdb *tableDescriptorBuilder) BuildExistingMutableTable() *Mutable {
	if tdb.maybeModified == nil {
		tdb.maybeModified = protoutil.Clone(tdb.original).(*descpb.TableDescriptor)
	}
	return &Mutable{
		wrapper: wrapper{
			TableDescriptor:            *tdb.maybeModified,
			postDeserializationChanges: tdb.changes,
		},
		ClusterVersion: *tdb.original,
	}
}

// BuildCreatedMutable implements the catalog.DescriptorBuilder interface.
func (tdb *tableDescriptorBuilder) BuildCreatedMutable() catalog.MutableDescriptor {
	return tdb.BuildCreatedMutableTable()
}

// BuildCreatedMutableTable returns a mutable descriptor for a table
// which is in the process of being created.
func (tdb *tableDescriptorBuilder) BuildCreatedMutableTable() *Mutable {
	desc := tdb.maybeModified
	if desc == nil {
		desc = tdb.original
	}
	return &Mutable{
		wrapper: wrapper{
			TableDescriptor:            *desc,
			postDeserializationChanges: tdb.changes,
		},
	}
}

// makeImmutable returns an immutable from the given TableDescriptor.
func makeImmutable(tbl *descpb.TableDescriptor) *immutable {
	desc := immutable{wrapper: wrapper{TableDescriptor: *tbl}}
	desc.mutationCache = newMutationCache(desc.TableDesc())
	desc.indexCache = newIndexCache(desc.TableDesc(), desc.mutationCache)
	desc.columnCache = newColumnCache(desc.TableDesc(), desc.mutationCache)

	desc.allChecks = make([]descpb.TableDescriptor_CheckConstraint, len(tbl.Checks))
	for i, c := range tbl.Checks {
		desc.allChecks[i] = *c
	}

	return &desc
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

	changes.UpgradedIndexFormatVersion = maybeUpgradePrimaryIndexFormatVersion(desc)
	for i := range desc.Indexes {
		changes.UpgradedIndexFormatVersion = changes.UpgradedIndexFormatVersion || maybeUpgradeSecondaryIndexFormatVersion(&desc.Indexes[i])
	}
	for i := range desc.Mutations {
		if idx := desc.Mutations[i].GetIndex(); idx != nil {
			changes.UpgradedIndexFormatVersion = changes.UpgradedIndexFormatVersion || maybeUpgradeSecondaryIndexFormatVersion(idx)
		}
	}

	changes.UpgradedPrivileges = descpb.MaybeFixPrivileges(desc.ID, desc.GetParentID(), &desc.Privileges, privilege.Table)

	changes.UpgradedNamespaceName = maybeUpgradeNamespaceName(desc)

	if dg != nil {
		changes.UpgradedForeignKeyRepresentation, err = maybeUpgradeForeignKeyRepresentation(
			ctx, dg, skipFKsWithNoMatchingTable /* skipFKsWithNoMatchingTable*/, desc)
	}
	if err != nil {
		return PostDeserializationTableDescriptorChanges{}, err
	}
	return changes, nil
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
				OriginColumnIDs:     idx.KeyColumnIDs[:numCols],
				ReferencedTableID:   ref.Table,
				ReferencedColumnIDs: referencedIndex.IndexDesc().KeyColumnIDs[:numCols],
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
						descpb.ColumnIDs(originIndex.KeyColumnIDs).HasPrefix(otherFK.OriginColumnIDs) {
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
					OriginColumnIDs:     originIndex.KeyColumnIDs[:numCols],
					ReferencedTableID:   desc.ID,
					ReferencedColumnIDs: idx.KeyColumnIDs[:numCols],
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
func maybeUpgradeFormatVersion(desc *descpb.TableDescriptor) (wasUpgraded bool) {
	for _, pair := range []struct {
		targetVersion descpb.FormatVersion
		upgradeFn     func(*descpb.TableDescriptor)
	}{
		{descpb.FamilyFormatVersion, upgradeToFamilyFormatVersion},
		{descpb.InterleavedFormatVersion, func(_ *descpb.TableDescriptor) {}},
	} {
		if desc.FormatVersion < pair.targetVersion {
			pair.upgradeFn(desc)
			desc.FormatVersion = pair.targetVersion
			wasUpgraded = true
		}
	}
	return wasUpgraded
}

func upgradeToFamilyFormatVersion(desc *descpb.TableDescriptor) {
	var primaryIndexColumnIDs catalog.TableColSet
	for _, colID := range desc.PrimaryIndex.KeyColumnIDs {
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
}

// maybeUpgradePrimaryIndexFormatVersion tries to promote a primary index to
// version descpb.PrimaryIndexWithStoredColumnsVersion whenever possible.
func maybeUpgradePrimaryIndexFormatVersion(desc *descpb.TableDescriptor) (hasChanged bool) {
	// Always set the correct encoding type for the primary index.
	desc.PrimaryIndex.EncodingType = descpb.PrimaryIndexEncoding
	// Check if primary index needs updating.
	switch desc.PrimaryIndex.Version {
	case descpb.PrimaryIndexWithStoredColumnsVersion:
		return false
	default:
		break
	}
	// Update primary index by populating StoreColumnIDs/Names slices.
	nonVirtualCols := make([]*descpb.ColumnDescriptor, 0, len(desc.Columns)+len(desc.Mutations))
	maybeAddCol := func(col *descpb.ColumnDescriptor) {
		if col == nil || col.Virtual {
			return
		}
		nonVirtualCols = append(nonVirtualCols, col)
	}
	for i := range desc.Columns {
		maybeAddCol(&desc.Columns[i])
	}
	for _, m := range desc.Mutations {
		maybeAddCol(m.GetColumn())
	}

	newStoreColumnIDs := make([]descpb.ColumnID, 0, len(nonVirtualCols))
	newStoreColumnNames := make([]string, 0, len(nonVirtualCols))
	keyColIDs := catalog.TableColSet{}
	for _, colID := range desc.PrimaryIndex.KeyColumnIDs {
		keyColIDs.Add(colID)
	}
	for _, col := range nonVirtualCols {
		if keyColIDs.Contains(col.ID) {
			continue
		}
		newStoreColumnIDs = append(newStoreColumnIDs, col.ID)
		newStoreColumnNames = append(newStoreColumnNames, col.Name)
	}
	if len(newStoreColumnIDs) == 0 {
		newStoreColumnIDs = nil
		newStoreColumnNames = nil
	}
	desc.PrimaryIndex.StoreColumnIDs = newStoreColumnIDs
	desc.PrimaryIndex.StoreColumnNames = newStoreColumnNames
	desc.PrimaryIndex.Version = descpb.PrimaryIndexWithStoredColumnsVersion
	return true
}

// maybeUpgradeSecondaryIndexFormatVersion tries to promote a secondary index to
// version descpb.StrictIndexColumnIDGuaranteesVersion whenever possible.
func maybeUpgradeSecondaryIndexFormatVersion(idx *descpb.IndexDescriptor) (hasChanged bool) {
	switch idx.Version {
	case descpb.SecondaryIndexFamilyFormatVersion:
		if idx.Type == descpb.IndexDescriptor_INVERTED {
			return false
		}
	case descpb.EmptyArraysInInvertedIndexesVersion:
		break
	default:
		return false
	}
	slice := make([]descpb.ColumnID, 0, len(idx.KeyColumnIDs)+len(idx.KeySuffixColumnIDs)+len(idx.StoreColumnIDs))
	slice = append(slice, idx.KeyColumnIDs...)
	slice = append(slice, idx.KeySuffixColumnIDs...)
	slice = append(slice, idx.StoreColumnIDs...)
	set := catalog.MakeTableColSet(slice...)
	if len(slice) != set.Len() {
		return false
	}
	if set.Contains(0) {
		return false
	}
	idx.Version = descpb.StrictIndexColumnIDGuaranteesVersion
	return true
}

// maybeUpgradeNamespaceName deals with upgrading the name field of the
// namespace table (30) to be "namespace" rather than "namespace2". This
// occurs in clusters which were bootstrapped before 21.2 and have not
// run the corresponding migration.
func maybeUpgradeNamespaceName(d *descpb.TableDescriptor) (hasChanged bool) {
	if d.ID != keys.NamespaceTableID || d.Name != catconstants.PreMigrationNamespaceTableName {
		return false
	}
	d.Name = catconstants.NamespaceTableName
	return true
}
