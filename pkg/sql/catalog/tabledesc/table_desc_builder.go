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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
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
	changes                    catalog.PostDeserializationChanges
	skipFKsWithNoMatchingTable bool
	isUncommittedVersion       bool
}

var _ TableDescriptorBuilder = &tableDescriptorBuilder{}

// NewBuilder creates a new catalog.DescriptorBuilder object for building
// table descriptors.
func NewBuilder(desc *descpb.TableDescriptor) TableDescriptorBuilder {
	return newBuilder(desc, false, /* isUncommittedVersion */
		catalog.PostDeserializationChanges{})
}

// NewBuilderForFKUpgrade should be used when attempting to upgrade the
// foreign key representation of a table descriptor.
// When skipFKsWithNoMatchingTable is set, the FK upgrade is allowed
// to proceed even in the case where a referenced table cannot be retrieved
// by the ValidationDereferencer. Such upgrades are then not fully complete.
func NewBuilderForFKUpgrade(
	desc *descpb.TableDescriptor, skipFKsWithNoMatchingTable bool,
) TableDescriptorBuilder {
	b := newBuilder(desc, false, /* isUncommittedVersion */
		catalog.PostDeserializationChanges{})
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

func newBuilder(
	desc *descpb.TableDescriptor,
	isUncommittedVersion bool,
	changes catalog.PostDeserializationChanges,
) *tableDescriptorBuilder {
	return &tableDescriptorBuilder{
		original:             protoutil.Clone(desc).(*descpb.TableDescriptor),
		isUncommittedVersion: isUncommittedVersion,
		changes:              changes,
	}
}

// DescriptorType implements the catalog.DescriptorBuilder interface.
func (tdb *tableDescriptorBuilder) DescriptorType() catalog.DescriptorType {
	return catalog.Table
}

// RunPostDeserializationChanges implements the catalog.DescriptorBuilder
// interface.
func (tdb *tableDescriptorBuilder) RunPostDeserializationChanges() {
	prevChanges := tdb.changes
	tdb.maybeModified = protoutil.Clone(tdb.original).(*descpb.TableDescriptor)
	tdb.changes = maybeFillInDescriptor(tdb.maybeModified)
	prevChanges.ForEach(func(change catalog.PostDeserializationChangeType) {
		tdb.changes.Add(change)
	})

}

// RunRestoreChanges implements the catalog.DescriptorBuilder interface.
func (tdb *tableDescriptorBuilder) RunRestoreChanges(
	descLookupFn func(id descpb.ID) catalog.Descriptor,
) (err error) {
	upgradedFK, err := maybeUpgradeForeignKeyRepresentation(
		descLookupFn,
		tdb.skipFKsWithNoMatchingTable,
		tdb.maybeModified,
	)
	if upgradedFK {
		tdb.changes.Add(catalog.UpgradedForeignKeyRepresentation)
	}
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
	imm.changes = tdb.changes
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
			TableDescriptor: *tdb.maybeModified,
			changes:         tdb.changes,
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
			TableDescriptor: *desc,
			changes:         tdb.changes,
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
	desc *descpb.TableDescriptor,
) (changes catalog.PostDeserializationChanges) {
	set := func(change catalog.PostDeserializationChangeType, cond bool) {
		if cond {
			changes.Add(change)
		}
	}
	set(catalog.UpgradedFormatVersion, maybeUpgradeFormatVersion(desc))
	set(catalog.FixedIndexEncodingType, maybeFixPrimaryIndexEncoding(&desc.PrimaryIndex))
	set(catalog.UpgradedIndexFormatVersion, maybeUpgradePrimaryIndexFormatVersion(desc))
	for i := range desc.Indexes {
		idx := &desc.Indexes[i]
		set(catalog.UpgradedIndexFormatVersion,
			maybeUpgradeSecondaryIndexFormatVersion(idx))
	}
	for i := range desc.Mutations {
		if idx := desc.Mutations[i].GetIndex(); idx != nil {
			set(catalog.UpgradedIndexFormatVersion,
				maybeUpgradeSecondaryIndexFormatVersion(idx))
		}
	}
	set(catalog.UpgradedNamespaceName, maybeUpgradeNamespaceName(desc))
	set(catalog.RemovedDefaultExprFromComputedColumn,
		maybeRemoveDefaultExprFromComputedColumns(desc))

	parentSchemaID := desc.GetUnexposedParentSchemaID()
	// TODO(richardjcai): Remove this case in 22.2.
	if parentSchemaID == descpb.InvalidID {
		parentSchemaID = keys.PublicSchemaID
	}
	fixedPrivileges := catprivilege.MaybeFixPrivileges(
		&desc.Privileges,
		desc.GetParentID(),
		parentSchemaID,
		privilege.Table,
		desc.GetName(),
	)
	addedGrantOptions := catprivilege.MaybeUpdateGrantOptions(desc.Privileges)
	set(catalog.UpgradedPrivileges, fixedPrivileges || addedGrantOptions)
	set(catalog.RemovedDuplicateIDsInRefs, maybeRemoveDuplicateIDsInRefs(desc))
	set(catalog.AddedConstraintIDs, maybeAddConstraintIDs(desc))
	return changes
}

// maybeRemoveDefaultExprFromComputedColumns removes DEFAULT expressions on
// computed columns. Although we now have a descriptor validation check to
// prevent this, this hasn't always been the case, so it's theoretically
// possible to encounter table descriptors which would fail this validation
// check. See issue #72881 for details.
func maybeRemoveDefaultExprFromComputedColumns(desc *descpb.TableDescriptor) (hasChanged bool) {
	doCol := func(col *descpb.ColumnDescriptor) {
		if col.IsComputed() && col.HasDefault() {
			col.DefaultExpr = nil
			hasChanged = true
		}
	}

	for i := range desc.Columns {
		doCol(&desc.Columns[i])
	}
	for _, m := range desc.Mutations {
		if col := m.GetColumn(); col != nil && m.Direction != descpb.DescriptorMutation_DROP {
			doCol(col)
		}
	}
	return hasChanged
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
	descLookupFn func(id descpb.ID) catalog.Descriptor,
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
			descLookupFn, otherUnupgradedTables, desc, &desc.Indexes[i], skipFKsWithNoMatchingTable,
		)
		if err != nil {
			return false, err
		}
		changed = changed || newChanged
	}
	newChanged, err := maybeUpgradeForeignKeyRepOnIndex(
		descLookupFn, otherUnupgradedTables, desc, &desc.PrimaryIndex, skipFKsWithNoMatchingTable,
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
	descLookupFn func(id descpb.ID) catalog.Descriptor,
	otherUnupgradedTables map[descpb.ID]catalog.TableDescriptor,
	desc *descpb.TableDescriptor,
	idx *descpb.IndexDescriptor,
	skipFKsWithNoMatchingTable bool,
) (bool, error) {
	updateUnupgradedTablesMap := func(id descpb.ID) (err error) {
		defer func() {
			if errors.Is(err, catalog.ErrDescriptorNotFound) && skipFKsWithNoMatchingTable {
				err = nil
			}
		}()
		if _, found := otherUnupgradedTables[id]; found {
			return nil
		}
		d := descLookupFn(id)
		if d == nil {
			return catalog.WrapTableDescRefErr(id, catalog.ErrDescriptorNotFound)
		}
		tbl, ok := d.(catalog.TableDescriptor)
		if !ok {
			return catalog.WrapTableDescRefErr(id, catalog.ErrDescriptorNotFound)
		}
		otherUnupgradedTables[id] = tbl
		return nil
	}

	var changed bool
	if idx.ForeignKey.IsSet() {
		ref := &idx.ForeignKey
		if err := updateUnupgradedTablesMap(ref.Table); err != nil {
			return false, err
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
				ConstraintID:        desc.GetNextConstraintID(),
			}
			desc.NextConstraintID++
			desc.OutboundFKs = append(desc.OutboundFKs, outFK)
		}
		changed = true
		idx.ForeignKey = descpb.ForeignKeyReference{}
	}

	for refIdx := range idx.ReferencedBy {
		ref := &(idx.ReferencedBy[refIdx])
		if err := updateUnupgradedTablesMap(ref.Table); err != nil {
			return false, err
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
					ConstraintID:        desc.GetNextConstraintID(),
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
					ConstraintID:        desc.GetNextConstraintID(),
				}
			}
			desc.NextConstraintID++
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

// FamilyPrimaryName is the name of the "primary" family, which is autogenerated
// the family clause is not specified.
const FamilyPrimaryName = "primary"

func upgradeToFamilyFormatVersion(desc *descpb.TableDescriptor) {
	var primaryIndexColumnIDs catalog.TableColSet
	for _, colID := range desc.PrimaryIndex.KeyColumnIDs {
		primaryIndexColumnIDs.Add(colID)
	}

	desc.Families = []descpb.ColumnFamilyDescriptor{
		{ID: 0, Name: FamilyPrimaryName},
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
// version PrimaryIndexWithStoredColumnsVersion whenever possible.
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
// version PrimaryIndexWithStoredColumnsVersion whenever possible.
func maybeUpgradeSecondaryIndexFormatVersion(idx *descpb.IndexDescriptor) (hasChanged bool) {
	switch idx.Version {
	case descpb.SecondaryIndexFamilyFormatVersion:
		if idx.Type == descpb.IndexDescriptor_INVERTED {
			return false
		}
	case descpb.EmptyArraysInInvertedIndexesVersion:
		break
	case descpb.StrictIndexColumnIDGuaranteesVersion:
		idx.Version = descpb.PrimaryIndexWithStoredColumnsVersion
		return true
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
	idx.Version = descpb.PrimaryIndexWithStoredColumnsVersion
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
	d.Name = string(catconstants.NamespaceTableName)
	return true
}

// maybeFixPrimaryIndexEncoding ensures that the index descriptor for a primary
// index has the correct encoding type set.
func maybeFixPrimaryIndexEncoding(idx *descpb.IndexDescriptor) (hasChanged bool) {
	if idx.EncodingType == descpb.PrimaryIndexEncoding {
		return false
	}
	idx.EncodingType = descpb.PrimaryIndexEncoding
	return true
}

// maybeRemoveDuplicateIDsInRefs ensures that IDs in references to other tables
// are not duplicated.
func maybeRemoveDuplicateIDsInRefs(d *descpb.TableDescriptor) (hasChanged bool) {
	// Strip duplicates from DependsOn.
	if s := cleanedIDs(d.DependsOn); len(s) < len(d.DependsOn) {
		d.DependsOn = s
		hasChanged = true
	}
	// Do the same for DependsOnTypes.
	if s := cleanedIDs(d.DependsOnTypes); len(s) < len(d.DependsOnTypes) {
		d.DependsOnTypes = s
		hasChanged = true
	}
	// Do the same for column IDs in DependedOnBy table references.
	for i := range d.DependedOnBy {
		ref := &d.DependedOnBy[i]
		s := catalog.MakeTableColSet(ref.ColumnIDs...).Ordered()
		// Also strip away O-IDs, which may have made their way in here in the past.
		// But only strip them if they're not the only ID. Otherwise this will
		// make for an even more confusing validation failure (we check that IDs
		// are not zero).
		if len(s) > 1 && s[0] == 0 {
			s = s[1:]
		}
		if len(s) < len(ref.ColumnIDs) {
			ref.ColumnIDs = s
			hasChanged = true
		}
	}
	// Do the same in columns for sequence refs.
	for i := range d.Columns {
		col := &d.Columns[i]
		if s := cleanedIDs(col.UsesSequenceIds); len(s) < len(col.UsesSequenceIds) {
			col.UsesSequenceIds = s
			hasChanged = true
		}
		if s := cleanedIDs(col.OwnsSequenceIds); len(s) < len(col.OwnsSequenceIds) {
			col.OwnsSequenceIds = s
			hasChanged = true
		}
	}
	return hasChanged
}

func cleanedIDs(input []descpb.ID) []descpb.ID {
	s := catalog.MakeDescriptorIDSet(input...).Ordered()
	if len(s) == 0 {
		return nil
	}
	return s
}

// maybeAddConstraintIDs ensures that all constraints have an ID associated with
// them.
func maybeAddConstraintIDs(desc *descpb.TableDescriptor) (hasChanged bool) {
	// Only assign constraint IDs to physical tables.
	if !desc.IsTable() {
		return false
	}
	initialConstraintID := desc.NextConstraintID
	// Maps index IDs to indexes for one which have
	// a constraint ID assigned.
	constraintIndexes := make(map[descpb.IndexID]*descpb.IndexDescriptor)
	if desc.NextConstraintID == 0 {
		desc.NextConstraintID = 1
	}
	nextConstraintID := func() descpb.ConstraintID {
		id := desc.GetNextConstraintID()
		desc.NextConstraintID++
		return id
	}
	// Loop over all constraints and assign constraint IDs.
	if desc.PrimaryIndex.ConstraintID == 0 {
		desc.PrimaryIndex.ConstraintID = nextConstraintID()
		constraintIndexes[desc.PrimaryIndex.ID] = &desc.PrimaryIndex
	}
	for i := range desc.Indexes {
		idx := &desc.Indexes[i]
		if idx.Unique && idx.ConstraintID == 0 {
			idx.ConstraintID = nextConstraintID()
			constraintIndexes[idx.ID] = idx
		}
	}
	for i := range desc.Checks {
		check := desc.Checks[i]
		if check.ConstraintID == 0 {
			check.ConstraintID = nextConstraintID()
		}
	}
	for i := range desc.InboundFKs {
		fk := &desc.InboundFKs[i]
		if fk.ConstraintID == 0 {
			fk.ConstraintID = nextConstraintID()
		}
	}
	for i := range desc.OutboundFKs {
		fk := &desc.OutboundFKs[i]
		if fk.ConstraintID == 0 {
			fk.ConstraintID = nextConstraintID()
		}
	}
	for i := range desc.UniqueWithoutIndexConstraints {
		unique := desc.UniqueWithoutIndexConstraints[i]
		if unique.ConstraintID == 0 {
			unique.ConstraintID = nextConstraintID()
		}
	}
	// Update mutations to add the constraint ID. In the case of a PK swap
	// we may need to maintain the same constraint ID.
	for _, mutation := range desc.GetMutations() {
		if idx := mutation.GetIndex(); idx != nil &&
			idx.ConstraintID == 0 &&
			mutation.Direction == descpb.DescriptorMutation_ADD &&
			idx.Unique {
			idx.ConstraintID = nextConstraintID()
			constraintIndexes[idx.ID] = idx
		} else if pkSwap := mutation.GetPrimaryKeySwap(); pkSwap != nil {
			for idx := range pkSwap.NewIndexes {
				oldIdx, firstOk := constraintIndexes[pkSwap.OldIndexes[idx]]
				newIdx := constraintIndexes[pkSwap.NewIndexes[idx]]
				if !firstOk {
					continue
				}
				newIdx.ConstraintID = oldIdx.ConstraintID
			}
		} else if constraint := mutation.GetConstraint(); constraint != nil {
			nextID := nextConstraintID()
			constraint.UniqueWithoutIndexConstraint.ConstraintID = nextID
			constraint.ForeignKey.ConstraintID = nextID
			constraint.Check.ConstraintID = nextID
		}

	}
	return desc.NextConstraintID != initialConstraintID
}
