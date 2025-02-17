// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tabledesc

import (
	"slices"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/seqexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
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
	DescriptorWasModified() bool
}

type tableDescriptorBuilder struct {
	original                   *descpb.TableDescriptor
	maybeModified              *descpb.TableDescriptor
	mvccTimestamp              hlc.Timestamp
	isUncommittedVersion       bool
	skipFKsWithNoMatchingTable bool
	changes                    catalog.PostDeserializationChanges
	// This is the raw bytes (tag + data) of the table descriptor in storage.
	rawBytesInStorage []byte
}

var _ TableDescriptorBuilder = &tableDescriptorBuilder{}

// NewBuilder returns a new TableDescriptorBuilder instance by delegating to
// NewBuilderWithMVCCTimestamp with an empty MVCC timestamp.
//
// Callers must assume that the given protobuf has already been treated with the
// MVCC timestamp beforehand.
func NewBuilder(desc *descpb.TableDescriptor) TableDescriptorBuilder {
	return NewBuilderWithMVCCTimestamp(desc, hlc.Timestamp{})
}

// NewBuilderWithMVCCTimestamp creates a new TableDescriptorBuilder instance
// for building table descriptors.
func NewBuilderWithMVCCTimestamp(
	desc *descpb.TableDescriptor, mvccTimestamp hlc.Timestamp,
) TableDescriptorBuilder {
	return newBuilder(
		desc,
		mvccTimestamp,
		false, /* isUncommittedVersion */
		catalog.PostDeserializationChanges{},
	)
}

// NewBuilderForFKUpgrade should be used when attempting to upgrade the
// foreign key representation of a table descriptor.
// When skipFKsWithNoMatchingTable is set, the FK upgrade is allowed
// to proceed even in the case where a referenced table cannot be retrieved
// by the ValidationDereferencer. Such upgrades are then not fully complete.
func NewBuilderForFKUpgrade(
	desc *descpb.TableDescriptor, skipFKsWithNoMatchingTable bool,
) TableDescriptorBuilder {
	b := newBuilder(
		desc,
		hlc.Timestamp{},
		false, /* isUncommittedVersion */
		catalog.PostDeserializationChanges{},
	)
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

// cloneWithModificationStamp will clone the table descriptor and
// set the modification timestamp on it.
func cloneWithModificationStamp(
	desc *descpb.TableDescriptor, mvccTimestamp hlc.Timestamp,
) (newDesc *descpb.TableDescriptor, updated bool) {
	newDesc = protoutil.Clone(desc).(*descpb.TableDescriptor)
	// Set the ModificationTime field before doing anything else.
	// Other changes may depend on it. Note: The post deserialization
	// will surface the errors from the must set modification time
	// call below.
	mustSetModTime, err := descpb.MustSetModificationTime(
		desc.ModificationTime, mvccTimestamp, desc.Version, desc.State,
	)
	// If we updated the modification time, then
	// now is a good time to pick up the CreateAs time.
	if err == nil && mustSetModTime {
		updated = true
		newDesc.ModificationTime = mvccTimestamp
		maybeSetCreateAsOfTime(newDesc)
	}
	return newDesc, updated
}

func newBuilder(
	desc *descpb.TableDescriptor,
	mvccTimestamp hlc.Timestamp,
	isUncommittedVersion bool,
	changes catalog.PostDeserializationChanges,
) *tableDescriptorBuilder {
	newDesc, modificationUpdated := cloneWithModificationStamp(desc, mvccTimestamp)
	if modificationUpdated {
		changes.Add(catalog.SetModTimeToMVCCTimestamp)
	}
	return &tableDescriptorBuilder{
		original:             newDesc,
		mvccTimestamp:        mvccTimestamp,
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
func (tdb *tableDescriptorBuilder) RunPostDeserializationChanges() (err error) {
	defer func() {
		err = errors.Wrapf(err, "table %q (%d)", tdb.original.Name, tdb.original.ID)
	}()
	// Validate that the modification timestamp is valid. This
	// is now done earlier, but we can't return errors if an
	// assertion is hit.
	_, err = descpb.MustSetModificationTime(
		tdb.original.ModificationTime, tdb.mvccTimestamp, tdb.original.Version, tdb.original.State,
	)
	if err != nil {
		return err
	}
	c, err := maybeFillInDescriptor(tdb)
	if err != nil {
		return err
	}
	c.ForEach(tdb.changes.Add)
	return nil
}

// DescriptorWasModified implements TableDescriptorBuilder
func (tdb *tableDescriptorBuilder) DescriptorWasModified() bool {
	return tdb.maybeModified != nil
}

// GetReadOnlyPrivilege implements catprivilege.PrivilegeDescriptorBuilder.
func (tdb *tableDescriptorBuilder) GetReadOnlyPrivilege() *catpb.PrivilegeDescriptor {
	p := tdb.getLatestDesc().Privileges
	if p == nil {
		return tdb.GetMutablePrivilege()
	}
	return p
}

// GetMutablePrivilege implements catprivilege.PrivilegeDescriptorBuilder.
func (tdb *tableDescriptorBuilder) GetMutablePrivilege() *catpb.PrivilegeDescriptor {
	d := tdb.getOrInitModifiedDesc()
	if d.Privileges == nil {
		d.Privileges = &catpb.PrivilegeDescriptor{}
	}
	return d.Privileges
}

// RunRestoreChanges implements the catalog.DescriptorBuilder interface.
func (tdb *tableDescriptorBuilder) RunRestoreChanges(
	version clusterversion.ClusterVersion, descLookupFn func(id descpb.ID) catalog.Descriptor,
) (err error) {
	// Upgrade FK representation
	upgradedFK, err := maybeUpgradeForeignKeyRepresentation(
		descLookupFn,
		tdb.skipFKsWithNoMatchingTable,
		tdb.getOrInitModifiedDesc(),
	)
	if err != nil {
		return err
	}
	if upgradedFK {
		tdb.changes.Add(catalog.UpgradedForeignKeyRepresentation)
	}

	// Upgrade sequence reference
	upgradedSequenceReference, err := maybeUpgradeSequenceReference(descLookupFn, tdb.getOrInitModifiedDesc())
	if err != nil {
		return err
	}
	if upgradedSequenceReference {
		tdb.changes.Add(catalog.UpgradedSequenceReference)
	}

	// All backups taken in versions 22.1 and later should already have
	// constraint IDs set. Technically, now that we are in v24.1, we no longer
	// support restoring from versions older than 22.1, but we still have
	// tests that restore from old versions. Until those fixtures are updated,
	// we need to handle the case where constraint IDs are not set already.
	// TODO(rafi): remove this once all fixtures are updated. See: https://github.com/cockroachdb/cockroach/issues/120146.
	if changed := maybeAddConstraintIDs(tdb.getOrInitModifiedDesc()); changed {
		tdb.changes.Add(catalog.AddedConstraintIDs)
	}

	// Upgrade the declarative schema changer state
	if scpb.MigrateDescriptorState(version, tdb.getOrInitModifiedDesc().ParentID, tdb.getOrInitModifiedDesc().DeclarativeSchemaChangerState) {
		tdb.changes.Add(catalog.UpgradedDeclarativeSchemaChangerState)
	}

	return err
}

// StripDanglingBackReferences implements the catalog.DescriptorBuilder
// interface.
func (tdb *tableDescriptorBuilder) StripDanglingBackReferences(
	descIDMightExist func(id descpb.ID) bool, nonTerminalJobIDMightExist func(id jobspb.JobID) bool,
) error {
	tbl := tdb.getOrInitModifiedDesc()
	// Strip dangling back-references in depended_on_by,
	{
		sliceIdx := 0
		for _, backref := range tbl.DependedOnBy {
			tbl.DependedOnBy[sliceIdx] = backref
			if descIDMightExist(backref.ID) {
				sliceIdx++
			}
		}
		if sliceIdx < len(tbl.DependedOnBy) {
			tbl.DependedOnBy = tbl.DependedOnBy[:sliceIdx]
			tdb.changes.Add(catalog.StrippedDanglingBackReferences)
		}
	}
	// ... in inbound foreign keys,
	{
		sliceIdx := 0
		for _, backref := range tbl.InboundFKs {
			tbl.InboundFKs[sliceIdx] = backref
			if descIDMightExist(backref.OriginTableID) {
				sliceIdx++
			}
		}
		if sliceIdx < len(tbl.InboundFKs) {
			tbl.InboundFKs = tbl.InboundFKs[:sliceIdx]
			tdb.changes.Add(catalog.StrippedDanglingBackReferences)
		}
	}
	// ... in the replacement_of field,
	if id := tbl.ReplacementOf.ID; id != descpb.InvalidID && !descIDMightExist(id) {
		tbl.ReplacementOf.Reset()
		tdb.changes.Add(catalog.StrippedDanglingBackReferences)
	}
	// ... in the drop_job field,
	if id := tbl.DropJobID; id != jobspb.InvalidJobID && !nonTerminalJobIDMightExist(id) {
		tbl.DropJobID = jobspb.InvalidJobID
		tdb.changes.Add(catalog.StrippedDanglingBackReferences)
	}
	// ... in the mutation_jobs slice,
	{
		sliceIdx := 0
		for _, backref := range tbl.MutationJobs {
			tbl.MutationJobs[sliceIdx] = backref
			if nonTerminalJobIDMightExist(backref.JobID) {
				sliceIdx++
			}
		}
		if sliceIdx < len(tbl.MutationJobs) {
			tbl.MutationJobs = tbl.MutationJobs[:sliceIdx]
			tdb.changes.Add(catalog.StrippedDanglingBackReferences)
		}
	}
	// ... in the ldr_job_ids slice,
	{
		tbl.LDRJobIDs = slices.DeleteFunc(tbl.LDRJobIDs, func(i catpb.JobID) bool {
			// Remove if the job ID is not found.
			if !nonTerminalJobIDMightExist(i) {
				tdb.changes.Add(catalog.StrippedDanglingBackReferences)
				return true
			}
			return false
		})
	}
	// ... in the sequence ownership field.
	if seq := tbl.SequenceOpts; seq != nil {
		if id := seq.SequenceOwner.OwnerTableID; id != descpb.InvalidID && !descIDMightExist(id) {
			seq.SequenceOwner.Reset()
			tdb.changes.Add(catalog.StrippedDanglingBackReferences)
		}
	}
	return nil
}

// StripNonExistentRoles implements the catalog.DescriptorBuilder
// interface.
func (tdb *tableDescriptorBuilder) StripNonExistentRoles(
	roleExists func(role username.SQLUsername) bool,
) error {
	// If the owner doesn't exist, change the owner to admin.
	if !roleExists(tdb.getLatestDesc().GetPrivileges().Owner()) {
		tdb.getOrInitModifiedDesc().Privileges.OwnerProto = username.AdminRoleName().EncodeProto()
		tdb.changes.Add(catalog.StrippedNonExistentRoles)
	}
	// Remove any non-existent roles from the privileges.
	newPrivs := make([]catpb.UserPrivileges, 0, len(tdb.maybeModified.Privileges.Users))
	for _, priv := range tdb.getLatestDesc().Privileges.Users {
		exists := roleExists(priv.UserProto.Decode())
		if exists {
			newPrivs = append(newPrivs, priv)
		}
	}
	if len(newPrivs) != len(tdb.getLatestDesc().Privileges.Users) {
		tdb.getOrInitModifiedDesc().Privileges.Users = newPrivs
		tdb.changes.Add(catalog.StrippedNonExistentRoles)
	}
	return nil
}

// SetRawBytesInStorage implements the catalog.DescriptorBuilder interface.
func (tdb *tableDescriptorBuilder) SetRawBytesInStorage(rawBytes []byte) {
	tdb.rawBytesInStorage = append([]byte(nil), rawBytes...) // deep-copy
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
	imm.rawBytesInStorage = append([]byte(nil), tdb.rawBytesInStorage...) // deep-copy
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
			TableDescriptor:   *tdb.maybeModified,
			changes:           tdb.changes,
			rawBytesInStorage: append([]byte(nil), tdb.rawBytesInStorage...), // deep-copy
		},
		original: makeImmutable(tdb.original),
	}
}

// BuildCreatedMutable implements the catalog.DescriptorBuilder interface.
func (tdb *tableDescriptorBuilder) BuildCreatedMutable() catalog.MutableDescriptor {
	return tdb.BuildCreatedMutableTable()
}

// getLatestDesc returns the modified descriptor if it exists, or else the
// original descriptor.
func (tdb *tableDescriptorBuilder) getLatestDesc() *descpb.TableDescriptor {
	desc := tdb.maybeModified
	if desc == nil {
		desc = tdb.original
	}
	return desc
}

// getOrInitModifiedDesc returns the modified descriptor, and clones it from
// the original descriptor if it is not already available. This is a helper
// function that makes it easier to lazily initialize the modified descriptor,
// since protoutil.Clone is expensive.
func (tdb *tableDescriptorBuilder) getOrInitModifiedDesc() *descpb.TableDescriptor {
	if tdb.maybeModified == nil {
		tdb.maybeModified = protoutil.Clone(tdb.original).(*descpb.TableDescriptor)
	}
	return tdb.maybeModified
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
			TableDescriptor:   *desc,
			changes:           tdb.changes,
			rawBytesInStorage: append([]byte(nil), tdb.rawBytesInStorage...), // deep-copy
		},
	}
}

// makeImmutable returns an immutable from the given TableDescriptor.
func makeImmutable(tbl *descpb.TableDescriptor) *immutable {
	desc := immutable{wrapper: wrapper{TableDescriptor: *tbl}}
	desc.mutationCache = newMutationCache(desc.TableDesc())
	desc.indexCache = newIndexCache(desc.TableDesc(), desc.mutationCache)
	desc.columnCache = newColumnCache(desc.TableDesc(), desc.mutationCache)
	desc.constraintCache = newConstraintCache(desc.TableDesc(), desc.indexCache, desc.mutationCache)
	return &desc
}

// maybeFillInDescriptor performs any modifications needed to the table descriptor.
// This includes format upgrades and optional changes that can be handled by all version
// (for example: additional default privileges).
func maybeFillInDescriptor(
	builder *tableDescriptorBuilder,
) (changes catalog.PostDeserializationChanges, err error) {
	set := func(change catalog.PostDeserializationChangeType, cond bool) {
		if cond {
			changes.Add(change)
		}
	}
	set(catalog.UpgradedFormatVersion, maybeUpgradeFormatVersion(builder))
	set(catalog.UpgradedIndexFormatVersion, maybeUpgradePrimaryIndexFormatVersion(builder))
	{
		orig := builder.getLatestDesc()
		for i := range orig.Indexes {
			origIdx := orig.Indexes[i]
			if origIdx.Version < descpb.StrictIndexColumnIDGuaranteesVersion {
				modifiedDesc := builder.getOrInitModifiedDesc()
				maybeUpgradeSecondaryIndexFormatVersion(&modifiedDesc.Indexes[i])
			}
			// TODO(rytaft): Remove this case in 24.1.
			if origIdx.NotVisible && origIdx.Invisibility == 0.0 {
				modifiedDesc := builder.getOrInitModifiedDesc()
				set(catalog.SetIndexInvisibility, true)
				modifiedDesc.Indexes[i].Invisibility = 1.0
			}
		}
	}
	{
		desc := builder.getLatestDesc()
		for i := range desc.Mutations {
			if idx := desc.Mutations[i].GetIndex(); idx != nil &&
				idx.Version < descpb.StrictIndexColumnIDGuaranteesVersion {
				idx = builder.getOrInitModifiedDesc().Mutations[i].GetIndex()
				set(catalog.UpgradedIndexFormatVersion,
					maybeUpgradeSecondaryIndexFormatVersion(idx))
			}
		}
	}
	{
		desc := builder.getLatestDesc()
		var objectType privilege.ObjectType
		if desc.IsSequence() {
			objectType = privilege.Sequence
		} else {
			objectType = privilege.Table
		}
		// Make sure the parent schema ID is valid, since during
		// upgrades its possible for the user table to not have
		// this set correctly.
		parentSchemaID := desc.GetUnexposedParentSchemaID()
		if parentSchemaID == descpb.InvalidID {
			parentSchemaID = keys.PublicSchemaID
		}
		fixedPrivileges, err := catprivilege.MaybeFixPrivilegesWithBuilder(
			builder,
			desc.GetParentID(),
			parentSchemaID,
			objectType,
			desc.GetName(),
		)
		if err != nil {
			return catalog.PostDeserializationChanges{}, err
		}
		set(catalog.UpgradedPrivileges, fixedPrivileges)
	}

	set(catalog.RemovedDuplicateIDsInRefs, maybeRemoveDuplicateIDsInRefs(builder))
	set(catalog.StrippedDanglingSelfBackReferences, maybeStripDanglingSelfBackReferences(builder))
	set(catalog.FixSecondaryIndexEncodingType, maybeFixSecondaryIndexEncodingType(builder))
	set(catalog.FixedIncorrectForeignKeyOrigins, maybeFixForeignKeySelfReferences(builder))
	identityColumnsFixed, err := maybeFixUsesSequencesIDForIdentityColumns(builder)
	if err != nil {
		return catalog.PostDeserializationChanges{}, err
	}
	set(catalog.FixedUsesSequencesIDForIdentityColumns, identityColumnsFixed)
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
			referencedIndex, err := catalog.MustFindIndexByID(tbl, ref.Index)
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
			originIndexI, err := catalog.MustFindIndexByID(otherTable, ref.Index)
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
				var forwardFK catalog.ForeignKeyConstraint
				for _, otherFK := range otherTable.OutboundForeignKeys() {
					// To find a match, we find a foreign key reference that has the same
					// referenced table ID, and that the index we point to is a valid
					// index to satisfy the columns in the foreign key.
					if otherFK.GetReferencedTableID() == desc.ID &&
						descpb.ColumnIDs(originIndex.KeyColumnIDs).HasPrefix(otherFK.ForeignKeyDesc().OriginColumnIDs) {
						// Found a match.
						forwardFK = otherFK
						break
					}
				}
				if forwardFK == nil {
					// Corrupted foreign key - there was no forward reference for the back
					// reference.
					return false, errors.AssertionFailedf(
						"error finding foreign key on table %d for backref %+v",
						otherTable.GetID(), ref)
				}
				inFK = descpb.ForeignKeyConstraint{
					OriginTableID:       ref.Table,
					OriginColumnIDs:     forwardFK.ForeignKeyDesc().OriginColumnIDs,
					ReferencedTableID:   desc.ID,
					ReferencedColumnIDs: forwardFK.ForeignKeyDesc().ReferencedColumnIDs,
					Name:                forwardFK.GetName(),
					Validity:            forwardFK.GetConstraintValidity(),
					OnDelete:            forwardFK.OnDelete(),
					OnUpdate:            forwardFK.OnUpdate(),
					Match:               forwardFK.Match(),
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
func maybeUpgradeFormatVersion(builder *tableDescriptorBuilder) (wasUpgraded bool) {
	desc := builder.getLatestDesc()
	for _, pair := range []struct {
		targetVersion descpb.FormatVersion
		upgradeFn     func(*descpb.TableDescriptor)
	}{
		{descpb.FamilyFormatVersion, upgradeToFamilyFormatVersion},
		{descpb.InterleavedFormatVersion, func(_ *descpb.TableDescriptor) {}},
	} {
		if desc.FormatVersion < pair.targetVersion {
			desc = builder.getOrInitModifiedDesc()
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
func maybeUpgradePrimaryIndexFormatVersion(builder *tableDescriptorBuilder) (hasChanged bool) {
	desc := builder.getLatestDesc()
	// Always set the correct encoding type for the primary index.
	if desc.PrimaryIndex.EncodingType != catenumpb.PrimaryIndexEncoding {
		desc = builder.getOrInitModifiedDesc()
		desc.PrimaryIndex.EncodingType = catenumpb.PrimaryIndexEncoding
	}
	// Check if primary index needs updating.
	switch desc.PrimaryIndex.Version {
	case descpb.PrimaryIndexWithStoredColumnsVersion:
		return false
	default:
		break
	}
	desc = builder.getOrInitModifiedDesc()
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
// version LatestIndexDescriptorVersion whenever possible.
//
// TODO(postamar): upgrade all the way to LatestIndexDescriptorVersion in 22.2
// This is not possible until then because of a limitation in 21.2 which affects
// mixed-21.2-22.1-version clusters (issue #78426).
func maybeUpgradeSecondaryIndexFormatVersion(idx *descpb.IndexDescriptor) (hasChanged bool) {
	switch idx.Version {
	case descpb.SecondaryIndexFamilyFormatVersion:
		// If the index type does not support STORING columns, then it's not
		// encoded differently based on whether column families are in use, so
		// nothing to do.
		if !idx.Type.SupportsStoring() {
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

// maybeAddConstraintIDs ensures that all constraints have an ID associated with
// them.
func maybeAddConstraintIDs(desc *descpb.TableDescriptor) (hasChanged bool) {
	// Only assign constraint IDs to physical tables.
	if !desc.IsTable() {
		return false
	}
	// Collect pointers to constraint ID variables.
	var idPtrs []*descpb.ConstraintID
	if len(desc.PrimaryIndex.KeyColumnIDs) > 0 {
		idPtrs = append(idPtrs, &desc.PrimaryIndex.ConstraintID)
	}
	for i := range desc.Indexes {
		idx := &desc.Indexes[i]
		if !idx.Unique || idx.UseDeletePreservingEncoding {
			continue
		}
		idPtrs = append(idPtrs, &idx.ConstraintID)
	}
	checkByName := make(map[string]*descpb.TableDescriptor_CheckConstraint)
	for i := range desc.Checks {
		ck := desc.Checks[i]
		idPtrs = append(idPtrs, &ck.ConstraintID)
		checkByName[ck.Name] = ck
	}
	fkByName := make(map[string]*descpb.ForeignKeyConstraint)
	for i := range desc.OutboundFKs {
		fk := &desc.OutboundFKs[i]
		idPtrs = append(idPtrs, &fk.ConstraintID)
		fkByName[fk.Name] = fk
	}
	for i := range desc.InboundFKs {
		idPtrs = append(idPtrs, &desc.InboundFKs[i].ConstraintID)
	}
	uwoiByName := make(map[string]*descpb.UniqueWithoutIndexConstraint)
	for i := range desc.UniqueWithoutIndexConstraints {
		uwoi := &desc.UniqueWithoutIndexConstraints[i]
		idPtrs = append(idPtrs, &uwoi.ConstraintID)
		uwoiByName[uwoi.Name] = uwoi
	}
	for _, m := range desc.GetMutations() {
		if idx := m.GetIndex(); idx != nil && idx.Unique && !idx.UseDeletePreservingEncoding {
			idPtrs = append(idPtrs, &idx.ConstraintID)
		} else if c := m.GetConstraint(); c != nil {
			switch c.ConstraintType {
			case descpb.ConstraintToUpdate_CHECK, descpb.ConstraintToUpdate_NOT_NULL:
				idPtrs = append(idPtrs, &c.Check.ConstraintID)
			case descpb.ConstraintToUpdate_FOREIGN_KEY:
				idPtrs = append(idPtrs, &c.ForeignKey.ConstraintID)
			case descpb.ConstraintToUpdate_UNIQUE_WITHOUT_INDEX:
				idPtrs = append(idPtrs, &c.UniqueWithoutIndexConstraint.ConstraintID)
			}
		}
	}
	// Set constraint ID counter to sane initial value.
	var maxID descpb.ConstraintID
	for _, p := range idPtrs {
		if id := *p; id > maxID {
			maxID = id
		}
	}
	if desc.NextConstraintID <= maxID {
		desc.NextConstraintID = maxID + 1
		hasChanged = true
	}
	// Update zero constraint IDs using counter.
	for _, p := range idPtrs {
		if *p != 0 {
			continue
		}
		*p = desc.NextConstraintID
		desc.NextConstraintID++
		hasChanged = true
	}
	// Reconcile constraint IDs between enforced slice and mutation.
	for _, m := range desc.GetMutations() {
		if c := m.GetConstraint(); c != nil {
			switch c.ConstraintType {
			case descpb.ConstraintToUpdate_CHECK, descpb.ConstraintToUpdate_NOT_NULL:
				if other, ok := checkByName[c.Check.Name]; ok {
					c.Check.ConstraintID = other.ConstraintID
				}
			case descpb.ConstraintToUpdate_FOREIGN_KEY:
				if other, ok := fkByName[c.ForeignKey.Name]; ok {
					c.ForeignKey.ConstraintID = other.ConstraintID
				}
			case descpb.ConstraintToUpdate_UNIQUE_WITHOUT_INDEX:
				if other, ok := uwoiByName[c.UniqueWithoutIndexConstraint.Name]; ok {
					c.UniqueWithoutIndexConstraint.ConstraintID = other.ConstraintID
				}
			}
		}
	}
	return hasChanged
}

// maybeRemoveDuplicateIDsInRefs ensures that IDs in references to other tables
// are not duplicated.
func maybeRemoveDuplicateIDsInRefs(builder *tableDescriptorBuilder) (hasChanged bool) {
	// Strip duplicates from DependsOn.
	d := builder.getLatestDesc()
	if s := cleanedIDs(d.DependsOn); len(s) < len(d.DependsOn) {
		d = builder.getOrInitModifiedDesc()
		d.DependsOn = s
		hasChanged = true
	}
	// Do the same for DependsOnTypes.
	if s := cleanedIDs(d.DependsOnTypes); len(s) < len(d.DependsOnTypes) {
		d = builder.getOrInitModifiedDesc()
		d.DependsOnTypes = s
		hasChanged = true
	}
	// Do the same for column IDs in DependedOnBy table references.
	for i := range d.DependedOnBy {
		ref := &d.DependedOnBy[i]
		s := catalog.MakeTableColSet(ref.ColumnIDs...).Ordered()
		if len(s) < len(ref.ColumnIDs) {
			builder.getOrInitModifiedDesc().DependedOnBy[i].ColumnIDs = s
			hasChanged = true
		}
	}
	// Do the same in columns for sequence refs.
	for i := range d.Columns {
		col := &d.Columns[i]
		if s := cleanedIDs(col.UsesSequenceIds); len(s) < len(col.UsesSequenceIds) {
			builder.getOrInitModifiedDesc().Columns[i].UsesSequenceIds = s
			hasChanged = true
		}
		if s := cleanedIDs(col.OwnsSequenceIds); len(s) < len(col.OwnsSequenceIds) {
			builder.getOrInitModifiedDesc().Columns[i].OwnsSequenceIds = s
			hasChanged = true
			break
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

// maybeStripDanglingSelfBackReferences removes any references to things
// within the table descriptor itself which don't exist.
//
// TODO(postamar): extend as needed to column references and whatnot
func maybeStripDanglingSelfBackReferences(builder *tableDescriptorBuilder) (hasChanged bool) {
	tbl := builder.getLatestDesc()
	var mutationIDs intsets.Fast
	for _, m := range tbl.Mutations {
		mutationIDs.Add(int(m.MutationID))
	}
	for _, backref := range tbl.MutationJobs {
		if !mutationIDs.Contains(int(backref.MutationID)) {
			tbl = builder.getOrInitModifiedDesc()
			hasChanged = true
			break
		}
	}
	if !hasChanged {
		return
	}
	// Remove mutation_jobs entries which don't reference a valid mutation ID.
	{
		sliceIdx := 0
		for _, backref := range tbl.MutationJobs {
			tbl.MutationJobs[sliceIdx] = backref
			if mutationIDs.Contains(int(backref.MutationID)) {
				sliceIdx++
			}
		}
		if sliceIdx < len(tbl.MutationJobs) {
			tbl.MutationJobs = tbl.MutationJobs[:sliceIdx]
		}
	}
	return hasChanged
}

func maybeFixSecondaryIndexEncodingType(builder *tableDescriptorBuilder) (hasChanged bool) {
	desc := builder.getLatestDesc()
	if desc.DeclarativeSchemaChangerState != nil || len(desc.Mutations) > 0 {
		// Don't try to fix the encoding type if a schema change is in progress.
		return false
	}
	for i := range desc.Indexes {
		idx := &desc.Indexes[i]
		if idx.EncodingType != catenumpb.SecondaryIndexEncoding {
			desc = builder.getOrInitModifiedDesc()
			idx = &desc.Indexes[i]
			idx.EncodingType = catenumpb.SecondaryIndexEncoding
			hasChanged = true
		}
	}
	return hasChanged
}

// maybeSetCreateAsOfTime ensures that the CreateAsOfTime field is set.
//
// CreateAsOfTime is used for CREATE TABLE ... AS ... and was introduced in
// v19.1. In general it is not critical to set except for tables in the ADD
// state which were created from CTAS so we should not assert on its not
// being set. It's not always sensical to set it from the passed MVCC
// timestamp. However, starting in 19.2 the CreateAsOfTime and
// ModificationTime fields are both unset for the first Version of a
// TableDescriptor and the code relies on the value being set based on the
// MVCC timestamp.
func maybeSetCreateAsOfTime(desc *descpb.TableDescriptor) (hasChanged bool) {
	if !desc.CreateAsOfTime.IsEmpty() || desc.Version > 1 || desc.ModificationTime.IsEmpty() {
		return false
	}
	// Ignore system tables, unless they're explicitly created using CTAS
	// How that could ever happen is unclear, but never mind.
	if desc.ParentID == keys.SystemDatabaseID && desc.CreateQuery == "" {
		return false
	}
	// The expectation is that this is only set when the version is 2.
	// For any version greater than that, this is not accurate but better than
	// nothing at all.
	desc.CreateAsOfTime = desc.ModificationTime
	return true
}

// maybeUpgradeSequenceReference attempts to upgrade by-name sequence references.
// If `rel` is a table: upgrade seq reference in each column;
// If `rel` is a view: upgrade seq reference in its view query;
// If `rel` is a sequence: upgrade its back-references to relations as "ByID".
// All these attempts are on a best-effort basis.
func maybeUpgradeSequenceReference(
	descLookupFn func(id descpb.ID) catalog.Descriptor, rel *descpb.TableDescriptor,
) (hasUpgraded bool, err error) {
	if rel.IsTable() {
		hasUpgraded, err = maybeUpgradeSequenceReferenceForTable(descLookupFn, rel)
		if err != nil {
			return hasUpgraded, err
		}
	} else if rel.IsView() {
		hasUpgraded, err = maybeUpgradeSequenceReferenceForView(descLookupFn, rel)
		if err != nil {
			return hasUpgraded, err
		}
	} else if rel.IsSequence() {
		// Upgrade all references to this sequence to "by-ID".
		for i, ref := range rel.DependedOnBy {
			if ref.ID != descpb.InvalidID && !ref.ByID {
				rel.DependedOnBy[i].ByID = true
				hasUpgraded = true
			}
		}
	} else {
		return hasUpgraded, errors.AssertionFailedf("table descriptor %v (%d) is not a "+
			"table, view, or sequence.", rel.Name, rel.ID)
	}

	return hasUpgraded, err
}

// maybeUpgradeSequenceReferenceForTable upgrades all by-name sequence references
// in `tableDesc` to by-ID.
func maybeUpgradeSequenceReferenceForTable(
	descLookupFn func(id descpb.ID) catalog.Descriptor, tableDesc *descpb.TableDescriptor,
) (hasUpgraded bool, err error) {
	if !tableDesc.IsTable() {
		return hasUpgraded, nil
	}

	for _, col := range tableDesc.Columns {
		// Find sequence names for all sequences used in this column.
		usedSequenceIDToNames, err := resolveTableNamesForIDs(descLookupFn, col.UsesSequenceIds)
		if err != nil {
			return hasUpgraded, err
		}

		// Upgrade sequence reference in DEFAULT expression, if any.
		if col.HasDefault() {
			hasUpgradedInDefault, err := seqexpr.UpgradeSequenceReferenceInExpr(col.DefaultExpr, usedSequenceIDToNames)
			if err != nil {
				return hasUpgraded, err
			}
			hasUpgraded = hasUpgraded || hasUpgradedInDefault
		}

		// Upgrade sequence reference in ON UPDATE expression, if any.
		if col.HasOnUpdate() {
			hasUpgradedInOnUpdate, err := seqexpr.UpgradeSequenceReferenceInExpr(col.OnUpdateExpr, usedSequenceIDToNames)
			if err != nil {
				return hasUpgraded, err
			}
			hasUpgraded = hasUpgraded || hasUpgradedInOnUpdate
		}
	}

	return hasUpgraded, nil
}

// maybeUpgradeSequenceReferenceForView similarily upgrades all by-name sequence references
// in `viewDesc` to by-ID.
func maybeUpgradeSequenceReferenceForView(
	descLookupFn func(id descpb.ID) catalog.Descriptor, viewDesc *descpb.TableDescriptor,
) (hasUpgraded bool, err error) {
	if !viewDesc.IsView() {
		return hasUpgraded, err
	}

	// Find sequence names for all those used sequences.
	usedSequenceIDToNames, err := resolveTableNamesForIDs(descLookupFn, viewDesc.DependsOn)
	if err != nil {
		return hasUpgraded, err
	}

	// A function that looks at an expression and replace any by-name sequence reference with
	// by-ID reference. It, of course, also append replaced sequence IDs to `upgradedSeqIDs`.
	replaceSeqFunc := func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		newExprStr := expr.String()
		hasUpgradedInExpr, err := seqexpr.UpgradeSequenceReferenceInExpr(&newExprStr, usedSequenceIDToNames)
		if err != nil {
			return false, expr, err
		}
		newExpr, err = parser.ParseExpr(newExprStr)
		if err != nil {
			return false, expr, err
		}

		hasUpgraded = hasUpgraded || hasUpgradedInExpr
		return false, newExpr, err
	}

	stmt, err := parser.ParseOne(viewDesc.GetViewQuery())
	if err != nil {
		return hasUpgraded, err
	}

	newStmt, err := tree.SimpleStmtVisit(stmt.AST, replaceSeqFunc)
	if err != nil {
		return hasUpgraded, err
	}

	viewDesc.ViewQuery = newStmt.String()

	return hasUpgraded, err
}

// Attempt to fully resolve table names for `ids` from a list of descriptors.
// IDs that do not exist or do not identify a table descriptor will be skipped.
//
// This is done on a best-effort basis, meaning if we cannot find a table's
// schema or database name from `descLookupFn`, they will be set to empty.
// Consumers of the return of this function should hence expect non-fully resolved
// table names.
func resolveTableNamesForIDs(
	descLookupFn func(id descpb.ID) catalog.Descriptor, ids []descpb.ID,
) (map[descpb.ID]*tree.TableName, error) {
	result := make(map[descpb.ID]*tree.TableName)

	for _, id := range ids {
		if _, exists := result[id]; exists {
			continue
		}

		// Attempt to retrieve the table descriptor for `id`; Skip if it does not exist or it does not
		// identify a table descriptor.
		d := descLookupFn(id)
		tableDesc, ok := d.(catalog.TableDescriptor)
		if !ok {
			continue
		}

		// Attempt to get its database and schema name on a best-effort basis.
		dbName := ""
		d = descLookupFn(tableDesc.GetParentID())
		if dbDesc, ok := d.(catalog.DatabaseDescriptor); ok {
			dbName = dbDesc.GetName()
		}

		scName := ""
		d = descLookupFn(tableDesc.GetParentSchemaID())
		if d != nil {
			if scDesc, ok := d.(catalog.SchemaDescriptor); ok {
				scName = scDesc.GetName()
			}
		} else {
			if tableDesc.GetParentSchemaID() == keys.PublicSchemaIDForBackup {
				// For backups created in 21.2 and prior, the "public" schema is descriptorless,
				// and always uses the const `keys.PublicSchemaIDForBackUp` as the "public"
				// schema ID.
				scName = catconstants.PublicSchemaName
			}
		}

		result[id] = tree.NewTableNameWithSchema(
			tree.Name(dbName),
			tree.Name(scName),
			tree.Name(tableDesc.GetName()),
		)
		if dbName == "" {
			result[id].ExplicitCatalog = false
		}
		if scName == "" {
			result[id].ExplicitSchema = false
		}
	}

	return result, nil
}

// maybeFixForeignKeySelfReferences fixes references that should point to the
// current descriptor inside OutboundFKs and InboundFKs.
func maybeFixForeignKeySelfReferences(builder *tableDescriptorBuilder) (wasRepaired bool) {
	tableDesc := builder.getLatestDesc()
	for idx := range tableDesc.OutboundFKs {
		fk := &tableDesc.OutboundFKs[idx]
		if fk.OriginTableID != tableDesc.ID {
			tableDesc = builder.getOrInitModifiedDesc()
			fk = &tableDesc.OutboundFKs[idx]
			fk.OriginTableID = tableDesc.ID
			wasRepaired = true
		}
	}
	for idx := range tableDesc.InboundFKs {
		fk := &tableDesc.InboundFKs[idx]
		if fk.ReferencedTableID != tableDesc.ID {
			tableDesc = builder.getOrInitModifiedDesc()
			fk = &tableDesc.InboundFKs[idx]
			fk.ReferencedTableID = tableDesc.ID
			wasRepaired = true
		}
	}
	return wasRepaired
}

// maybeFixUsesSequencesIDForIdentityColumns fixes sequence references for
// identity / serial column expressions.
func maybeFixUsesSequencesIDForIdentityColumns(
	builder *tableDescriptorBuilder,
) (wasRepaired bool, err error) {
	tableDesc := builder.getLatestDesc()
	for idx := range tableDesc.Columns {
		column := &tableDesc.Columns[idx]
		if column.GeneratedAsIdentityType == catpb.GeneratedAsIdentityType_NOT_IDENTITY_COLUMN {
			continue
		}
		// If the column doesn't have exactly one sequence reference then something
		// is wrong.
		if len(column.UsesSequenceIds) == 1 {
			continue
		}
		if len(column.OwnsSequenceIds) == 0 {
			// With serial_normalization=rowid, a table definition like
			// `CREATE TABLE t (a SERIAL GENERATED ALWAYS AS IDENTITY)`
			// creates an identity column without any backing sequence. If that's the
			// case, there's no need to add a sequence reference.
			continue
		}
		tableDesc = builder.getOrInitModifiedDesc()
		column = &tableDesc.Columns[idx]
		column.UsesSequenceIds = append(column.UsesSequenceIds, column.OwnsSequenceIds[0])
		wasRepaired = true
	}
	return wasRepaired, nil
}
