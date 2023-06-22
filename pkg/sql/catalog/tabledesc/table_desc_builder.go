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
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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
	original             *descpb.TableDescriptor
	maybeModified        *descpb.TableDescriptor
	mvccTimestamp        hlc.Timestamp
	isUncommittedVersion bool
	changes              catalog.PostDeserializationChanges
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
	mvccTimestamp hlc.Timestamp,
	isUncommittedVersion bool,
	changes catalog.PostDeserializationChanges,
) *tableDescriptorBuilder {
	return &tableDescriptorBuilder{
		original:             protoutil.Clone(desc).(*descpb.TableDescriptor),
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
	{
		orig := tdb.getLatestDesc()
		// Set the ModificationTime field before doing anything else.
		// Other changes may depend on it.
		mustSetModTime, err := descpb.MustSetModificationTime(
			orig.ModificationTime, tdb.mvccTimestamp, orig.Version,
		)
		if err != nil {
			return err
		}
		if mustSetModTime {
			modifiedDesc := tdb.getOrInitModifiedDesc()
			modifiedDesc.ModificationTime = tdb.mvccTimestamp
			tdb.changes.Add(catalog.SetModTimeToMVCCTimestamp)
		}
	}
	c, err := maybeFillInDescriptor(tdb)
	if err != nil {
		return err
	}
	c.ForEach(tdb.changes.Add)
	return nil
}

// RunRestoreChanges implements the catalog.DescriptorBuilder interface.
func (tdb *tableDescriptorBuilder) RunRestoreChanges(
	version clusterversion.ClusterVersion, descLookupFn func(id descpb.ID) catalog.Descriptor,
) (err error) {
	// Upgrade the declarative schema changer state
	if scpb.MigrateDescriptorState(version, tdb.maybeModified.DeclarativeSchemaChangerState) {
		tdb.changes.Add(catalog.UpgradedDeclarativeSchemaChangerState)
	}

	return err
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
	set(catalog.SetCreateAsOfTimeUsingModTime, maybeSetCreateAsOfTime(builder))
	{
		orig := builder.getLatestDesc()
		for i := range orig.Indexes {
			origIdx := orig.Indexes[i]
			// TODO(rytaft): Remove this case in 24.1.
			if origIdx.NotVisible && origIdx.Invisibility == 0.0 {
				modifiedDesc := builder.getOrInitModifiedDesc()
				set(catalog.SetIndexInvisibility, true)
				modifiedDesc.Indexes[i].Invisibility = 1.0
			}
		}
	}
	set(catalog.SetCheckConstraintColumnIDs, maybeSetCheckConstraintColumnIDs(builder))
	return changes, nil
}

// FamilyPrimaryName is the name of the "primary" family, which is autogenerated
// the family clause is not specified.
const FamilyPrimaryName = "primary"

// maybeSetCheckConstraintColumnIDs ensures that all check constraints have a
// ColumnIDs slice which is populated if it should be.
func maybeSetCheckConstraintColumnIDs(builder *tableDescriptorBuilder) (hasChanged bool) {
	orig := builder.getLatestDesc()
	// Collect valid column names.
	nonDropColumnIDs := make(map[string]descpb.ColumnID, len(orig.Columns))
	for i := range orig.Columns {
		nonDropColumnIDs[orig.Columns[i].Name] = orig.Columns[i].ID
	}
	for _, m := range orig.Mutations {
		if col := m.GetColumn(); col != nil && m.Direction != descpb.DescriptorMutation_DROP {
			nonDropColumnIDs[col.Name] = col.ID
		}
	}
	var colIDsUsed catalog.TableColSet
	visitFn := func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		if vBase, ok := expr.(tree.VarName); ok {
			v, err := vBase.NormalizeVarName()
			if err != nil {
				return false, nil, err
			}
			if c, ok := v.(*tree.ColumnItem); ok {
				colID, found := nonDropColumnIDs[string(c.ColumnName)]
				if !found {
					return false, nil, errors.New("column not found")
				}
				colIDsUsed.Add(colID)
			}
			return false, v, nil
		}
		return true, expr, nil
	}

	for i, ck := range orig.Checks {
		if len(ck.ColumnIDs) > 0 {
			continue
		}
		parsed, err := parser.ParseExpr(ck.Expr)
		if err != nil {
			// We do this on a best-effort basis.
			continue
		}
		colIDsUsed = catalog.TableColSet{}
		if _, err := tree.SimpleVisit(parsed, visitFn); err != nil {
			// We do this on a best-effort basis.
			continue
		}
		if !colIDsUsed.Empty() {
			modified := builder.getOrInitModifiedDesc()
			modified.Checks[i].ColumnIDs = colIDsUsed.Ordered()
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
func maybeSetCreateAsOfTime(builder *tableDescriptorBuilder) (hasChanged bool) {
	desc := builder.getLatestDesc()
	if !desc.CreateAsOfTime.IsEmpty() || desc.Version > 1 || desc.ModificationTime.IsEmpty() {
		return false
	}
	// The expectation is that this is only set when the version is 2.
	// For any version greater than that, this is not accurate but better than
	// nothing at all.
	modifiedDesc := builder.getOrInitModifiedDesc()
	modifiedDesc.CreateAsOfTime = desc.ModificationTime
	return true
}
