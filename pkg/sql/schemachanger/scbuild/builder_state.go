// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuild

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/seqexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	plpgsql "github.com/cockroachdb/cockroach/pkg/sql/plpgsql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild/internal/scbuildstmt"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdecomp"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/plpgsqltree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/lib/pq/oid"
)

var _ scbuildstmt.BuilderState = (*builderState)(nil)

// QueryByID implements the scbuildstmt.BuilderState interface.
func (b *builderState) QueryByID(id catid.DescID) scbuildstmt.ElementResultSet {
	b.ensureDescriptor(id)
	c := b.descCache[id]
	if c.cachedCollection == nil {
		c.cachedCollection = b.newCollection(id)
	}
	return c.cachedCollection
}

// Ensure implements the scbuildstmt.BuilderState interface.
func (b *builderState) Ensure(e scpb.Element, target scpb.TargetStatus, meta scpb.TargetMetadata) {
	dst := b.getExistingElementState(e)
	switch target {
	case scpb.ToAbsent, scpb.ToPublic, scpb.Transient:
		// Sanity check.
	default:
		panic(errors.AssertionFailedf("unsupported target %s", target.Status()))
	}

	if dst == nil {
		// We're adding both a new element and a target for it.
		if target == scpb.ToAbsent {
			// Ignore targets to remove something that doesn't exist yet.
			return
		}
		// Set a target for the element but check for concurrent schema changes.
		_ = b.checkForConcurrentSchemaChanges(e)
		b.addNewElementState(elementState{
			element:  e,
			initial:  scpb.Status_ABSENT,
			current:  scpb.Status_ABSENT,
			target:   target,
			metadata: meta,
		})
		return
	}
	// At this point, we're setting a new target to an existing element.
	if dst.target == target {
		// Ignore no-op changes.
		return
	}

	// Check that there are no concurrent schema changes on the descriptors
	// referenced by this element. Re-assign dst because of potential
	// re-allocations.
	dst = b.checkForConcurrentSchemaChanges(e)

	// We were about to overwrite an element's target and metadata. Assert one
	// disallowed case: reviving a "ghost" element, that is, add an element that
	// was previously added and then dropped. This assertion is artificial per se
	// and should not be taken as holy and unbreakable. It is just that we don't
	// think why someone would need to do this for now, so we simply put a gate
	// here as a sanity check. If we do find a case where we need to revive a
	// ghost element in the future, feel free to remove this check.
	// We fall back to legacy schema changer if it happens since users do not need
	// to see this but still it will make to the logs so developers can be aware
	// of it and investigate it further if needed.
	if dst.current == scpb.Status_ABSENT &&
		dst.target == scpb.ToAbsent &&
		(target == scpb.ToPublic || target == scpb.Transient) &&
		dst.metadata.IsLinkedToSchemaChange() {
		panic(scerrors.NotImplementedErrorf(
			nil,
			redact.Sprintf(
				"attempt to revive a ghost element: [elem=%v],[current=ABSENT],[target=ToAbsent],[newTarget=%v]",
				dst.element.String(), target.Status(),
			),
		))
	}

	// Henceforth all possibilities lead to the target and metadata being
	// overwritten. See below for explanations as to why this is legal.
	oldTarget, oldStatementID := dst.target, dst.metadata.StatementID
	dst.target, dst.metadata = target, meta
	if dst.metadata.Size() == 0 {
		// The element has never had a target set before.
		// We can freely overwrite it.
		return
	}
	if oldStatementID == meta.StatementID {
		// The element has had a target set before, but it was in the same build.
		// We can freely overwrite it or unset it.
		if target.Status() == dst.initial {
			// We're undoing the earlier target, as if it were never set
			// in the first place.
			dst.metadata.Reset()
		}
		return
	}
	// At this point, the element has had a target set in a previous build.
	// The element may since have transitioned towards the target which we now
	// want to overwrite as a result of executing at least one statement stage.
	// As a result, we may no longer unset the target, even if it is a no-op
	// one we reach the pre-commit phase.
	switch oldTarget {
	case scpb.ToPublic:
		// Here the new target is either to-absent, which effectively undoes the
		// incumbent target, or it's transient, which to-public targets can
		// straightforwardly be promoted to (the public opgen path is a subset
		// of the transient one when both exist, which must be the case here).
		return
	case scpb.ToAbsent:
		// Here the new target is either to-public, which effectively undoes the
		// incumbent target, or it's transient. The to-absent path is a subset of
		// the transient path so it can be done but we must take care to migrate
		// the current status to its transient equivalent if need be in order to
		// avoid the possibility of a future transition to PUBLIC.
		if target == scpb.Transient {
			var ok bool
			dst.current, ok = scpb.GetTransientEquivalent(dst.current)
			if !ok {
				panic(errors.AssertionFailedf(
					"cannot promote to-absent target for %s to transient, "+
						"current status %s has no transient equivalent",
					screl.ElementString(e), dst.current))
			}
		}
		return
	case scpb.Transient:
		// Here the new target is either to-absent, which effectively undoes the
		// incumbent target, or it's to-public. In both cases if the current
		// status is TRANSIENT_ we need to migrate it to its non-transient
		// equivalent because TRANSIENT_ statuses aren't featured on either the
		// to-absent or the to-public opgen path.
		if nonTransient, ok := scpb.GetNonTransientEquivalent(dst.current); ok {
			dst.current = nonTransient
		}
		return
	}
	panic(errors.AssertionFailedf("unsupported incumbent target %s", oldTarget.Status()))
}

func (b *builderState) checkForConcurrentSchemaChanges(e scpb.Element) *elementState {
	b.ensureDescriptors(e)
	// Check that there are no descriptors which are undergoing a concurrent
	// schema change which might interfere with this one.
	screl.AllTargetDescIDs(e).ForEach(func(id descpb.ID) {
		if c := b.descCache[id]; c != nil && c.desc != nil && c.desc.HasConcurrentSchemaChanges() {
			panic(scerrors.ConcurrentSchemaChangeError(c.desc))
		}
	})
	// We may have mutated the builder state for this element.
	// Specifically, the output slice might have grown and have been realloc'ed.
	return b.getExistingElementState(e)
}

// ensureDescriptors ensures the presence of all elements for all
// descriptors referenced inside the element.
//
// This provides us with all of the ID -> name mappings required to
// comprehensively decorate any EXPLAIN (DDL) output.
func (b *builderState) ensureDescriptors(e scpb.Element) {
	_ = screl.WalkDescIDs(e, func(id *catid.DescID) error {
		b.ensureDescriptor(*id)
		return nil
	})
}

func (b *builderState) upsertElementState(es elementState) {
	if existing := b.getExistingElementState(es.element); existing != nil {
		if err := b.localMemAcc.Grow(b.ctx, es.byteSize()-existing.byteSize()); err != nil {
			panic(err)
		}
		*existing = es
	} else {
		b.addNewElementState(es)
	}
}

func (b *builderState) getExistingElementState(e scpb.Element) *elementState {
	if e == nil {
		panic(errors.AssertionFailedf("cannot define target for nil element"))
	}
	id := screl.GetDescID(e)
	b.ensureDescriptor(id)
	c := b.descCache[id]

	key := screl.ElementString(e)
	i, ok := c.elementIndexMap[key]
	if !ok {
		return nil
	}
	es := &b.output[i]
	if !screl.EqualElementKeys(es.element, e) {
		panic(errors.AssertionFailedf("actual element key %v does not match expected %s",
			key, screl.ElementString(es.element)))
	}
	return es
}

func (b *builderState) addNewElementState(es elementState) {
	if err := b.localMemAcc.Grow(b.ctx, es.byteSize()); err != nil {
		panic(err)
	}
	id := screl.GetDescID(es.element)
	key := screl.ElementString(es.element)
	c := b.descCache[id]
	c.outputIndexes = append(c.outputIndexes, len(b.output))
	c.elementIndexMap[key] = len(b.output)
	b.output = append(b.output, es)
	c.cachedCollection = nil
}

// LogEventForExistingTarget implements the scbuildstmt.BuilderState interface.
func (b *builderState) LogEventForExistingTarget(e scpb.Element) {
	id := screl.GetDescID(e)
	key := screl.ElementString(e)

	c, ok := b.descCache[id]
	if !ok {
		panic(errors.AssertionFailedf(
			"elements for descriptor ID %d not found in builder state, %s expected", id, key))
	}
	i, ok := c.elementIndexMap[key]
	if !ok {
		panic(errors.AssertionFailedf("element %s expected in builder state but not found", key))
	}
	es := &b.output[i]
	if es.target == scpb.InvalidTarget {
		panic(errors.AssertionFailedf("no target set for element %s in builder state", key))
	}
	es.withLogEvent = true
}

// LogEventForExistingPayload implements the scbuildstmt.BuilderState interface.
func (b *builderState) LogEventForExistingPayload(e scpb.Element, payload logpb.EventPayload) {
	id := screl.GetDescID(e)
	key := screl.ElementString(e)

	c, ok := b.descCache[id]
	if !ok {
		panic(errors.AssertionFailedf(
			"elements for descriptor ID %d not found in builder state, %s expected", id, key))
	}
	i, ok := c.elementIndexMap[key]
	if !ok {
		panic(errors.AssertionFailedf("element %s expected in builder state but not found", key))
	}
	es := &b.output[i]
	if es.target == scpb.InvalidTarget {
		panic(errors.AssertionFailedf("no target set for element %s in builder state", key))
	}
	es.withLogEvent = true
	es.maybePayload = payload
}

var _ scbuildstmt.PrivilegeChecker = (*builderState)(nil)

// HasOwnership implements the scbuildstmt.PrivilegeChecker interface.
func (b *builderState) HasOwnership(e scpb.Element) bool {
	if b.hasAdmin {
		return true
	}
	id := screl.GetDescID(e)
	b.ensureDescriptor(id)
	return b.descCache[id].hasOwnership
}

func (b *builderState) mustOwn(id catid.DescID) {
	if b.hasAdmin {
		return
	}
	b.ensureDescriptor(id)
	if c := b.descCache[id]; !c.hasOwnership {
		panic(pgerror.Newf(pgcode.InsufficientPrivilege,
			"must be owner of %s %s", c.desc.DescriptorType(), tree.Name(c.desc.GetName())))
	}
}

// CheckPrivilege implements the scbuildstmt.PrivilegeChecker interface.
func (b *builderState) CheckPrivilege(e scpb.Element, privilege privilege.Kind) error {
	return b.checkPrivilege(screl.GetDescID(e), privilege)
}

// checkPrivilege checks if current user has privilege `priv` on descriptor with `id`.
func (b *builderState) checkPrivilege(id catid.DescID, priv privilege.Kind) error {
	b.ensureDescriptor(id)
	c := b.descCache[id]
	if c.hasOwnership {
		return nil
	}
	err, found := c.privileges[priv]
	if !found {
		// Validate if this descriptor can be resolved under the current schema.
		if c.desc.DescriptorType() != catalog.Schema &&
			c.desc.DescriptorType() != catalog.Database {
			scpb.ForEachSchemaParent(
				b.QueryByID(id),
				func(current scpb.Status, _ scpb.TargetStatus, e *scpb.SchemaParent) {
					if current == scpb.Status_PUBLIC {
						b.requirePrivilege(e.SchemaID, privilege.USAGE)
					}
				},
			)
		}
		err = b.auth.CheckPrivilege(b.ctx, c.desc, priv)
		c.privileges[priv] = err
	}
	return err
}

// requirePrivilege is a must version of checkPrivilege where it panics if non-nil error.
func (b *builderState) requirePrivilege(id catid.DescID, priv privilege.Kind) {
	if err := b.checkPrivilege(id, priv); err != nil {
		panic(err)
	}
}

// CheckGlobalPrivilege implements the scbuildstmt.PrivilegeChecker interface.
func (b *builderState) CheckGlobalPrivilege(privilege privilege.Kind) error {
	return b.auth.CheckPrivilege(b.ctx, syntheticprivilege.GlobalPrivilegeObject, privilege)
}

// HasGlobalPrivilegeOrRoleOption implements the scbuildstmt.PrivilegeChecker interface.
func (b *builderState) HasGlobalPrivilegeOrRoleOption(
	ctx context.Context, privilege privilege.Kind,
) (bool, error) {
	return b.auth.HasGlobalPrivilegeOrRoleOption(ctx, privilege)
}

// CurrentUserHasAdminOrIsMemberOf implements the scbuildstmt.PrivilegeChecker interface.
func (b *builderState) CurrentUserHasAdminOrIsMemberOf(role username.SQLUsername) bool {
	if b.hasAdmin {
		return true
	}
	if b.evalCtx.SessionData().User() == role {
		return true
	}
	memberships, err := b.auth.MemberOfWithAdminOption(b.ctx, role)
	if err != nil {
		panic(err)
	}
	_, ok := memberships[b.evalCtx.SessionData().User()]
	return ok
}

func (b *builderState) CurrentUser() username.SQLUsername {
	return b.evalCtx.SessionData().User()
}

// CheckRoleExists implements the scbuild.AuthorizationAccessor interface.
func (b *builderState) CheckRoleExists(ctx context.Context, role username.SQLUsername) error {
	return b.auth.CheckRoleExists(ctx, role)
}

var _ scbuildstmt.TableHelpers = (*builderState)(nil)

// NextTableColumnID implements the scbuildstmt.TableHelpers interface.
func (b *builderState) NextTableColumnID(table *scpb.Table) (ret catid.ColumnID) {
	{
		b.ensureDescriptor(table.TableID)
		desc := b.descCache[table.TableID].desc
		tbl, ok := desc.(catalog.TableDescriptor)
		if !ok {
			panic(errors.AssertionFailedf("Expected table descriptor for ID %d, instead got %s",
				desc.GetID(), desc.DescriptorType()))
		}
		ret = tbl.GetNextColumnID()
	}
	scpb.ForEachColumn(
		b.QueryByID(table.TableID),
		func(_ scpb.Status, _ scpb.TargetStatus, column *scpb.Column) {
			if column.IsSystemColumn {
				return
			}
			if column.ColumnID >= ret {
				ret = column.ColumnID + 1
			}
		},
	)
	return ret
}

// NextColumnFamilyID implements the scbuildstmt.TableHelpers interface.
func (b *builderState) NextColumnFamilyID(table *scpb.Table) (ret catid.FamilyID) {
	{
		b.ensureDescriptor(table.TableID)
		desc := b.descCache[table.TableID].desc
		tbl, ok := desc.(catalog.TableDescriptor)
		if !ok {
			panic(errors.AssertionFailedf("Expected table descriptor for ID %d, instead got %s",
				desc.GetID(), desc.DescriptorType()))
		}
		ret = tbl.GetNextFamilyID()
	}
	scpb.ForEachColumnFamily(
		b.QueryByID(table.TableID),
		func(_ scpb.Status, _ scpb.TargetStatus, cf *scpb.ColumnFamily) {
			if cf.FamilyID >= ret {
				ret = cf.FamilyID + 1
			}
		},
	)
	return ret
}

// NextTableIndexID implements the scbuildstmt.TableHelpers interface.
func (b *builderState) NextTableIndexID(tableID catid.DescID) (ret catid.IndexID) {
	return b.nextIndexID(tableID)
}

// NextViewIndexID implements the scbuildstmt.TableHelpers interface.
func (b *builderState) NextViewIndexID(view *scpb.View) (ret catid.IndexID) {
	if !view.IsMaterialized {
		panic(errors.AssertionFailedf("expected materialized view: %s", screl.ElementString(view)))
	}
	return b.nextIndexID(view.ViewID)
}

// NextTableConstraintID implements the scbuildstmt.TableHelpers interface.
func (b *builderState) NextTableConstraintID(tableID catid.DescID) (ret catid.ConstraintID) {
	{
		b.ensureDescriptor(tableID)
		desc := b.descCache[tableID].desc
		tbl, ok := desc.(catalog.TableDescriptor)
		if !ok {
			panic(errors.AssertionFailedf("Expected table descriptor for ID %d, instead got %s",
				desc.GetID(), desc.DescriptorType()))
		}
		ret = tbl.GetNextConstraintID()
		if ret == 0 {
			ret = 1
		}
	}
	// Consult all present element in case they have a ConstraintID field and it's larger.
	b.QueryByID(tableID).ForEach(func(
		_ scpb.Status, _ scpb.TargetStatus, e scpb.Element,
	) {
		v, _ := screl.Schema.GetAttribute(screl.ConstraintID, e)
		if id, ok := v.(catid.ConstraintID); ok {
			if id < catid.ConstraintID(scbuildstmt.TableTentativeIdsStart) && id >= ret {
				ret = id + 1
			}
		}
	})
	return ret
}

// NextTableTriggerID implements the scbuildstmt.TableHelpers interface.
func (b *builderState) NextTableTriggerID(tableID catid.DescID) (ret catid.TriggerID) {
	{
		b.ensureDescriptor(tableID)
		desc := b.descCache[tableID].desc
		tbl, ok := desc.(catalog.TableDescriptor)
		if !ok {
			panic(errors.AssertionFailedf("Expected table descriptor for ID %d, instead got %s",
				desc.GetID(), desc.DescriptorType()))
		}
		ret = tbl.GetNextTriggerID()
		if ret == 0 {
			ret = 1
		}
	}
	// Consult all present element in case they have a larger TriggerID field.
	b.QueryByID(tableID).ForEach(func(
		_ scpb.Status, _ scpb.TargetStatus, e scpb.Element,
	) {
		v, _ := screl.Schema.GetAttribute(screl.TriggerID, e)
		if id, ok := v.(catid.TriggerID); ok {
			if id < catid.TriggerID(scbuildstmt.TableTentativeIdsStart) && id >= ret {
				ret = id + 1
			}
		}
	})
	return ret
}

// NextTablePolicyID implements the scbuildstmt.TableHelpers interface.
func (b *builderState) NextTablePolicyID(tableID catid.DescID) (ret catid.PolicyID) {
	{
		b.ensureDescriptor(tableID)
		desc := b.descCache[tableID].desc
		tbl, ok := desc.(catalog.TableDescriptor)
		if !ok {
			panic(errors.AssertionFailedf("Expected table descriptor for ID %d, instead got %s",
				desc.GetID(), desc.DescriptorType()))
		}
		ret = tbl.GetNextPolicyID()
		if ret == 0 {
			ret = 1
		}
	}
	// Consult all present element in case they have a larger PolicyID field.
	b.QueryByID(tableID).ForEach(func(
		_ scpb.Status, _ scpb.TargetStatus, e scpb.Element,
	) {
		v, _ := screl.Schema.GetAttribute(screl.PolicyID, e)
		if id, ok := v.(catid.PolicyID); ok {
			if id < catid.PolicyID(scbuildstmt.TableTentativeIdsStart) && id >= ret {
				ret = id + 1
			}
		}
	})
	return ret
}

// NextTableTentativeIndexID implements the scbuildstmt.TableHelpers interface.
func (b *builderState) NextTableTentativeIndexID(tableID catid.DescID) (ret catid.IndexID) {
	ret = catid.IndexID(scbuildstmt.TableTentativeIdsStart)

	// Consult all present index-related element in case their ID is larger.
	b.QueryByID(tableID).ForEach(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
		switch e.(type) {
		case *scpb.PrimaryIndex, *scpb.TemporaryIndex, *scpb.SecondaryIndex:
			val, _ := screl.Schema.GetAttribute(screl.IndexID, e)
			indexID := val.(catid.IndexID)
			if indexID >= catid.IndexID(scbuildstmt.TableTentativeIdsStart) && indexID >= ret {
				ret = indexID + 1
			}
		}
	})
	return ret
}

// NextTableTentativeConstraintID implements the scbuildstmt.TableHelpers interface.
func (b *builderState) NextTableTentativeConstraintID(
	tableID catid.DescID,
) (ret catid.ConstraintID) {
	ret = catid.ConstraintID(scbuildstmt.TableTentativeIdsStart)

	// Consult all present element in case they have a ConstraintID field and it's larger.
	b.QueryByID(tableID).ForEach(func(
		_ scpb.Status, _ scpb.TargetStatus, e scpb.Element,
	) {
		v, _ := screl.Schema.GetAttribute(screl.ConstraintID, e)
		if id, ok := v.(catid.ConstraintID); ok {
			if id >= catid.ConstraintID(scbuildstmt.TableTentativeIdsStart) && id >= ret {
				ret = id + 1
			}
		}
	})
	return ret
}

func (b *builderState) IsTableEmpty(table *scpb.Table) bool {
	// Scan the table for any rows, if they exist the lack of a default value
	// should lead to an error.
	elts := b.QueryByID(table.TableID)
	_, _, index := scpb.FindPrimaryIndex(elts)
	return b.tr.IsTableEmpty(b.ctx, table.TableID, index.IndexID)
}

func (b *builderState) nextIndexID(id catid.DescID) (ret catid.IndexID) {
	{
		b.ensureDescriptor(id)
		desc := b.descCache[id].desc
		tbl, ok := desc.(catalog.TableDescriptor)
		if !ok {
			panic(errors.AssertionFailedf("Expected table descriptor for ID %d, instead got %s",
				desc.GetID(), desc.DescriptorType()))
		}
		ret = tbl.GetNextIndexID()
	}
	// Consult all present index-related element in case their ID is larger.
	b.QueryByID(id).ForEach(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
		switch e.(type) {
		case *scpb.PrimaryIndex, *scpb.TemporaryIndex, *scpb.SecondaryIndex:
			val, _ := screl.Schema.GetAttribute(screl.IndexID, e)
			indexID := val.(catid.IndexID)
			if indexID < catid.IndexID(scbuildstmt.TableTentativeIdsStart) && indexID >= ret {
				ret = indexID + 1
			}
		}
	})
	return ret
}

// IndexPartitioningDescriptor implements the scbuildstmt.TableHelpers
// interface.
func (b *builderState) IndexPartitioningDescriptor(
	indexName string, index *scpb.Index, columns []*scpb.IndexColumn, partBy *tree.PartitionBy,
) catpb.PartitioningDescriptor {
	b.ensureDescriptor(index.TableID)
	bd := b.descCache[index.TableID]
	desc := bd.desc
	tbl, ok := desc.(catalog.TableDescriptor)
	if !ok {
		panic(errors.AssertionFailedf("Expected table descriptor for ID %d, instead got %s",
			desc.GetID(), desc.DescriptorType()))
	}
	var oldNumImplicitColumns int

	scpb.ForEachIndexPartitioning(
		b.QueryByID(index.TableID),
		func(_ scpb.Status, _ scpb.TargetStatus, p *scpb.IndexPartitioning) {
			if p.IndexID == index.IndexID {
				oldNumImplicitColumns = int(p.PartitioningDescriptor.NumImplicitColumns)
			}
		},
	)

	keyColumns := make([]*scpb.IndexColumn, 0, len(columns))
	for _, column := range columns {
		if column.Kind != scpb.IndexColumn_KEY {
			continue
		}
		keyColumns = append(keyColumns, column)
	}
	sort.Slice(keyColumns, func(i, j int) bool {
		return keyColumns[i].OrdinalInKind < keyColumns[j].OrdinalInKind
	})
	oldKeyColumnNames := make([]string, len(keyColumns))
	for i, ic := range keyColumns {
		scpb.ForEachColumnName(
			b.QueryByID(index.TableID),
			func(_ scpb.Status, _ scpb.TargetStatus, cn *scpb.ColumnName) {
				if cn.ColumnID != ic.ColumnID {
					return
				}
				oldKeyColumnNames[i] = cn.Name
			},
		)
	}
	var allowedNewColumnNames []tree.Name
	scpb.ForEachColumnName(
		b.QueryByID(index.TableID),
		func(current scpb.Status, target scpb.TargetStatus, cn *scpb.ColumnName) {
			if current != scpb.Status_PUBLIC && target == scpb.ToPublic {
				allowedNewColumnNames = append(allowedNewColumnNames, tree.Name(cn.Name))
			}
		},
	)
	allowImplicitPartitioning := b.evalCtx.SessionData().ImplicitColumnPartitioningEnabled ||
		tbl.IsLocalityRegionalByRow()
	_, ret, err := b.createPartCCL(
		b.ctx,
		b.clusterSettings,
		b.evalCtx,
		func(name tree.Name) (catalog.Column, error) {
			return catalog.MustFindColumnByTreeName(tbl, name)
		},
		oldNumImplicitColumns,
		oldKeyColumnNames,
		partBy,
		allowedNewColumnNames,
		allowImplicitPartitioning, /* allowImplicitPartitioning */
	)
	if err != nil {
		panic(err)
	}
	// Validate the index partitioning descriptor.
	partitionNames := make(map[string]struct{})
	for _, rangePart := range ret.Range {
		if _, ok := partitionNames[rangePart.Name]; ok {
			panic(pgerror.Newf(pgcode.InvalidObjectDefinition, "PARTITION %s: name must be unique (used twice in index %q)",
				rangePart.Name, indexName))
		}
		partitionNames[rangePart.Name] = struct{}{}
	}
	for _, listPart := range ret.List {
		if _, ok := partitionNames[listPart.Name]; ok {
			panic(pgerror.Newf(pgcode.InvalidObjectDefinition, "PARTITION %s: name must be unique (used twice in index %q)",
				listPart.Name, indexName))
		}
		partitionNames[listPart.Name] = struct{}{}
	}
	return ret
}

// ResolveTypeRef implements the scbuildstmt.TableHelpers interface.
func (b *builderState) ResolveTypeRef(ref tree.ResolvableTypeReference) scpb.TypeT {
	toType, err := tree.ResolveType(b.ctx, ref, b.cr)
	if err != nil {
		panic(err)
	}
	return newTypeT(toType)
}

func newTypeT(t *types.T) scpb.TypeT {
	return scpb.TypeT{Type: t, ClosedTypeIDs: typedesc.GetTypeDescriptorClosure(t).Ordered(), TypeName: t.SQLString()}
}

// WrapExpression implements the scbuildstmt.TableHelpers interface.
// N.B. The user should ensure that the input expression has UDF names replaced with OID references.
func (b *builderState) WrapExpression(tableID catid.DescID, expr tree.Expr) *scpb.Expression {
	// We will serialize and reparse the expression, so that type information
	// annotations are directly embedded inside, otherwise while parsing the
	// expression table record implicit types will not be correctly detected
	// by the TypeCollectorVisitor.
	expr, err := parser.ParseExpr(tree.Serialize(expr))
	if err != nil {
		panic(err)
	}
	if expr == nil {
		return nil
	}
	// Collect type IDs.
	var typeIDs catalog.DescriptorIDSet
	{
		visitor := &tree.TypeCollectorVisitor{OIDs: make(map[oid.Oid]struct{})}
		tree.WalkExpr(visitor, expr)
		for oid := range visitor.OIDs {
			if !types.IsOIDUserDefinedType(oid) {
				continue
			}
			id := typedesc.UserDefinedTypeOIDToID(oid)
			b.ensureDescriptor(id)
			desc := b.descCache[id].desc
			// Implicit record types will lead to table references, which will be
			// disallowed.
			if desc.DescriptorType() == catalog.Table {
				panic(pgerror.Newf(pgcode.DependentObjectsStillExist,
					"cannot modify table record type %q", desc.GetName()))
			}
			// Validate that no cross DB type references will exist here.
			// Determine the parent database ID, since cross database references are
			// disallowed.
			_, _, parentNamespace := scpb.FindNamespace(b.QueryByID(tableID))
			if desc.GetParentID() != parentNamespace.DatabaseID {
				typeName := tree.MakeTypeNameWithPrefix(b.descCache[id].prefix, desc.GetName())
				panic(pgerror.Newf(
					pgcode.FeatureNotSupported,
					"cross database type references are not supported: %s",
					typeName.String()))
			}
			typ, err := catalog.AsTypeDescriptor(desc)
			if err != nil {
				panic(err)
			}
			typ.GetIDClosure().ForEach(typeIDs.Add)
		}
	}
	// Collect sequence IDs.
	var seqIDs catalog.DescriptorIDSet
	{
		seqIdentifiers, err := seqexpr.GetUsedSequences(expr)
		if err != nil {
			panic(err)
		}
		seqNameToID := make(map[string]descpb.ID)
		for _, seqIdentifier := range seqIdentifiers {
			if seqIdentifier.IsByID() {
				seqIDs.Add(catid.DescID(seqIdentifier.SeqID))
				continue
			}
			uqName, err := parser.ParseTableName(seqIdentifier.SeqName)
			if err != nil {
				panic(err)
			}
			elts := b.ResolveSequence(uqName, scbuildstmt.ResolveParams{
				IsExistenceOptional: false,
				RequiredPrivilege:   privilege.SELECT,
			})
			_, _, seq := scpb.FindSequence(elts)
			seqNameToID[seqIdentifier.SeqName] = seq.SequenceID
			seqIDs.Add(seq.SequenceID)
		}
		if len(seqNameToID) > 0 {
			expr, err = seqexpr.ReplaceSequenceNamesWithIDs(expr, seqNameToID)
			if err != nil {
				panic(err)
			}
		}
	}
	// Collect function IDs, assuming that the UDF names has been replaced with OID references.
	fnIDs, err := schemaexpr.GetUDFIDs(expr)
	if err != nil {
		panic(err)
	}

	ids, err := scbuildstmt.ExtractColumnIDsInExpr(b, tableID, expr)
	if err != nil {
		panic(err)
	}
	ret := &scpb.Expression{
		Expr:                catpb.Expression(tree.Serialize(expr)),
		UsesSequenceIDs:     seqIDs.Ordered(),
		UsesTypeIDs:         typeIDs.Ordered(),
		UsesFunctionIDs:     fnIDs.Ordered(),
		ReferencedColumnIDs: ids.Ordered(),
	}
	return ret
}

// ComputedColumnExpression implements the scbuildstmt.TableHelpers interface.
func (b *builderState) ComputedColumnExpression(tbl *scpb.Table, d *tree.ColumnTableDef) tree.Expr {
	_, _, ns := scpb.FindNamespace(b.QueryByID(tbl.TableID))
	tn := tree.MakeTableNameFromPrefix(b.NamePrefix(tbl), tree.Name(ns.Name))
	b.ensureDescriptor(tbl.TableID)
	// TODO(postamar): this doesn't work when referencing newly added columns.
	expr, _, err := schemaexpr.ValidateComputedColumnExpression(
		b.ctx,
		b.descCache[tbl.TableID].desc.(catalog.TableDescriptor),
		d,
		&tn,
		tree.ComputedColumnExprContext(d.IsVirtual()),
		b.semaCtx,
		b.clusterSettings.Version.ActiveVersion(b.ctx),
	)
	if err != nil {
		// This may be referencing newly added columns, so cheat and return
		// a not implemented error.
		panic(errors.Wrapf(errors.WithSecondaryError(scerrors.NotImplementedError(d), err),
			"computed column validation error"))
	}
	parsedExpr, err := parser.ParseExpr(expr)
	if err != nil {
		panic(err)
	}
	return parsedExpr
}

// PartialIndexPredicateExpression implements the scbuildstmt.TableHelpers interface.
func (b *builderState) PartialIndexPredicateExpression(
	tableID catid.DescID, expr tree.Expr,
) tree.Expr {
	// Ensure that an namespace entry exists for the table.
	_, _, ns := scpb.FindNamespace(b.QueryByID(tableID))
	if ns == nil {
		panic(errors.AssertionFailedf("unable to find namespace for %d.", tableID))
	}
	tn := tree.MakeTableNameFromPrefix(b.NamePrefix(ns), tree.Name(ns.Name))
	// Confirm that we have a table descriptor.
	if _, ok := b.descCache[tableID].desc.(catalog.TableDescriptor); !ok {
		panic(errors.AssertionFailedf("descriptor %d is not a table.", tableID))
	}
	// TODO(fqazi): this doesn't work when referencing newly added columns, this
	// is not problematic today since declarative schema changer is only enabled
	// for single statement transactions.
	validatedExpr, err := schemaexpr.ValidatePartialIndexPredicate(
		b.ctx,
		b.descCache[tableID].desc.(catalog.TableDescriptor),
		expr,
		&tn,
		b.semaCtx,
		b.clusterSettings.Version.ActiveVersion(b.ctx))
	if err != nil {
		panic(err)
	}
	parsedExpr, err := parser.ParseExpr(validatedExpr)
	if err != nil {
		panic(err)
	}
	return parsedExpr
}

var _ scbuildstmt.ElementReferences = (*builderState)(nil)

// ForwardReferences implements the scbuildstmt.ElementReferences interface.
func (b *builderState) ForwardReferences(e scpb.Element) scbuildstmt.ElementResultSet {
	ids := screl.AllDescIDs(e)
	return b.newCollection(ids.Ordered()...)
}

// BackReferences implements the scbuildstmt.ElementReferences interface.
func (b *builderState) BackReferences(id catid.DescID) scbuildstmt.ElementResultSet {
	if id == catid.InvalidDescID {
		return nil
	}
	var ids catalog.DescriptorIDSet
	{
		b.ensureDescriptor(id)
		c := b.descCache[id]
		b.resolveBackReferences(c)
		c.backrefs.ForEach(ids.Add)
		c.backrefs.ForEach(b.ensureDescriptor)
		for i := range b.output {
			es := &b.output[i]
			if es.current == scpb.Status_PUBLIC || es.target != scpb.ToPublic {
				continue
			}
			descID := screl.GetDescID(es.element)
			if ids.Contains(descID) || descID == id {
				continue
			}
			if !screl.ContainsDescID(es.element, id) {
				continue
			}
			ids.Add(descID)
		}
		ids.ForEach(b.ensureDescriptor)
	}
	return b.newCollection(ids.Ordered()...)
}

func (b *builderState) newCollection(ids ...descpb.ID) *scpb.ElementCollection[scpb.Element] {
	var n int
	for _, id := range ids {
		b.ensureDescriptor(id)
		n = n + len(b.descCache[id].outputIndexes)
	}
	indexes := make([]int, 0, n)
	for _, id := range ids {
		indexes = append(indexes, b.descCache[id].outputIndexes...)
	}
	return scpb.NewElementCollection(b, indexes)
}

var _ scbuildstmt.NameResolver = (*builderState)(nil)

// NamePrefix implements the scbuildstmt.NameResolver interface.
func (b *builderState) NamePrefix(e scpb.Element) tree.ObjectNamePrefix {
	id := screl.GetDescID(e)
	b.ensureDescriptor(id)
	return b.descCache[id].prefix
}

// ResolveDatabase implements the scbuildstmt.NameResolver interface.
func (b *builderState) ResolveDatabase(
	name tree.Name, p scbuildstmt.ResolveParams,
) scbuildstmt.ElementResultSet {
	db := b.cr.MayResolveDatabase(b.ctx, name)
	if db == nil {
		if p.IsExistenceOptional {
			return nil
		}
		if string(name) == "" {
			panic(pgerror.New(pgcode.Syntax, "empty database name"))
		}
		panic(sqlerrors.NewUndefinedDatabaseError(name.String()))
	}
	b.ensureDescriptor(db.GetID())
	b.requirePrivilege(db.GetID(), p.RequiredPrivilege)
	return b.QueryByID(db.GetID())
}

// ResolveSchema implements the scbuildstmt.NameResolver interface.
func (b *builderState) ResolveSchema(
	name tree.ObjectNamePrefix, p scbuildstmt.ResolveParams,
) scbuildstmt.ElementResultSet {
	_, sc := b.cr.MayResolveSchema(b.ctx, name, p.WithOffline)
	if sc == nil {
		if p.IsExistenceOptional {
			return nil
		}
		panic(sqlerrors.NewUndefinedSchemaError(name.Schema()))
	}
	b.checkOwnershipOrPrivilegesOnSchemaDesc(name, sc, p)
	return b.QueryByID(sc.GetID())
}

// ResolveTargetObject implements the scbuildstmt.NameResolver interface.
func (b *builderState) ResolveTargetObject(
	name *tree.UnresolvedObjectName, requiredSchemaPriv privilege.Kind,
) (dbElts scbuildstmt.ElementResultSet, scElts scbuildstmt.ElementResultSet) {
	objName := name.ToTableName()
	db, sc := b.cr.MayResolvePrefix(b.ctx, objName.ObjectNamePrefix)
	// If the database or schema are not found during resolution,
	// then generate an appropriate error.
	if db == nil || sc == nil {
		if !objName.ExplicitSchema && !objName.ExplicitCatalog {
			panic(
				pgerror.New(pgcode.InvalidName, "no database specified"),
			)
		}
		panic(errors.WithHint(pgerror.Newf(pgcode.InvalidSchemaName,
			"cannot create %q because the target database or schema does not exist",
			tree.ErrString(&objName)),
			"verify that the current database and search_path are valid and/or the target database exists"))
	}
	b.ensureDescriptor(db.GetID())
	if sc.SchemaKind() == catalog.SchemaVirtual {
		panic(sqlerrors.NewCannotModifyVirtualSchemaError(sc.GetName()))
	}
	if sc.SchemaKind() == catalog.SchemaTemporary {
		panic(unimplemented.NewWithIssue(104687, "cannot create UDFs under a temporary schema"))
	}
	b.ensureDescriptor(sc.GetID())
	b.checkOwnershipOrPrivilegesOnSchemaDesc(objName.ObjectNamePrefix, sc, scbuildstmt.ResolveParams{RequiredPrivilege: requiredSchemaPriv})
	return b.QueryByID(db.GetID()), b.QueryByID(sc.GetID())
}

// checkOwnershipOrPrivilegesOnSchemaDesc checks ownership or privileges
// specified in `p` of current user on schema descriptor `sc`.
//
// Note: Virtual schemas and temporary schemas are descriptor-less concepts
// and will cause a panic.
func (b *builderState) checkOwnershipOrPrivilegesOnSchemaDesc(
	name tree.ObjectNamePrefix, sc catalog.SchemaDescriptor, p scbuildstmt.ResolveParams,
) {
	switch sc.SchemaKind() {
	case catalog.SchemaTemporary:
		// Nothing needs to be done.
	case catalog.SchemaPublic, catalog.SchemaVirtual:
		if p.RequireOwnership {
			if ok, err := b.auth.HasOwnership(b.ctx, sc); err != nil {
				panic(err)
			} else if !ok {
				panic(pgerror.Newf(pgcode.InsufficientPrivilege,
					"must be owner of schema %s", tree.Name(name.Schema())))
			}
		} else {
			if err := b.auth.CheckPrivilege(b.ctx, sc, p.RequiredPrivilege); err != nil {
				panic(err)
			}
		}
	case catalog.SchemaUserDefined:
		b.ensureDescriptor(sc.GetID())
		if p.RequireOwnership {
			b.mustOwn(sc.GetID())
		} else {
			b.requirePrivilege(sc.GetID(), p.RequiredPrivilege)
		}
	default:
		panic(errors.AssertionFailedf("unknown schema kind %d", sc.SchemaKind()))
	}
}

// ResolveUserDefinedTypeType implements the scbuildstmt.NameResolver interface.
func (b *builderState) ResolveUserDefinedTypeType(
	name *tree.UnresolvedObjectName, p scbuildstmt.ResolveParams,
) scbuildstmt.ElementResultSet {
	p.ResolveTypes = true
	prefix, typ := b.cr.MayResolveType(b.ctx, *name)
	if typ == nil {
		if p.IsExistenceOptional {
			return nil
		}
		panic(sqlerrors.NewUndefinedTypeError(name))
	}
	switch typ.GetKind() {
	case descpb.TypeDescriptor_ALIAS:
		// The implicit array types are not directly modifiable.
		panic(pgerror.Newf(pgcode.DependentObjectsStillExist,
			"%q is an implicit array type and cannot be modified", typ.GetName()))
	case descpb.TypeDescriptor_MULTIREGION_ENUM:
		typeName := tree.MakeTypeNameWithPrefix(prefix.NamePrefix(), typ.GetName())
		// Multi-region enums are not directly modifiable.
		panic(errors.WithHintf(pgerror.Newf(pgcode.DependentObjectsStillExist,
			"%q is a multi-region enum and cannot be modified directly", typeName.FQString()),
			"try ALTER DATABASE %s DROP REGION %s", prefix.Database.GetName(), typ.GetName()))
	case descpb.TypeDescriptor_ENUM:
		b.ensureDescriptor(typ.GetID())
		b.mustOwn(typ.GetID())
	case descpb.TypeDescriptor_COMPOSITE:
		b.ensureDescriptor(typ.GetID())
		b.mustOwn(typ.GetID())
	case descpb.TypeDescriptor_TABLE_IMPLICIT_RECORD_TYPE:
		// Implicit record types are not directly modifiable.
		panic(pgerror.Newf(pgcode.DependentObjectsStillExist,
			"cannot modify table record type %q", typ.GetName()))
	default:
		panic(errors.AssertionFailedf("unknown type kind %s", typ.GetKind()))
	}
	if p.RequireOwnership {
		b.mustOwn(typ.GetID())
	}
	return b.QueryByID(typ.GetID())
}

// ResolveRelation implements the scbuildstmt.NameResolver interface.
func (b *builderState) ResolveRelation(
	name *tree.UnresolvedObjectName, p scbuildstmt.ResolveParams,
) scbuildstmt.ElementResultSet {
	c := b.resolveRelation(name, p)
	if c == nil {
		return nil
	}
	if p.RequireOwnership {
		b.mustOwn(c.desc.GetID())
	}
	return b.QueryByID(c.desc.GetID())
}

func (b *builderState) resolveRelation(
	name *tree.UnresolvedObjectName, p scbuildstmt.ResolveParams,
) *cachedDesc {
	var prefix catalog.ResolvedObjectPrefix
	var rel catalog.Descriptor
	prefix, rel = b.cr.MayResolveTable(b.ctx, *name)
	if rel == nil {
		if p.ResolveTypes {
			prefix, rel = b.cr.MayResolveType(b.ctx, *name)
		}
		if rel == nil {
			if p.IsExistenceOptional {
				return nil
			}
			panic(sqlerrors.NewUndefinedRelationError(name))
		}
	}
	if t, isTable := rel.(catalog.TableDescriptor); isTable {
		if t.IsVirtualTable() {
			if prefix.Schema.GetName() == catconstants.PgCatalogName {
				panic(pgerror.Newf(pgcode.InsufficientPrivilege,
					"%s is a system catalog", tree.ErrNameString(rel.GetName())))
			}
			panic(pgerror.Newf(pgcode.WrongObjectType,
				"%s is a virtual object and cannot be modified", tree.ErrNameString(rel.GetName())))
		}
		if t.IsTemporary() {
			panic(scerrors.NotImplementedErrorf(nil /* n */, "dropping a temporary table"))
		}
	} else if typ, isType := rel.(catalog.TypeDescriptor); isType {
		if typ.GetKind() == descpb.TypeDescriptor_ALIAS && typ.GetID() == descpb.InvalidID {
			// This case handles the types in types.PublicSchemaAliases -- BOX2D,
			// GEOGRAPHY, and GEOMETRY.
			panic(pgerror.Newf(pgcode.WrongObjectType,
				"%s is a built-in type and cannot be modified", tree.ErrNameString(typ.GetName())))
		}
	}

	// If we own the schema then we can manipulate the underlying relation,
	// regardless what privilege is required on relation.
	b.ensureDescriptor(rel.GetID())
	c := b.descCache[rel.GetID()]
	b.ensureDescriptor(rel.GetParentSchemaID())
	if b.descCache[rel.GetParentSchemaID()].hasOwnership {
		c.hasOwnership = true
		return c
	}

	err, found := c.privileges[p.RequiredPrivilege]
	if !found {
		// Validate if this descriptor can be resolved under the current schema.
		b.requirePrivilege(rel.GetParentSchemaID(), privilege.USAGE)
		err = b.auth.CheckPrivilege(b.ctx, rel, p.RequiredPrivilege)
		c.privileges[p.RequiredPrivilege] = err
	}
	if err == nil {
		return c
	}
	if p.RequiredPrivilege != privilege.CREATE {
		panic(err)
	}
	relationType := "relation"
	if t, isTable := rel.(catalog.TableDescriptor); isTable {
		relationType = "table"
		if t.IsView() {
			relationType = "view"
		} else if t.IsSequence() {
			relationType = "sequence"
		}
	} else if _, isType := rel.(catalog.TypeDescriptor); isType {
		relationType = "type"
	}
	panic(pgerror.Newf(pgcode.InsufficientPrivilege,
		"must be owner of %s %s or have %s privilege on %s %s",
		relationType,
		tree.Name(rel.GetName()),
		p.RequiredPrivilege,
		relationType,
		tree.Name(rel.GetName())))
}

// ResolveTable implements the scbuildstmt.NameResolver interface.
func (b *builderState) ResolveTable(
	name *tree.UnresolvedObjectName, p scbuildstmt.ResolveParams,
) scbuildstmt.ElementResultSet {
	c := b.resolveRelation(name, p)
	if c == nil {
		return nil
	}
	if rel, ok := c.desc.(catalog.TableDescriptor); !ok || !rel.IsTable() {
		panic(pgerror.Newf(pgcode.WrongObjectType, "%q is not a table", c.desc.GetName()))
	}
	if p.RequireOwnership {
		b.mustOwn(c.desc.GetID())
	}
	return b.QueryByID(c.desc.GetID())
}

// ResolvePhysicalTable implements the scbuildstmt.NameResolver interface.
func (b *builderState) ResolvePhysicalTable(
	name *tree.UnresolvedObjectName, p scbuildstmt.ResolveParams,
) scbuildstmt.ElementResultSet {
	c := b.resolveRelation(name, p)
	if c == nil {
		return nil
	}
	if rel, ok := c.desc.(catalog.TableDescriptor); !ok || !rel.IsPhysicalTable() {
		panic(pgerror.Newf(pgcode.WrongObjectType, "%q is not a table, view, or sequence", c.desc.GetName()))
	}
	return b.QueryByID(c.desc.GetID())
}

// ResolveSequence implements the scbuildstmt.NameResolver interface.
func (b *builderState) ResolveSequence(
	name *tree.UnresolvedObjectName, p scbuildstmt.ResolveParams,
) scbuildstmt.ElementResultSet {
	c := b.resolveRelation(name, p)
	if c == nil {
		return nil
	}
	if rel, ok := c.desc.(catalog.TableDescriptor); !ok || !rel.IsSequence() {
		panic(pgerror.Newf(pgcode.WrongObjectType, "%q is not a sequence", c.desc.GetName()))
	}
	if p.RequireOwnership {
		b.mustOwn(c.desc.GetID())
	}
	return b.QueryByID(c.desc.GetID())
}

// ResolveView implements the scbuildstmt.NameResolver interface.
func (b *builderState) ResolveView(
	name *tree.UnresolvedObjectName, p scbuildstmt.ResolveParams,
) scbuildstmt.ElementResultSet {
	c := b.resolveRelation(name, p)
	if c == nil {
		return nil
	}
	if rel, ok := c.desc.(catalog.TableDescriptor); !ok || !rel.IsView() {
		panic(pgerror.Newf(pgcode.WrongObjectType, "%q is not a view", c.desc.GetName()))
	}
	if p.RequireOwnership {
		b.mustOwn(c.desc.GetID())
	}
	return b.QueryByID(c.desc.GetID())
}

// ResolveIndex implements the scbuildstmt.NameResolver interface.
func (b *builderState) ResolveIndex(
	relationID catid.DescID, indexName tree.Name, p scbuildstmt.ResolveParams,
) scbuildstmt.ElementResultSet {
	b.ensureDescriptor(relationID)
	rel := b.descCache[relationID].desc.(catalog.TableDescriptor)
	if !rel.IsPhysicalTable() || rel.IsSequence() {
		panic(pgerror.Newf(pgcode.WrongObjectType,
			"%q is not an indexable table or a materialized view", rel.GetName()))
	}
	b.requirePrivilege(rel.GetID(), p.RequiredPrivilege)
	elts := b.QueryByID(rel.GetID())
	var indexID catid.IndexID
	scpb.ForEachIndexName(elts, func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.IndexName) {
		if tree.Name(e.Name) == indexName {
			indexID = e.IndexID
		}
	})
	if indexID == 0 && (indexName == "" || indexName == tabledesc.LegacyPrimaryKeyIndexName) {
		indexID = rel.GetPrimaryIndexID()
	}
	if indexID == 0 {
		if p.IsExistenceOptional {
			return nil
		}
		panic(pgerror.Newf(pgcode.UndefinedObject,
			"index %q not found in relation %q", indexName, rel.GetName()))
	}
	return elts.Filter(func(_ scpb.Status, _ scpb.TargetStatus, element scpb.Element) bool {
		idI, _ := screl.Schema.GetAttribute(screl.IndexID, element)
		return idI != nil && idI.(catid.IndexID) == indexID
	})
}

// ResolveIndexByName implements the scbuildstmt.NameResolver interface.
func (b *builderState) ResolveIndexByName(
	tableIndexName *tree.TableIndexName, p scbuildstmt.ResolveParams,
) scbuildstmt.ElementResultSet {
	// If a table name is specified, confirm it is not a system table first.
	if tableIndexName.Table.ObjectName != "" {
		un := tableIndexName.Table.ToUnresolvedObjectName()
		desc := b.resolveRelation(un, p)
		if desc == nil {
			return nil
		}
		tableDesc, ok := desc.desc.(catalog.TableDescriptor)
		if !ok {
			panic(pgerror.Newf(pgcode.WrongObjectType, "%q is not a table", un))
		}
		b.ensureDescriptor(tableDesc.GetParentSchemaID())
		if catalog.IsSystemDescriptor(tableDesc) {
			schemaDesc := b.descCache[tableDesc.GetParentSchemaID()].desc
			panic(catalog.NewMutableAccessToDescriptorlessSchemaError(schemaDesc.(catalog.SchemaDescriptor)))
		}
	}

	found, prefix, tbl, idx := b.cr.MayResolveIndex(b.ctx, *tableIndexName)
	if !found {
		if !p.IsExistenceOptional {
			panic(pgerror.Newf(pgcode.UndefinedObject, "index %q does not exist", tableIndexName.Index))
		}
		return nil
	}
	tableIndexName.Table.CatalogName = tree.Name(prefix.Database.GetName())
	tableIndexName.Table.SchemaName = tree.Name(prefix.Schema.GetName())
	tableIndexName.Table.ObjectName = tree.Name(tbl.GetName())
	return b.ResolveIndex(tbl.GetID(), tree.Name(idx.GetName()), p)
}

// ResolveColumn implements the scbuildstmt.NameResolver interface.
// N.B. Column target statuses should be handled outside of this logic (ex. resolving a column by name that is in the
// dropping state shouldn't prevent a column of the same name being added).
func (b *builderState) ResolveColumn(
	relationID catid.DescID, columnName tree.Name, p scbuildstmt.ResolveParams,
) scbuildstmt.ElementResultSet {
	b.ensureDescriptor(relationID)
	rel := b.descCache[relationID].desc.(catalog.TableDescriptor)
	elts := b.QueryByID(rel.GetID())
	b.requirePrivilege(rel.GetID(), p.RequiredPrivilege)
	var columnID catid.ColumnID
	scpb.ForEachColumnName(elts, func(status scpb.Status, _ scpb.TargetStatus, e *scpb.ColumnName) {
		if tree.Name(e.Name) == columnName {
			columnID = e.ColumnID
		}
	})
	if columnID == 0 {
		if p.IsExistenceOptional {
			return nil
		}
		panic(colinfo.NewUndefinedColumnError(string(columnName)))
	}
	return elts.Filter(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) bool {
		idI, _ := screl.Schema.GetAttribute(screl.ColumnID, e)
		return idI != nil && idI.(catid.ColumnID) == columnID
	})
}

// ResolveConstraint implements the scbuildstmt.NameResolver interface.
func (b *builderState) ResolveConstraint(
	relationID catid.DescID, constraintName tree.Name, p scbuildstmt.ResolveParams,
) scbuildstmt.ElementResultSet {
	b.ensureDescriptor(relationID)
	rel := b.descCache[relationID].desc.(catalog.TableDescriptor)
	elts := b.QueryByID(rel.GetID())
	var constraintID catid.ConstraintID
	scpb.ForEachConstraintWithoutIndexName(elts,
		func(status scpb.Status, _ scpb.TargetStatus, e *scpb.ConstraintWithoutIndexName) {
			if tree.Name(e.Name) == constraintName {
				constraintID = e.ConstraintID
			}
		},
	)

	if constraintID == 0 {
		var indexID catid.IndexID
		scpb.ForEachIndexName(elts, func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.IndexName) {
			if tree.Name(e.Name) == constraintName {
				indexID = e.IndexID
			}
		})
		if indexID != 0 {
			scpb.ForEachPrimaryIndex(elts,
				func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.PrimaryIndex) {
					if e.IndexID == indexID {
						constraintID = e.ConstraintID
					}
				},
			)
			scpb.ForEachSecondaryIndex(elts,
				func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.SecondaryIndex) {
					if e.IndexID == indexID && e.IsUnique {
						constraintID = e.ConstraintID
					}
				},
			)
		}
	}

	if constraintID == 0 {
		if p.IsExistenceOptional {
			return nil
		}
		panic(sqlerrors.NewUndefinedConstraintError(string(constraintName), rel.GetName()))
	}

	return elts.Filter(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) bool {
		idI, _ := screl.Schema.GetAttribute(screl.ConstraintID, e)
		return idI != nil && idI.(catid.ConstraintID) == constraintID
	})
}

func (b *builderState) ResolveRoutine(
	routineObj *tree.RoutineObj, p scbuildstmt.ResolveParams, routineType tree.RoutineType,
) scbuildstmt.ElementResultSet {
	name := routineObj.FuncName.ToUnresolvedObjectName().ToUnresolvedName()
	// TODO(mgartner): We can probably make the distinction between the two
	// types of unresolved routine names at parsing-time (or shortly after),
	// rather than here. Ideally, the UnresolvedRoutineName interface can be
	// incorporated with ResolvableFunctionReference.
	var routineName tree.UnresolvedRoutineName
	if routineType == tree.ProcedureRoutine {
		routineName = tree.MakeUnresolvedProcedureName(name)
	} else {
		routineName = tree.MakeUnresolvedFunctionName(name)
	}
	fd, err := b.cr.ResolveFunction(b.ctx, routineName, b.semaCtx.SearchPath)
	if err != nil {
		if p.IsExistenceOptional && errors.Is(err, tree.ErrRoutineUndefined) {
			return nil
		}
		panic(err)
	}

	ol, err := fd.MatchOverload(
		b.ctx, b.cr, routineObj, b.semaCtx.SearchPath,
		routineType, p.InDropContext, false, /* tryDefaultExprs */
	)
	if err != nil {
		if p.IsExistenceOptional && errors.Is(err, tree.ErrRoutineUndefined) {
			return nil
		}
		panic(err)
	}

	// ResolveRoutine is not concerned with builtin functions that are defined using
	// a SQL string, so we don't check ol.HasSQLBody() here.
	if ol.Type == tree.BuiltinRoutine {
		panic(
			errors.Errorf(
				"cannot perform schema change on function %s%s because it is required by the database system",
				routineObj.FuncName.Object(), ol.Signature(true),
			),
		)
	}

	fnID := funcdesc.UserDefinedFunctionOIDToID(ol.Oid)
	if p.RequireOwnership {
		b.mustOwn(fnID)
	}
	b.ensureDescriptor(fnID)
	return b.QueryByID(fnID)
}

func (b *builderState) ResolveTrigger(
	relationID catid.DescID, triggerName tree.Name, p scbuildstmt.ResolveParams,
) scbuildstmt.ElementResultSet {
	b.ensureDescriptor(relationID)
	rel := b.descCache[relationID].desc.(catalog.TableDescriptor)
	elts := b.QueryByID(rel.GetID())
	var triggerID catid.TriggerID
	elts.ForEach(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
		if t, ok := e.(*scpb.TriggerName); ok && t.Name == string(triggerName) {
			triggerID = t.TriggerID
		}
	})
	if triggerID == 0 {
		if p.IsExistenceOptional {
			return nil
		}
		panic(sqlerrors.NewUndefinedTriggerError(string(triggerName), rel.GetName()))
	}
	return elts.Filter(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) bool {
		id, _ := screl.Schema.GetAttribute(screl.TriggerID, e)
		return id != nil && id.(catid.TriggerID) == triggerID
	})
}

func (b *builderState) ResolvePolicy(
	tableID catid.DescID, policyName tree.Name, p scbuildstmt.ResolveParams,
) scbuildstmt.ElementResultSet {
	b.ensureDescriptor(tableID)
	tbl := b.descCache[tableID].desc.(catalog.TableDescriptor)
	elems := b.QueryByID(tbl.GetID())
	var policyID catid.PolicyID
	elems.ForEach(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
		if t, ok := e.(*scpb.PolicyName); ok && t.Name == string(policyName) {
			policyID = t.PolicyID
		}
	})
	if policyID == 0 {
		if p.IsExistenceOptional {
			return nil
		}
		panic(sqlerrors.NewUndefinedPolicyError(string(policyName), tbl.GetName()))
	}
	return elems.Filter(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) bool {
		id, _ := screl.Schema.GetAttribute(screl.PolicyID, e)
		return id != nil && id.(catid.PolicyID) == policyID
	})
}

func (b *builderState) newCachedDesc(id descpb.ID) *cachedDesc {
	return &cachedDesc{
		desc:            b.readDescriptor(id),
		privileges:      make(map[privilege.Kind]error),
		hasOwnership:    b.hasAdmin,
		elementIndexMap: map[string]int{},
	}
}

func (b *builderState) newCachedDescForNewDesc() *cachedDesc {
	return &cachedDesc{
		privileges:      make(map[privilege.Kind]error),
		hasOwnership:    true,
		elementIndexMap: map[string]int{},
	}
}

// resolveBackReferences resolves any back references that will take
// additional look ups. This type of scan operation is expensive, so
// limit to when its actually needed.
func (b *builderState) resolveBackReferences(c *cachedDesc) {
	// Already resolved, no need for extra work.
	if c.backrefsResolved {
		return
	}
	// Name prefix and namespace lookups.
	switch d := c.desc.(type) {
	case catalog.DatabaseDescriptor:
		if !d.HasPublicSchemaWithDescriptor() {
			panic(scerrors.NotImplementedErrorf(nil, /* n */
				redact.Sprintf(
					"database %q (%d) with a descriptorless public schema",
					d.GetName(), d.GetID()),
			))
		}
		// Handle special case of database children, which may include temporary
		// schemas, which aren't explicitly referenced in the database's schemas
		// map.
		childSchemas := b.cr.GetAllSchemasInDatabase(b.ctx, d)
		_ = childSchemas.ForEachDescriptor(func(desc catalog.Descriptor) error {
			sc, err := catalog.AsSchemaDescriptor(desc)
			if err != nil {
				panic(err)
			}
			switch sc.SchemaKind() {
			case catalog.SchemaVirtual:
				return nil
			case catalog.SchemaTemporary:
				b.tempSchemas[sc.GetID()] = sc
			}
			c.backrefs.Add(sc.GetID())
			return nil
		})
	case catalog.SchemaDescriptor:
		b.ensureDescriptor(c.desc.GetParentID())
		db := b.descCache[c.desc.GetParentID()].desc
		// Handle special case of schema children, which have to be added to
		// the back-referenced ID set but which aren't explicitly referenced in
		// the schema descriptor itself.
		objects := b.cr.GetAllObjectsInSchema(b.ctx, db.(catalog.DatabaseDescriptor), d)
		_ = objects.ForEachDescriptor(func(desc catalog.Descriptor) error {
			c.backrefs.Add(desc.GetID())
			return nil
		})
	default:
		// These are always done
	}
	c.backrefsResolved = true
}

// ensureDescriptor ensures descriptor `id` is "tracked" inside the builder state,
// as a *cacheDesc.
// For newly created descriptors, it merely creates a zero-valued *cacheDesc entry;
// For existing descriptors, it creates and populate the *cacheDesc entry and
// decompose it into elements (as tracked in b.output).
// For descriptorless IDs associated with named ranges, it creates and populates
// a descriptorless *cacheDesc entry -- decomposing it into NamedRangeZoneConfig
// elements (as tracked in b.output).
func (b *builderState) ensureDescriptor(id catid.DescID) {
	if _, found := b.descCache[id]; found {
		return
	}
	if b.newDescriptors.Contains(id) {
		b.descCache[id] = b.newCachedDescForNewDesc()
		return
	}
	// For pseudo-table IDs used in named ranges, we take precaution to avoid
	// reading the descriptor (readDescriptor) as these IDs are descriptor-less.
	_, ok := zonepb.NamedZonesByID[uint32(id)]
	if ok {
		b.ensurePseudoDescriptor(id)
		return
	}

	c := b.newCachedDesc(id)
	// Collect privileges
	if !c.hasOwnership {
		var err error
		c.hasOwnership, err = b.auth.HasOwnership(b.ctx, c.desc)
		if err != nil {
			panic(err)
		}
	}
	// Collect backrefs and elements.
	b.descCache[id] = c
	crossRefLookupFn := func(id catid.DescID) catalog.Descriptor {
		return b.readDescriptor(id)
	}
	visitorFn := func(status scpb.Status, e scpb.Element) {
		b.addNewElementState(elementState{
			element: e,
			initial: status,
			current: status,
			target:  scpb.AsTargetStatus(status),
		})
	}

	c.backrefs = scdecomp.WalkDescriptor(b.ctx, c.desc, crossRefLookupFn, visitorFn,
		b.commentGetter, b.zoneConfigReader, b.evalCtx.Settings.Version.ActiveVersion(b.ctx))
	// Name prefix and namespace lookups.
	switch c.desc.(type) {
	case catalog.DatabaseDescriptor:
		// Nothing to do here.
	case catalog.SchemaDescriptor:
		b.ensureDescriptor(c.desc.GetParentID())
		db := b.descCache[c.desc.GetParentID()].desc
		c.prefix.CatalogName = tree.Name(db.GetName())
		c.prefix.ExplicitCatalog = true
	default:
		b.ensureDescriptor(c.desc.GetParentID())
		db := b.descCache[c.desc.GetParentID()].desc
		c.prefix.CatalogName = tree.Name(db.GetName())
		c.prefix.ExplicitCatalog = true
		b.ensureDescriptor(c.desc.GetParentSchemaID())
		sc := b.descCache[c.desc.GetParentSchemaID()].desc
		c.prefix.SchemaName = tree.Name(sc.GetName())
		c.prefix.ExplicitSchema = true
	}
}

func (b *builderState) ensurePseudoDescriptor(id catid.DescID) {
	crossRefLookupFn := func(id catid.DescID) catalog.Descriptor {
		return nil
	}
	visitorFn := func(status scpb.Status, e scpb.Element) {
		b.addNewElementState(elementState{
			element: e,
			initial: status,
			current: status,
			target:  scpb.AsTargetStatus(status),
		})
	}
	b.descCache[id] = b.newCachedDesc(id)
	scdecomp.WalkNamedRanges(b.ctx, nil, crossRefLookupFn, visitorFn,
		b.commentGetter, b.zoneConfigReader, b.evalCtx.Settings.Version.ActiveVersion(b.ctx), id)
}

func (b *builderState) readDescriptor(id catid.DescID) catalog.Descriptor {
	_, ok := zonepb.NamedZonesByID[uint32(id)]
	if ok {
		return nil
	}
	if id == catid.InvalidDescID {
		panic(errors.AssertionFailedf("invalid descriptor ID %d", id))
	}
	if id == keys.SystemPublicSchemaID || id == keys.PublicSchemaIDForBackup || id == keys.PublicSchemaID {
		panic(scerrors.NotImplementedErrorf(nil /* n */, redact.Sprintf("descriptorless public schema %d", id)))
	}
	if tempSchema := b.tempSchemas[id]; tempSchema != nil {
		return tempSchema
	}
	return b.cr.MustReadDescriptor(b.ctx, id)
}

func (b *builderState) GenerateUniqueDescID() catid.DescID {
	id, err := b.evalCtx.DescIDGenerator.GenerateUniqueDescID(b.ctx)
	if err != nil {
		panic(err)
	}
	b.newDescriptors.Add(id)
	return id
}

func (b *builderState) BuildReferenceProvider(stmt tree.Statement) scbuildstmt.ReferenceProvider {
	provider, err := b.referenceProviderFactory.NewReferenceProvider(b.ctx, stmt)
	if err != nil {
		panic(err)
	}
	return provider
}

func (b *builderState) BuildUserPrivilegesFromDefaultPrivileges(
	db *scpb.Database,
	sc *scpb.Schema,
	descID descpb.ID,
	objType privilege.TargetObjectType,
	owner username.SQLUsername,
) (*scpb.Owner, []*scpb.UserPrivileges) {
	b.ensureDescriptor(db.DatabaseID)
	dbDesc, err := catalog.AsDatabaseDescriptor(b.descCache[db.DatabaseID].desc)
	if err != nil {
		panic(err)
	}
	dbDefaultPrivDesc := dbDesc.GetDefaultPrivilegeDescriptor()

	var scDefaultPrivDesc catalog.DefaultPrivilegeDescriptor
	if sc != nil && !sc.IsTemporary {
		b.ensureDescriptor(sc.SchemaID)
		scDesc, err := catalog.AsSchemaDescriptor(b.descCache[sc.SchemaID].desc)
		if err != nil {
			panic(err)
		}
		scDefaultPrivDesc = scDesc.GetDefaultPrivilegeDescriptor()
	}

	pd, err := catprivilege.CreatePrivilegesFromDefaultPrivileges(
		dbDefaultPrivDesc,
		scDefaultPrivDesc,
		db.DatabaseID,
		b.CurrentUser(),
		objType,
	)
	if err != nil {
		panic(err)
	}
	pd.SetOwner(owner)

	ownerElem := &scpb.Owner{
		DescriptorID: descID,
		Owner:        pd.Owner().Normalized(),
	}

	upsElems := make([]*scpb.UserPrivileges, 0, len(pd.Users))
	for _, up := range pd.Users {
		upsElems = append(upsElems, &scpb.UserPrivileges{
			DescriptorID:    descID,
			UserName:        up.User().Normalized(),
			Privileges:      up.Privileges,
			WithGrantOption: up.WithGrantOption,
		})
	}
	sort.Slice(upsElems, func(i, j int) bool {
		return upsElems[i].UserName < upsElems[j].UserName
	})
	return ownerElem, upsElems
}

func (b *builderState) WrapFunctionBody(
	fnID descpb.ID,
	bodyStr string,
	lang catpb.Function_Language,
	returnType tree.ResolvableTypeReference,
	refProvider scbuildstmt.ReferenceProvider,
) *scpb.FunctionBody {
	// Trigger functions do not analyze SQL statements beyond parsing, so type and
	// sequence names should not be replaced during trigger-function creation.
	var lazilyEvalSQL bool
	if returnType != nil {
		if typ, ok := returnType.(*types.T); ok && typ.Identical(types.Trigger) {
			lazilyEvalSQL = true
		}
	}
	if !lazilyEvalSQL {
		bodyStr = b.replaceSeqNamesWithIDs(bodyStr, lang)
		bodyStr = b.serializeUserDefinedTypes(bodyStr, lang)
	}
	fnBody := &scpb.FunctionBody{
		FunctionID: fnID,
		Body:       bodyStr,
		Lang:       catpb.FunctionLanguage{Lang: lang},
	}
	if err := refProvider.ForEachTableReference(func(tblID descpb.ID, idxID descpb.IndexID, colIDs descpb.ColumnIDs) error {
		fnBody.UsesTables = append(
			fnBody.UsesTables,
			scpb.FunctionBody_TableReference{
				TableID:   tblID,
				ColumnIDs: colIDs,
				IndexID:   idxID,
			},
		)
		return nil
	}); err != nil {
		panic(err)
	}

	if err := refProvider.ForEachViewReference(func(viewID descpb.ID, colIDs descpb.ColumnIDs) error {
		fnBody.UsesViews = append(
			fnBody.UsesViews,
			scpb.FunctionBody_ViewReference{
				ViewID:    viewID,
				ColumnIDs: colIDs,
			},
		)
		return nil
	}); err != nil {
		panic(err)
	}

	fnBody.UsesFunctionIDs = refProvider.ReferencedRoutines().Ordered()
	fnBody.UsesSequenceIDs = refProvider.ReferencedSequences().Ordered()
	fnBody.UsesTypeIDs = refProvider.ReferencedTypes().Ordered()
	return fnBody
}

func (b *builderState) ReplaceSeqTypeNamesInStatements(
	queryStr string, lang catpb.Function_Language,
) string {
	newQueryStr := b.replaceSeqNamesWithIDs(queryStr, lang)
	return b.serializeUserDefinedTypes(newQueryStr, lang)
}

func (b *builderState) replaceSeqNamesWithIDs(
	queryStr string, lang catpb.Function_Language,
) string {
	replaceSeqFunc := func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		seqIdentifiers, err := seqexpr.GetUsedSequences(expr)
		if err != nil {
			return false, expr, err
		}
		seqNameToID := make(map[string]descpb.ID)
		for _, seqIdentifier := range seqIdentifiers {
			if seqIdentifier.IsByID() {
				continue
			}
			seq := b.getSequenceFromName(seqIdentifier.SeqName)
			seqNameToID[seqIdentifier.SeqName] = seq.SequenceID
		}
		newExpr, err = seqexpr.ReplaceSequenceNamesWithIDs(expr, seqNameToID)
		if err != nil {
			return false, expr, err
		}
		return false, newExpr, nil
	}

	fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
	switch lang {
	case catpb.Function_SQL:
		parsedStmts, err := parser.Parse(queryStr)
		if err != nil {
			panic(err)
		}
		stmts := make(tree.Statements, len(parsedStmts))
		for i, s := range parsedStmts {
			stmts[i] = s.AST
		}
		for i, stmt := range stmts {
			newStmt, err := tree.SimpleStmtVisit(stmt, replaceSeqFunc)
			if err != nil {
				panic(err)
			}
			if i > 0 {
				fmtCtx.WriteString("\n")
			}
			fmtCtx.FormatNode(newStmt)
			fmtCtx.WriteString(";")
		}
	case catpb.Function_PLPGSQL:
		var stmts plpgsqltree.Statement
		plstmt, err := plpgsql.Parse(queryStr)
		if err != nil {
			panic(errors.Wrap(err, "failed to parse query string"))
		}
		stmts = plstmt.AST

		v := plpgsqltree.SQLStmtVisitor{Fn: replaceSeqFunc}
		newStmt := plpgsqltree.Walk(&v, stmts)
		fmtCtx.FormatNode(newStmt)
	default:
		panic(errors.AssertionFailedf("unexpected function language: %s", lang))
	}
	return fmtCtx.String()
}

func (b *builderState) getSequenceFromName(seqName string) *scpb.Sequence {
	parsedName, err := parser.ParseTableName(seqName)
	if err != nil {
		panic(err)
	}
	elts := b.ResolveSequence(parsedName, scbuildstmt.ResolveParams{RequiredPrivilege: privilege.SELECT})
	_, _, seq := scpb.FindSequence(elts)
	return seq
}

func (b *builderState) serializeUserDefinedTypes(
	queryStr string, lang catpb.Function_Language,
) string {
	replaceFunc := func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		var innerExpr tree.Expr
		var typRef tree.ResolvableTypeReference
		switch n := expr.(type) {
		case *tree.CastExpr:
			innerExpr = n.Expr
			typRef = n.Type
		case *tree.AnnotateTypeExpr:
			innerExpr = n.Expr
			typRef = n.Type
		default:
			return true, expr, nil
		}
		var typ *types.T
		typ, err = tree.ResolveType(b.ctx, typRef, b.semaCtx.TypeResolver)
		if err != nil {
			return false, expr, err
		}
		if !typ.UserDefined() {
			return true, expr, nil
		}
		{
			// We cannot type-check subqueries without using optbuilder, so we
			// currently do not support casting expressions with subqueries to
			// UDTs.
			defer b.semaCtx.Properties.Restore(b.semaCtx.Properties)
			b.semaCtx.Properties.Require("casts to enums within UDFs", tree.RejectSubqueries)
		}
		texpr, err := innerExpr.TypeCheck(b.ctx, b.semaCtx, typ)
		if err != nil {
			return false, expr, err
		}
		s := tree.Serialize(texpr)
		parsedExpr, err := parser.ParseExpr(s)
		if err != nil {
			return false, expr, err
		}
		return false, parsedExpr, nil
	}
	// replaceTypeFunc is a visitor function that replaces type annotations
	// containing user defined types with their IDs. This is currently only
	// necessary for some kinds of PLpgSQL statements.
	replaceTypeFunc := func(typ tree.ResolvableTypeReference) (newTyp tree.ResolvableTypeReference, err error) {
		if typ == nil {
			return typ, nil
		}
		// semaCtx may be nil if this is a virtual view being created at
		// init time.
		var typeResolver tree.TypeReferenceResolver
		if b.semaCtx != nil {
			typeResolver = b.semaCtx.TypeResolver
		}
		var t *types.T
		t, err = tree.ResolveType(b.ctx, typ, typeResolver)
		if err != nil {
			return typ, err
		}
		if !t.UserDefined() {
			return typ, nil
		}
		return &tree.OIDTypeReference{OID: t.Oid()}, nil
	}

	fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
	switch lang {
	case catpb.Function_SQL:
		var stmts tree.Statements
		parsedStmts, err := parser.Parse(queryStr)
		if err != nil {
			panic(err)
		}
		stmts = make(tree.Statements, len(parsedStmts))
		for i, stmt := range parsedStmts {
			stmts[i] = stmt.AST
		}
		for i, stmt := range stmts {
			newStmt, err := tree.SimpleStmtVisit(stmt, replaceFunc)
			if err != nil {
				panic(err)
			}
			if i > 0 {
				fmtCtx.WriteString("\n")
			}
			fmtCtx.FormatNode(newStmt)
			fmtCtx.WriteString(";")
		}
	case catpb.Function_PLPGSQL:
		var stmts plpgsqltree.Statement
		plstmt, err := plpgsql.Parse(queryStr)
		if err != nil {
			panic(errors.Wrap(err, "failed to parse query string"))
		}
		stmts = plstmt.AST

		v := plpgsqltree.SQLStmtVisitor{Fn: replaceFunc}
		newStmt := plpgsqltree.Walk(&v, stmts)
		// Some PLpgSQL statements (i.e., declarations), may contain type
		// annotations containing the UDT. We need to walk the AST to replace them,
		// too.
		v2 := plpgsqltree.TypeRefVisitor{Fn: replaceTypeFunc}
		newStmt = plpgsqltree.Walk(&v2, newStmt)
		fmtCtx.FormatNode(newStmt)
	default:
		panic(errors.AssertionFailedf("unexpected function language: %s", lang))
	}
	return fmtCtx.CloseAndGetString()
}

func (b *builderState) ResolveDatabasePrefix(schemaPrefix *tree.ObjectNamePrefix) {
	if schemaPrefix.SchemaName == "" || !schemaPrefix.ExplicitSchema {
		panic(pgerror.Newf(pgcode.Syntax, "empty schema name"))
	}
	if schemaPrefix.CatalogName == "" {
		schemaPrefix.CatalogName = tree.Name(b.cr.CurrentDatabase())
		schemaPrefix.ExplicitCatalog = true
	}
}

var _ scpb.ElementCollectionGetter = &builderState{}

func (b *builderState) Get(ordinal int) (_ scpb.Status, _ scpb.TargetStatus, _ scpb.Element) {
	es := &b.output[ordinal]
	return es.current, es.target, es.element
}

func (b *builderState) Size() int {
	return len(b.output)
}
