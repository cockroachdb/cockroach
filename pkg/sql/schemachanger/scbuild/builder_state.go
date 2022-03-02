// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuild

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/seqexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild/internal/scbuildstmt"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdecomp"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

var _ scbuildstmt.BuilderState = (*builderState)(nil)

// QueryByID implements the scbuildstmt.BuilderState interface.
func (b *builderState) QueryByID(id catid.DescID) scbuildstmt.ElementResultSet {
	if id == catid.InvalidDescID {
		return nil
	}
	b.ensureDescriptor(id)
	return b.descCache[id].ers
}

// Ensure implements the scbuildstmt.BuilderState interface.
func (b *builderState) Ensure(
	current scpb.Status, target scpb.TargetStatus, e scpb.Element, meta scpb.TargetMetadata,
) {
	if e == nil {
		panic(errors.AssertionFailedf("cannot define target for nil element"))
	}
	id := screl.GetDescID(e)
	b.ensureDescriptor(id)
	c := b.descCache[id]
	var found bool
	for _, i := range c.ers.indexes {
		es := &b.output[i]
		if !screl.EqualElements(es.element, e) {
			continue
		}
		if found {
			panic(errors.AssertionFailedf("element is not unique: %s", screl.ElementString(es.element)))
		}
		if current != scpb.Status_UNKNOWN {
			es.current = current
		}
		es.target = target
		es.element = e
		es.metadata = meta
		found = true
	}
	if found {
		return
	}
	if current == scpb.Status_UNKNOWN {
		if target == scpb.ToAbsent {
			panic(errors.AssertionFailedf("element not found: %s", screl.ElementString(e)))
		}
		current = scpb.Status_ABSENT
	}
	c.ers.indexes = append(c.ers.indexes, len(b.output))
	b.output = append(b.output, elementState{
		element:  e,
		target:   target,
		current:  current,
		metadata: meta,
	})
}

// ForEachElementStatus implements the scpb.ElementStatusIterator interface.
func (b *builderState) ForEachElementStatus(
	fn func(current scpb.Status, target scpb.TargetStatus, e scpb.Element),
) {
	for _, es := range b.output {
		fn(es.current, es.target, es.element)
	}
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
			"must be owner of %s %s", c.desc.DescriptorType(), c.desc.GetName()))
	}
}

// CheckPrivilege implements the scbuildstmt.PrivilegeChecker interface.
func (b *builderState) CheckPrivilege(e scpb.Element, privilege privilege.Kind) {
	b.checkPrivilege(screl.GetDescID(e), privilege)
}

func (b *builderState) checkPrivilege(id catid.DescID, privilege privilege.Kind) {
	b.ensureDescriptor(id)
	c := b.descCache[id]
	if c.hasOwnership {
		return
	}
	err, found := c.privileges[privilege]
	if !found {
		err = b.auth.CheckPrivilege(b.ctx, c.desc, privilege)
		c.privileges[privilege] = err
	}
	if err != nil {
		panic(err)
	}
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
	scpb.ForEachColumn(b, func(_ scpb.Status, _ scpb.TargetStatus, column *scpb.Column) {
		if column.TableID == table.TableID && column.ColumnID >= ret {
			ret = column.ColumnID + 1
		}
	})
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
	scpb.ForEachColumnFamily(b, func(_ scpb.Status, _ scpb.TargetStatus, cf *scpb.ColumnFamily) {
		if cf.TableID == table.TableID && cf.FamilyID >= ret {
			ret = cf.FamilyID + 1
		}
	})
	return ret
}

// NextTableIndexID implements the scbuildstmt.TableHelpers interface.
func (b *builderState) NextTableIndexID(table *scpb.Table) (ret catid.IndexID) {
	return b.nextIndexID(table.TableID)
}

// NextViewIndexID implements the scbuildstmt.TableHelpers interface.
func (b *builderState) NextViewIndexID(view *scpb.View) (ret catid.IndexID) {
	if !view.IsMaterialized {
		panic(errors.AssertionFailedf("expected materialized view: %s", screl.ElementString(view)))
	}
	return b.nextIndexID(view.ViewID)
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
	scpb.ForEachPrimaryIndex(b, func(_ scpb.Status, _ scpb.TargetStatus, index *scpb.PrimaryIndex) {
		if index.TableID == id && index.IndexID >= ret {
			ret = index.IndexID + 1
		}
	})
	scpb.ForEachSecondaryIndex(b, func(_ scpb.Status, _ scpb.TargetStatus, index *scpb.SecondaryIndex) {
		if index.TableID == id && index.IndexID >= ret {
			ret = index.IndexID + 1
		}
	})
	return ret
}

// SecondaryIndexPartitioningDescriptor implements the scbuildstmt.TableHelpers
// interface.
func (b *builderState) SecondaryIndexPartitioningDescriptor(
	index *scpb.SecondaryIndex, partBy *tree.PartitionBy,
) catpb.PartitioningDescriptor {
	b.ensureDescriptor(index.TableID)
	desc := b.descCache[index.TableID].desc
	tbl, ok := desc.(catalog.TableDescriptor)
	if !ok {
		panic(errors.AssertionFailedf("Expected table descriptor for ID %d, instead got %s",
			desc.GetID(), desc.DescriptorType()))
	}
	var oldNumImplicitColumns int
	scpb.ForEachIndexPartitioning(b, func(_ scpb.Status, _ scpb.TargetStatus, p *scpb.IndexPartitioning) {
		if p.TableID != index.TableID || p.IndexID != index.IndexID {
			return
		}
		oldNumImplicitColumns = int(p.PartitioningDescriptor.NumImplicitColumns)
	})
	oldKeyColumnNames := make([]string, len(index.KeyColumnIDs))
	for i, colID := range index.KeyColumnIDs {
		scpb.ForEachColumnName(b, func(_ scpb.Status, _ scpb.TargetStatus, cn *scpb.ColumnName) {
			if cn.TableID != index.TableID && cn.ColumnID != colID {
				return
			}
			oldKeyColumnNames[i] = cn.Name
		})
	}
	var allowedNewColumnNames []tree.Name
	scpb.ForEachColumnName(b, func(current scpb.Status, target scpb.TargetStatus, cn *scpb.ColumnName) {
		if cn.TableID != index.TableID && current != scpb.Status_PUBLIC && target == scpb.ToPublic {
			allowedNewColumnNames = append(allowedNewColumnNames, tree.Name(cn.Name))
		}
	})
	_, ret, err := b.createPartCCL(
		b.ctx,
		b.clusterSettings,
		b.evalCtx,
		tbl.FindColumnWithName,
		oldNumImplicitColumns,
		oldKeyColumnNames,
		partBy,
		allowedNewColumnNames,
		true, /* allowImplicitPartitioning */
	)
	if err != nil {
		panic(err)
	}
	return ret
}

// CheckNoConcurrentSchemaChanges implements the scbuildstmt.TableHelpers
// interface.
func (b *builderState) CheckNoConcurrentSchemaChanges(tbl *scpb.Table) {
	b.ensureDescriptor(tbl.TableID)
	desc, ok := b.descCache[tbl.TableID].desc.(catalog.TableDescriptor)
	if !ok || !catalog.HasConcurrentSchemaChanges(desc) {
		return
	}
	panic(scerrors.ConcurrentSchemaChangeError(desc))
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
	m, err := typedesc.GetTypeDescriptorClosure(t)
	if err != nil {
		panic(err)
	}
	var ids catalog.DescriptorIDSet
	for id := range m {
		ids.Add(id)
	}
	return scpb.TypeT{Type: t, ClosedTypeIDs: ids.Ordered()}
}

// WrapExpression implements the scbuildstmt.TableHelpers interface.
func (b *builderState) WrapExpression(expr tree.Expr) *scpb.Expression {
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
			id, err := typedesc.UserDefinedTypeOIDToID(oid)
			if err != nil {
				panic(err)
			}
			b.ensureDescriptor(id)
			typ, err := catalog.AsTypeDescriptor(b.descCache[id].desc)
			if err != nil {
				panic(err)
			}
			ids, err := typ.GetIDClosure()
			if err != nil {
				panic(err)
			}
			for id = range ids {
				typeIDs.Add(id)
			}
		}
	}
	// Collect sequence IDs.
	var seqIDs catalog.DescriptorIDSet
	{
		seqIdentifiers, err := seqexpr.GetUsedSequences(expr)
		if err != nil {
			panic(err)
		}
		seqNameToID := make(map[string]int64)
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
			seqNameToID[seqIdentifier.SeqName] = int64(seq.SequenceID)
			seqIDs.Add(seq.SequenceID)
		}
		if len(seqNameToID) > 0 {
			expr, err = seqexpr.ReplaceSequenceNamesWithIDs(expr, seqNameToID)
			if err != nil {
				panic(err)
			}
		}
	}
	ret := &scpb.Expression{
		Expr:            catpb.Expression(tree.Serialize(expr)),
		UsesSequenceIDs: seqIDs.Ordered(),
		UsesTypeIDs:     typeIDs.Ordered(),
	}
	return ret
}

// ComputedColumnExpression implements the scbuildstmt.TableHelpers interface.
func (b *builderState) ComputedColumnExpression(
	tbl *scpb.Table, d *tree.ColumnTableDef,
) (tree.Expr, scpb.TypeT) {
	_, _, ns := scpb.FindNamespace(b.QueryByID(tbl.TableID))
	tn := tree.MakeTableNameFromPrefix(b.NamePrefix(tbl), tree.Name(ns.Name))
	b.ensureDescriptor(tbl.TableID)
	// TODO(postamar): this doesn't work when referencing newly added columns.
	expr, typ, err := schemaexpr.ValidateComputedColumnExpression(
		b.ctx,
		b.descCache[tbl.TableID].desc.(catalog.TableDescriptor),
		d,
		&tn,
		"computed column",
		b.semaCtx,
	)
	if err != nil {
		panic(err)
	}
	parsedExpr, err := parser.ParseExpr(expr)
	if err != nil {
		panic(err)
	}
	return parsedExpr, newTypeT(typ)
}

var _ scbuildstmt.ElementReferences = (*builderState)(nil)

// ForwardReferences implements the scbuildstmt.ElementReferences interface.
func (b *builderState) ForwardReferences(e scpb.Element) scbuildstmt.ElementResultSet {
	ids := screl.AllDescIDs(e)
	var c int
	ids.ForEach(func(id descpb.ID) {
		b.ensureDescriptor(id)
		c = c + len(b.descCache[id].ers.indexes)
	})
	ret := &elementResultSet{b: b, indexes: make([]int, 0, c)}
	ids.ForEach(func(id descpb.ID) {
		ret.indexes = append(ret.indexes, b.descCache[id].ers.indexes...)
	})
	return ret
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
			if !screl.AllDescIDs(es.element).Contains(id) {
				continue
			}
			ids.Add(descID)
		}
		ids.ForEach(b.ensureDescriptor)
	}
	ret := &elementResultSet{b: b}
	ids.ForEach(func(id descpb.ID) {
		ret.indexes = append(ret.indexes, b.descCache[id].ers.indexes...)
	})
	return ret
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
	b.checkPrivilege(db.GetID(), p.RequiredPrivilege)
	return b.descCache[db.GetID()].ers
}

// ResolveSchema implements the scbuildstmt.NameResolver interface.
func (b *builderState) ResolveSchema(
	name tree.ObjectNamePrefix, p scbuildstmt.ResolveParams,
) scbuildstmt.ElementResultSet {
	_, sc := b.cr.MayResolveSchema(b.ctx, name)
	if sc == nil {
		if p.IsExistenceOptional {
			return nil
		}
		panic(sqlerrors.NewUndefinedSchemaError(name.Schema()))
	}
	switch sc.SchemaKind() {
	case catalog.SchemaPublic, catalog.SchemaVirtual, catalog.SchemaTemporary:
		panic(pgerror.Newf(pgcode.InsufficientPrivilege,
			"%s permission denied for schema %q", p.RequiredPrivilege.String(), name))
	case catalog.SchemaUserDefined:
		b.ensureDescriptor(sc.GetID())
		b.mustOwn(sc.GetID())
	default:
		panic(errors.AssertionFailedf("unknown schema kind %d", sc.SchemaKind()))
	}
	return b.descCache[sc.GetID()].ers
}

// ResolveEnumType implements the scbuildstmt.NameResolver interface.
func (b *builderState) ResolveEnumType(
	name *tree.UnresolvedObjectName, p scbuildstmt.ResolveParams,
) scbuildstmt.ElementResultSet {
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
	case descpb.TypeDescriptor_TABLE_IMPLICIT_RECORD_TYPE:
		// Implicit record types are not directly modifiable.
		panic(pgerror.Newf(pgcode.DependentObjectsStillExist,
			"cannot modify table record type %q", typ.GetName()))
	default:
		panic(errors.AssertionFailedf("unknown type kind %s", typ.GetKind()))
	}
	return b.descCache[typ.GetID()].ers
}

// ResolveRelation implements the scbuildstmt.NameResolver interface.
func (b *builderState) ResolveRelation(
	name *tree.UnresolvedObjectName, p scbuildstmt.ResolveParams,
) scbuildstmt.ElementResultSet {
	c := b.resolveRelation(name, p)
	if c == nil {
		return nil
	}
	return c.ers
}

func (b *builderState) resolveRelation(
	name *tree.UnresolvedObjectName, p scbuildstmt.ResolveParams,
) *cachedDesc {
	prefix, rel := b.cr.MayResolveTable(b.ctx, *name)
	if rel == nil {
		if p.IsExistenceOptional {
			return nil
		}
		panic(sqlerrors.NewUndefinedRelationError(name))
	}
	if rel.IsVirtualTable() {
		if prefix.Schema.GetName() == catconstants.PgCatalogName {
			panic(pgerror.Newf(pgcode.InsufficientPrivilege,
				"%s is a system catalog", tree.ErrNameString(rel.GetName())))
		}
		panic(pgerror.Newf(pgcode.WrongObjectType,
			"%s is a virtual object and cannot be modified", tree.ErrNameString(rel.GetName())))
	}
	if rel.IsTemporary() {
		panic(scerrors.NotImplementedErrorf(nil /* n */, "dropping a temporary table"))
	}
	// If we own the schema then we can manipulate the underlying relation.
	b.ensureDescriptor(rel.GetID())
	c := b.descCache[rel.GetID()]
	b.ensureDescriptor(rel.GetParentSchemaID())
	if b.descCache[rel.GetParentSchemaID()].hasOwnership {
		c.hasOwnership = true
		return c
	}
	err, found := c.privileges[p.RequiredPrivilege]
	if !found {
		err = b.auth.CheckPrivilege(b.ctx, rel, p.RequiredPrivilege)
		c.privileges[p.RequiredPrivilege] = err
	}
	if err == nil {
		return c
	}
	if p.RequiredPrivilege != privilege.CREATE {
		panic(err)
	}
	relationType := "table"
	if rel.IsView() {
		relationType = "view"
	} else if rel.IsSequence() {
		relationType = "sequence"
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
	if rel := c.desc.(catalog.TableDescriptor); !rel.IsTable() {
		panic(pgerror.Newf(pgcode.WrongObjectType, "%q is not a table", rel.GetName()))
	}
	return c.ers
}

// ResolveSequence implements the scbuildstmt.NameResolver interface.
func (b *builderState) ResolveSequence(
	name *tree.UnresolvedObjectName, p scbuildstmt.ResolveParams,
) scbuildstmt.ElementResultSet {
	c := b.resolveRelation(name, p)
	if c == nil {
		return nil
	}
	if rel := c.desc.(catalog.TableDescriptor); !rel.IsSequence() {
		panic(pgerror.Newf(pgcode.WrongObjectType, "%q is not a sequence", rel.GetName()))
	}
	return c.ers
}

// ResolveView implements the scbuildstmt.NameResolver interface.
func (b *builderState) ResolveView(
	name *tree.UnresolvedObjectName, p scbuildstmt.ResolveParams,
) scbuildstmt.ElementResultSet {
	c := b.resolveRelation(name, p)
	if c == nil {
		return nil
	}
	if rel := c.desc.(catalog.TableDescriptor); !rel.IsView() {
		panic(pgerror.Newf(pgcode.WrongObjectType, "%q is not a view", rel.GetName()))
	}
	return c.ers
}

// ResolveIndex implements the scbuildstmt.NameResolver interface.
func (b *builderState) ResolveIndex(
	relationID catid.DescID, indexName tree.Name, p scbuildstmt.ResolveParams,
) scbuildstmt.ElementResultSet {
	b.ensureDescriptor(relationID)
	c := b.descCache[relationID]
	rel := c.desc.(catalog.TableDescriptor)
	if !rel.IsPhysicalTable() || rel.IsSequence() {
		panic(pgerror.Newf(pgcode.WrongObjectType,
			"%q is not an indexable table or a materialized view", rel.GetName()))
	}
	b.checkPrivilege(rel.GetID(), p.RequiredPrivilege)
	var indexID catid.IndexID
	scpb.ForEachIndexName(c.ers, func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.IndexName) {
		if e.TableID == relationID && tree.Name(e.Name) == indexName {
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
	return c.ers.Filter(func(_ scpb.Status, _ scpb.TargetStatus, element scpb.Element) bool {
		idI, _ := screl.Schema.GetAttribute(screl.IndexID, element)
		return idI != nil && idI.(catid.IndexID) == indexID
	})
}

// ResolveColumn implements the scbuildstmt.NameResolver interface.
func (b *builderState) ResolveColumn(
	relationID catid.DescID, columnName tree.Name, p scbuildstmt.ResolveParams,
) scbuildstmt.ElementResultSet {
	b.ensureDescriptor(relationID)
	c := b.descCache[relationID]
	rel := c.desc.(catalog.TableDescriptor)
	b.checkPrivilege(rel.GetID(), p.RequiredPrivilege)
	var columnID catid.ColumnID
	scpb.ForEachColumnName(c.ers, func(status scpb.Status, _ scpb.TargetStatus, e *scpb.ColumnName) {
		if e.TableID == relationID && tree.Name(e.Name) == columnName {
			columnID = e.ColumnID
		}
	})
	if columnID == 0 {
		if p.IsExistenceOptional {
			return nil
		}
		panic(pgerror.Newf(pgcode.UndefinedColumn,
			"column %q not found in relation %q", columnName, rel.GetName()))
	}
	return c.ers.Filter(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) bool {
		idI, _ := screl.Schema.GetAttribute(screl.ColumnID, e)
		return idI != nil && idI.(catid.ColumnID) == columnID
	})
}

func (b *builderState) ensureDescriptor(id catid.DescID) {
	if _, found := b.descCache[id]; found {
		return
	}
	c := &cachedDesc{
		desc:         b.readDescriptor(id),
		privileges:   make(map[privilege.Kind]error),
		hasOwnership: b.hasAdmin,
		ers:          &elementResultSet{b: b},
	}
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
		c.ers.indexes = append(c.ers.indexes, len(b.output))
		b.output = append(b.output, elementState{
			element: e,
			target:  scpb.AsTargetStatus(status),
			current: status,
		})
	}
	c.backrefs = scdecomp.WalkDescriptor(c.desc, crossRefLookupFn, visitorFn)
	// Name prefix and namespace lookups.
	switch d := c.desc.(type) {
	case catalog.DatabaseDescriptor:
		if !d.HasPublicSchemaWithDescriptor() {
			panic(scerrors.NotImplementedErrorf(nil, /* n */
				"database %q (%d) with a descriptorless public schema",
				d.GetName(), d.GetID()))
		}
		// Handle special case of database children, which may include temporary
		// schemas, which aren't explicitly referenced in the database's schemas
		// map.
		childSchemas := b.cr.MustGetSchemasForDatabase(b.ctx, d)
		for schemaID, schemaName := range childSchemas {
			c.backrefs.Add(schemaID)
			if strings.HasPrefix(schemaName, catconstants.PgTempSchemaName) {
				b.tempSchemas[schemaID] = schemadesc.NewTemporarySchema(schemaName, schemaID, d.GetID())
			}
		}
	case catalog.SchemaDescriptor:
		db := b.readDescriptor(c.desc.GetParentID())
		c.prefix.CatalogName = tree.Name(db.GetName())
		c.prefix.ExplicitCatalog = true
		// Handle special case of schema children, which have to be added to
		// the back-referenced ID set but which aren't explicitly referenced in
		// the schema descriptor itself.
		_, objectIDs := b.cr.ReadObjectNamesAndIDs(b.ctx, db.(catalog.DatabaseDescriptor), d)
		for _, objectID := range objectIDs {
			c.backrefs.Add(objectID)
		}
	default:
		db := b.readDescriptor(c.desc.GetParentID())
		c.prefix.CatalogName = tree.Name(db.GetName())
		c.prefix.ExplicitCatalog = true
		sc := b.readDescriptor(c.desc.GetParentSchemaID())
		c.prefix.SchemaName = tree.Name(sc.GetName())
		c.prefix.ExplicitSchema = true
	}
}

func (b *builderState) readDescriptor(id catid.DescID) catalog.Descriptor {
	if id == catid.InvalidDescID {
		panic(errors.AssertionFailedf("invalid descriptor ID %d", id))
	}
	if id == keys.SystemPublicSchemaID || id == keys.PublicSchemaIDForBackup || id == keys.PublicSchemaID {
		panic(scerrors.NotImplementedErrorf(nil /* n */, "descriptorless public schema %d", id))
	}
	if tempSchema := b.tempSchemas[id]; tempSchema != nil {
		return tempSchema
	}
	return b.cr.MustReadDescriptor(b.ctx, id)
}

type elementResultSet struct {
	b       *builderState
	indexes []int
}

var _ scbuildstmt.ElementResultSet = &elementResultSet{}

// ForEachElementStatus implements the scpb.ElementStatusIterator interface.
func (ers *elementResultSet) ForEachElementStatus(
	fn func(current scpb.Status, target scpb.TargetStatus, element scpb.Element),
) {
	if ers.IsEmpty() {
		return
	}
	for _, i := range ers.indexes {
		es := &ers.b.output[i]
		fn(es.current, es.target, es.element)
	}
}

// IsEmpty implements the scbuildstmt.ElementResultSet interface.
func (ers *elementResultSet) IsEmpty() bool {
	return ers == nil || len(ers.indexes) == 0
}

// Filter implements the scbuildstmt.ElementResultSet interface.
func (ers *elementResultSet) Filter(
	predicate func(current scpb.Status, target scpb.TargetStatus, e scpb.Element) bool,
) scbuildstmt.ElementResultSet {
	return ers.filter(predicate)
}

func (ers *elementResultSet) filter(
	predicate func(current scpb.Status, target scpb.TargetStatus, e scpb.Element) bool,
) *elementResultSet {
	if ers.IsEmpty() {
		return nil
	}
	ret := elementResultSet{b: ers.b}
	for _, i := range ers.indexes {
		es := &ers.b.output[i]
		if predicate(es.current, es.target, es.element) {
			ret.indexes = append(ret.indexes, i)
		}
	}
	if len(ret.indexes) == 0 {
		return nil
	}
	return &ret
}
