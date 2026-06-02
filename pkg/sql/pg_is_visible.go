// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/oidext"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// These builtins (pg_function_is_visible, pg_table_is_visible,
// pg_type_is_visible) answer "would the unqualified name of this object resolve
// back to this OID through the current search_path?" — Postgres's notion of
// visibility, where an object is hidden if an earlier schema on the path defines
// the same name (the same name *and* argument types, for functions).
//
// They are invoked once per row by ORM and psql `\d` introspection queries, so
// the implementation walks the (short) search path doing leased-cache-backed
// lookups rather than scanning the pg_catalog virtual tables, which would be
// O(catalog size) per call.

// PGFunctionIsVisible is part of the eval.Planner interface.
func (p *planner) PGFunctionIsVisible(ctx context.Context, o oid.Oid) (*tree.DBool, error) {
	name, target, err := p.ResolveFunctionByOID(ctx, o)
	if err != nil {
		if errors.Is(err, tree.ErrRoutineUndefined) {
			return nil, nil //nolint:returnerrcheck
		}
		return nil, err
	}
	// The virtual pg_proc only exposes the current database's UDFs (plus
	// builtins, which resolve to the current database). A UDF in another
	// database has no pg_proc row, so report NULL.
	if name.Catalog() != p.CurrentDatabase() {
		return nil, nil
	}

	// Resolve the bare function name through the search path to obtain every
	// candidate overload (builtins + UDFs) that the name could refer to.
	searchPath := p.CurrentSearchPath()
	unresolved := tree.MakeUnresolvedName(name.Object())
	resolved, err := p.ResolveFunction(
		ctx, tree.MakeUnresolvedFunctionName(&unresolved), &searchPath,
	)
	if err != nil {
		if errors.Is(err, tree.ErrRoutineUndefined) {
			return tree.DBoolFalse, nil
		}
		return nil, err
	}

	// Among the overloads whose argument types match the target's signature,
	// find the one in the earliest schema on the search path. The function is
	// visible iff that overload is the target. This mirrors Postgres's
	// signature-aware FunctionIsVisible.
	targetArgs := target.Types.Types()
	bestPos := -1
	var bestOid oid.Oid
	for i := range resolved.Overloads {
		ov := &resolved.Overloads[i]
		if !argTypesEqual(ov.Types.Types(), targetArgs) {
			continue
		}
		pos := searchPathPosition(searchPath, ov.Schema)
		if pos < 0 {
			continue
		}
		if bestPos < 0 || pos < bestPos {
			bestPos, bestOid = pos, ov.Oid
		}
	}
	if bestPos < 0 {
		return tree.DBoolFalse, nil
	}
	return tree.MakeDBool(tree.DBool(bestOid == o)), nil
}

// PGTableIsVisible is part of the eval.Planner interface.
func (p *planner) PGTableIsVisible(ctx context.Context, o oid.Oid) (*tree.DBool, error) {
	relName, scName, ok, err := p.relNameAndSchemaForVisibility(ctx, o)
	if err != nil || !ok {
		return nil, err
	}
	visible, err := p.objectVisibleInSearchPath(ctx, scName,
		func(ctx context.Context, candidateSchema string) (bool, error) {
			return p.relationExistsInSchema(ctx, candidateSchema, relName)
		})
	if err != nil {
		return nil, err
	}
	return tree.MakeDBool(tree.DBool(visible)), nil
}

// PGTypeIsVisible is part of the eval.Planner interface.
func (p *planner) PGTypeIsVisible(ctx context.Context, o oid.Oid) (*tree.DBool, error) {
	typName, scName, ok, err := p.typeNameAndSchemaForVisibility(ctx, o)
	if err != nil || !ok {
		return nil, err
	}
	visible, err := p.objectVisibleInSearchPath(ctx, scName,
		func(ctx context.Context, candidateSchema string) (bool, error) {
			return p.typeExistsInSchema(ctx, candidateSchema, typName)
		})
	if err != nil {
		return nil, err
	}
	return tree.MakeDBool(tree.DBool(visible)), nil
}

// objectVisibleInSearchPath implements Postgres's *IsVisible walk: scanning the
// current search path in order, the object is visible iff we reach its own
// schema (targetSchema) before any other schema that also defines the name (per
// existsInSchema). Returns false if targetSchema is not on the path at all.
func (p *planner) objectVisibleInSearchPath(
	ctx context.Context,
	targetSchema string,
	existsInSchema func(ctx context.Context, candidateSchema string) (bool, error),
) (bool, error) {
	// An object can only be visible if its own schema is on the search path. If
	// it is not, the answer is false regardless of what any other schema
	// contains, so we can skip probing entirely. This matters because pg_class /
	// pg_type enumerate every catalog object — including all of crdb_internal and
	// information_schema, neither of which is on the search path — and probing
	// each of those would issue a system.namespace lookup per row.
	if searchPathPosition(p.CurrentSearchPath(), targetSchema) < 0 {
		return false, nil
	}
	iter := p.CurrentSearchPath().Iter()
	for scName, ok := iter.Next(); ok; scName, ok = iter.Next() {
		if scName == targetSchema {
			return true, nil
		}
		exists, err := existsInSchema(ctx, scName)
		if err != nil {
			return false, err
		}
		if exists {
			return false, nil
		}
	}
	return false, nil
}

// relNameAndSchemaForVisibility resolves a pg_class OID to its relation name and
// schema name. ok is false (with no error) when no relation has the OID.
func (p *planner) relNameAndSchemaForVisibility(
	ctx context.Context, o oid.Oid,
) (relName, scName string, ok bool, err error) {
	// Virtual tables (e.g. pg_catalog.pg_class, crdb_internal.*,
	// information_schema.*) have OIDs in the hashed range but resolve from the
	// in-memory virtual catalog with no KV access. They appear in every
	// database's pg_class and are not owned by a real database, so the
	// current-database scoping applied to real relations below does not apply.
	vs := p.ExecCfg().VirtualSchemas
	if vobj, found := vs.GetVirtualObjectByID(descpb.ID(o)); found {
		vdesc := vobj.Desc()
		sc, scFound := vs.GetVirtualSchemaByID(vdesc.GetParentSchemaID())
		if !scFound {
			return "", "", false, errors.AssertionFailedf(
				"virtual schema %d for object %q not found",
				vdesc.GetParentSchemaID(), vdesc.GetName())
		}
		return vdesc.GetName(), sc.Desc().GetName(), true, nil
	}

	// Composite types and index entries have hashed OIDs that are not descriptor
	// IDs. Probing the descriptor store by ID for them is a guaranteed miss (a KV
	// round trip per call), so they are resolved through pg_class, whose oid
	// index is served from the already-populated descriptor collection. This path
	// is comparatively slow but rarely the common case.
	if oidext.IsMaybeHashedOid(o) {
		return p.relNameAndSchemaFromPGClass(ctx, o)
	}

	// Tables, views, and sequences have a pg_class OID equal to their descriptor
	// ID, so they can be resolved directly and cheaply. WithoutNonPublic matches
	// pg_class, which only enumerates public relations: offline (e.g. mid-IMPORT)
	// and being-added relations have no pg_class row, so their OIDs are reported as
	// NULL here, just as Postgres does for an OID absent from pg_class.
	desc, err := p.byIDGetterBuilder().WithoutNonPublic().Get().Table(ctx, descpb.ID(o))
	if err != nil {
		if errors.Is(err, catalog.ErrDescriptorNotFound) || sqlerrors.IsUndefinedRelationError(err) {
			// No relation has this OID.
			return "", "", false, nil //nolint:returnerrcheck
		}
		return "", "", false, err
	}

	// The virtual pg_class only contains the current database's relations, so a
	// relation in another database has no row and is reported as NULL.
	curDBID, err := p.currentDatabaseID(ctx)
	if err != nil {
		return "", "", false, err
	}
	if desc.GetParentID() != curDBID {
		return "", "", false, nil
	}
	sc, err := p.byIDGetterBuilder().WithoutNonPublic().Get().Schema(ctx, desc.GetParentSchemaID())
	if err != nil {
		return "", "", false, err
	}
	return desc.GetName(), sc.GetName(), true, nil
}

// relNameAndSchemaFromPGClass resolves a pg_class OID that is not a directly
// resolvable table descriptor — a composite type or an index entry, both of
// which have hashed OIDs — by reading pg_class. Any other OID has no pg_class
// row. This path is rarely exercised (e.g. \di over indexes) and is allowed to
// fall back to the slower virtual-table scan.
func (p *planner) relNameAndSchemaFromPGClass(
	ctx context.Context, o oid.Oid,
) (relName, scName string, ok bool, err error) {
	if !oidext.IsMaybeHashedOid(o) {
		return "", "", false, nil
	}
	row, err := p.QueryRowEx(ctx, "pg_table_is_visible", sessiondata.NoSessionDataOverride,
		`SELECT c.relname, n.nspname
		 FROM pg_catalog.pg_class c
		 JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
		 WHERE c.oid = $1`,
		tree.NewDOid(o))
	if err != nil {
		return "", "", false, err
	}
	if row == nil {
		return "", "", false, nil
	}
	return string(tree.MustBeDString(row[0])), string(tree.MustBeDString(row[1])), true, nil
}

// relationExistsInSchema reports whether a relation named relName exists in the
// given schema. It uses the object resolver so that both physical and virtual
// (e.g. pg_catalog) schemas are handled.
func (p *planner) relationExistsInSchema(
	ctx context.Context, schemaName, relName string,
) (bool, error) {
	tn := tree.MakeTableNameWithSchema(
		tree.Name(p.CurrentDatabase()), tree.Name(schemaName), tree.Name(relName),
	)
	_, desc, err := resolver.ResolveExistingTableObject(ctx, p, &tn,
		tree.ObjectLookupFlags{Required: false, DesiredObjectKind: tree.TableObject})
	if err != nil {
		return false, err
	}
	return desc != nil, nil
}

// typeNameAndSchemaForVisibility resolves a pg_type OID to its type name and
// schema name. ok is false (with no error) when no type has the OID.
func (p *planner) typeNameAndSchemaForVisibility(
	ctx context.Context, o oid.Oid,
) (typName, scName string, ok bool, err error) {
	if t, found := types.OidToType[o]; found {
		// Predefined types live in pg_catalog. PGName returns the lowercase name
		// (e.g. "int4", "_int4") that matches both the keys of typNameLiterals and
		// the descriptor names that typeExistsInSchema looks up; the uppercase form
		// from oidext.TypeName would miss those case-sensitive lookups.
		return t.PGName(), catconstants.PgCatalogName, true, nil
	}
	if !types.IsOIDUserDefinedType(o) {
		return "", "", false, nil
	}
	curDBID, err := p.currentDatabaseID(ctx)
	if err != nil {
		return "", "", false, err
	}
	id := typedesc.UserDefinedTypeOIDToID(o)
	sc, typDesc, err := getSchemaAndTypeByTypeID(ctx, p, id, false /* includeMetaData */)
	if err != nil {
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			return "", "", false, nil //nolint:returnerrcheck
		}
		return "", "", false, err
	}
	if typDesc != nil {
		if typDesc.Dropped() || typDesc.GetParentID() != curDBID {
			return "", "", false, nil
		}
		return typDesc.GetName(), sc.GetName(), true, nil
	}

	// The OID was not a type descriptor; it may be the implicit record type of a
	// table, whose pg_type name and schema are the table's. (getSchemaAndTypeByTypeID
	// returns no descriptor for these.)
	tbl, err := p.byIDGetterBuilder().WithoutNonPublic().Get().Table(ctx, id)
	if err != nil {
		if errors.Is(err, catalog.ErrDescriptorNotFound) ||
			sqlerrors.IsUndefinedRelationError(err) {
			return "", "", false, nil //nolint:returnerrcheck
		}
		return "", "", false, err
	}
	if tbl.GetParentID() != curDBID {
		return "", "", false, nil
	}
	scDesc, err := p.byIDGetterBuilder().WithoutNonPublic().Get().Schema(ctx, tbl.GetParentSchemaID())
	if err != nil {
		return "", "", false, err
	}
	return tbl.GetName(), scDesc.GetName(), true, nil
}

// typeExistsInSchema reports whether a type named typName exists in the given
// schema. pg_type contains predefined types (pg_catalog), user-defined types,
// and the implicit record type of every table; the latter two share
// system.namespace with their schema's objects.
func (p *planner) typeExistsInSchema(
	ctx context.Context, schemaName, typName string,
) (bool, error) {
	if schemaName == catconstants.PgCatalogName {
		if _, found, _ := types.TypeForNonKeywordTypeName(typName); found {
			return true, nil
		}
	}
	found, prefix, err := p.LookupSchema(ctx, p.CurrentDatabase(), schemaName)
	if err != nil || !found {
		return false, err
	}
	id, err := p.Descriptors().LookupObjectID(
		ctx, p.txn, prefix.Database.GetID(), prefix.Schema.GetID(), typName,
	)
	if err != nil {
		return false, err
	}
	return id != descpb.InvalidID, nil
}

// currentDatabaseID returns the descriptor ID of the session's current
// database, used to scope visibility checks to objects the per-database virtual
// catalogs (pg_class, pg_type, pg_proc) actually expose.
func (p *planner) currentDatabaseID(ctx context.Context) (descpb.ID, error) {
	db, err := p.Descriptors().ByName(p.txn).Get().Database(ctx, p.CurrentDatabase())
	if err != nil {
		return descpb.InvalidID, err
	}
	return db.GetID(), nil
}

// searchPathPosition returns the position of schemaName in the current search
// path (with implicit schemas included), or -1 if it is not present.
func searchPathPosition(path sessiondata.SearchPath, schemaName string) int {
	iter := path.Iter()
	pos := 0
	for scName, ok := iter.Next(); ok; scName, ok = iter.Next() {
		if scName == schemaName {
			return pos
		}
		pos++
	}
	return -1
}

// argTypesEqual reports whether two argument type lists are identical by OID,
// matching Postgres's proargtypes equality used to distinguish overloads.
func argTypesEqual(a, b []*types.T) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Oid() != b[i].Oid() {
			return false
		}
	}
	return true
}
