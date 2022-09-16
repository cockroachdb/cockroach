// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sctestdeps

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdeps"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec/scmutationexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

var _ scbuild.Dependencies = (*TestState)(nil)

// AuthorizationAccessor implements the scbuild.Dependencies interface.
func (s *TestState) AuthorizationAccessor() scbuild.AuthorizationAccessor {
	return s
}

// CatalogReader implements the scbuild.Dependencies interface.
func (s *TestState) CatalogReader() scbuild.CatalogReader {
	return s
}

// ClusterID implements the scbuild.Dependencies interface.
func (s *TestState) ClusterID() uuid.UUID {
	return uuid.Nil
}

// Codec implements the scbuild.Dependencies interface.
func (s *TestState) Codec() keys.SQLCodec {
	return keys.SystemSQLCodec
}

// SessionData implements the scbuild.Dependencies interface.
func (s *TestState) SessionData() *sessiondata.SessionData {
	return &s.sessionData
}

// ClusterSettings implements the scbuild.Dependencies interface.
func (s *TestState) ClusterSettings() *cluster.Settings {
	return cluster.MakeTestingClusterSettings()
}

// Statements implements the scbuild.Dependencies interface.
func (s *TestState) Statements() []string {
	return s.statements
}

// IncrementSchemaChangeAlterCounter implements the scbuild.Dependencies
// interface.
func (s *TestState) IncrementSchemaChangeAlterCounter(counterType string, extra ...string) {
	var maybeExtra string
	if len(extra) > 0 {
		maybeExtra = "." + extra[0]
	}
	s.LogSideEffectf("increment telemetry for sql.schema.alter_%s%s", counterType, maybeExtra)
}

// IncrementSchemaChangeDropCounter implements the scbuild.Dependencies
// interface.
func (s *TestState) IncrementSchemaChangeDropCounter(counterType string) {
	s.LogSideEffectf("increment telemetry for sql.schema.drop_%s", counterType)
}

// IncrementSchemaChangeAddColumnTypeCounter  implements the scbuild.Dependencies
// interface.
func (s *TestState) IncrementSchemaChangeAddColumnTypeCounter(typeName string) {
	s.LogSideEffectf("increment telemetry for sql.schema.new_column_type.%s", typeName)
}

// IncrementSchemaChangeAddColumnQualificationCounter  implements the scbuild.Dependencies
// interface.
func (s *TestState) IncrementSchemaChangeAddColumnQualificationCounter(qualification string) {
	s.LogSideEffectf("increment telemetry for sql.schema.qualifcation.%s", qualification)
}

// IncrementUserDefinedSchemaCounter implements the scbuild.Dependencies
// interface.
func (s *TestState) IncrementUserDefinedSchemaCounter(
	counterType sqltelemetry.UserDefinedSchemaTelemetryType,
) {
	s.LogSideEffectf("increment telemetry for sql.uds.%s", counterType)
}

// IncrementEnumCounter implements the scbuild.Dependencies interface.
func (s *TestState) IncrementEnumCounter(counterType sqltelemetry.EnumTelemetryType) {
	s.LogSideEffectf("increment telemetry for sql.udts.%s", counterType)
}

// IncrementDropOwnedByCounter implements the scbuild.Dependencies interface.
func (s *TestState) IncrementDropOwnedByCounter() {
	s.LogSideEffectf("increment telemetry for sql.drop_owned_by")
}

var _ scbuild.AuthorizationAccessor = (*TestState)(nil)

// CheckPrivilege implements the scbuild.AuthorizationAccessor interface.
func (s *TestState) CheckPrivilege(
	ctx context.Context, privilegeObject catalog.PrivilegeObject, privilege privilege.Kind,
) error {
	return nil
}

// HasAdminRole implements the scbuild.AuthorizationAccessor interface.
func (s *TestState) HasAdminRole(ctx context.Context) (bool, error) {
	return true, nil
}

// HasOwnership implements the scbuild.AuthorizationAccessor interface.
func (s *TestState) HasOwnership(
	ctx context.Context, privilegeObject catalog.PrivilegeObject,
) (bool, error) {
	return true, nil
}

// CheckPrivilegeForUser implements the scbuild.AuthorizationAccessor interface.
func (s *TestState) CheckPrivilegeForUser(
	ctx context.Context,
	privilegeObject catalog.PrivilegeObject,
	privilege privilege.Kind,
	user username.SQLUsername,
) error {
	return nil
}

// MemberOfWithAdminOption implements the scbuild.AuthorizationAccessor interface.
func (s *TestState) MemberOfWithAdminOption(
	ctx context.Context, member username.SQLUsername,
) (map[username.SQLUsername]bool, error) {
	return nil, nil
}

// IndexPartitioningCCLCallback implements the scbuild.Dependencies interface.
func (s *TestState) IndexPartitioningCCLCallback() scbuild.CreatePartitioningCCLCallback {
	if ccl := scdeps.CreatePartitioningCCL; ccl != nil {
		return ccl
	}
	return func(
		ctx context.Context,
		st *cluster.Settings,
		evalCtx *eval.Context,
		columnLookupFn func(tree.Name) (catalog.Column, error),
		oldNumImplicitColumns int,
		oldKeyColumnNames []string,
		partBy *tree.PartitionBy,
		allowedNewColumnNames []tree.Name,
		allowImplicitPartitioning bool,
	) (newImplicitCols []catalog.Column, newPartitioning catpb.PartitioningDescriptor, err error) {
		newPartitioning.NumColumns = uint32(len(partBy.Fields))
		return nil, newPartitioning, nil
	}
}

var _ scbuild.CatalogReader = (*TestState)(nil)

// MayResolveDatabase implements the scbuild.CatalogReader interface.
func (s *TestState) MayResolveDatabase(
	ctx context.Context, name tree.Name,
) catalog.DatabaseDescriptor {
	desc := s.mayGetByName(0, 0, name.String())
	if desc == nil {
		return nil
	}
	db, err := catalog.AsDatabaseDescriptor(desc)
	if err != nil {
		panic(err)
	}
	if db.Dropped() || db.Offline() {
		return nil
	}
	return db
}

// MayResolveSchema implements the scbuild.CatalogReader interface.
func (s *TestState) MayResolveSchema(
	ctx context.Context, name tree.ObjectNamePrefix,
) (catalog.DatabaseDescriptor, catalog.SchemaDescriptor) {
	dbName := name.Catalog()
	scName := name.Schema()
	if !name.ExplicitCatalog && !name.ExplicitSchema {
		return nil, nil
	}
	if !name.ExplicitCatalog || !name.ExplicitSchema {
		dbName = s.CurrentDatabase()
		if name.ExplicitCatalog {
			scName = name.Catalog()
		} else {
			scName = name.Schema()
		}
	}
	dbDesc := s.mayGetByName(0, 0, dbName)
	if dbDesc == nil || dbDesc.Dropped() || dbDesc.Offline() {
		if dbName == s.CurrentDatabase() {
			panic(errors.AssertionFailedf("Invalid current database %q", s.CurrentDatabase()))
		}
		return nil, nil
	}
	db, err := catalog.AsDatabaseDescriptor(dbDesc)
	if err != nil {
		panic(err)
	}
	scDesc := s.mayGetByName(db.GetID(), 0, scName)
	if scDesc == nil || scDesc.Dropped() || scDesc.Offline() {
		return nil, nil
	}
	sc, err := catalog.AsSchemaDescriptor(scDesc)
	if err != nil {
		panic(err)
	}
	return db, sc
}

// MayResolveTable implements the scbuild.CatalogReader interface.
func (s *TestState) MayResolveTable(
	ctx context.Context, name tree.UnresolvedObjectName,
) (catalog.ResolvedObjectPrefix, catalog.TableDescriptor) {
	prefix, desc, err := s.mayResolveObject(name)
	if err != nil {
		panic(err)
	}
	if desc == nil {
		return prefix, nil
	}
	table, err := catalog.AsTableDescriptor(desc)
	if err != nil {
		panic(err)
	}
	return prefix, table
}

// MayResolveType implements the scbuild.CatalogReader interface.
func (s *TestState) MayResolveType(
	ctx context.Context, name tree.UnresolvedObjectName,
) (catalog.ResolvedObjectPrefix, catalog.TypeDescriptor) {
	prefix, desc, err := s.mayResolveObject(name)
	if err != nil {
		panic(err)
	}
	if desc == nil {
		return prefix, nil
	}
	typ, err := catalog.AsTypeDescriptor(desc)
	if err != nil {
		panic(err)
	}
	return prefix, typ
}

// MayResolveIndex implements the scbuild.CatalogReader interface.
func (s *TestState) MayResolveIndex(
	ctx context.Context, tableIndexName tree.TableIndexName,
) (
	found bool,
	prefix catalog.ResolvedObjectPrefix,
	tbl catalog.TableDescriptor,
	idx catalog.Index,
) {
	if tableIndexName.Table.Object() != "" {
		prefix, tbl := s.MayResolveTable(ctx, *tableIndexName.Table.ToUnresolvedObjectName())
		if tbl == nil {
			return false, catalog.ResolvedObjectPrefix{}, nil, nil
		}
		idx, err := tbl.FindNonDropIndexWithName(string(tableIndexName.Index))
		if err != nil {
			return false, catalog.ResolvedObjectPrefix{}, nil, nil
		}
		return true, prefix, tbl, idx
	}

	db, schema := s.mayResolvePrefix(tableIndexName.Table.ObjectNamePrefix)
	dsNames, _ := s.ReadObjectNamesAndIDs(ctx, db.(catalog.DatabaseDescriptor), schema.(catalog.SchemaDescriptor))
	for _, dsName := range dsNames {
		prefix, tbl := s.MayResolveTable(ctx, *dsName.ToUnresolvedObjectName())
		if tbl == nil {
			continue
		}
		idx, err := tbl.FindNonDropIndexWithName(string(tableIndexName.Index))
		if err == nil {
			return true, prefix, tbl, idx
		}
	}

	return false, catalog.ResolvedObjectPrefix{}, nil, nil
}

func (s *TestState) mayResolveObject(
	name tree.UnresolvedObjectName,
) (prefix catalog.ResolvedObjectPrefix, desc catalog.Descriptor, err error) {
	tn := name.ToTableName()
	{
		db, sc := s.mayResolvePrefix(tn.ObjectNamePrefix)
		if db == nil || sc == nil {
			return catalog.ResolvedObjectPrefix{}, nil, nil
		}
		prefix.ExplicitDatabase = true
		prefix.ExplicitSchema = true
		prefix.Database, err = catalog.AsDatabaseDescriptor(db)
		if err != nil {
			return catalog.ResolvedObjectPrefix{}, nil, err
		}
		prefix.Schema, err = catalog.AsSchemaDescriptor(sc)
		if err != nil {
			return catalog.ResolvedObjectPrefix{}, nil, err
		}
	}
	desc = s.mayGetByName(prefix.Database.GetID(), prefix.Schema.GetID(), name.Object())
	if desc == nil {
		return prefix, nil, nil
	}
	if desc.Dropped() || desc.Offline() {
		return prefix, nil, nil
	}
	return prefix, desc, nil
}

func (s *TestState) mayResolvePrefix(name tree.ObjectNamePrefix) (db, sc catalog.Descriptor) {
	if name.ExplicitCatalog && name.ExplicitSchema {
		db = s.mayGetByName(0, 0, name.Catalog())
		if db == nil || db.Dropped() || db.Offline() {
			return nil, nil
		}
		sc = s.mayGetByName(db.GetID(), 0, name.Schema())
		if sc == nil || sc.Dropped() || sc.Offline() {
			return nil, nil
		}
		return db, sc
	}

	db = s.mayGetByName(0, 0, s.CurrentDatabase())
	if db == nil || db.Dropped() || db.Offline() {
		panic(errors.AssertionFailedf("Invalid current database %q", s.CurrentDatabase()))
	}

	if !name.ExplicitCatalog && !name.ExplicitSchema {
		sc = s.mayGetByName(db.GetID(), 0, catconstants.PublicSchemaName)
		if sc == nil || sc.Dropped() || sc.Offline() {
			return nil, nil
		}
		return db, sc
	}

	var prefixName string
	if name.ExplicitCatalog {
		prefixName = name.Catalog()
	} else {
		prefixName = name.Schema()
	}

	sc = s.mayGetByName(db.GetID(), 0, prefixName)
	if sc != nil && !sc.Dropped() && !sc.Offline() {
		return db, sc
	}

	db = s.mayGetByName(0, 0, prefixName)
	if db == nil || db.Dropped() || db.Offline() {
		return nil, nil
	}
	sc = s.mayGetByName(db.GetID(), 0, catconstants.PublicSchemaName)
	if sc == nil || sc.Dropped() || sc.Offline() {
		return nil, nil
	}
	return db, sc
}

func (s *TestState) mayGetByName(
	parentID, parentSchemaID descpb.ID, name string,
) catalog.Descriptor {
	key := descpb.NameInfo{
		ParentID:       parentID,
		ParentSchemaID: parentSchemaID,
		Name:           name,
	}
	ne := s.uncommitted.LookupNamespaceEntry(key)
	if ne == nil {
		return nil
	}
	id := ne.GetID()
	if id == descpb.InvalidID {
		return nil
	}
	if id == keys.PublicSchemaID {
		return schemadesc.GetPublicSchema()
	}
	desc, _ := s.mustReadImmutableDescriptor(id)
	return desc
}

// ReadObjectNamesAndIDs implements the scbuild.CatalogReader interface.
func (s *TestState) ReadObjectNamesAndIDs(
	ctx context.Context, db catalog.DatabaseDescriptor, schema catalog.SchemaDescriptor,
) (names tree.TableNames, ids descpb.IDs) {
	m := make(map[string]descpb.ID)
	_ = s.uncommitted.ForEachNamespaceEntry(func(e nstree.NamespaceEntry) error {
		if e.GetParentID() == db.GetID() && e.GetParentSchemaID() == schema.GetID() {
			m[e.GetName()] = e.GetID()
			names = append(names, tree.MakeTableNameWithSchema(
				tree.Name(db.GetName()),
				tree.Name(schema.GetName()),
				tree.Name(e.GetName()),
			))
		}
		return nil
	})
	sort.Slice(names, func(i, j int) bool {
		return names[i].Object() < names[j].Object()
	})
	for _, name := range names {
		ids = append(ids, m[name.Object()])
	}
	return names, ids
}

// ResolveType implements the scbuild.CatalogReader interface.
func (s *TestState) ResolveType(
	ctx context.Context, name *tree.UnresolvedObjectName,
) (*types.T, error) {
	prefix, obj, err := s.mayResolveObject(*name)
	if err != nil {
		return nil, err
	}
	if obj == nil {
		return nil, errors.Wrapf(catalog.ErrDescriptorNotFound, "resolving type %q", name.String())
	}
	typ, err := catalog.AsTypeDescriptor(obj)
	if err != nil {
		return nil, err
	}
	tn := tree.MakeQualifiedTypeName(prefix.Database.GetName(), prefix.Schema.GetName(), typ.GetName())
	return typ.MakeTypesT(ctx, &tn, s)
}

// ResolveTypeByOID implements the scbuild.CatalogReader interface.
func (s *TestState) ResolveTypeByOID(ctx context.Context, oid oid.Oid) (*types.T, error) {
	id, err := typedesc.UserDefinedTypeOIDToID(oid)
	if err != nil {
		return nil, err
	}
	name, typ, err := s.GetTypeDescriptor(ctx, id)
	if err != nil {
		return nil, err
	}
	return typ.MakeTypesT(ctx, &name, s)
}

var _ catalog.TypeDescriptorResolver = (*TestState)(nil)

// GetTypeDescriptor implements the scbuild.CatalogReader interface.
func (s *TestState) GetTypeDescriptor(
	ctx context.Context, id descpb.ID,
) (tree.TypeName, catalog.TypeDescriptor, error) {
	desc, err := s.mustReadImmutableDescriptor(id)
	if err != nil {
		return tree.TypeName{}, nil, err
	}
	typ, err := catalog.AsTypeDescriptor(desc)
	if err != nil {
		return tree.TypeName{}, nil, err
	}
	tn, err := s.getQualifiedObjectNameByID(typ.GetID())
	if err != nil {
		return tree.TypeName{}, nil, err
	}
	return tree.MakeTypeNameWithPrefix(tn.ObjectNamePrefix, tn.Object()), typ, nil
}

// GetQualifiedTableNameByID implements the scbuild.CatalogReader interface.
func (s *TestState) GetQualifiedTableNameByID(
	ctx context.Context, id int64, requiredType tree.RequiredTableKind,
) (*tree.TableName, error) {
	return s.getQualifiedObjectNameByID(descpb.ID(id))
}

func (s *TestState) getQualifiedObjectNameByID(id descpb.ID) (*tree.TableName, error) {
	obj, err := s.mustReadImmutableDescriptor(id)
	if err != nil {
		return nil, err
	}
	db, err := s.mustReadImmutableDescriptor(obj.GetParentID())
	if err != nil {
		return nil, errors.Wrapf(err, "parent database for object #%d", id)
	}
	sc, err := s.mustReadImmutableDescriptor(obj.GetParentSchemaID())
	if err != nil {
		return nil, errors.Wrapf(err, "parent schema for object #%d", id)
	}
	return tree.NewTableNameWithSchema(tree.Name(db.GetName()), tree.Name(sc.GetName()), tree.Name(obj.GetName())), nil
}

// CurrentDatabase implements the scbuild.CatalogReader interface.
func (s *TestState) CurrentDatabase() string {
	return s.currentDatabase
}

// MustGetSchemasForDatabase implements the scbuild.CatalogReader interface.
func (s *TestState) MustGetSchemasForDatabase(
	ctx context.Context, database catalog.DatabaseDescriptor,
) map[descpb.ID]string {
	schemas := make(map[descpb.ID]string)
	err := database.ForEachSchema(func(id descpb.ID, name string) error {
		schemas[id] = name
		return nil
	})
	if err != nil {
		panic(err)
	}
	return schemas
}

// MustReadDescriptor implements the scbuild.CatalogReader interface.
func (s *TestState) MustReadDescriptor(ctx context.Context, id descpb.ID) catalog.Descriptor {
	desc, err := s.mustReadImmutableDescriptor(id)
	if err != nil {
		panic(err)
	}
	return desc
}

var _ scmutationexec.SyntheticDescriptorStateUpdater = (*TestState)(nil)

// AddSyntheticDescriptor is part of the
// scmutationexec.SyntheticDescriptorStateUpdater interface.
func (s *TestState) AddSyntheticDescriptor(desc catalog.Descriptor) {
	s.LogSideEffectf("add synthetic descriptor #%d:\n%s", desc.GetID(), s.descriptorDiff(desc))
}

// mustReadImmutableDescriptor looks up a descriptor and returns a immutable
// deep copy.
func (s *TestState) mustReadImmutableDescriptor(id descpb.ID) (catalog.Descriptor, error) {
	mut, err := s.mustReadMutableDescriptor(id)
	if err != nil {
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			return nil, errors.Wrapf(catalog.ErrDescriptorNotFound, "reading immutable descriptor #%d", id)
		}
		return nil, err
	}
	return mut.ImmutableCopy(), nil
}

// mustReadMutableDescriptor looks up a descriptor and returns a mutable
// deep copy.
func (s *TestState) mustReadMutableDescriptor(id descpb.ID) (catalog.MutableDescriptor, error) {
	u := s.uncommitted.LookupDescriptorEntry(id)
	if u == nil {
		return nil, errors.Wrapf(catalog.ErrDescriptorNotFound, "reading mutable descriptor #%d", id)
	}
	c := s.committed.LookupDescriptorEntry(id)
	if c == nil {
		return u.NewBuilder().BuildCreatedMutable(), nil
	}
	mut := c.NewBuilder().BuildExistingMutable()
	pb := u.NewBuilder().BuildImmutable().DescriptorProto()
	tbl, db, typ, sc, fn := descpb.FromDescriptorWithMVCCTimestamp(pb, s.mvccTimestamp())
	switch m := mut.(type) {
	case *tabledesc.Mutable:
		m.TableDescriptor = *tbl
	case *dbdesc.Mutable:
		m.DatabaseDescriptor = *db
	case *typedesc.Mutable:
		m.TypeDescriptor = *typ
	case *schemadesc.Mutable:
		m.SchemaDescriptor = *sc
	case *funcdesc.Mutable:
		m.FunctionDescriptor = *fn
	default:
		return nil, errors.AssertionFailedf("Unknown mutable descriptor type %T", mut)
	}
	return mut, nil
}

var _ scexec.Dependencies = (*TestState)(nil)

// Clock is part of the scexec.Dependencies interface.
func (s *TestState) Clock() scmutationexec.Clock {
	return s
}

// ApproximateTime is part of the scmutationexec.Clock interface.
func (s *TestState) ApproximateTime() time.Time {
	return s.approximateTimestamp
}

// Catalog implements the scexec.Dependencies interface.
func (s *TestState) Catalog() scexec.Catalog {
	return s
}

var _ scexec.Catalog = (*TestState)(nil)

// MustReadImmutableDescriptors implements the scmutationexec.CatalogReader interface.
func (s *TestState) MustReadImmutableDescriptors(
	ctx context.Context, ids ...descpb.ID,
) ([]catalog.Descriptor, error) {
	out := make([]catalog.Descriptor, 0, len(ids))
	for _, id := range ids {
		d, err := s.mustReadImmutableDescriptor(id)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, nil
}

// MustReadMutableDescriptor implements the scexec.Catalog interface.
func (s *TestState) MustReadMutableDescriptor(
	ctx context.Context, id descpb.ID,
) (catalog.MutableDescriptor, error) {
	return s.mustReadMutableDescriptor(id)
}

// GetFullyQualifiedName implements scexec.Catalog
func (s *TestState) GetFullyQualifiedName(ctx context.Context, id descpb.ID) (string, error) {
	obj, err := s.mustReadImmutableDescriptor(id)
	if err != nil {
		return "", err
	}
	dbName := ""
	if obj.GetParentID() != descpb.InvalidID {
		db, err := s.mustReadImmutableDescriptor(obj.GetParentID())
		if err != nil {
			return "", errors.Wrapf(err, "parent database for object #%d", id)
		}
		dbName = db.GetName()
	}
	scName := ""
	if obj.GetParentSchemaID() != descpb.InvalidID {
		scName = tree.PublicSchema
		if obj.GetParentSchemaID() != keys.PublicSchemaID {
			sc, err := s.mustReadImmutableDescriptor(obj.GetParentSchemaID())
			if err != nil {
				return "", errors.Wrapf(err, "parent schema for object #%d", id)
			}
			scName = sc.GetName()
		}
	}
	// Sanity checks:
	// 1) Both table and types will have both a schema and database name.
	// 2) Schemas will only have a database name.
	// 3) Databases should not have either set.
	switch obj.DescriptorType() {
	case catalog.Table:
		fallthrough
	case catalog.Type:
		if scName == "" || dbName == "" {
			return "", errors.AssertionFailedf("schema or database missing for type/relation %d", id)
		}
	case catalog.Schema:
		if scName != "" || dbName == "" {
			return "", errors.AssertionFailedf("schema or database are invalid for schema %d", id)
		}
	case catalog.Database:
		if scName != "" || dbName != "" {
			return "", errors.AssertionFailedf("schema or database are set for database %d", id)
		}
	}
	return tree.NewTableNameWithSchema(tree.Name(dbName), tree.Name(scName), tree.Name(obj.GetName())).FQString(), nil
}

// NewCatalogChangeBatcher implements the scexec.Catalog interface.
func (s *TestState) NewCatalogChangeBatcher() scexec.CatalogChangeBatcher {
	return &testCatalogChangeBatcher{
		s:             s,
		namesToDelete: make(map[descpb.NameInfo]descpb.ID),
	}
}

type testCatalogChangeBatcher struct {
	s                   *TestState
	descs               []catalog.Descriptor
	namesToDelete       map[descpb.NameInfo]descpb.ID
	descriptorsToDelete catalog.DescriptorIDSet
	zoneConfigsToDelete catalog.DescriptorIDSet
}

var _ scexec.CatalogChangeBatcher = (*testCatalogChangeBatcher)(nil)

// CreateOrUpdateDescriptor implements the scexec.CatalogChangeBatcher interface.
func (b *testCatalogChangeBatcher) CreateOrUpdateDescriptor(
	ctx context.Context, desc catalog.MutableDescriptor,
) error {
	b.descs = append(b.descs, desc)
	return nil
}

// DeleteName implements the scexec.CatalogChangeBatcher interface.
func (b *testCatalogChangeBatcher) DeleteName(
	ctx context.Context, nameInfo descpb.NameInfo, id descpb.ID,
) error {
	b.namesToDelete[nameInfo] = id
	return nil
}

// DeleteDescriptor implements the scexec.CatalogChangeBatcher interface.
func (b *testCatalogChangeBatcher) DeleteDescriptor(ctx context.Context, id descpb.ID) error {
	b.descriptorsToDelete.Add(id)
	return nil
}

// DeleteZoneConfig implements the scexec.CatalogChangeBatcher interface.
func (b *testCatalogChangeBatcher) DeleteZoneConfig(ctx context.Context, id descpb.ID) error {
	b.zoneConfigsToDelete.Add(id)
	return nil
}

// ValidateAndRun implements the scexec.CatalogChangeBatcher interface.
func (b *testCatalogChangeBatcher) ValidateAndRun(ctx context.Context) error {
	names := make([]descpb.NameInfo, 0, len(b.namesToDelete))
	for nameInfo := range b.namesToDelete {
		names = append(names, nameInfo)
	}
	sort.Slice(names, func(i, j int) bool {
		return b.namesToDelete[names[i]] < b.namesToDelete[names[j]]
	})
	for _, nameInfo := range names {
		expectedID := b.namesToDelete[nameInfo]
		ne := b.s.uncommitted.LookupNamespaceEntry(nameInfo)
		if ne == nil {
			return errors.AssertionFailedf(
				"cannot delete missing namespace entry %v", nameInfo)
		}

		if actualID := ne.GetID(); actualID != expectedID {
			return errors.AssertionFailedf(
				"expected deleted namespace entry %v to have ID %d, instead is %d", nameInfo, expectedID, actualID)
		}
		nameType := "object"
		if nameInfo.ParentSchemaID == 0 {
			if nameInfo.ParentID == 0 {
				nameType = "database"
			} else {
				nameType = "schema"
			}
		}
		b.s.LogSideEffectf("delete %s namespace entry %v -> %d", nameType, nameInfo, expectedID)
		b.s.uncommitted.DeleteNamespaceEntry(nameInfo)
	}
	for _, desc := range b.descs {
		b.s.LogSideEffectf("upsert descriptor #%d\n%s", desc.GetID(), b.s.descriptorDiff(desc))
		b.s.uncommitted.UpsertDescriptorEntry(desc)
	}
	for _, deletedID := range b.descriptorsToDelete.Ordered() {
		b.s.LogSideEffectf("delete descriptor #%d", deletedID)
		b.s.uncommitted.DeleteDescriptorEntry(deletedID)
	}
	for _, deletedID := range b.zoneConfigsToDelete.Ordered() {
		b.s.LogSideEffectf("deleting zone config for #%d", deletedID)
	}
	ve := b.s.uncommitted.Validate(
		ctx,
		clusterversion.TestingClusterVersion,
		catalog.NoValidationTelemetry,
		catalog.ValidationLevelAllPreTxnCommit,
		b.descs...,
	)
	return ve.CombinedError()
}

// IndexSpanSplitter implements the scexec.Dependencies interface.
func (s *TestState) IndexSpanSplitter() scexec.IndexSpanSplitter {
	return s.indexSpanSplitter
}

// IndexBackfiller implements the scexec.Dependencies interface.
func (s *TestState) IndexBackfiller() scexec.Backfiller {
	return s.backfiller
}

// IndexMerger implements the scexec.Dependencies interface.
func (s *TestState) IndexMerger() scexec.Merger {
	return s.merger
}

// PeriodicProgressFlusher implements the scexec.Dependencies interface.
func (s *TestState) PeriodicProgressFlusher() scexec.PeriodicProgressFlusher {
	return scdeps.NewNoopPeriodicProgressFlusher()
}

// TransactionalJobRegistry implements the scexec.Dependencies interface.
func (s *TestState) TransactionalJobRegistry() scexec.TransactionalJobRegistry {
	return s
}

var _ scexec.TransactionalJobRegistry = (*TestState)(nil)

// CreateJob implements the scexec.TransactionalJobRegistry interface.
func (s *TestState) CreateJob(ctx context.Context, record jobs.Record) error {
	if record.JobID == 0 {
		return errors.New("invalid 0 job ID")
	}
	record.JobID = jobspb.JobID(1 + len(s.jobs))
	s.createdJobsInCurrentTxn = append(s.createdJobsInCurrentTxn, record.JobID)
	s.jobs = append(s.jobs, record)
	s.LogSideEffectf("create job #%d (non-cancelable: %v): %q\n  descriptor IDs: %v",
		record.JobID,
		record.NonCancelable,
		record.Description,
		record.DescriptorIDs,
	)
	return nil
}

// CreatedJobs implements the scexec.TransactionalJobRegistry interface.
func (s *TestState) CreatedJobs() []jobspb.JobID {
	return s.createdJobsInCurrentTxn
}

// CheckPausepoint is a no-op.
func (s *TestState) CheckPausepoint(name string) error {
	return nil
}

// UseLegacyGCJob is false.
func (s *TestState) UseLegacyGCJob(ctx context.Context) bool {
	return false
}

// UpdateSchemaChangeJob implements the scexec.TransactionalJobRegistry interface.
func (s *TestState) UpdateSchemaChangeJob(
	ctx context.Context, id jobspb.JobID, fn scexec.JobUpdateCallback,
) error {
	var scJob *jobs.Record
	for i, job := range s.jobs {
		if job.JobID == id {
			scJob = &s.jobs[i]
		}
	}
	if scJob == nil {
		return errors.AssertionFailedf("schema change job not found")
	}
	oldProgress := jobspb.Progress{
		Progress:       nil,
		ModifiedMicros: 0,
		RunningStatus:  "",
		Details:        jobspb.WrapProgressDetails(scJob.Progress),
		TraceID:        0,
	}
	oldPayload := jobspb.Payload{
		Description:                  scJob.Description,
		Statement:                    scJob.Statements,
		UsernameProto:                scJob.Username.EncodeProto(),
		StartedMicros:                0,
		FinishedMicros:               0,
		DescriptorIDs:                scJob.DescriptorIDs,
		Error:                        "",
		ResumeErrors:                 nil,
		CleanupErrors:                nil,
		FinalResumeError:             nil,
		Noncancelable:                scJob.NonCancelable,
		Details:                      jobspb.WrapPayloadDetails(scJob.Details),
		PauseReason:                  "",
		RetriableExecutionFailureLog: nil,
	}
	oldJobMetadata := jobs.JobMetadata{
		ID:       scJob.JobID,
		Status:   jobs.StatusRunning,
		Payload:  &oldPayload,
		Progress: &oldProgress,
		RunStats: nil,
	}
	updateProgress := func(newProgress *jobspb.Progress) {
		scJob.Progress = *newProgress.GetNewSchemaChange()
		s.LogSideEffectf("update progress of schema change job #%d: %q", scJob.JobID, newProgress.RunningStatus)
	}
	updatePayload := func(newPayload *jobspb.Payload) {
		if newPayload.Noncancelable {
			scJob.NonCancelable = true
			s.LogSideEffectf("set schema change job #%d to non-cancellable", scJob.JobID)
		}
		newIDs := fmt.Sprintf("%v", newPayload.DescriptorIDs)
		if oldIDs := fmt.Sprintf("%v", oldPayload.DescriptorIDs); oldIDs != newIDs {
			s.LogSideEffectf("updated schema change job #%d descriptor IDs to %s", scJob.JobID, newIDs)
		}
	}
	return fn(oldJobMetadata, updateProgress, updatePayload)
}

// MakeJobID implements the scexec.TransactionalJobRegistry interface.
func (s *TestState) MakeJobID() jobspb.JobID {
	if s.jobCounter == 0 {
		// Reserve 1 for the schema changer job.
		s.jobCounter = 1
	}
	s.jobCounter++
	return jobspb.JobID(s.jobCounter)
}

// SchemaChangerJobID implements the scexec.TransactionalJobRegistry
// interface.
func (s *TestState) SchemaChangerJobID() jobspb.JobID {
	return 1
}

// TestingKnobs exposes the testing knobs.
func (s *TestState) TestingKnobs() *scexec.TestingKnobs {
	return s.testingKnobs
}

// Phase implements the scexec.Dependencies interface.
func (s *TestState) Phase() scop.Phase {
	return s.phase
}

// User implements the scrun.SchemaChangeJobCreationDependencies interface.
func (s *TestState) User() username.SQLUsername {
	return username.RootUserName()
}

var _ scrun.JobRunDependencies = (*TestState)(nil)

// WithTxnInJob implements the scrun.JobRunDependencies interface.
func (s *TestState) WithTxnInJob(ctx context.Context, fn scrun.JobTxnFunc) (err error) {
	s.WithTxn(func(s *TestState) { err = fn(ctx, s) })
	return err
}

// ValidateForwardIndexes implements the index validator interface.
func (s *TestState) ValidateForwardIndexes(
	_ context.Context,
	tbl catalog.TableDescriptor,
	indexes []catalog.Index,
	_ sessiondata.InternalExecutorOverride,
) error {
	ids := make([]descpb.IndexID, len(indexes))
	for i, idx := range indexes {
		ids[i] = idx.GetID()
	}
	s.LogSideEffectf("validate forward indexes %v in table #%d", ids, tbl.GetID())
	return nil
}

// ValidateInvertedIndexes implements the index validator interface.
func (s *TestState) ValidateInvertedIndexes(
	_ context.Context,
	tbl catalog.TableDescriptor,
	indexes []catalog.Index,
	_ sessiondata.InternalExecutorOverride,
) error {
	ids := make([]descpb.IndexID, len(indexes))
	for i, idx := range indexes {
		ids[i] = idx.GetID()
	}
	s.LogSideEffectf("validate inverted indexes %v in table #%d", ids, tbl.GetID())
	return nil
}

// IndexValidator implements the scexec.Dependencies interface.
func (s *TestState) IndexValidator() scexec.IndexValidator {
	return s
}

// LogEvent implements scexec.EventLogger.
func (s *TestState) LogEvent(
	_ context.Context, details eventpb.CommonSQLEventDetails, event logpb.EventPayload,
) error {
	s.LogSideEffectf("write %T to event log: %s", event, details.Statement)
	return nil
}

// LogEventForSchemaChange implements scexec.EventLogger
func (s *TestState) LogEventForSchemaChange(ctx context.Context, event logpb.EventPayload) error {
	s.LogSideEffectf("write %T to event log", event)
	return nil
}

// EventLogger implements scexec.Dependencies.
func (s *TestState) EventLogger() scexec.EventLogger {
	return s
}

// UpsertDescriptorComment implements scexec.DescriptorMetadataUpdater.
func (s *TestState) UpsertDescriptorComment(
	id int64, subID int64, commentType keys.CommentType, comment string,
) error {
	s.LogSideEffectf("upsert %s comment for descriptor #%d of type %s",
		comment, id, commentType)
	return nil
}

// DeleteAllCommentsForTables implements scexec.DescriptorMetadataUpdater.
func (s *TestState) DeleteAllCommentsForTables(ids catalog.DescriptorIDSet) error {
	s.LogSideEffectf("delete all comments for table descriptors %v", ids.Ordered())
	return nil
}

// DeleteDescriptorComment implements scexec.DescriptorMetadataUpdater.
func (s *TestState) DeleteDescriptorComment(
	id int64, subID int64, commentType keys.CommentType,
) error {
	s.LogSideEffectf("delete comment for descriptor #%d of type %s",
		id, commentType)
	return nil
}

// UpsertConstraintComment implements scexec.DescriptorMetadataUpdater.
func (s *TestState) UpsertConstraintComment(
	tableID descpb.ID, constraintID descpb.ConstraintID, comment string,
) error {
	s.LogSideEffectf("upsert comment %s for constraint on #%d, constraint id: %d"+
		comment, tableID, constraintID)
	return nil
}

// DeleteConstraintComment implements scexec.DescriptorMetadataUpdater.
func (s *TestState) DeleteConstraintComment(
	tableID descpb.ID, constraintID descpb.ConstraintID,
) error {
	s.LogSideEffectf("delete comment for constraint on #%d, constraint id: %d",
		tableID, constraintID)
	return nil
}

// DeleteDatabaseRoleSettings implements scexec.DescriptorMetadataUpdater.
func (s *TestState) DeleteDatabaseRoleSettings(_ context.Context, dbID descpb.ID) error {
	s.LogSideEffectf("delete role settings for database on #%d", dbID)
	return nil
}

// SwapDescriptorSubComment implements scexec.DescriptorMetadataUpdater.
func (s *TestState) SwapDescriptorSubComment(
	id int64, oldSubID int64, newSubID int64, commentType keys.CommentType,
) error {
	s.LogSideEffectf("swapping sub comments on descriptor %d from "+
		"%d to %d of type %s",
		id,
		oldSubID,
		newSubID,
		commentType)
	return nil
}

// DeleteSchedule implements scexec.DescriptorMetadataUpdater
func (s *TestState) DeleteSchedule(ctx context.Context, id int64) error {
	s.LogSideEffectf("delete scheduleId: %d", id)
	return nil
}

// DeleteZoneConfig implements scexec.DescriptorMetadataUpdater.
func (s *TestState) DeleteZoneConfig(
	ctx context.Context, id descpb.ID,
) (numAffectedRows int, err error) {
	if _, ok := s.zoneConfigs[id]; !ok {
		return 0, nil
	}
	delete(s.zoneConfigs, id)
	return 1, nil
}

// UpsertZoneConfig implements scexec.DescriptorMetadataUpdater.
func (s *TestState) UpsertZoneConfig(
	ctx context.Context, id descpb.ID, zone *zonepb.ZoneConfig,
) (numAffected int, err error) {
	s.zoneConfigs[id] = zone
	return 1, nil
}

// DescriptorMetadataUpdater implement scexec.Dependencies.
func (s *TestState) DescriptorMetadataUpdater(
	ctx context.Context,
) scexec.DescriptorMetadataUpdater {
	return s
}

// IsTableEmpty implement scbuild.TableReader.
func (s *TestState) IsTableEmpty(
	ctx context.Context, id descpb.ID, primaryIndexID descpb.IndexID,
) bool {
	return true
}

// TableReader implement scexec.Dependencies.
func (s *TestState) TableReader() scbuild.TableReader {
	return s
}

// StatsRefresher implement scexec.Dependencies.
func (s *TestState) StatsRefresher() scexec.StatsRefreshQueue {
	return s
}

// Telemetry implement scexec.Dependencies.
func (s *TestState) Telemetry() scexec.Telemetry {
	return s
}

// IncrementSchemaChangeErrorType implements scexec.Telemetry
func (s *TestState) IncrementSchemaChangeErrorType(typ string) {
	s.LogSideEffectf("incrementing schema change error type metric %s", typ)
}

// GetTestingKnobs implement scexec.Dependencies.
func (s *TestState) GetTestingKnobs() *scexec.TestingKnobs {
	return &scexec.TestingKnobs{}
}

// AddTableForStatsRefresh implements scexec.StatsRefreshQueue
func (s *TestState) AddTableForStatsRefresh(id descpb.ID) {
	s.LogSideEffectf("adding table for stats refresh: %d", id)
}

// ResolveFunction implements the scbuild.CatalogReader interface.
func (s *TestState) ResolveFunction(
	ctx context.Context, name *tree.UnresolvedName, path tree.SearchPath,
) (*tree.ResolvedFunctionDefinition, error) {
	// TODO(chengxiong): add UDF support for test.
	fn, err := name.ToFunctionName()
	if err != nil {
		return nil, err
	}
	fd, err := tree.GetBuiltinFuncDefinitionOrFail(fn, path)
	if err != nil {
		return nil, err
	}
	return fd, nil
}

// ResolveFunctionByOID implements the scbuild.CatalogReader interface.
func (s *TestState) ResolveFunctionByOID(
	ctx context.Context, oid oid.Oid,
) (string, *tree.Overload, error) {
	// TODO(chengxiong): add UDF support for test.
	name, ok := tree.OidToBuiltinName[oid]
	if !ok {
		return "", nil, errors.Newf("function %d not found", oid)
	}
	funcDef := tree.FunDefs[name]
	for _, o := range funcDef.Definition {
		if o.Oid == oid {
			return funcDef.Name, o, nil
		}
	}
	return "", nil, errors.Newf("function %d not found", oid)
}

// ZoneConfigGetter implement scexec.Dependencies.
func (s *TestState) ZoneConfigGetter() scbuild.ZoneConfigGetter {
	return s
}

// GetZoneConfig implements scexec.Dependencies.
func (s *TestState) GetZoneConfig(ctx context.Context, id descpb.ID) (*zonepb.ZoneConfig, error) {
	return s.zoneConfigs[id], nil
}
