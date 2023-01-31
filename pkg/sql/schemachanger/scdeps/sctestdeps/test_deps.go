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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdeps"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdeps/sctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec/scmutationexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
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

// IncrementSchemaChangeIndexCounter implements the scbuild.Dependencies interface.
func (s *TestState) IncrementSchemaChangeIndexCounter(counterType string) {
	s.LogSideEffectf("increment telemetry for sql.schema.%s_index", counterType)
}

var _ scbuild.AuthorizationAccessor = (*TestState)(nil)

// CheckPrivilege implements the scbuild.AuthorizationAccessor interface.
func (s *TestState) CheckPrivilege(
	ctx context.Context, privilegeObject privilege.Object, privilege privilege.Kind,
) error {
	return nil
}

// HasAdminRole implements the scbuild.AuthorizationAccessor interface.
func (s *TestState) HasAdminRole(ctx context.Context) (bool, error) {
	return true, nil
}

// HasOwnership implements the scbuild.AuthorizationAccessor interface.
func (s *TestState) HasOwnership(
	ctx context.Context, privilegeObject privilege.Object,
) (bool, error) {
	return true, nil
}

// HasPrivilege implements the scbuild.AuthorizationAccessor interface.
func (s *TestState) HasPrivilege(
	ctx context.Context,
	privilegeObject privilege.Object,
	privilege privilege.Kind,
	user username.SQLUsername,
) (bool, error) {
	return true, nil
}

// HasAnyPrivilege implements the scbuild.AuthorizationAccessor interface.
func (s *TestState) HasAnyPrivilege(
	ctx context.Context, privilegeObject privilege.Object,
) (bool, error) {
	return true, nil
}

// CheckPrivilegeForUser implements the scbuild.AuthorizationAccessor interface.
func (s *TestState) CheckPrivilegeForUser(
	ctx context.Context,
	privilegeObject privilege.Object,
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

func (s *TestState) MustResolvePrefix(
	ctx context.Context, name tree.ObjectNamePrefix,
) (catalog.DatabaseDescriptor, catalog.SchemaDescriptor) {
	db, sc := s.mayResolvePrefix(name)
	if sc == nil {
		panic(errors.AssertionFailedf("prefix %s does not exist", name.String()))
	}
	return db.(catalog.DatabaseDescriptor), sc.(catalog.SchemaDescriptor)
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
		prefix, tbl = s.MayResolveTable(ctx, *tableIndexName.Table.ToUnresolvedObjectName())
		if tbl == nil {
			return false, catalog.ResolvedObjectPrefix{}, nil, nil
		}
		idx = catalog.FindIndex(
			tbl,
			catalog.IndexOpts{AddMutations: true},
			func(idx catalog.Index) bool {
				return idx.GetName() == string(tableIndexName.Index)
			},
		)
	} else {
		db, schema := s.mayResolvePrefix(tableIndexName.Table.ObjectNamePrefix)
		prefix = catalog.ResolvedObjectPrefix{
			ExplicitDatabase: true,
			ExplicitSchema:   true,
			Database:         db.(catalog.DatabaseDescriptor),
			Schema:           schema.(catalog.SchemaDescriptor),
		}
		var objects nstree.Catalog
		if db != nil && schema != nil {
			objects = s.GetAllObjectsInSchema(ctx, prefix.Database, prefix.Schema)
		}
		_ = objects.ForEachDescriptor(func(desc catalog.Descriptor) error {
			var ok bool
			tbl, ok = desc.(catalog.TableDescriptor)
			if !ok {
				return nil
			}
			idx = catalog.FindIndex(
				tbl,
				catalog.IndexOpts{AddMutations: true},
				func(idx catalog.Index) bool {
					return idx.GetName() == string(tableIndexName.Index)
				},
			)
			if idx != nil {
				return iterutil.StopIteration()
			}
			return nil
		})
	}
	if idx != nil {
		return true, prefix, tbl, idx
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
	ne := s.uncommittedInMemory.LookupNamespaceEntry(key)
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

// GetAllObjectsInSchema implements the scbuild.CatalogReader interface.
func (s *TestState) GetAllObjectsInSchema(
	ctx context.Context, db catalog.DatabaseDescriptor, schema catalog.SchemaDescriptor,
) nstree.Catalog {
	var ret nstree.MutableCatalog
	_ = s.uncommittedInMemory.ForEachDescriptor(func(desc catalog.Descriptor) error {
		if desc.GetParentSchemaID() == schema.GetID() {
			ret.UpsertDescriptor(desc)
		}
		return nil
	})
	return ret.Catalog
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
	return typedesc.HydratedTFromDesc(ctx, &tn, typ, s)
}

// ResolveTypeByOID implements the scbuild.CatalogReader interface.
func (s *TestState) ResolveTypeByOID(ctx context.Context, oid oid.Oid) (*types.T, error) {
	return typedesc.ResolveHydratedTByOID(ctx, oid, s)
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

// GetAllSchemasInDatabase implements the scbuild.CatalogReader interface.
func (s *TestState) GetAllSchemasInDatabase(
	_ context.Context, database catalog.DatabaseDescriptor,
) nstree.Catalog {
	var ret nstree.MutableCatalog
	_ = s.uncommittedInMemory.ForEachDescriptor(func(desc catalog.Descriptor) error {
		if desc.GetParentID() == database.GetID() && desc.GetParentSchemaID() == descpb.InvalidID {
			ret.UpsertDescriptor(desc)
		}
		return nil
	})
	return ret.Catalog
}

// MustReadDescriptor implements the scbuild.CatalogReader interface.
func (s *TestState) MustReadDescriptor(ctx context.Context, id descpb.ID) catalog.Descriptor {
	desc, err := s.mustReadImmutableDescriptor(id)
	if err != nil {
		panic(err)
	}
	return desc
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
	u := s.uncommittedInMemory.LookupDescriptor(id)
	if u == nil {
		return nil, errors.Wrapf(catalog.ErrDescriptorNotFound, "reading mutable descriptor #%d", id)
	}
	c := s.committed.LookupDescriptor(id)
	return descbuilder.BuildMutable(c, u.DescriptorProto(), s.mvccTimestamp())
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

// CreateOrUpdateDescriptor implements the scexec.Catalog interface.
func (s *TestState) CreateOrUpdateDescriptor(
	ctx context.Context, desc catalog.MutableDescriptor,
) error {
	s.catalogChanges.descs = append(s.catalogChanges.descs, desc)
	return nil
}

// DeleteName implements the scexec.Catalog interface.
func (s *TestState) DeleteName(ctx context.Context, nameInfo descpb.NameInfo, id descpb.ID) error {
	if s.catalogChanges.namesToDelete == nil {
		s.catalogChanges.namesToDelete = make(map[descpb.NameInfo]descpb.ID)
	}
	s.catalogChanges.namesToDelete[nameInfo] = id
	return nil
}

// DeleteDescriptor implements the scexec.Catalog interface.
func (s *TestState) DeleteDescriptor(ctx context.Context, id descpb.ID) error {
	s.catalogChanges.descriptorsToDelete.Add(id)
	return nil
}

// DeleteZoneConfig implements the scexec.Catalog interface.
func (s *TestState) DeleteZoneConfig(ctx context.Context, id descpb.ID) error {
	s.catalogChanges.zoneConfigsToDelete.Add(id)
	return nil
}

// UpdateComment implements the scexec.Catalog interface.
func (s *TestState) UpdateComment(
	ctx context.Context, key catalogkeys.CommentKey, cmt string,
) error {
	if s.catalogChanges.commentsToUpdate == nil {
		s.catalogChanges.commentsToUpdate = make(map[catalogkeys.CommentKey]string)
	}
	s.catalogChanges.commentsToUpdate[key] = cmt
	return nil
}

// DeleteComment implements the scexec.Catalog interface.
func (s *TestState) DeleteComment(ctx context.Context, key catalogkeys.CommentKey) error {
	if s.catalogChanges.commentsToUpdate == nil {
		s.catalogChanges.commentsToUpdate = make(map[catalogkeys.CommentKey]string)
	}
	s.catalogChanges.commentsToUpdate[key] = ""
	return nil
}

// Validate implements the scexec.Catalog interface.
func (s *TestState) Validate(ctx context.Context) error {
	names := make([]descpb.NameInfo, 0, len(s.catalogChanges.namesToDelete))
	for nameInfo := range s.catalogChanges.namesToDelete {
		names = append(names, nameInfo)
	}
	sort.Slice(names, func(i, j int) bool {
		return s.catalogChanges.namesToDelete[names[i]] < s.catalogChanges.namesToDelete[names[j]]
	})
	for _, nameInfo := range names {
		expectedID := s.catalogChanges.namesToDelete[nameInfo]
		ne := s.uncommittedInMemory.LookupNamespaceEntry(nameInfo)
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
		s.LogSideEffectf("delete %s namespace entry %v -> %d", nameType, nameInfo, expectedID)
		s.uncommittedInMemory.DeleteByName(nameInfo)
	}
	for _, desc := range s.catalogChanges.descs {
		mut := desc.NewBuilder().BuildCreatedMutable()
		mut.ResetModificationTime()
		desc = mut.ImmutableCopy()
		s.LogSideEffectf("upsert descriptor #%d\n%s", desc.GetID(), s.descriptorDiff(desc))
		s.uncommittedInMemory.UpsertDescriptor(desc)
	}
	for _, deletedID := range s.catalogChanges.descriptorsToDelete.Ordered() {
		s.LogSideEffectf("delete descriptor #%d", deletedID)
		s.uncommittedInMemory.DeleteByID(deletedID)
	}
	for _, deletedID := range s.catalogChanges.zoneConfigsToDelete.Ordered() {
		s.LogSideEffectf("deleting zone config for #%d", deletedID)
		s.uncommittedInMemory.DeleteZoneConfig(deletedID)
	}
	commentKeys := make([]catalogkeys.CommentKey, 0, len(s.catalogChanges.commentsToUpdate))
	for key := range s.catalogChanges.commentsToUpdate {
		commentKeys = append(commentKeys, key)
	}
	sort.Slice(commentKeys, func(i, j int) bool {
		if d := int(commentKeys[i].CommentType) - int(commentKeys[j].CommentType); d != 0 {
			return d < 0
		}
		if d := int(commentKeys[i].ObjectID) - int(commentKeys[j].ObjectID); d != 0 {
			return d < 0
		}
		return int(commentKeys[i].SubID)-int(commentKeys[j].SubID) < 0
	})
	for _, key := range commentKeys {
		if cmt := s.catalogChanges.commentsToUpdate[key]; cmt == "" {
			s.LogSideEffectf("delete comment %s(objID: %d, subID: %d)",
				key.CommentType, key.ObjectID, key.SubID)
			s.uncommittedInMemory.DeleteComment(key)
		} else {
			s.LogSideEffectf("upsert comment %s(objID: %d, subID: %d) -> %q",
				key.CommentType, key.ObjectID, key.SubID, cmt)
			s.uncommittedInMemory.UpsertComment(key, cmt)
		}
	}
	ve := s.uncommittedInMemory.Validate(
		ctx,
		clusterversion.TestingClusterVersion,
		catalog.NoValidationTelemetry,
		catalog.ValidationLevelAllPreTxnCommit,
		s.catalogChanges.descs...,
	)
	s.catalogChanges = catalogChanges{}
	return ve.CombinedError()
}

// Run implements the scexec.Catalog interface.
func (s *TestState) Run(ctx context.Context) error {
	s.LogSideEffectf("persist all catalog changes to storage")
	s.uncommittedInStorage = catalogDeepCopy(s.uncommittedInMemory.Catalog)
	s.catalogChanges = catalogChanges{}
	return nil
}

// Reset implements the scexec.Catalog interface.
func (s *TestState) Reset(ctx context.Context) error {
	s.LogSideEffectf("undo all catalog changes within txn #%d", s.txnCounter)
	s.uncommittedInMemory = catalogDeepCopy(s.committed.Catalog)
	s.catalogChanges = catalogChanges{}
	return nil
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

// CurrentJob implements the scexec.TransactionalJobRegistry
// interface.
func (s *TestState) CurrentJob() *jobs.Job {
	// Not implemented and only used for validation, which will never
	// use this object.
	return nil
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
	s.WithTxn(func(s *TestState) { err = fn(ctx, s, s) })
	return err
}

// ValidateForwardIndexes implements the validator interface.
func (s *TestState) ValidateForwardIndexes(
	_ context.Context,
	_ *jobs.Job,
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

// ValidateInvertedIndexes implements the validator interface.
func (s *TestState) ValidateInvertedIndexes(
	_ context.Context,
	_ *jobs.Job,
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

// Validator implements the scexec.Dependencies interface.
func (s *TestState) Validator() scexec.Validator {
	return s
}

// ValidateConstraint implements the validator interface.
func (s *TestState) ValidateConstraint(
	ctx context.Context,
	tbl catalog.TableDescriptor,
	constraint catalog.Constraint,
	indexIDForValidation descpb.IndexID,
	override sessiondata.InternalExecutorOverride,
) error {
	s.LogSideEffectf("validate %v constraint %v in table #%d",
		catalog.GetConstraintType(constraint), constraint.GetName(), tbl.GetID())
	return nil
}

func (s *TestState) ValidateForeignKeyConstraint(
	ctx context.Context,
	out catalog.TableDescriptor,
	in catalog.TableDescriptor,
	constraint catalog.Constraint,
	override sessiondata.InternalExecutorOverride,
) error {
	s.LogSideEffectf("validate foreign key constraint %v from table #%d to table #%d",
		constraint.GetName(), out.GetID(), in.GetID())
	return nil
}

// EventLogger implements scbuild.Dependencies.
func (s *TestState) EventLogger() scbuild.EventLogger {
	return s
}

var _ scbuild.EventLogger = (*TestState)(nil)

// LogEvent implements scbuild.EventLogger.
func (s *TestState) LogEvent(
	_ context.Context, details eventpb.CommonSQLEventDetails, event logpb.EventPayload,
) error {
	sqlCommon, ok := event.(eventpb.EventWithCommonSQLPayload)
	if !ok {
		return errors.AssertionFailedf("invalid event type, missing SQL payload: %T", event)
	}
	*sqlCommon.CommonSQLDetails() = details
	return s.logEvent(event)
}

// LogEventForSchemaChange implements scrun.EventLogger
func (s *TestState) LogEventForSchemaChange(_ context.Context, event logpb.EventPayload) error {
	_, ok := event.(eventpb.EventWithCommonSchemaChangePayload)
	if !ok {
		return errors.AssertionFailedf("invalid event type, missing schema change payload: %T", event)
	}
	return s.logEvent(event)
}

func (s *TestState) logEvent(event logpb.EventPayload) error {
	pb, ok := event.(protoutil.Message)
	if !ok {
		return errors.AssertionFailedf("invalid type, not a protobuf message: %T", event)
	}
	const emitDefaults = false
	yaml, err := sctestutils.ProtoToYAML(pb, emitDefaults, func(in interface{}) {
		// Remove common details from text output, they're never decorated.
		if inM, ok := in.(map[string]interface{}); ok {
			delete(inM, "common")
		}
	})
	if err != nil {
		return err
	}
	indented := strings.TrimSpace(strings.ReplaceAll(yaml, "\n", "\n  "))
	s.LogSideEffectf("write %T to event log:\n  %s", event, indented)
	return err
}

// DeleteDatabaseRoleSettings implements scexec.DescriptorMetadataUpdater.
func (s *TestState) DeleteDatabaseRoleSettings(_ context.Context, dbID descpb.ID) error {
	s.LogSideEffectf("delete role settings for database on #%d", dbID)
	return nil
}

// DeleteSchedule implements scexec.DescriptorMetadataUpdater
func (s *TestState) DeleteSchedule(ctx context.Context, id int64) error {
	s.LogSideEffectf("delete job schedule #%d", id)
	return nil
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
	fnName, err := name.ToFunctionName()
	if err != nil {
		return nil, err
	}
	fd, err := tree.GetBuiltinFuncDefinition(fnName, path)
	if err != nil {
		return nil, err
	}
	if fd != nil {
		return fd, nil
	}

	_, sc := s.mayResolvePrefix(fnName.ObjectNamePrefix)
	scDesc, err := catalog.AsSchemaDescriptor(sc)
	if err != nil {
		return nil, err
	}
	fd, found := scDesc.GetResolvedFuncDefinition(fnName.Object())
	if !found {
		return nil, tree.ErrFunctionUndefined
	}
	return fd, nil
}

// ResolveFunctionByOID implements the scbuild.CatalogReader interface.
func (s *TestState) ResolveFunctionByOID(
	ctx context.Context, oid oid.Oid,
) (string, *tree.Overload, error) {
	if !funcdesc.IsOIDUserDefinedFunc(oid) {
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

	fnID := funcdesc.UserDefinedFunctionOIDToID(oid)
	desc, err := s.mustReadImmutableDescriptor(fnID)
	if err != nil {
		return "", nil, err
	}
	fnDesc, err := catalog.AsFunctionDescriptor(desc)
	if err != nil {
		return "", nil, err
	}
	ol, err := fnDesc.ToOverload()
	if err != nil {
		return "", nil, err
	}
	return fnDesc.GetName(), ol, nil
}

// ZoneConfigGetter implements scexec.Dependencies.
func (s *TestState) ZoneConfigGetter() scbuild.ZoneConfigGetter {
	return s
}

// GetZoneConfig implements scexec.Dependencies.
func (s *TestState) GetZoneConfig(ctx context.Context, id descpb.ID) (catalog.ZoneConfig, error) {
	return s.uncommittedInMemory.LookupZoneConfig(id), nil
}

func (s *TestState) get(
	objID catid.DescID, subID uint32, commentType catalogkeys.CommentType,
) (comment string, ok bool) {
	commentKey := catalogkeys.MakeCommentKey(uint32(objID), subID, commentType)
	comment, ok = s.uncommittedInMemory.LookupComment(commentKey)
	return comment, ok
}

// GetDatabaseComment implements the scdecomp.CommentGetter interface.
func (s *TestState) GetDatabaseComment(dbID catid.DescID) (comment string, ok bool) {
	return s.get(dbID, 0, catalogkeys.DatabaseCommentType)
}

// GetSchemaComment implements the scdecomp.CommentGetter interface.
func (s *TestState) GetSchemaComment(schemaID catid.DescID) (comment string, ok bool) {
	return s.get(schemaID, 0, catalogkeys.SchemaCommentType)
}

// GetTableComment implements the scdecomp.CommentGetter interface.
func (s *TestState) GetTableComment(tableID catid.DescID) (comment string, ok bool) {
	return s.get(tableID, 0, catalogkeys.TableCommentType)
}

// GetColumnComment implements the scdecomp.CommentGetter interface.
func (s *TestState) GetColumnComment(
	tableID catid.DescID, pgAttrNum catid.PGAttributeNum,
) (comment string, ok bool) {
	return s.get(tableID, uint32(pgAttrNum), catalogkeys.ColumnCommentType)
}

// GetIndexComment implements the scdecomp.CommentGetter interface.
func (s *TestState) GetIndexComment(
	tableID catid.DescID, indexID catid.IndexID,
) (comment string, ok bool) {
	return s.get(tableID, uint32(indexID), catalogkeys.IndexCommentType)
}

// GetConstraintComment implements the scdecomp.CommentGetter interface.
func (s *TestState) GetConstraintComment(
	tableID catid.DescID, constraintID catid.ConstraintID,
) (comment string, ok bool) {
	return s.get(tableID, uint32(constraintID), catalogkeys.ConstraintCommentType)
}
