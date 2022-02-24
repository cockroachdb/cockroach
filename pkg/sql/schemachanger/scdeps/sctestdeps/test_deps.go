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
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
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

var _ scbuild.AuthorizationAccessor = (*TestState)(nil)

// CheckPrivilege implements the scbuild.AuthorizationAccessor interface.
func (s *TestState) CheckPrivilege(
	ctx context.Context, descriptor catalog.Descriptor, privilege privilege.Kind,
) error {
	return nil
}

// HasAdminRole implements the scbuild.AuthorizationAccessor interface.
func (s *TestState) HasAdminRole(ctx context.Context) (bool, error) {
	return true, nil
}

// HasOwnership implements the scbuild.AuthorizationAccessor interface.
func (s *TestState) HasOwnership(ctx context.Context, descriptor catalog.Descriptor) (bool, error) {
	return true, nil
}

// IndexPartitioningCCLCallback implements the scbuild.Dependencies interface.
func (s *TestState) IndexPartitioningCCLCallback() scbuild.CreatePartitioningCCLCallback {
	if ccl := scdeps.CreatePartitioningCCL; ccl != nil {
		return ccl
	}
	return func(
		ctx context.Context,
		st *cluster.Settings,
		evalCtx *tree.EvalContext,
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
	id := s.catalog.LookupNamespaceEntry(key)
	if id == descpb.InvalidID {
		return nil
	}
	if id == keys.PublicSchemaID {
		return schemadesc.GetPublicSchema()
	}
	b := s.descBuilder(id)
	if b == nil {
		return nil
	}
	return b.BuildImmutable()
}

// ReadObjectNamesAndIDs implements the scbuild.CatalogReader interface.
func (s *TestState) ReadObjectNamesAndIDs(
	ctx context.Context, db catalog.DatabaseDescriptor, schema catalog.SchemaDescriptor,
) (names tree.TableNames, ids descpb.IDs) {
	m := make(map[string]descpb.ID)
	_ = s.catalog.ForEachNamespaceEntry(func(e catalog.NameEntry) error {
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
	err := database.ForEachNonDroppedSchema(func(id descpb.ID, name string) error {
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

func (s *TestState) mustReadMutableDescriptor(id descpb.ID) (catalog.MutableDescriptor, error) {
	if s.synthetic.LookupDescriptorEntry(id) != nil {
		return nil, errors.AssertionFailedf("attempted mutable access of synthetic descriptor %d", id)
	}
	b := s.descBuilder(id)
	if b == nil {
		return nil, errors.Wrapf(catalog.ErrDescriptorNotFound, "reading mutable descriptor #%d", id)
	}
	return b.BuildExistingMutable(), nil
}

func (s *TestState) mustReadImmutableDescriptor(id descpb.ID) (catalog.Descriptor, error) {
	b := s.descBuilderWithSynthetic(id)
	if b == nil {
		return nil, errors.Wrapf(catalog.ErrDescriptorNotFound, "reading immutable descriptor #%d", id)
	}
	return b.BuildImmutable(), nil
}

// descBuilder is used to ensure that the contents of descs are copied on read.
func (s *TestState) descBuilder(id descpb.ID) catalog.DescriptorBuilder {
	if desc := s.catalog.LookupDescriptorEntry(id); desc != nil {
		return desc.NewBuilder()
	}
	return nil
}

// descBuilder is used to ensure that the contents of descs are copied on read.
func (s *TestState) descBuilderWithSynthetic(id descpb.ID) catalog.DescriptorBuilder {
	if desc := s.synthetic.LookupDescriptorEntry(id); desc != nil {
		return desc.NewBuilder()
	}
	return s.descBuilder(id)
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

// AddSyntheticDescriptor implements the scmutationexec.CatalogReader interface.
func (s *TestState) AddSyntheticDescriptor(desc catalog.Descriptor) {
	s.synthetic.UpsertDescriptorEntry(desc)
}

// RemoveSyntheticDescriptor implements the scmutationexec.CatalogReader interface.
func (s *TestState) RemoveSyntheticDescriptor(id descpb.ID) {
	s.synthetic.DeleteDescriptorEntry(id)
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
		actualID := b.s.catalog.LookupNamespaceEntry(nameInfo)
		if actualID == descpb.InvalidID {
			return errors.AssertionFailedf(
				"cannot delete missing namespace entry %v", nameInfo)
		}
		if actualID != expectedID {
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
		b.s.catalog.DeleteNamespaceEntry(nameInfo)
	}
	for _, desc := range b.descs {
		var old protoutil.Message
		if b := b.s.descBuilder(desc.GetID()); b != nil {
			old = b.BuildImmutable().DescriptorProto()
		}
		diff := sctestutils.ProtoDiff(old, desc.DescriptorProto(), sctestutils.DiffArgs{
			Indent:       "  ",
			CompactLevel: 3,
		})
		b.s.LogSideEffectf("upsert descriptor #%d\n%s", desc.GetID(), diff)
		b.s.catalog.UpsertDescriptorEntry(desc)
	}
	for _, deletedID := range b.descriptorsToDelete.Ordered() {
		b.s.LogSideEffectf("delete descriptor #%d", deletedID)
		b.s.catalog.DeleteDescriptorEntry(deletedID)
	}
	for _, deletedID := range b.zoneConfigsToDelete.Ordered() {
		b.s.LogSideEffectf("deleting zone config for #%d", deletedID)
	}
	ve := b.s.catalog.Validate(ctx, clusterversion.TestingClusterVersion, catalog.NoValidationTelemetry, catalog.ValidationLevelAllPreTxnCommit, b.descs...)
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
	s.jobs = append(s.jobs, record)
	s.LogSideEffectf("create job #%d: %q\n  descriptor IDs: %v",
		record.JobID,
		record.Description,
		record.DescriptorIDs,
	)
	return nil
}

// CheckPausepoint is a no-op.
func (s *TestState) CheckPausepoint(name string) error {
	return nil
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
	progress := jobspb.Progress{
		Progress:       nil,
		ModifiedMicros: 0,
		RunningStatus:  "",
		Details:        jobspb.WrapProgressDetails(scJob.Progress),
		TraceID:        0,
	}
	payload := jobspb.Payload{
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
	updateProgress := func(progress *jobspb.Progress) {
		scJob.Progress = *progress.GetNewSchemaChange()
		s.LogSideEffectf("update progress of schema change job #%d", scJob.JobID)
	}
	setNonCancelable := func() {
		scJob.NonCancelable = true
		s.LogSideEffectf("set schema change job #%d to non-cancellable", scJob.JobID)
	}
	md := jobs.JobMetadata{
		ID:       scJob.JobID,
		Status:   jobs.StatusRunning,
		Payload:  &payload,
		Progress: &progress,
		RunStats: nil,
	}
	return fn(md, updateProgress, setNonCancelable)
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
func (s *TestState) TestingKnobs() *scrun.TestingKnobs {
	return s.testingKnobs
}

// Phase implements the scexec.Dependencies interface.
func (s *TestState) Phase() scop.Phase {
	return s.phase
}

// User implements the scrun.SchemaChangeJobCreationDependencies interface.
func (s *TestState) User() security.SQLUsername {
	return security.RootUserName()
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
	_ context.Context,
	descID descpb.ID,
	details eventpb.CommonSQLEventDetails,
	event eventpb.EventPayload,
) error {
	s.LogSideEffectf("write %T to event log for descriptor #%d: %s",
		event, descID, details.Statement)
	return nil
}

// EventLogger implements scexec.Dependencies.
func (s *TestState) EventLogger() scexec.EventLogger {
	return s
}

// UpsertDescriptorComment implements scexec.DescriptorMetaDataUpdater.
func (s *TestState) UpsertDescriptorComment(
	id int64, subID int64, commentType keys.CommentType, comment string,
) error {
	s.LogSideEffectf("upsert %s comment for descriptor #%d of type %s",
		comment, id, commentType)
	return nil
}

// DeleteDescriptorComment implements scexec.DescriptorMetaDataUpdater.
func (s *TestState) DeleteDescriptorComment(
	id int64, subID int64, commentType keys.CommentType,
) error {
	s.LogSideEffectf("delete comment for descriptor #%d of type %s",
		id, commentType)
	return nil
}

//UpsertConstraintComment implements scexec.DescriptorMetaDataUpdater.
func (s *TestState) UpsertConstraintComment(
	tableID descpb.ID, constraintID descpb.ConstraintID, comment string,
) error {
	s.LogSideEffectf("upsert comment %s for constraint on #%d, constraint id: %d"+
		comment, tableID, constraintID)
	return nil
}

//DeleteConstraintComment implements scexec.DescriptorMetaDataUpdater.
func (s *TestState) DeleteConstraintComment(
	tableID descpb.ID, constraintID descpb.ConstraintID,
) error {
	s.LogSideEffectf("delete comment for constraint on #%d, constraint id: %d",
		tableID, constraintID)
	return nil
}

// DeleteDatabaseRoleSettings implements scexec.DescriptorMetaDataUpdater.
func (s *TestState) DeleteDatabaseRoleSettings(_ context.Context, dbID descpb.ID) error {
	s.LogSideEffectf("delete role settings for database on #%d", dbID)
	return nil
}

// SwapDescriptorSubComment implements scexec.DescriptorMetaDataUpdater.
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

// DescriptorMetadataUpdater implement scexec.Dependencies.
func (s *TestState) DescriptorMetadataUpdater(
	ctx context.Context,
) scexec.DescriptorMetadataUpdater {
	return s
}
