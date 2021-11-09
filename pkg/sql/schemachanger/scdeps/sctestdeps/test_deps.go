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

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec/scmutationexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
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
	id, found := s.namespace[key]
	if !found {
		return nil
	}
	if id == keys.PublicSchemaID {
		return schemadesc.GetPublicSchema()
	}
	descriptorEntry := s.descriptors.GetByID(id)
	if descriptorEntry == nil {
		return nil
	}
	return descriptorEntry.(catalog.Descriptor)
}

// ReadObjectNamesAndIDs implements the scbuild.CatalogReader interface.
func (s *TestState) ReadObjectNamesAndIDs(
	ctx context.Context, db catalog.DatabaseDescriptor, schema catalog.SchemaDescriptor,
) (names tree.TableNames, ids descpb.IDs) {
	m := make(map[string]descpb.ID)
	for nameInfo, id := range s.namespace {
		if nameInfo.ParentID == db.GetID() && nameInfo.GetParentSchemaID() == schema.GetID() {
			m[nameInfo.Name] = id
			names = append(names, tree.MakeTableNameWithSchema(
				tree.Name(db.GetName()),
				tree.Name(schema.GetName()),
				tree.Name(nameInfo.Name),
			))
		}
	}
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
	db := s.descriptors.GetByID(obj.GetParentID())
	if db == nil {
		return nil, errors.Wrapf(catalog.ErrDescriptorNotFound, "parent database descriptor #%d", obj.GetParentID())
	}
	sc := s.descriptors.GetByID(obj.GetParentSchemaID())
	if sc == nil {
		return nil, errors.Wrapf(catalog.ErrDescriptorNotFound, "parent schema descriptor #%d", obj.GetParentSchemaID())
	}
	return tree.NewTableNameWithSchema(tree.Name(db.GetName()), tree.Name(sc.GetName()), tree.Name(obj.GetName())), nil
}

// CurrentDatabase implements the scbuild.CatalogReader interface.
func (s *TestState) CurrentDatabase() string {
	return s.currentDatabase
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
	entry := s.descriptors.GetByID(id)
	if entry == nil {
		return nil, errors.Wrapf(catalog.ErrDescriptorNotFound, "reading mutable descriptor #%d", id)
	}
	return entry.(catalog.MutableDescriptor), nil
}

func (s *TestState) mustReadImmutableDescriptor(id descpb.ID) (catalog.Descriptor, error) {
	syntheticEntry := s.syntheticDescriptors.GetByID(id)
	if syntheticEntry != nil {
		return syntheticEntry.(catalog.Descriptor), nil
	}
	entry := s.descriptors.GetByID(id)
	if entry != nil {
		return entry.(catalog.Descriptor), nil
	}
	return nil, errors.Wrapf(catalog.ErrDescriptorNotFound, "reading immutable descriptor #%d", id)
}

var _ scexec.Dependencies = (*TestState)(nil)

// Catalog implements the scexec.Dependencies interface.
func (s *TestState) Catalog() scexec.Catalog {
	return s
}

var _ scmutationexec.CatalogReader = (*TestState)(nil)

// MustReadImmutableDescriptor implements the scmutationexec.CatalogReader interface.
func (s *TestState) MustReadImmutableDescriptor(
	ctx context.Context, id descpb.ID,
) (catalog.Descriptor, error) {
	return s.mustReadMutableDescriptor(id)
}

// AddSyntheticDescriptor implements the scmutationexec.CatalogReader interface.
func (s *TestState) AddSyntheticDescriptor(desc catalog.Descriptor) {
	s.syntheticDescriptors.Upsert(desc)
}

// RemoveSyntheticDescriptor implements the scmutationexec.CatalogReader interface.
func (s *TestState) RemoveSyntheticDescriptor(id descpb.ID) {
	s.syntheticDescriptors.Remove(id)
}

var _ scexec.Catalog = (*TestState)(nil)

// MustReadMutableDescriptor implements the scexec.Catalog interface.
func (s *TestState) MustReadMutableDescriptor(
	ctx context.Context, id descpb.ID,
) (catalog.MutableDescriptor, error) {
	return s.mustReadMutableDescriptor(id)
}

// NewCatalogChangeBatcher implements the scexec.Catalog interface.
func (s *TestState) NewCatalogChangeBatcher() scexec.CatalogChangeBatcher {
	return &testCatalogChangeBatcher{
		s:             s,
		namesToDelete: make(map[descpb.NameInfo]descpb.ID),
	}
}

type testCatalogChangeBatcher struct {
	s             *TestState
	descs         []catalog.Descriptor
	namesToDelete map[descpb.NameInfo]descpb.ID
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

// ValidateAndRun implements the scexec.CatalogChangeBatcher interface.
func (b *testCatalogChangeBatcher) ValidateAndRun(ctx context.Context) error {
	for nameInfo, id := range b.namesToDelete {
		actualID, hasEntry := b.s.namespace[nameInfo]
		if !hasEntry {
			return errors.AssertionFailedf(
				"cannot delete missing namespace entry %v", nameInfo)
		}
		if actualID != id {
			return errors.AssertionFailedf(
				"expected deleted namespace entry %v to have ID %d, instead is %d", nameInfo, id, actualID)
		}
		delete(b.s.namespace, nameInfo)
	}
	for _, desc := range b.descs {
		b.s.descriptors.Upsert(desc)
	}
	return catalog.Validate(ctx, b.s, catalog.NoValidationTelemetry, catalog.ValidationLevelAllPreTxnCommit, b.descs...).CombinedError()
}

var _ catalog.DescGetter = (*TestState)(nil)

// GetDesc implements the catalog.DescGetter interface.
func (s *TestState) GetDesc(ctx context.Context, id descpb.ID) (catalog.Descriptor, error) {
	// Read mutable descriptor to bypass synthetic descriptors.
	return s.mustReadMutableDescriptor(id)
}

// GetNamespaceEntry implements the catalog.DescGetter interface.
func (s *TestState) GetNamespaceEntry(
	ctx context.Context, parentID, parentSchemaID descpb.ID, name string,
) (id descpb.ID, _ error) {
	id = s.namespace[descpb.NameInfo{
		ParentID:       parentID,
		ParentSchemaID: parentSchemaID,
		Name:           name,
	}]
	return id, nil
}

// IndexBackfiller implements the scexec.Dependencies interface.
func (s *TestState) IndexBackfiller() scexec.IndexBackfiller {
	return s
}

var _ scexec.IndexBackfiller = (*TestState)(nil)

// BackfillIndex implements the scexec.IndexBackfiller interface.
func (s *TestState) BackfillIndex(
	_ context.Context,
	_ scexec.JobProgressTracker,
	_ catalog.TableDescriptor,
	_ descpb.IndexID,
	_ ...descpb.IndexID,
) error {
	return nil
}

// IndexSpanSplitter implements the scexec.Dependencies interface.
func (s *TestState) IndexSpanSplitter() scexec.IndexSpanSplitter {
	return s
}

var _ scexec.IndexSpanSplitter = (*TestState)(nil)

// MaybeSplitIndexSpans implements the scexec.IndexSpanSplitter interface.
func (s *TestState) MaybeSplitIndexSpans(
	_ context.Context, _ catalog.TableDescriptor, _ catalog.Index,
) error {
	return nil
}

// JobProgressTracker implements the scexec.Dependencies interface.
func (s *TestState) JobProgressTracker() scexec.JobProgressTracker {
	return s
}

var _ scexec.JobProgressTracker = (*TestState)(nil)

// GetResumeSpans implements the scexec.JobProgressTracker interface.
func (s *TestState) GetResumeSpans(
	ctx context.Context, tableID descpb.ID, indexID descpb.IndexID,
) ([]roachpb.Span, error) {
	desc, err := s.mustReadImmutableDescriptor(tableID)
	if err != nil {
		return nil, err
	}
	table, err := catalog.AsTableDescriptor(desc)
	if err != nil {
		return nil, err
	}
	return []roachpb.Span{table.IndexSpan(s.Codec(), indexID)}, nil
}

// SetResumeSpans implements the scexec.JobProgressTracker interface.
func (s *TestState) SetResumeSpans(
	ctx context.Context, tableID descpb.ID, indexID descpb.IndexID, total, done []roachpb.Span,
) error {
	panic("implement me")
}

// TransactionalJobCreator implements the scexec.Dependencies interface.
func (s *TestState) TransactionalJobCreator() scexec.TransactionalJobCreator {
	return s
}

var _ scexec.TransactionalJobCreator = (*TestState)(nil)

// CreateJob implements the scexec.TransactionalJobCreator interface.
func (s *TestState) CreateJob(ctx context.Context, record jobs.Record) (jobspb.JobID, error) {
	record.JobID = jobspb.JobID(1 + len(s.jobs))
	s.jobs = append(s.jobs, record)
	return record.JobID, nil
}

// TestingKnobs implements the scexec.Dependencies interface.
func (s *TestState) TestingKnobs() *scexec.NewSchemaChangerTestingKnobs {
	return s.testingKnobs
}

// Phase implements the scexec.Dependencies interface.
func (s *TestState) Phase() scop.Phase {
	return s.phase
}

var _ scrun.SchemaChangeJobCreationDependencies = (*TestState)(nil)

// User implements the scrun.SchemaChangeJobCreationDependencies interface.
func (s *TestState) User() security.SQLUsername {
	return security.RootUserName()
}

var _ scrun.SchemaChangeJobExecutionDependencies = (*TestState)(nil)

// WithTxnInJob implements the scrun.SchemaChangeJobExecutionDependencies interface.
func (s *TestState) WithTxnInJob(
	ctx context.Context,
	fn func(ctx context.Context, txndeps scrun.SchemaChangeJobTxnDependencies) error,
) (err error) {
	s.WithTxn(func(s *TestState) {
		err = fn(ctx, s)
	})
	return err
}

var _ scrun.SchemaChangeJobTxnDependencies = (*TestState)(nil)

// UpdateSchemaChangeJob implements the scrun.SchemaChangeJobTxnDependencies interface.
func (s *TestState) UpdateSchemaChangeJob(
	ctx context.Context, fn func(md jobs.JobMetadata, ju scrun.JobProgressUpdater) error,
) error {
	var scjob *jobs.Record
	for i, job := range s.jobs {
		if job.Username == s.User() {
			scjob = &s.jobs[i]
			break
		}
	}
	if scjob == nil {
		return errors.AssertionFailedf("schema change job not found")
	}
	progress := jobspb.Progress{
		Progress:       nil,
		ModifiedMicros: 0,
		RunningStatus:  "",
		Details:        jobspb.WrapProgressDetails(scjob.Progress),
		TraceID:        0,
	}
	payload := jobspb.Payload{
		Description:                  scjob.Description,
		Statement:                    scjob.Statements,
		UsernameProto:                scjob.Username.EncodeProto(),
		StartedMicros:                0,
		FinishedMicros:               0,
		DescriptorIDs:                scjob.DescriptorIDs,
		Error:                        "",
		ResumeErrors:                 nil,
		CleanupErrors:                nil,
		FinalResumeError:             nil,
		Noncancelable:                false,
		Details:                      jobspb.WrapPayloadDetails(scjob.Details),
		PauseReason:                  "",
		RetriableExecutionFailureLog: nil,
	}
	ju := testJobUpdater{
		md: jobs.JobMetadata{
			ID:       scjob.JobID,
			Status:   jobs.StatusRunning,
			Payload:  &payload,
			Progress: &progress,
			RunStats: nil,
		},
	}
	err := fn(ju.md, &ju)
	if err != nil {
		return err
	}
	scjob.Progress = ju.md.Progress.GetNewSchemaChange()
	return nil
}

type testJobUpdater struct {
	md jobs.JobMetadata
}

var _ scrun.JobProgressUpdater = (*testJobUpdater)(nil)

// UpdateProgress implements the JobProgressUpdater interface
func (ju *testJobUpdater) UpdateProgress(progress *jobspb.Progress) {
	ju.md.Progress = progress
}

// ExecutorDependencies implements the scrun.SchemaChangeJobTxnDependencies interface.
func (s *TestState) ExecutorDependencies() scexec.Dependencies {
	return s
}
