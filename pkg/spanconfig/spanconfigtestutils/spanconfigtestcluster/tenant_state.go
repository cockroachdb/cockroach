// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanconfigtestcluster

import (
	"context"
	gosql "database/sql"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigreconciler"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigtestutils"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

// Tenant captures per-tenant span config state and encapsulates convenient
// span config testing primitives. It's safe for concurrent use.
type Tenant struct {
	serverutils.ApplicationLayerInterface

	t          *testing.T
	db         *sqlutils.SQLRunner
	reconciler *spanconfigreconciler.Reconciler
	recorder   *spanconfigtestutils.KVAccessorRecorder
	cleanup    func()

	mu struct {
		syncutil.Mutex
		lastCheckpoint, tsAfterLastSQLChange hlc.Timestamp
	}
}

// ExecCfg returns a handle to the tenant's ExecutorConfig.
func (s *Tenant) ExecCfg() sql.ExecutorConfig {
	return s.ExecutorConfig().(sql.ExecutorConfig)
}

// ProtectedTimestampProvider returns a handle to the tenant's protected
// timestamp provider.
func (s *Tenant) ProtectedTimestampProvider() protectedts.Provider {
	return s.DistSQLServer().(*distsql.ServerImpl).ServerConfig.ProtectedTimestampProvider
}

// JobsRegistry returns a handle to the tenant's job registry.
func (s *Tenant) JobsRegistry() *jobs.Registry {
	return s.JobRegistry().(*jobs.Registry)
}

func (s *Tenant) updateTimestampAfterLastSQLChange() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.tsAfterLastSQLChange = s.Clock().Now()
}

// Exec is a wrapper around gosql.Exec that kills the test on error. It records
// the execution timestamp for subsequent use.
func (s *Tenant) Exec(query string, args ...interface{}) {
	s.db.Exec(s.t, query, args...)
	s.updateTimestampAfterLastSQLChange()
}

// ExecWithErr is like Exec but returns the error, if any. It records the
// execution timestamp for subsequent use.
func (s *Tenant) ExecWithErr(query string, args ...interface{}) error {
	_, err := s.db.DB.ExecContext(context.Background(), query, args...)
	s.updateTimestampAfterLastSQLChange()
	return err
}

// TimestampAfterLastSQLChange returns a timestamp after the last time Exec was
// invoked. It can be used for transactional ordering guarantees.
func (s *Tenant) TimestampAfterLastSQLChange() hlc.Timestamp {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.tsAfterLastSQLChange
}

// RecordCheckpoint is used to record the reconciliation checkpoint, retrievable
// via LastCheckpoint.
func (s *Tenant) RecordCheckpoint() {
	ts := s.Reconciler().Checkpoint()

	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.lastCheckpoint = ts
}

// LastCheckpoint returns the last recorded checkpoint timestamp.
func (s *Tenant) LastCheckpoint() hlc.Timestamp {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.lastCheckpoint
}

// Query is a wrapper around gosql.Query that kills the test on error.
func (s *Tenant) Query(query string, args ...interface{}) *gosql.Rows {
	return s.db.Query(s.t, query, args...)
}

// QueryRow is a wrapper around gosql.QueryRow that kills the test on error.
func (s *Tenant) QueryRow(query string, args ...interface{}) *sqlutils.Row {
	return s.db.QueryRow(s.t, query, args...)
}

// Reconciler returns the reconciler associated with the given tenant.
func (s *Tenant) Reconciler() spanconfig.Reconciler {
	return s.reconciler
}

// KVAccessorRecorder returns the underlying recorder capturing KVAccessor
// mutations made by the tenant.
func (s *Tenant) KVAccessorRecorder() *spanconfigtestutils.KVAccessorRecorder {
	return s.recorder
}

// WithMutableDatabaseDescriptor invokes the provided callback with a mutable
// database descriptor, changes to which are then committed back to the system.
// The callback needs to be idempotent.
func (s *Tenant) WithMutableDatabaseDescriptor(
	ctx context.Context, dbName string, f func(*dbdesc.Mutable),
) {
	descsDB := s.ExecutorConfig().(sql.ExecutorConfig).InternalDB
	require.NoError(s.t, descsDB.DescsTxn(ctx, func(
		ctx context.Context, txn descs.Txn,
	) error {
		imm, err := txn.Descriptors().ByName(txn.KV()).WithOffline().Get().Database(ctx, dbName)
		if err != nil {
			return err
		}
		mut, err := txn.Descriptors().MutableByID(txn.KV()).Database(ctx, imm.GetID())
		if err != nil {
			return err
		}
		f(mut)
		const kvTrace = false
		return txn.Descriptors().WriteDesc(ctx, kvTrace, mut, txn.KV())
	}))
}

// WithMutableTableDescriptor invokes the provided callback with a mutable table
// descriptor, changes to which are then committed back to the system. The
// callback needs to be idempotent.
func (s *Tenant) WithMutableTableDescriptor(
	ctx context.Context, dbName string, tbName string, f func(*tabledesc.Mutable),
) {
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	require.NoError(s.t, sql.DescsTxn(ctx, &execCfg, func(
		ctx context.Context, txn isql.Txn, descsCol *descs.Collection,
	) error {
		g := descsCol.ByName(txn.KV()).WithOffline().Get()
		tn := tree.NewTableNameWithSchema(tree.Name(dbName), "public", tree.Name(tbName))
		_, imm, err := descs.PrefixAndTable(ctx, g, tn)
		if err != nil {
			return err
		}
		mut, err := descsCol.MutableByID(txn.KV()).Table(ctx, imm.GetID())
		if err != nil {
			return err
		}
		f(mut)
		return descsCol.WriteDesc(ctx, false /* kvTrace */, mut, txn.KV())
	}))
}

// LookupTableDescriptorByID returns the table identified by the given ID.
func (s *Tenant) LookupTableDescriptorByID(
	ctx context.Context, id descpb.ID,
) (desc catalog.TableDescriptor) {
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	require.NoError(s.t, sql.DescsTxn(ctx, &execCfg, func(
		ctx context.Context, txn isql.Txn, descsCol *descs.Collection,
	) error {
		var err error
		desc, err = descsCol.ByIDWithoutLeased(txn.KV()).Get().Table(ctx, id)
		return err
	}))
	return desc
}

// LookupTableByName returns the table descriptor identified by the given name.
func (s *Tenant) LookupTableByName(
	ctx context.Context, dbName string, tbName string,
) (desc catalog.TableDescriptor) {
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	require.NoError(s.t, sql.DescsTxn(ctx, &execCfg, func(
		ctx context.Context, txn isql.Txn, descsCol *descs.Collection,
	) error {
		var err error
		g := descsCol.ByName(txn.KV()).WithOffline().MaybeGet()
		tn := tree.NewTableNameWithSchema(tree.Name(dbName), "public", tree.Name(tbName))
		_, desc, err = descs.PrefixAndTable(ctx, g, tn)
		return err
	}))
	return desc
}

// LookupDatabaseByName returns the database descriptor identified by the given
// name.
func (s *Tenant) LookupDatabaseByName(
	ctx context.Context, dbName string,
) (desc catalog.DatabaseDescriptor) {
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	require.NoError(s.t, sql.DescsTxn(ctx, &execCfg,
		func(ctx context.Context, txn isql.Txn, descsCol *descs.Collection) error {
			var err error
			desc, err = descsCol.ByName(txn.KV()).WithOffline().Get().Database(ctx, dbName)
			return err
		}))
	return desc
}

// MakeProtectedTimestampRecordAndProtect will construct a ptpb.Record, and
// persist it in the protected timestamp system table of the tenant.
func (s *Tenant) MakeProtectedTimestampRecordAndProtect(
	ctx context.Context, recordID string, protectTS int, target *ptpb.Target,
) {
	jobID := s.JobsRegistry().MakeJobID()
	require.NoError(s.t, s.ExecCfg().InternalDB.Txn(ctx,
		func(ctx context.Context, txn isql.Txn) (err error) {
			require.Len(s.t, recordID, 1,
				"datadriven test only supports single character record IDs")
			recID, err := uuid.FromBytes([]byte(strings.Repeat(recordID, 16)))
			require.NoError(s.t, err)
			rec := jobsprotectedts.MakeRecord(recID, int64(jobID),
				hlc.Timestamp{WallTime: int64(protectTS)}, nil, /* deprecatedSpans */
				jobsprotectedts.Jobs, target)
			return s.ProtectedTimestampProvider().WithTxn(txn).Protect(ctx, rec)
		}))
	s.updateTimestampAfterLastSQLChange()
}

// ReleaseProtectedTimestampRecord will release a ptpb.Record.
func (s *Tenant) ReleaseProtectedTimestampRecord(ctx context.Context, recordID string) {
	require.NoError(s.t, s.ExecCfg().InternalDB.Txn(ctx,
		func(ctx context.Context, txn isql.Txn) error {
			require.Len(s.t, recordID, 1,
				"datadriven test only supports single character record IDs")
			recID, err := uuid.FromBytes([]byte(strings.Repeat(recordID, 16)))
			require.NoError(s.t, err)
			return s.ProtectedTimestampProvider().WithTxn(txn).Release(ctx, recID)
		}))
	s.updateTimestampAfterLastSQLChange()
}
