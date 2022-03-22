// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigtestcluster

import (
	"context"
	gosql "database/sql"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigreconciler"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigtestutils"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
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
	serverutils.TestTenantInterface

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

// Reconciler returns the reconciler associated with the given tenant.
func (s *Tenant) Reconciler() spanconfig.Reconciler {
	return s.reconciler
}

// KVAccessorRecorder returns the underlying recorder capturing KVAccessor
// mutations made by the tenant.
func (s *Tenant) KVAccessorRecorder() *spanconfigtestutils.KVAccessorRecorder {
	return s.recorder
}

// WithMutableTableDescriptor invokes the provided callback with a mutable table
// descriptor, changes to which are then committed back to the system. The
// callback needs to be idempotent.
func (s *Tenant) WithMutableTableDescriptor(
	ctx context.Context, dbName string, tbName string, f func(*tabledesc.Mutable),
) {
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	require.NoError(s.t, sql.DescsTxn(ctx, &execCfg, func(
		ctx context.Context, txn *kv.Txn, descsCol *descs.Collection,
	) error {
		_, desc, err := descsCol.GetMutableTableByName(
			ctx,
			txn,
			tree.NewTableNameWithSchema(tree.Name(dbName), "public", tree.Name(tbName)),
			tree.ObjectLookupFlags{
				CommonLookupFlags: tree.CommonLookupFlags{
					Required:       true,
					IncludeOffline: true,
				},
			},
		)
		if err != nil {
			return err
		}
		f(desc)
		return descsCol.WriteDesc(ctx, false, desc, txn)
	}))
}

// descLookupFlags is the set of look up flags used when fetching descriptors.
var descLookupFlags = tree.CommonLookupFlags{
	IncludeDropped: true,
	IncludeOffline: true,
	AvoidLeased:    true, // we want consistent reads
}

// LookupTableDescriptorByID returns the table identified by the given ID.
func (s *Tenant) LookupTableDescriptorByID(
	ctx context.Context, id descpb.ID,
) (desc catalog.TableDescriptor) {
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	require.NoError(s.t, sql.DescsTxn(ctx, &execCfg, func(
		ctx context.Context, txn *kv.Txn, descsCol *descs.Collection,
	) error {
		var err error
		desc, err = descsCol.GetImmutableTableByID(ctx, txn, id,
			tree.ObjectLookupFlags{
				CommonLookupFlags: descLookupFlags,
			},
		)
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
		ctx context.Context, txn *kv.Txn, descsCol *descs.Collection,
	) error {
		var err error
		_, desc, err = descsCol.GetImmutableTableByName(ctx, txn,
			tree.NewTableNameWithSchema(tree.Name(dbName), "public", tree.Name(tbName)),
			tree.ObjectLookupFlags{
				CommonLookupFlags: descLookupFlags,
			},
		)
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
		func(ctx context.Context, txn *kv.Txn, descsCol *descs.Collection) error {
			var err error
			desc, err = descsCol.GetImmutableDatabaseByName(ctx, txn, dbName,
				tree.DatabaseLookupFlags{
					Required:       true,
					IncludeOffline: true,
					AvoidLeased:    true,
				},
			)
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
	require.NoError(s.t, s.ExecCfg().DB.Txn(ctx,
		func(ctx context.Context, txn *kv.Txn) (err error) {
			require.Len(s.t, recordID, 1,
				"datadriven test only supports single character record IDs")
			recID, err := uuid.FromBytes([]byte(strings.Repeat(recordID, 16)))
			require.NoError(s.t, err)
			rec := jobsprotectedts.MakeRecord(recID, int64(jobID),
				hlc.Timestamp{WallTime: int64(protectTS)}, nil, /* deprecatedSpans */
				jobsprotectedts.Jobs, target)
			return s.ProtectedTimestampProvider().Protect(ctx, txn, rec)
		}))
	s.updateTimestampAfterLastSQLChange()
}

// ReleaseProtectedTimestampRecord will release a ptpb.Record.
func (s *Tenant) ReleaseProtectedTimestampRecord(ctx context.Context, recordID string) {
	require.NoError(s.t, s.ExecCfg().DB.Txn(ctx,
		func(ctx context.Context, txn *kv.Txn) error {
			require.Len(s.t, recordID, 1,
				"datadriven test only supports single character record IDs")
			recID, err := uuid.FromBytes([]byte(strings.Repeat(recordID, 16)))
			require.NoError(s.t, err)
			return s.ProtectedTimestampProvider().Release(ctx, txn, recID)
		}))
	s.updateTimestampAfterLastSQLChange()
}
