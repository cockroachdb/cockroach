// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbackup_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbackup"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestCreateDeclarativeSchemaChangeJobs tests two scenarios around
// creating declarative schema changer jobs during RESTORE:
//
// 1. All involving targets have reached their target status. RESTORE will clear
// the declarative schema changer state in all involving descriptors and skip
// creating a declarative schema changer job.
//
// 2. A descriptor's declarative schema changer state is non-nil but empty, and
// this descriptor is not referenced in the targetState's elements (AllTargetDescIDs).
// RESTORE will clear the declarative schema changer state in such descriptor.
//
// This test is supplemental to the BACK/RESTORE test suites in declarative schema
// changer package which has already tested those two scenarios *implicitly* (see
// "create_index_create_schema_separate_statements"). This data-driven test will allow
// us to explicitly test them.
func TestCreateDeclarativeSchemaChangeJobs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	t.Run("all-targets-reached-their-target-status", func(t *testing.T) {
		mutCatalog := nstree.MutableCatalog{}
		dbDesc := dbdesc.NewBuilder(&descpb.DatabaseDescriptor{
			ID:   1,
			Name: "movr",
			DeclarativeSchemaChangerState: &scpb.DescriptorState{
				JobID:      1000,
				InRollback: true,
			},
		}).BuildExistingMutable()
		tableDesc := tabledesc.NewBuilder(&descpb.TableDescriptor{
			ID:   2,
			Name: "t",
			DeclarativeSchemaChangerState: &scpb.DescriptorState{
				JobID:      1000,
				InRollback: true,
				Targets: []scpb.Target{
					{
						ElementProto: scpb.ElementProto{ElementOneOf: &scpb.ElementProto_SecondaryIndex{}},
						Metadata:     scpb.TargetMetadata{SourceElementID: 1, SubWorkID: 1},
						TargetStatus: scpb.Status_ABSENT,
					},
				},
				CurrentStatuses: []scpb.Status{scpb.Status_ABSENT},
				TargetRanks:     []uint32{0},
			},
		}).BuildExistingMutable()
		mutCatalog.UpsertDescriptor(dbDesc)
		mutCatalog.UpsertDescriptor(tableDesc)

		execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
		err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			jobIDs, err := scbackup.CreateDeclarativeSchemaChangeJobs(ctx, execCfg.JobRegistry, txn, mutCatalog.Catalog)
			require.NoError(t, err)
			require.Equal(t, 0, len(jobIDs))
			require.Nil(t, mutCatalog.LookupDescriptor(1).GetDeclarativeSchemaChangerState())
			require.Nil(t, mutCatalog.LookupDescriptor(2).GetDeclarativeSchemaChangerState())
			return nil
		})
		require.NoError(t, err)
	})

	t.Run("empty-declarative-state-and-unreferenced-in-target-state", func(t *testing.T) {
		mutCatalog := nstree.MutableCatalog{}
		dbDesc := dbdesc.NewBuilder(&descpb.DatabaseDescriptor{
			ID:   1,
			Name: "movr",
			DeclarativeSchemaChangerState: &scpb.DescriptorState{
				JobID:      1000,
				InRollback: true,
			},
		}).BuildExistingMutable()
		tableDesc := tabledesc.NewBuilder(&descpb.TableDescriptor{
			ID:   2,
			Name: "t",
			DeclarativeSchemaChangerState: &scpb.DescriptorState{
				JobID:      1000,
				InRollback: true,
				Targets: []scpb.Target{
					{
						ElementProto: scpb.ElementProto{ElementOneOf: &scpb.ElementProto_SecondaryIndex{}},
						Metadata:     scpb.TargetMetadata{SourceElementID: 1, SubWorkID: 1},
						TargetStatus: scpb.Status_ABSENT,
					},
				},
				CurrentStatuses: []scpb.Status{scpb.Status_DELETE_ONLY},
				TargetRanks:     []uint32{0},
				Authorization:   scpb.Authorization{UserName: "root"},
			},
		}).BuildExistingMutable()
		mutCatalog.UpsertDescriptor(dbDesc)
		mutCatalog.UpsertDescriptor(tableDesc)

		execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
		err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			jobIDs, err := scbackup.CreateDeclarativeSchemaChangeJobs(ctx, execCfg.JobRegistry, txn, mutCatalog.Catalog)
			require.NoError(t, err)
			require.Equal(t, 1, len(jobIDs))
			require.Nil(t, mutCatalog.LookupDescriptor(1).GetDeclarativeSchemaChangerState())
			require.NotNil(t, mutCatalog.LookupDescriptor(2).GetDeclarativeSchemaChangerState())
			return nil
		})
		require.NoError(t, err)
	})
}
