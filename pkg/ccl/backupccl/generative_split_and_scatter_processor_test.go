// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

// TestRunGenerativeSplitAndScatterContextCancel verifies that
// runGenerativeSplitAndScatter can be interrupted by canceling the supplied
// context. This test would time out if the context cancellation does not
// interrupt the function.
func TestRunGenerativeSplitAndScatterContextCancel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1000
	const localFoo = "nodelocal://1/foo"
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	tc, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts,
		InitManualReplication)
	defer cleanupFn()

	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)

	testDiskMonitor := execinfra.NewTestDiskMonitor(ctx, st)
	defer testDiskMonitor.Stop(ctx)

	// Set up the test so that the test context is canceled after the first entry
	// has been processed by the generative split and scatterer.
	s0 := tc.Server(0)
	registry := tc.Server(0).JobRegistry().(*jobs.Registry)
	execCfg := s0.ExecutorConfig().(sql.ExecutorConfig)
	flowCtx := execinfra.FlowCtx{
		Cfg: &execinfra.ServerConfig{
			Settings:       st,
			DB:             s0.InternalDB().(descs.DB),
			JobRegistry:    registry,
			ExecutorConfig: &execCfg,
			TestingKnobs: execinfra.TestingKnobs{
				BackupRestoreTestingKnobs: &sql.BackupRestoreTestingKnobs{
					RunAfterSplitAndScatteringEntry: func(ctx context.Context) {
						cancel()
					},
				},
			},
		},
		EvalCtx:     &evalCtx,
		Mon:         evalCtx.TestingMon,
		DiskMonitor: testDiskMonitor,
		NodeID:      evalCtx.NodeID,
	}

	sqlDB.Exec(t, `SET CLUSTER SETTING bulkio.backup.file_size = '1'`)
	sqlDB.Exec(t, `BACKUP INTO $1`, localFoo)

	backups := sqlDB.QueryStr(t, `SHOW BACKUPS IN $1`, localFoo)
	require.Equal(t, 1, len(backups))
	uri := localFoo + "/" + backups[0][0]

	codec := keys.MakeSQLCodec(s0.RPCContext().TenantID)
	backupTableDesc := desctestutils.TestingGetPublicTableDescriptor(tc.Servers[0].DB(), codec, "data", "bank")
	backupStartKey := backupTableDesc.PrimaryIndexSpan(codec).Key

	spec := makeTestingGenerativeSplitAndScatterSpec(
		[]string{uri},
		[]roachpb.Span{{
			Key:    backupStartKey,
			EndKey: backupStartKey.PrefixEnd(),
		}},
	)

	oldID := backupTableDesc.GetID()
	newID := backupTableDesc.GetID() + 1
	newDesc := protoutil.Clone(backupTableDesc.TableDesc()).(*descpb.TableDescriptor)
	newDesc.ID = newID
	tableRekeys := []execinfrapb.TableRekey{
		{
			OldID:   uint32(oldID),
			NewDesc: mustMarshalDesc(t, newDesc),
		},
	}

	kr, err := MakeKeyRewriterFromRekeys(keys.SystemSQLCodec, tableRekeys, nil, false)
	require.NoError(t, err)

	chunkSplitScatterers := []splitAndScatterer{makeSplitAndScatterer(flowCtx.Cfg.DB.KV(), kr)}
	chunkEntrySpliterScatterers := []splitAndScatterer{makeSplitAndScatterer(flowCtx.Cfg.DB.KV(), kr)}

	cache := routingDatumCache{
		cache: make(map[roachpb.NodeID]rowenc.EncDatum),
	}

	// Large enough so doneScatterCh never blocks.
	doneScatterCh := make(chan entryNode, 1000)
	err = runGenerativeSplitAndScatter(ctx, &flowCtx, &spec, chunkSplitScatterers, chunkEntrySpliterScatterers, doneScatterCh, &cache)

	require.Error(t, err, "context canceled")
}

// TestRunGenerativeSplitAndScatterRandomizedDestOnFailScatter verifies that if
// a chunk fails to scatter, then the chunk will be sent to a random destination
// that we've already seen before.
func TestRunGenerativeSplitAndScatterRandomizedDestOnFailScatter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1000
	const localFoo = "nodelocal://0/foo"
	ctx := context.Background()
	tc, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts,
		InitManualReplication)
	defer cleanupFn()

	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)
	evalCtx.NodeID = base.NewSQLIDContainerForNode(tc.Server(0).RPCContext().NodeID)

	testDiskMonitor := execinfra.NewTestDiskMonitor(ctx, st)
	defer testDiskMonitor.Stop(ctx)

	s0 := tc.Server(0)
	registry := tc.Server(0).JobRegistry().(*jobs.Registry)
	execCfg := s0.ExecutorConfig().(sql.ExecutorConfig)
	flowCtx := execinfra.FlowCtx{
		Cfg: &execinfra.ServerConfig{
			Settings:       st,
			DB:             s0.InternalDB().(descs.DB),
			JobRegistry:    registry,
			ExecutorConfig: &execCfg,
		},
		EvalCtx:     &evalCtx,
		Mon:         evalCtx.TestingMon,
		DiskMonitor: testDiskMonitor,
		NodeID:      evalCtx.NodeID,
	}

	sqlDB.Exec(t, `SET CLUSTER SETTING bulkio.backup.file_size = '1'`)
	sqlDB.Exec(t, `BACKUP INTO $1`, localFoo)

	backups := sqlDB.QueryStr(t, `SHOW BACKUPS IN $1`, localFoo)
	require.Equal(t, 1, len(backups))
	uri := localFoo + "/" + backups[0][0]

	codec := keys.MakeSQLCodec(s0.RPCContext().TenantID)
	backupTableDesc := desctestutils.TestingGetPublicTableDescriptor(tc.Servers[0].DB(), codec, "data", "bank")
	backupStartKey := backupTableDesc.PrimaryIndexSpan(codec).Key

	spec := makeTestingGenerativeSplitAndScatterSpec(
		[]string{uri},
		[]roachpb.Span{{
			Key:    backupStartKey,
			EndKey: backupStartKey.PrefixEnd(),
		}},
	)

	// These split and scatterers will always fail the scatter and return 0 as the
	// chunk destination.
	chunkSplitScatterers := []splitAndScatterer{
		&scatterAlwaysFailsSplitScatterer{},
		&scatterAlwaysFailsSplitScatterer{},
		&scatterAlwaysFailsSplitScatterer{},
	}

	// Fake some entries in the routing datum cache for nodes that we've scattered
	// to already.
	cache := routingDatumCache{
		cache:   map[roachpb.NodeID]rowenc.EncDatum{8: {}, 9: {}},
		nodeIDs: []roachpb.NodeID{8, 9},
	}

	// Large enough so doneScatterCh never blocks.
	doneScatterCh := make(chan entryNode, 1000)
	err := runGenerativeSplitAndScatter(ctx, &flowCtx, &spec, chunkSplitScatterers, chunkSplitScatterers, doneScatterCh, &cache)
	require.NoError(t, err)

	close(doneScatterCh)
	var doneEntries []entryNode
	numEntriesByNode := make(map[roachpb.NodeID]int)

	for e := range doneScatterCh {
		doneEntries = append(doneEntries, e)
		numEntriesByNode[e.node]++
	}

	// There are at least 10 splits from the original backed up bank table.
	// Because file spans are end key inclusive, this should result in at least
	// 9 import spans. Sanity check this.
	require.GreaterOrEqual(t, len(doneEntries), 9)

	// The failed scatter chunks should be scattered to the nodes that have been
	// scattered to before and cached.
	for node, cnt := range numEntriesByNode {
		require.Contains(t, cache.nodeIDs, node)

		// Sanity check that we have at least a couple of entries per node.
		// This assertion is dependent on the hashing mechanism of the entries
		// and can break if it changes.
		require.GreaterOrEqual(t, cnt, 2)
	}
}

// scatterAlwaysFailsSplitScatterer always fails the scatter and returns 0 as
// the chunk destination.
type scatterAlwaysFailsSplitScatterer struct {
}

func (t *scatterAlwaysFailsSplitScatterer) split(
	ctx context.Context, codec keys.SQLCodec, splitKey roachpb.Key,
) error {
	return nil
}

func (t *scatterAlwaysFailsSplitScatterer) scatter(
	ctx context.Context, codec keys.SQLCodec, scatterKey roachpb.Key,
) (roachpb.NodeID, error) {
	return 0, nil
}

func makeTestingGenerativeSplitAndScatterSpec(
	backupURIs []string, requiredSpans []roachpb.Span,
) execinfrapb.GenerativeSplitAndScatterSpec {
	return execinfrapb.GenerativeSplitAndScatterSpec{
		ValidateOnly:         false,
		URIs:                 backupURIs,
		Encryption:           nil,
		EndTime:              hlc.Timestamp{},
		Spans:                requiredSpans,
		BackupLocalityInfo:   nil,
		HighWater:            nil,
		UserProto:            "",
		ChunkSize:            1,
		TargetSize:           1,
		NumEntries:           1,
		NumNodes:             1,
		JobID:                0,
		UseSimpleImportSpans: false,
	}
}
