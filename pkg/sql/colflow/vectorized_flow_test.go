// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colflow

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colflow/colrpc"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

type callbackRemoteComponentCreator struct {
	newOutboxFn func(*colmem.Allocator, colexecop.Operator, []*types.T, []colexecop.MetadataSource) (*colrpc.Outbox, error)
	newInboxFn  func(allocator *colmem.Allocator, typs []*types.T, streamID execinfrapb.StreamID) (*colrpc.Inbox, error)
}

func (c callbackRemoteComponentCreator) newOutbox(
	allocator *colmem.Allocator,
	input colexecop.Operator,
	typs []*types.T,
	_ func() []*execinfrapb.ComponentStats,
	metadataSources []colexecop.MetadataSource,
	_ []colexecop.Closer,
) (*colrpc.Outbox, error) {
	return c.newOutboxFn(allocator, input, typs, metadataSources)
}

func (c callbackRemoteComponentCreator) newInbox(
	allocator *colmem.Allocator, typs []*types.T, streamID execinfrapb.StreamID,
) (*colrpc.Inbox, error) {
	return c.newInboxFn(allocator, typs, streamID)
}

func intCols(numCols int) []*types.T {
	cols := make([]*types.T, numCols)
	for i := range cols {
		cols[i] = types.Int
	}
	return cols
}

// TestDrainOnlyInputDAG is a regression test for #39137 to ensure
// that queries don't hang using the following scenario:
// Consider two nodes n1 and n2, an outbox (o1) and inbox (i1) on n1, and an
// arbitrary flow on n2.
// At the end of the query, o1 will drain its metadata sources when it
// encounters a zero-length batch from its input. If one of these metadata
// sources is i1, there is a possibility that a cycle is unknowingly created
// since i1 (as an example) could be pulling from a remote operator that itself
// is pulling from o1, which is at this moment attempting to drain i1.
// This test verifies that no metadata sources are added to an outbox that are
// not explicitly known to be in its input DAG. The diagram below outlines
// the main point of this test. The outbox's input ends up being some inbox
// pulling from somewhere upstream (in this diagram, node 3, but this detail is
// not important). If it drains the depicted inbox, that is pulling from node 2
// which is in turn pulling from an outbox, a cycle is created and the flow is
// blocked.
//          +------------+
//          |  Node 3    |
//          +-----+------+
//                ^
//      Node 1    |           Node 2
// +------------------------+-----------------+
//          +------------+  |
//     Spec C +--------+ |  |
//          | |  noop  | |  |
//          | +---+----+ |  |
//          |     ^      |  |
//          |  +--+---+  |  |
//          |  |outbox|  +<----------+
//          |  +------+  |  |        |
//          +------------+  |        |
// Drain cycle!---+         |   +----+-----------------+
//                v         |   |Any group of operators|
//          +------------+  |   +----+-----------------+
//          |  +------+  |  |        ^
//     Spec A  |inbox +--------------+
//          |  +------+  |  |
//          +------------+  |
//                ^         |
//                |         |
//          +-----+------+  |
//     Spec B    noop    |  |
//          |materializer|  +
//          +------------+
func TestDrainOnlyInputDAG(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const (
		numInputTypesToOutbox       = 3
		numInputTypesToMaterializer = 1
	)
	// procs are the ProcessorSpecs that we pass in to create the flow. Note that
	// we order the inbox first so that the flow creator instantiates it before
	// anything else.
	procs := []execinfrapb.ProcessorSpec{
		{
			// This is i1, the inbox which should be drained by the materializer, not
			// o1.
			// Spec A in the diagram.
			Input: []execinfrapb.InputSyncSpec{
				{
					Streams:     []execinfrapb.StreamEndpointSpec{{Type: execinfrapb.StreamEndpointSpec_REMOTE, StreamID: 1}},
					ColumnTypes: intCols(numInputTypesToMaterializer),
				},
			},
			Core: execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
			Output: []execinfrapb.OutputRouterSpec{
				{
					Type: execinfrapb.OutputRouterSpec_PASS_THROUGH,
					// We set up a local output so that the inbox is created independently.
					Streams: []execinfrapb.StreamEndpointSpec{
						{Type: execinfrapb.StreamEndpointSpec_LOCAL, StreamID: 2},
					},
				},
			},
			ResultTypes: intCols(numInputTypesToMaterializer),
		},
		// This is the root of the flow. The noop operator that will read from i1
		// and the materializer.
		// Spec B in the diagram.
		{
			Input: []execinfrapb.InputSyncSpec{
				{
					Streams:     []execinfrapb.StreamEndpointSpec{{Type: execinfrapb.StreamEndpointSpec_LOCAL, StreamID: 2}},
					ColumnTypes: intCols(numInputTypesToMaterializer),
				},
			},
			Core: execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
			Output: []execinfrapb.OutputRouterSpec{
				{
					Type:    execinfrapb.OutputRouterSpec_PASS_THROUGH,
					Streams: []execinfrapb.StreamEndpointSpec{{Type: execinfrapb.StreamEndpointSpec_SYNC_RESPONSE}},
				},
			},
			ResultTypes: intCols(numInputTypesToMaterializer),
		},
		{
			// Because creating a table reader is too complex (you need to create a
			// bunch of other state) we simulate this by creating a noop operator with
			// a remote input, which is treated as having no local edges during
			// topological processing.
			// Spec C in the diagram.
			Input: []execinfrapb.InputSyncSpec{
				{
					Streams: []execinfrapb.StreamEndpointSpec{{Type: execinfrapb.StreamEndpointSpec_REMOTE}},
					// Use three Int columns as the types to be able to distinguish
					// between input DAGs when creating the inbox.
					ColumnTypes: intCols(numInputTypesToOutbox),
				},
			},
			Core: execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
			// This is o1, the outbox that will drain metadata.
			Output: []execinfrapb.OutputRouterSpec{
				{
					Type:    execinfrapb.OutputRouterSpec_PASS_THROUGH,
					Streams: []execinfrapb.StreamEndpointSpec{{Type: execinfrapb.StreamEndpointSpec_REMOTE}},
				},
			},
			ResultTypes: intCols(numInputTypesToOutbox),
		},
	}

	inboxToNumInputTypes := make(map[*colrpc.Inbox][]*types.T)
	outboxCreated := false
	componentCreator := callbackRemoteComponentCreator{
		newOutboxFn: func(
			allocator *colmem.Allocator,
			op colexecop.Operator,
			typs []*types.T,
			sources []colexecop.MetadataSource,
		) (*colrpc.Outbox, error) {
			require.False(t, outboxCreated)
			outboxCreated = true
			// Verify that there is only one metadata source: the inbox that is the
			// input to the noop operator. This is verified by first checking the
			// number of metadata sources and then that the input types are what we
			// expect from the input DAG.
			require.Len(t, sources, 1)
			require.Len(t, inboxToNumInputTypes[sources[0].(*colexec.InvariantsChecker).Input.(*colrpc.Inbox)], numInputTypesToOutbox)
			return colrpc.NewOutbox(allocator, op, typs, nil /* getStats */, sources, nil /* toClose */)
		},
		newInboxFn: func(allocator *colmem.Allocator, typs []*types.T, streamID execinfrapb.StreamID) (*colrpc.Inbox, error) {
			inbox, err := colrpc.NewInbox(allocator, typs, streamID)
			inboxToNumInputTypes[inbox] = typs
			return inbox, err
		},
	}

	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	ctx := context.Background()
	defer evalCtx.Stop(ctx)
	f := &flowinfra.FlowBase{
		FlowCtx: execinfra.FlowCtx{EvalCtx: &evalCtx,
			NodeID: base.TestingIDContainer,
		},
	}
	var wg sync.WaitGroup
	vfc := newVectorizedFlowCreator(
		&vectorizedFlowCreatorHelper{f: f}, componentCreator, false, false, &wg, &execinfra.RowChannel{},
		nil /* batchSyncFlowConsumer */, nil /* nodeDialer */, execinfrapb.FlowID{}, colcontainer.DiskQueueCfg{},
		nil /* fdSemaphore */, descs.DistSQLTypeResolver{},
	)

	_, _, err := vfc.setupFlow(ctx, &f.FlowCtx, procs, nil /* localProcessors */, flowinfra.FuseNormally)
	defer vfc.cleanup(ctx)
	require.NoError(t, err)

	// Verify that an outbox was actually created.
	require.True(t, outboxCreated)
}

// TestVectorizedFlowTempDirectory tests a flow's interactions with the
// temporary directory that will be used when spilling execution. Refer to
// subtests for a more thorough explanation.
func TestVectorizedFlowTempDirectory(t *testing.T) {
	defer leaktest.AfterTest(t)()

	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	ctx := context.Background()
	defer evalCtx.Stop(ctx)

	// We use an on-disk engine for this test since we're testing FS interactions
	// and want to get the same behavior as a non-testing environment.
	tempPath, dirCleanup := testutils.TempDir(t)
	ngn, err := storage.NewDefaultEngine(0 /* cacheSize */, base.StorageConfig{Dir: tempPath})
	require.NoError(t, err)
	defer ngn.Close()
	defer dirCleanup()

	newVectorizedFlow := func() *vectorizedFlow {
		return NewVectorizedFlow(
			&flowinfra.FlowBase{
				FlowCtx: execinfra.FlowCtx{
					Cfg: &execinfra.ServerConfig{
						TempFS:          ngn,
						TempStoragePath: tempPath,
						VecFDSemaphore:  &colexecop.TestingSemaphore{},
						Metrics:         &execinfra.DistSQLMetrics{},
					},
					EvalCtx:     &evalCtx,
					NodeID:      base.TestingIDContainer,
					DiskMonitor: execinfra.NewTestDiskMonitor(ctx, st),
				},
			},
		).(*vectorizedFlow)
	}

	dirs, err := ngn.List(tempPath)
	require.NoError(t, err)
	numDirsTheTestStartedWith := len(dirs)
	checkDirs := func(t *testing.T, numExtraDirs int) {
		t.Helper()
		dirs, err := ngn.List(tempPath)
		require.NoError(t, err)
		expectedNumDirs := numDirsTheTestStartedWith + numExtraDirs
		require.Equal(t, expectedNumDirs, len(dirs), "expected %d directories but found %d: %s", expectedNumDirs, len(dirs), dirs)
	}

	// LazilyCreated asserts that a directory is not created during flow Setup
	// but is done so when an operator spills to disk.
	t.Run("LazilyCreated", func(t *testing.T) {
		vf := newVectorizedFlow()
		var creator *vectorizedFlowCreator
		vf.testingKnobs.onSetupFlow = func(c *vectorizedFlowCreator) {
			creator = c
		}

		_, _, err := vf.Setup(ctx, &execinfrapb.FlowSpec{}, flowinfra.FuseNormally)
		require.NoError(t, err)

		// No directory should have been created.
		checkDirs(t, 0)

		// After the call to Setup, creator should be non-nil (i.e. the testing knob
		// should have been called).
		require.NotNil(t, creator)

		// Now simulate an operator spilling to disk. The flow should have set this
		// up to create its directory.
		creator.diskQueueCfg.GetPather.GetPath(ctx)

		// We should now have one directory, the flow's temporary storage directory.
		checkDirs(t, 1)

		// Another operator calling GetPath again should not create a new
		// directory.
		creator.diskQueueCfg.GetPather.GetPath(ctx)
		checkDirs(t, 1)

		// When the flow is Cleaned up, this directory should be removed.
		vf.Cleanup(ctx)
		checkDirs(t, 0)
	})

	t.Run("DirCreationRace", func(t *testing.T) {
		vf := newVectorizedFlow()
		var creator *vectorizedFlowCreator
		vf.testingKnobs.onSetupFlow = func(c *vectorizedFlowCreator) {
			creator = c
		}

		_, _, err := vf.Setup(ctx, &execinfrapb.FlowSpec{}, flowinfra.FuseNormally)
		require.NoError(t, err)

		createTempDir := creator.diskQueueCfg.GetPather.GetPath
		errCh := make(chan error)
		go func() {
			createTempDir(ctx)
			errCh <- ngn.MkdirAll(filepath.Join(vf.GetPath(ctx), "async"))
		}()
		createTempDir(ctx)
		// Both goroutines should be able to create their subdirectories within the
		// flow's temporary directory.
		require.NoError(t, ngn.MkdirAll(filepath.Join(vf.GetPath(ctx), "main_goroutine")))
		require.NoError(t, <-errCh)
		vf.Cleanup(ctx)
		checkDirs(t, 0)
	})
}
