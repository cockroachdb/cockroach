// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkmerge

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/stretchr/testify/require"
)

func TestReserveRPCMemory(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	// makeProcessor builds a minimal bulkMergeProcessor for testing
	// reserveRPCMemory. localFiles SSTs are assigned to instance 1 (local),
	// remoteFiles SSTs are assigned to instance 2 (remote).
	makeProcessor := func(
		t *testing.T,
		localFiles, remoteFiles int,
		fraction float64,
		window int64,
		budget int64,
	) *bulkMergeProcessor {
		t.Helper()

		st := cluster.MakeTestingClusterSettings()
		rpcInflightFraction.Override(ctx, &st.SV, fraction)
		blobs.FlowControlWindow.Override(ctx, &st.SV, window)
		targetFileSize.Override(ctx, &st.SV, 1<<30)

		evalCtx := eval.MakeTestingEvalContext(st)
		t.Cleanup(func() { evalCtx.Stop(ctx) })

		nodeIDContainer := &base.NodeIDContainer{}
		nodeIDContainer.Set(ctx, 1)
		sqlIDContainer := base.NewSQLIDContainerForNode(nodeIDContainer)

		rootMon := mon.NewUnlimitedMonitor(ctx, mon.Options{
			Name:     mon.MakeName("test-root"),
			Settings: st,
		})
		t.Cleanup(func() { rootMon.Stop(ctx) })

		budgetMon := mon.NewMonitor(mon.Options{
			Name:     mon.MakeName("test-budget"),
			Limit:    budget,
			Settings: st,
		})
		budgetReserved := rootMon.MakeBoundAccount()
		t.Cleanup(func() { budgetReserved.Close(ctx) })
		budgetMon.Start(ctx, rootMon, &budgetReserved)
		t.Cleanup(func() { budgetMon.Stop(ctx) })

		var ssts []execinfrapb.BulkMergeSpec_SST
		for i := 0; i < localFiles; i++ {
			ssts = append(ssts, execinfrapb.BulkMergeSpec_SST{
				URI: fmt.Sprintf("nodelocal://1/data/file-local-%d.sst", i),
			})
		}
		for i := 0; i < remoteFiles; i++ {
			ssts = append(ssts, execinfrapb.BulkMergeSpec_SST{
				URI: fmt.Sprintf("nodelocal://2/data/file-remote-%d.sst", i),
			})
		}

		mp := &bulkMergeProcessor{
			spec: execinfrapb.BulkMergeSpec{
				SSTs: ssts,
			},
			flowCtx: &execinfra.FlowCtx{
				EvalCtx: &evalCtx,
				NodeID:  sqlIDContainer,
				Cfg: &execinfra.ServerConfig{
					BulkMonitor: budgetMon,
				},
			},
		}
		t.Cleanup(func() { mp.rpcMemAcct.Close(ctx) })
		return mp
	}

	t.Run("all-local-files", func(t *testing.T) {
		mp := makeProcessor(t, 5, 0, 0.33, 16, 1)
		err := mp.reserveRPCMemory(ctx)
		require.NoError(t, err)
	})

	t.Run("flow-control-disabled", func(t *testing.T) {
		mp := makeProcessor(t, 0, 10, 0.33, 0, 1)
		err := mp.reserveRPCMemory(ctx)
		require.NoError(t, err)
	})

	t.Run("fits-in-budget", func(t *testing.T) {
		// 2 remote files, fraction 0.60, window 16 =>
		// inflight = ceil(2 * 0.60) = 2
		// memory = 2 * 16 * ChunkSize = 2 * 16 * 131072 = 4 MiB.
		// Use 2x the required amount as budget to account for monitor
		// pool allocation rounding.
		budget := int64(2 * 16 * blobs.ChunkSize * 2)
		mp := makeProcessor(t, 0, 2, 0.60, 16, budget)
		err := mp.reserveRPCMemory(ctx)
		require.NoError(t, err)
	})

	// Error cases: verify the error string contains expected content.
	type errorCase struct {
		name        string
		remoteFiles int
		fraction    float64
		window      int64
		budget      int64
	}

	errorCases := []errorCase{{
		name:        "exceeds-budget",
		remoteFiles: 100,
		fraction:    0.60,
		window:      16,
		budget:      1,
	}, {
		name:        "high-fraction-exceeds",
		remoteFiles: 10,
		fraction:    1.0,
		window:      16,
		budget:      1,
	}, {
		name:        "large-window-exceeds",
		remoteFiles: 10,
		fraction:    0.60,
		window:      64,
		budget:      1,
	}}

	for _, tc := range errorCases {
		t.Run(tc.name, func(t *testing.T) {
			mp := makeProcessor(t, 0, tc.remoteFiles, tc.fraction, tc.window, tc.budget)
			err := mp.reserveRPCMemory(ctx)
			require.Error(t, err)
			errStr := err.Error()

			// Verify the error mentions the reservation context.
			require.Contains(t, errStr, "reserving")
			require.Contains(t, errStr, "RPC transport buffers")

			// Verify remote file count and inflight estimate.
			inflight := int64(math.Ceil(float64(tc.remoteFiles) * tc.fraction))
			rpcMemory := inflight * tc.window * int64(blobs.ChunkSize)
			require.Contains(t, errStr, fmt.Sprintf("%d remote files", tc.remoteFiles))
			require.Contains(t, errStr, fmt.Sprintf("%d estimated inflight", inflight))
			require.Contains(t, errStr, string(humanizeutil.IBytes(rpcMemory)))

			// Verify all four remediation suggestions.
			require.Contains(t, errStr, "--max-sql-memory")
			require.Contains(t, errStr, "bulkio.merge.rpc_inflight_fraction")
			require.Contains(t, errStr, fmt.Sprintf("currently %.2f", tc.fraction))
			require.Contains(t, errStr, "bulkio.blob.flow_control_window")
			require.Contains(t, errStr, fmt.Sprintf("currently %d", tc.window))
			require.Contains(t, errStr, "bulkio.merge.file_size")

			// Verify stream buffer size is displayed.
			streamBuf := tc.window * int64(blobs.ChunkSize)
			require.Contains(t, errStr, string(humanizeutil.IBytes(streamBuf)))
		})
	}
}
