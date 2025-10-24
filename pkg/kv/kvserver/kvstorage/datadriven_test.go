// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstorage

import (
	"context"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/dd"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

type env struct {
	eng storage.Engine
	tr  *tracing.Tracer
}

func newEnv(t *testing.T) *env {
	ctx := context.Background()
	eng := storage.NewDefaultInMemForTesting()
	// TODO(tbg): ideally this would do full bootstrap, which requires
	// moving a lot more code from kvserver. But then we could unit test
	// all of it with the datadriven harness!
	require.NoError(t, WriteClusterVersion(ctx, eng, clusterversion.TestingClusterVersion))
	require.NoError(t, InitEngine(ctx, eng, roachpb.StoreIdent{
		ClusterID: uuid.MakeV4(),
		NodeID:    1,
		StoreID:   1,
	}))
	tr := tracing.NewTracer()
	tr.SetRedactable(true)
	return &env{
		eng: eng,
		tr:  tr,
	}
}

func (e *env) close() {
	e.eng.Close()
	e.tr.Close()
}

func (e *env) handleNewReplica(
	t *testing.T,
	ctx context.Context,
	id roachpb.FullReplicaID,
	skipRaftReplicaID bool,
	k, ek roachpb.RKey,
) *roachpb.RangeDescriptor {
	sl := MakeStateLoader(id.RangeID)
	require.NoError(t, sl.SetHardState(ctx, e.eng, raftpb.HardState{}))
	if !skipRaftReplicaID && id.ReplicaID != 0 {
		require.NoError(t, sl.SetRaftReplicaID(ctx, e.eng, id.ReplicaID))
	}
	if len(ek) == 0 {
		return nil
	}
	desc := &roachpb.RangeDescriptor{
		RangeID:  id.RangeID,
		StartKey: keys.MustAddr(roachpb.Key(k)),
		EndKey:   keys.MustAddr(roachpb.Key(ek)),
		InternalReplicas: []roachpb.ReplicaDescriptor{{
			NodeID:    1,
			StoreID:   1,
			ReplicaID: id.ReplicaID,
		}},
		NextReplicaID: id.ReplicaID + 1,
	}
	var v roachpb.Value
	require.NoError(t, v.SetProto(desc))
	ts := hlc.Timestamp{WallTime: 123}
	require.NoError(t, e.eng.PutMVCC(storage.MVCCKey{
		Key:       keys.RangeDescriptorKey(desc.StartKey),
		Timestamp: ts,
	}, storage.MVCCValue{Value: v}))
	return desc
}

func TestDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()

	reStripFileLinePrefix := regexp.MustCompile(`^[^ ]+ `)
	// Scan stats (shown after loading the range descriptors) can be non-deterministic.
	reStripScanStats := regexp.MustCompile(`stats: .*$`)

	dir := filepath.Join(datapathutils.TestDataPath(t), t.Name())
	datadriven.Walk(t, dir, func(t *testing.T, path string) {
		e := newEnv(t)
		defer e.close()
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) (output string) {
			ctx, finishAndGet := tracing.ContextWithRecordingSpan(context.Background(), e.tr, path)
			// This method prints all output to `buf`.
			var buf strings.Builder
			var printTrace bool // if true, trace printed to buf on return
			if d.HasArg("trace") {
				d.ScanArgs(t, "trace", &printTrace)
			}

			defer func() {
				if r := recover(); r != nil {
					fmt.Fprintln(&buf, r)
				}
				rec := finishAndGet()[0]
				for _, l := range rec.Logs {
					if !printTrace || !strings.Contains(string(l.Message), "kvstorage") {
						continue
					}
					msg := string(l.Message)
					msg = reStripFileLinePrefix.ReplaceAllString(msg, ``)
					msg = reStripScanStats.ReplaceAllString(msg, `stats: <redacted>`)

					fmt.Fprintln(&buf, msg)
				}
				if buf.Len() == 0 {
					fmt.Fprintln(&buf, "ok")
				}
				output = buf.String()
			}()

			switch d.Cmd {
			case "new-replica":
				// ReplicaID is optional to allow making incomplete state.
				var cmd struct {
					RangeID    roachpb.RangeID   `name:"range-id"`
					ReplicaID  roachpb.ReplicaID `name:"replica-id" opt:"true"`
					Key        string            `name:"k" opt:"true"`
					EndKey     string            `name:"ek" opt:"true"`
					SkipReplID bool              `name:"skip-raft-replica-id" opt:"true"`
				}
				dd.ParseCommand(t, d, &cmd)

				if desc := e.handleNewReplica(t, ctx,
					roachpb.FullReplicaID{RangeID: cmd.RangeID, ReplicaID: cmd.ReplicaID}, cmd.SkipReplID,
					keys.MustAddr(roachpb.Key(cmd.Key)), keys.MustAddr(roachpb.Key(cmd.EndKey)),
				); desc != nil {
					fmt.Fprintln(&buf, desc)
				}

			case "load-and-reconcile":
				replicas, err := LoadAndReconcileReplicas(ctx, e.eng)
				if err != nil {
					fmt.Fprintln(&buf, err)
					break
				}
				for _, repl := range replicas {
					fmt.Fprintf(&buf, "%s: ", repl.ID())
					if desc := repl.Desc; desc != nil {
						fmt.Fprint(&buf, desc)
					} else {
						fmt.Fprintf(&buf, "uninitialized")
					}
					fmt.Fprintln(&buf)
				}
			default:
				t.Fatalf("unknown command %s", d.Cmd)
			}
			return "" // defer will do it
		})
	})

}
