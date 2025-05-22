// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raftstorebench

import (
	"context"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func Run(t T, cfg Config) Result {
	smEng, raftEng := makeEngines(t, cfg)

	q := makeReplicaQueue(cfg.NumReplicas)

	var wg sync.WaitGroup
	s := newAggStats()

	statsCtx, stopStats := context.WithCancel(context.Background())
	defer stopStats()
	go statsLoop(t, statsCtx, cfg, s, raftEng, smEng)

	o := writeOptions{
		cfg: cfg, smEng: smEng, raftEng: raftEng,
		keyLen: keyLen, valueLen: valueLen, batchLen: batchLen,
	}
	var durabilityCallbackCount atomic.Int64
	tStartWorkers := timeutil.Now()
	for i := 0; i < cfg.NumWorkers; i++ {
		wg.Add(1)
		w := &worker{
			t: t, s: s, o: o, rng: rand.New(rand.NewSource(int64(i))),
			durabilityCallbackCount: &durabilityCallbackCount,
		}
		go w.run(t, q, &wg)
	}
	logf(t, "started workers")

	o.smEng.RegisterFlushCompletedCallback(func() {
		n := durabilityCallbackCount.Add(1)
		if s.ops.Load() >= cfg.NumWrites {
			// Already done, we're compacting, don't log.
			return
		}
		logf(t, "state machine flush #%d completed", n)
	})
	if !cfg.SingleEngine {
		var bytesFlushed uint64
		var n int
		notifyCh := make(chan struct{}, 1)
		go func() {
			for {
				select {
				case <-statsCtx.Done():
					return
				case <-notifyCh:
					n++
					l0Metrics := o.raftEng.GetMetrics().Levels[0]
					newBytesFlushed := l0Metrics.TableBytesFlushed + l0Metrics.BlobBytesFlushed
					logf(t, "raft engine flush #%d completed; flushed %s", n,
						humanizeutil.IBytes(int64(newBytesFlushed-bytesFlushed)))
					bytesFlushed = newBytesFlushed
				}
			}
		}()
		o.raftEng.RegisterFlushCompletedCallback(func() {
			// Can't call o.raftEng.Metrics() on this goroutine.
			select {
			case notifyCh <- struct{}{}:
			default:
			}
		})
	}

	wg.Wait()
	stopStats()
	duration := timeutil.Since(tStartWorkers)
	logf(t, "done working")

	lsmStatsToFile(t, cfg, "uncompacted", o.smEng.GetMetrics(), o.raftEng.GetMetrics())

	logf(t, "compacting")
	require.NoError(t, smEng.Compact(context.Background()))
	if !cfg.SingleEngine {
		require.NoError(t, raftEng.Compact(context.Background()))
	}
	logf(t, "done compacting")

	smMetrics := o.smEng.GetMetrics()
	if cfg.SMDisableWAL {
		// TODO(storage): this is still populated when the WAL is disabled.
		// Seems like a bug. For now, just clear it so the computations below
		// don't lie about w-amp.
		smMetrics.WAL = pebble.Metrics{}.WAL
	}
	raftMetrics := o.raftEng.GetMetrics()
	lsmStatsToFile(t, cfg, "compacted", smMetrics, raftMetrics)

	// Compute write amps and print them.

	// The pure payloads - i.e. length of keys and values we handed to the state
	// machine (not accounting for overhead of constructing WriteBatch, padding,
	// compression, compaction, etc.). The WAL bytes should not be much larger
	// than this value (could be smaller if it were compressed) if the payload
	// got written to it only once. In the single engine case, where the payload
	// hits the same WAL twice, there's a factor of two.
	payloadBytes := s.keyBytes.Load() + s.valBytes.Load()
	goodputMBsec := float64(1e3*payloadBytes) / float64(1+duration) // 1e-6/1e-9=1e3

	res := Result{
		Name:        t.Name(),
		PayloadGB:   float64(payloadBytes) / 1e9,
		DurationSec: duration.Seconds(),
		GoodputMB:   goodputMBsec,
		Truncations: s.truncs.Load(),
	}

	smWAMP := writeAmp(smMetrics, payloadBytes)
	if cfg.SingleEngine {
		res.CombinedEng = smWAMP.Human
	} else {
		res.StateEng = smWAMP.Human
		raftWAMP := writeAmp(raftMetrics, payloadBytes)
		res.RaftEng = raftWAMP.Human
		combWAMP := raftWAMP.add(smWAMP)
		res.CombinedEng = combWAMP.Human
	}

	// This is already set in the top-level struct.
	res.CombinedEng.PayloadGB = 0
	res.StateEng.PayloadGB = 0
	res.RaftEng.PayloadGB = 0

	logf(t, "results (see also artifacts in config directory %s):\n%s", cfg.Dir, yamlEncode(t, res))

	return res
}

func lsmStatsToFile(t T, cfg Config, infix string, smMetrics, raftMetrics storage.Metrics) {
	t.Helper()
	if cfg.SingleEngine {
		out := filepath.Join(cfg.Dir, "raft_and_state_lsm_"+infix+".txt")
		require.NoError(t, os.WriteFile(out, []byte(smMetrics.String()), 0644))
	} else {
		outState, outRaft :=
			filepath.Join(cfg.Dir, "state_lsm_"+infix+".txt"),
			filepath.Join(cfg.Dir, "raft_lsm_"+infix+".txt")
		require.NoError(t, os.WriteFile(outState, []byte(smMetrics.String()), 0644))
		require.NoError(t, os.WriteFile(outRaft, []byte(raftMetrics.String()), 0644))
	}
}

type T = testing.TB

func yamlEncode(t T, obj any) string {
	var buf strings.Builder
	require.NoError(t, yaml.NewEncoder(&buf).Encode(obj))
	return buf.String()
}
