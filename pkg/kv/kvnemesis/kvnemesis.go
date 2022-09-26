// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvnemesis

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type loggerKey struct{}

func l(ctx context.Context, debug bool, format string, args ...interface{}) {
	if logger := ctx.Value(loggerKey{}); logger != nil && !debug {
		if t, ok := logger.(interface {
			Helper()
		}); ok {
			t.Helper()
		}
		logger.(Logger).Logf(format, args...)
	}
	log.InfofDepth(ctx, 2, format, args...)
}

// RunNemesis generates and applies a series of Operations to exercise the KV
// api. It returns a slice of the logical failures encountered.
func RunNemesis(
	ctx context.Context,
	rng *rand.Rand,
	env *Env,
	config GeneratorConfig,
	numSteps int,
	dbs ...*kv.DB,
) ([]error, error) {
	if env.L != nil {
		ctx = context.WithValue(ctx, loggerKey{}, env.L)
	}
	const concurrency = 5
	if numSteps <= 0 {
		return nil, fmt.Errorf("numSteps must be >0, got %v", numSteps)
	}

	dataSpan := GeneratorDataSpan()

	g, err := MakeGenerator(config, newGetReplicasFn(dbs...))
	if err != nil {
		return nil, err
	}
	a := MakeApplier(env, dbs...)
	w, err := Watch(ctx, env, dbs, dataSpan)
	if err != nil {
		return nil, err
	}
	defer func() { _ = w.Finish() }()

	var stepsStartedAtomic int64
	stepsByWorker := make([][]Step, concurrency)

	workerFn := func(ctx context.Context, workerIdx int) error {
		workerName := fmt.Sprintf(`%d`, workerIdx)
		var buf strings.Builder
		for atomic.AddInt64(&stepsStartedAtomic, 1) <= int64(numSteps) {
			step := g.RandStep(rng)

			buf.Reset()
			fmt.Fprintf(&buf, "step:")
			step.format(&buf, formatCtx{indent: `  ` + workerName + ` PRE  `})
			trace, err := a.Apply(ctx, &step)
			buf.WriteString(trace.String())
			step.Trace = buf.String()
			if err != nil {
				buf.Reset()
				step.format(&buf, formatCtx{indent: `  ` + workerName + ` ERR `})
				l(ctx, false, "error: %+v\n\n%s", err, buf.String())
				return err
			}
			buf.Reset()
			fmt.Fprintf(&buf, "\n  before: %s", step.Before)
			step.format(&buf, formatCtx{indent: `  ` + workerName + ` OP  `})
			fmt.Fprintf(&buf, "\n  after: %s", step.After)
			l(ctx, true, "%v", buf.String())
			stepsByWorker[workerIdx] = append(stepsByWorker[workerIdx], step)
		}
		return nil
	}
	if err := ctxgroup.GroupWorkers(ctx, concurrency, workerFn); err != nil {
		return nil, err
	}

	allSteps := make(steps, 0, numSteps)
	for _, steps := range stepsByWorker {
		allSteps = append(allSteps, steps...)
	}

	// TODO(dan): Also slurp the splits. The meta ranges use expiration based
	// leases, so we can't use RangeFeed/Watcher to do it. Maybe ExportRequest?
	if err := w.WaitForFrontier(ctx, allSteps.After()); err != nil {
		return nil, err
	}
	kvs := w.Finish()
	defer kvs.Close()

	failures := Validate(allSteps, kvs, env.Tracker)

	// Run consistency checks across the data span, primarily to check the
	// accuracy of evaluated MVCC stats.
	failures = append(failures, env.CheckConsistency(ctx, dataSpan)...)

	if len(failures) > 0 {
		l(ctx, false, "reproduction steps:\n%s", printRepro(stepsByWorker))
		l(ctx, false, "kvs (recorded from rangefeed):\n%s", kvs.DebugPrint("  "))

		scanKVs, err := dbs[0].Scan(ctx, dataSpan.Key, dataSpan.EndKey, -1)
		if err != nil {
			log.Infof(ctx, "could not scan actual latest values: %+v", err)
		} else {
			var kvsBuf strings.Builder
			for _, kv := range scanKVs {
				fmt.Fprintf(&kvsBuf, "  %s %s -> %s\n", kv.Key, kv.Value.Timestamp, kv.Value.PrettyPrint())
			}
			l(ctx, false, "kvs (scan of latest values according to crdb):\n%s", kvsBuf.String())
		}
	}

	return failures, nil
}

func printRepro(stepsByWorker [][]Step) string {
	// TODO(dan): Make this more copy and paste, especially the error handling.
	var buf strings.Builder
	buf.WriteString("g := ctxgroup.WithContext(ctx)\n")
	for _, steps := range stepsByWorker {
		buf.WriteString("g.GoCtx(func(ctx context.Context) error {")
		for _, step := range steps {
			fctx := formatCtx{receiver: fmt.Sprintf(`db%d`, step.DBID), indent: "  "}
			buf.WriteString("\n")
			buf.WriteString(fctx.indent)
			step.Op.format(&buf, fctx)
			buf.WriteString(step.Trace)
			buf.WriteString("\n")
		}
		buf.WriteString("\n  return nil\n")
		buf.WriteString("})\n")
	}
	buf.WriteString("g.Wait()\n")
	return buf.String()
}
