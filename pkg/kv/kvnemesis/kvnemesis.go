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

// RunNemesis generates and applies a series of Operations to exercise the KV
// api. It returns a slice of the logical failures encountered.
func RunNemesis(
	ctx context.Context,
	rng *rand.Rand,
	ct ClosedTimestampTargetInterval,
	config GeneratorConfig,
	dbs ...*kv.DB,
) ([]error, error) {
	const concurrency, numSteps = 5, 30

	g, err := MakeGenerator(config, newGetReplicasFn(dbs...))
	if err != nil {
		return nil, err
	}
	a := MakeApplier(dbs...)
	w, err := Watch(ctx, dbs, ct, GeneratorDataSpan())
	if err != nil {
		return nil, err
	}
	defer func() { _ = w.Finish() }()

	var stepsStartedAtomic int64
	stepsByWorker := make([][]Step, concurrency)

	workerFn := func(ctx context.Context, workerIdx int) error {
		workerName := fmt.Sprintf(`%d`, workerIdx)
		var buf strings.Builder
		for atomic.AddInt64(&stepsStartedAtomic, 1) <= numSteps {
			step := g.RandStep(rng)
			if err := a.Apply(ctx, &step); err != nil {
				buf.Reset()
				step.format(&buf, formatCtx{indent: `  ` + workerName + ` ERR `})
				log.Infof(ctx, "error: %+v\n\n%s", err, buf.String())
				return err
			}
			buf.Reset()
			step.format(&buf, formatCtx{indent: `  ` + workerName + ` OP  `})
			log.Info(ctx, buf.String())
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
	failures := Validate(allSteps, kvs)

	if len(failures) > 0 {
		log.Infof(ctx, "reproduction steps:\n%s", printRepro(stepsByWorker))
		log.Infof(ctx, "kvs (recorded from rangefeed):\n%s", kvs.DebugPrint("  "))

		span := GeneratorDataSpan()
		scanKVs, err := dbs[0].Scan(ctx, span.Key, span.EndKey, -1)
		if err != nil {
			log.Infof(ctx, "could not scan actual latest values: %+v", err)
		} else {
			var kvsBuf strings.Builder
			for _, kv := range scanKVs {
				fmt.Fprintf(&kvsBuf, "  %s %s -> %s\n", kv.Key, kv.Value.Timestamp, kv.Value.PrettyPrint())
			}
			log.Infof(ctx, "kvs (scan of latest values according to crdb):\n%s", kvsBuf.String())
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
		}
		buf.WriteString("\n  return nil\n")
		buf.WriteString("})\n")
	}
	buf.WriteString("g.Wait()\n")
	return buf.String()
}
