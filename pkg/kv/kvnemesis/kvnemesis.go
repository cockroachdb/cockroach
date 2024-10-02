// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvnemesis

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

var errInjected = errors.New("injected error")

type loggerKey struct{}

type logLogger struct {
	dir string
}

func (l *logLogger) WriteFile(basename string, contents string) string {
	f, err := os.Create(filepath.Join(l.dir, basename))
	if err != nil {
		return err.Error()
	}
	defer f.Close()
	_, err = io.WriteString(f, contents)
	if err != nil {
		return err.Error()
	}
	return f.Name()
}

func (l *logLogger) Helper() { /* no-op */ }

func (l *logLogger) Logf(format string, args ...interface{}) {
	log.InfofDepth(context.Background(), 2, format, args...)
}

func l(ctx context.Context, basename string, format string, args ...interface{}) (optFile string) {
	var logger Logger
	logger, _ = ctx.Value(loggerKey{}).(Logger)
	if logger == nil {
		logger = &logLogger{dir: datapathutils.DebuggableTempDir()}
	}
	logger.Helper()

	if basename != "" {
		return logger.WriteFile(basename, fmt.Sprintf(format, args...))
	}

	logger.Logf(format, args...)
	return ""
}

// RunNemesis generates and applies a series of Operations to exercise the KV
// api. It returns a slice of the logical failures encountered.
func RunNemesis(
	ctx context.Context,
	rng *rand.Rand,
	env *Env,
	config GeneratorConfig,
	concurrency int,
	numSteps int,
	dbs ...*kv.DB,
) ([]error, error) {
	if env.L != nil {
		ctx = context.WithValue(ctx, loggerKey{}, env.L)
	}
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
		stepIdx := -1
		for atomic.AddInt64(&stepsStartedAtomic, 1) <= int64(numSteps) {
			stepIdx++
			step := g.RandStep(rng)

			stepPrefix := fmt.Sprintf("w%d_step%d", workerIdx, stepIdx)
			basename := fmt.Sprintf("%s_%T", stepPrefix, reflect.Indirect(reflect.ValueOf(step.Op.GetValue())).Interface())

			{
				// Write next step into file so we know steps if test deadlock and has
				// to be killed.
				var buf strings.Builder
				step.format(&buf, formatCtx{indent: `  ` + workerName + ` PRE `})
				l(ctx, basename, "%s", &buf)
			}

			trace, err := a.Apply(ctx, &step)
			step.Trace = l(ctx, fmt.Sprintf("%s_trace", stepPrefix), "%s", trace.String())

			stepsByWorker[workerIdx] = append(stepsByWorker[workerIdx], step)

			prefix := ` OP  `
			if err != nil {
				prefix = ` ERR `
			}

			{
				var buf strings.Builder
				fmt.Fprintf(&buf, "  before: %s", step.Before)
				step.format(&buf, formatCtx{indent: `  ` + workerName + prefix})
				fmt.Fprintf(&buf, "\n  after: %s", step.After)
				l(ctx, basename, "%s", &buf)
			}

			if err != nil {
				return err
			}
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
		var failuresFile string
		{
			var buf strings.Builder
			for _, err := range failures {
				l(ctx, "", "%s", err)
				fmt.Fprintf(&buf, "%+v\n", err)
				fmt.Fprintln(&buf, strings.Repeat("=", 80))
			}
			failuresFile = l(ctx, "failures", "%s", &buf)
		}

		reproFile := l(ctx, "repro.go", "// Reproduction steps:\n%s", printRepro(stepsByWorker))
		rangefeedFile := l(ctx, "kvs-rangefeed.txt", "kvs (recorded from rangefeed):\n%s", kvs.DebugPrint("  "))
		kvsFile := "<error>"
		var scanKVs []kv.KeyValue
		for i := 0; ; i++ {
			var err error
			scanKVs, err = dbs[0].Scan(ctx, dataSpan.Key, dataSpan.EndKey, -1)
			if errors.Is(err, errInjected) && i < 100 {
				// The scan may end up resolving intents, and the intent resolution may
				// fail with an injected reproposal error. We do want to know the
				// contents anyway, so retry appropriately. (The interceptor lowers the
				// probability of injecting an error with successive attempts, so this
				// is essentially guaranteed to work out).
				//
				// Just in case there is a real infinite loop here, we only try this
				// 100 times.
				continue
			} else if err != nil {
				l(ctx, "", "could not scan actual latest values: %+v", err)
				break
			}
			var kvsBuf strings.Builder
			for _, kv := range scanKVs {
				fmt.Fprintf(&kvsBuf, "  %s %s -> %s\n", kv.Key, kv.Value.Timestamp, kv.Value.PrettyPrint())
			}
			kvsFile = l(ctx, "kvs-scan.txt", "kvs (scan of latest values according to crdb):\n%s", kvsBuf.String())
			break
		}
		l(ctx, "", `failures(verbose): %s
repro steps: %s
rangefeed KVs: %s
scan KVs: %s`,
			failuresFile, reproFile, rangefeedFile, kvsFile)
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
			if len(step.Trace) > 0 {
				fmt.Fprintf(&buf, "\n  // ^-- trace in: %s\n", step.Trace)
			}
			buf.WriteString("\n")
		}
		buf.WriteString("\n  return nil\n")
		buf.WriteString("})\n\n")
	}
	buf.WriteString("require.NoError(t, g.Wait())\n")
	return buf.String()
}
