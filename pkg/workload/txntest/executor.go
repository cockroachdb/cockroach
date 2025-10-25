// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txntest

import (
	"context"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	crdberrors "github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
)

// executeWorkload runs the workload with the provided connection URLs and config.
func executeWorkload(
	ctx context.Context,
	urls []string,
	w WorkloadSpec,
	run RunConfig,
	hist *history,
) error {
	cfg := workload.MultiConnPoolCfg{
		MaxTotalConnections: run.Concurrency + 1,
		MaxConnsPerPool:     run.Concurrency + 1,
		Method:              "exec",
	}
	mcp, err := workload.NewMultiConnPool(ctx, cfg, urls...)
	if err != nil {
		return err
	}
	defer mcp.Close()

	// Precompute weighted choices.
	templates := make([]TxnTemplate, 0, len(w.Templates))
	weights := make([]int, 0, len(w.Templates))
	var totalWeight int
	for _, t := range w.Templates {
		wt := t.Weight
		if wt <= 0 {
			wt = 1
		}
		templates = append(templates, t)
		weights = append(weights, wt)
		totalWeight += wt
	}
	if len(templates) == 0 {
		return nil
	}

	// Iteration-only mode: each worker executes exactly run.Iterations transactions.

	var wg sync.WaitGroup
	// Capture the first error from any worker so we can return it instead of
	// leaking a generic context cancellation.
	errCh := make(chan error, 1)
	wg.Add(run.Concurrency)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Derive deterministic worker RNG seeds from the standard test RNG.
	baseRng, _ := randutil.NewTestRand()
	workerSeeds := make([]int64, run.Concurrency)
	for i := 0; i < run.Concurrency; i++ {
		workerSeeds[i] = baseRng.Int63()
	}

	for workerIdx := 0; workerIdx < run.Concurrency; workerIdx++ {
		workerIdx := workerIdx
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(workerSeeds[workerIdx]))
			iters := run.Iterations
			if iters <= 0 {
				iters = 1
			}
			for i := 0; i < iters; i++ {

				// Select a template by weight.
				choice := weightedPick(rng, weights, totalWeight)
				template := templates[choice]
				if err := executeTemplateOnce(ctx, mcp, rng, template, hist); err != nil {
					// Preserve the first error and cancel others.
					select {
					case errCh <- err:
					default:
					}
					cancel()
					return
				}
				select {
				case <-ctx.Done():
					return
				default:
				}
			}
		}()
	}
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func weightedPick(rng *rand.Rand, weights []int, total int) int {
	if total <= 0 {
		return 0
	}
	x := rng.Intn(total)
	for i, w := range weights {
		if x < w {
			return i
		}
		x -= w
	}
	return len(weights) - 1
}

func executeTemplateOnce(
	ctx context.Context,
	mcp *workload.MultiConnPool,
	rng *rand.Rand,
	template TxnTemplate,
	hist *history,
) (retErr error) {
	vars := Vars{}
	var bindings Bindings
	if template.ParamGen != nil {
		bindings = template.ParamGen(rng, vars)
	}

	retry := template.Retry == RetrySerializable

	for attempt := 0; ; attempt++ {
		// Start a transaction.
		tx, err := mcp.Get().BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			return err
		}

		committed := false

		for _, s := range template.Steps {
			switch st := s.(type) {
			case *execStep:
				args, err := evalArgs(st.args, vars, bindings)
				if err != nil {
					retErr = err
					break
				}
				if _, err := tx.Exec(ctx, st.sql, args...); err != nil {
					retErr = err
					break
				}
			case *queryRowStep:
				args, err := evalArgs(st.args, vars, bindings)
				if err != nil {
					retErr = err
					break
				}
				row := tx.QueryRow(ctx, st.sql, args...)
				if len(st.capture) > 0 {
					dests := make([]interface{}, len(st.capture))
					for i := range dests {
						var v interface{}
						dests[i] = &v
					}
					if err := row.Scan(dests...); err != nil {
						retErr = err
						break
					}
					for i, name := range st.capture {
						vars[name] = *(dests[i].(*interface{}))
					}
				} else {
					// No capture: still attempt the query to ensure execution.
					var dummy interface{}
					if err := row.Scan(&dummy); err != nil {
						retErr = err
						break
					}
				}
			case *sleepStep:
				time.Sleep(st.d)
			case *savepointRestartStep:
				// No-op placeholder (savepoint-based retries not implemented yet).
			default:
				// Unknown step type.
			}
			if retErr != nil {
				break
			}
		}

		if retErr == nil {
			if err := tx.Commit(ctx); err != nil {
				retErr = err
			} else {
				committed = true
			}
		}

		// Ensure rollback if not committed before leaving this attempt.
		if !committed {
			_ = tx.Rollback(ctx)
		}

		// Record basic history.
		if hist != nil {
			if retErr != nil {
				hist.Add(template.Name, attempt, retErr)
			} else {
				hist.Add(template.Name, attempt, nil)
			}
		}

		if retErr == nil {
			return nil
		}
		if !retry || !isRetriableError(retErr) {
			return retErr
		}
		// Retry.
		retErr = nil
	}
}

func evalArgs(exprs []Expr, vars Vars, bindings Bindings) ([]interface{}, error) {
	if len(exprs) == 0 {
		return nil, nil
	}
	args := make([]interface{}, len(exprs))
	for i, e := range exprs {
		val, err := e.eval(vars, bindings)
		if err != nil {
			return nil, err
		}
		args[i] = val
	}
	return args, nil
}

func isRetriableError(err error) bool {
	// Conservatively retry on PostgreSQL serialization failure code 40001 observed
	// through pgx. The error string typically contains "SQLSTATE 40001".
	// Avoid depending on concrete error types to keep this lightweight.
	if err == nil {
		return false
	}
	const marker = "SQLSTATE 40001"
	if containsErr(err, marker) {
		return true
	}
	return false
}

func containsErr(err error, substr string) bool {
	for e := err; e != nil; e = unwrapOnce(e) {
		if msg := e.Error(); msg != "" && strings.Contains(msg, substr) {
			return true
		}
	}
	return false
}

// unwrapOnce attempts to unwrap a single layer of an error.
func unwrapOnce(err error) error { return crdberrors.Unwrap(err) }
