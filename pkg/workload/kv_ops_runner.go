// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package workload

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/pkg/errors"
	"golang.org/x/time/rate"
)

type KvOpsRunOptions struct {
	Duration       time.Duration
	Histograms     string
	MaxOps         uint64
	MaxRate        float64
	Ramp           time.Duration
	TolerateErrors bool
}

func KvOpsRun(
	ctx context.Context, done chan os.Signal, eng engine.Engine, opser KvOpser,
	hookser Hookser, opts KvOpsRunOptions,
) error {
	// Number of successful operations
	var numOps uint64

	var limiter *rate.Limiter
	if opts.MaxRate > 0 {
		// Create a limiter using maxRate specified on the command line and
		// with allowed burst of 1 at the maximum allowed rate.
		limiter = rate.NewLimiter(rate.Limit(opts.MaxRate), 1)
	}

	reg := histogram.NewRegistry()
	var ops QueryLoad
	for {
		var err error
		ops, err = opser.KvOps(eng, reg)
		if err == nil {
			break
		}
		if !opts.TolerateErrors {
			return err
		}
		log.Infof(ctx, "retrying after error while creating load: %v", err)
	}

	start := timeutil.Now()
	errCh := make(chan error)
	var rampDone chan struct{}
	if opts.Ramp > 0 {
		// Create a channel to signal when the ramp period finishes. Will
		// be reset to nil when consumed by the process loop below.
		rampDone = make(chan struct{})
	}

	workersCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()
	var wg sync.WaitGroup
	wg.Add(len(ops.WorkerFns))
	go func() {
		// If a ramp period was specified, start all of the workers gradually
		// with a new context.
		var rampCtx context.Context
		if rampDone != nil {
			var cancel func()
			rampCtx, cancel = context.WithTimeout(workersCtx, opts.Ramp)
			defer cancel()
		}

		for i, workFn := range ops.WorkerFns {
			go func(i int, workFn func(context.Context) error) {
				// workerRun is an infinite loop in which the worker continuously attempts to
				// read / write blocks of random data into a table in cockroach DB. The function
				// returns only when the provided context is canceled.
				workerRun := func(
					ctx context.Context,
					errCh chan<- error,
					wg *sync.WaitGroup,
					limiter *rate.Limiter,
					workFn func(context.Context) error,
				) {
					if wg != nil {
						defer wg.Done()
					}

					for {
						if ctx.Err() != nil {
							return
						}

						// Limit how quickly the load generator sends requests based on --max-rate.
						if limiter != nil {
							if err := limiter.Wait(ctx); err != nil {
								if err == ctx.Err() {
									return
								}
								panic(err)
							}
						}

						if err := workFn(ctx); err != nil {
							if errors.Cause(err) == ctx.Err() {
								return
							}
							errCh <- err
							continue
						}

						v := atomic.AddUint64(&numOps, 1)
						if opts.MaxOps > 0 && v >= opts.MaxOps {
							return
						}
					}
				}
				// If a ramp period was specified, start all of the workers
				// gradually with a new context.
				if rampCtx != nil {
					rampPerWorker := opts.Ramp / time.Duration(len(ops.WorkerFns))
					time.Sleep(time.Duration(i) * rampPerWorker)
					workerRun(rampCtx, errCh, nil /* wg */, limiter, workFn)
				}

				// Start worker again, this time with the main context.
				workerRun(workersCtx, errCh, &wg, limiter, workFn)
			}(i, workFn)
		}

		if rampCtx != nil {
			// Wait for the ramp period to finish, then notify the process loop
			// below to reset timers and histograms.
			<-rampCtx.Done()
			close(rampDone)
		}
	}()

	var numErr int
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	go func() {
		wg.Wait()
		done <- os.Interrupt
	}()

	if opts.Duration > 0 {
		go func() {
			time.Sleep(opts.Duration + opts.Ramp)
			done <- os.Interrupt
		}()
	}

	var jsonEnc *json.Encoder
	if opts.Histograms != "" {
		_ = os.MkdirAll(filepath.Dir(opts.Histograms), 0755)
		jsonF, err := os.Create(opts.Histograms)
		if err != nil {
			return err
		}
		jsonEnc = json.NewEncoder(jsonF)
	}

	everySecond := log.Every(time.Second)
	for i := 0; ; {
		select {
		case err := <-errCh:
			numErr++
			if opts.TolerateErrors {
				if everySecond.ShouldLog() {
					log.Error(ctx, err)
				}
				continue
			}
			return err

		case <-ticker.C:
			startElapsed := timeutil.Since(start)
			reg.Tick(func(t histogram.Tick) {
				if i%20 == 0 {
					fmt.Println("_elapsed___errors__ops/sec(inst)___ops/sec(cum)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)")
				}
				i++
				fmt.Printf("%8s %8d %14.1f %14.1f %8.1f %8.1f %8.1f %8.1f %s\n",
					time.Duration(startElapsed.Seconds()+0.5)*time.Second,
					numErr,
					float64(t.Hist.TotalCount())/t.Elapsed.Seconds(),
					float64(t.Cumulative.TotalCount())/startElapsed.Seconds(),
					time.Duration(t.Hist.ValueAtQuantile(50)).Seconds()*1000,
					time.Duration(t.Hist.ValueAtQuantile(95)).Seconds()*1000,
					time.Duration(t.Hist.ValueAtQuantile(99)).Seconds()*1000,
					time.Duration(t.Hist.ValueAtQuantile(100)).Seconds()*1000,
					t.Name,
				)
				if jsonEnc != nil && rampDone == nil {
					_ = jsonEnc.Encode(t.Snapshot())
				}
			})

		// Once the load generator is fully ramped up, we reset the histogram
		// and the start time to throw away the stats for the the ramp up period.
		case <-rampDone:
			rampDone = nil
			start = timeutil.Now()
			i = 0
			reg.Tick(func(t histogram.Tick) {
				t.Cumulative.Reset()
				t.Hist.Reset()
			})

		case <-done:
			cancelWorkers()
			if ops.Close != nil {
				ops.Close(ctx)
			}
			const totalHeader = "\n_elapsed___errors_____ops(total)___ops/sec(cum)__avg(ms)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)"
			fmt.Println(totalHeader + `__total`)
			startElapsed := timeutil.Since(start)
			printTotalHist := func(t histogram.Tick) {
				if t.Cumulative == nil {
					return
				}
				if t.Cumulative.TotalCount() == 0 {
					return
				}
				fmt.Printf("%7.1fs %8d %14d %14.1f %8.1f %8.1f %8.1f %8.1f %8.1f  %s\n",
					startElapsed.Seconds(), numErr,
					t.Cumulative.TotalCount(),
					float64(t.Cumulative.TotalCount())/startElapsed.Seconds(),
					time.Duration(t.Cumulative.Mean()).Seconds()*1000,
					time.Duration(t.Cumulative.ValueAtQuantile(50)).Seconds()*1000,
					time.Duration(t.Cumulative.ValueAtQuantile(95)).Seconds()*1000,
					time.Duration(t.Cumulative.ValueAtQuantile(99)).Seconds()*1000,
					time.Duration(t.Cumulative.ValueAtQuantile(100)).Seconds()*1000,
					t.Name,
				)
			}

			resultTick := histogram.Tick{Name: ops.ResultHist}
			reg.Tick(func(t histogram.Tick) {
				printTotalHist(t)
				if jsonEnc != nil {
					// Note that we're outputting the delta from the last tick. The
					// cumulative histogram can be computed by merging all of the
					// per-tick histograms.
					_ = jsonEnc.Encode(t.Snapshot())
				}
				if ops.ResultHist == `` || ops.ResultHist == t.Name {
					if resultTick.Cumulative == nil {
						resultTick.Now = t.Now
						resultTick.Cumulative = t.Cumulative
					} else {
						resultTick.Cumulative.Merge(t.Cumulative)
					}
				}
			})

			fmt.Println(totalHeader + `__result`)
			printTotalHist(resultTick)

			if hookser != nil {
				if hookser.Hooks().PostRun != nil {
					if err := hookser.Hooks().PostRun(startElapsed); err != nil {
						fmt.Printf("failed post-run hook: %v\n", err)
					}
				}
			}
			return nil
		}
	}
}
