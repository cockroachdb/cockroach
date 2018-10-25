package systembench

import (
	"context"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/codahale/hdrhistogram"
	"github.com/pkg/errors"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	minLatency = 100 * time.Microsecond
	maxLatency = 10 * time.Second
)

var numOps uint64
var numBytes uint64

// Options holds parameters for the test.
type HDDOptions struct {
	Dir         string
	Concurrency int
	Duration    time.Duration

	Run128Test bool
}

// latency is a histogram for the latency of a single worker.
type latency struct {
	syncutil.Mutex
	*hdrhistogram.WindowedHistogram
}

// worker represents a single worker process generating load.
type worker interface {
	getLatencyHistogram() *latency
	run(ctx context.Context, wg *sync.WaitGroup)
}

// worker128MB holds a temp file and random byte array to write to
// the temp file.
type worker128MB struct {
	latency
	data     []byte
	tempFile *os.File
}

// new128MBWorker creates a worker that writes 128MB blocks to a file.
func new128MBWorker(ctx context.Context, file *os.File) worker {
	// Creating a 128MB byte array.
	data := make([]byte, 134217728)
	_, err := rand.Read(data)
	if err != nil {
		log.Error(ctx, "Failed to fill byte array with random bytes %s", err)
	}

	w := &worker128MB{data: data,
		tempFile: file,
	}
	w.latency.WindowedHistogram = hdrhistogram.NewWindowed(1,
		minLatency.Nanoseconds(), maxLatency.Nanoseconds(), 1)
	return w
}

// worker128MB implements the worker interface.
func (w *worker128MB) run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		start := timeutil.Now()
		if _, err := w.tempFile.Write(w.data); err != nil {
			w.tempFile.Close()
			log.Fatal(ctx, err)
		}

		bytes := uint64(len(w.data))
		atomic.AddUint64(&numOps, 1)
		atomic.AddUint64(&numBytes, bytes)
		elapsed := timeutil.Since(start)
		w.latency.Lock()
		if err := w.latency.Current.RecordValue(elapsed.Nanoseconds()); err != nil {
			log.Fatal(ctx, err)
		}
		w.latency.Unlock()
	}
}

// worker128MB implements the worker interface.
func (w *worker128MB) getLatencyHistogram() *latency {
	return &w.latency
}

// Run runs I/O benchmarks specified by hddOpts.
func Run(hddOpts HDDOptions) error {
	ctx := context.Background()

	// Check if the directory exists.
	_, err := os.Stat(hddOpts.Dir)
	if err != nil {
		return errors.Errorf("error: supplied path '%s' must exist", hddOpts.Dir)
	}

	log.Infof(ctx, "writing to %s\n", hddOpts.Dir)

	workers := make([]worker, hddOpts.Concurrency)
	// We're creating a temp file like this instead of using ioutil.TempFile
	// to open in append mode.
	tempName := strconv.Itoa(rand.Int()) + ".txt"
	tmpfile, err := os.OpenFile(filepath.Join(hddOpts.Dir, tempName),
		os.O_RDWR|os.O_APPEND|os.O_CREATE, 0660)
	if err != nil {
		log.Fatal(ctx, err)
	}
	defer os.Remove(tmpfile.Name()) // clean up

	var workerCreator func(ctx context.Context, file *os.File) worker
	if hddOpts.Run128Test {
		workerCreator = new128MBWorker
	} else {
		return errors.Errorf("Please specify a subtest.")
	}

	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		workers[i] = workerCreator(ctx, tmpfile)
	}

	for i := range workers {
		go workers[i].run(ctx, &wg)
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	done := make(chan os.Signal, 3)
	signal.Notify(done, os.Interrupt)

	go func() {
		wg.Wait()
		done <- sysutil.Signal(0)
	}()

	if hddOpts.Duration > 0 {
		go func() {
			time.Sleep(hddOpts.Duration)
			done <- sysutil.Signal(0)
		}()
	}

	start := timeutil.Now()
	lastNow := start
	var lastOps uint64
	var lastBytes uint64

	// This is a histogram that combines the output for multiple workers.
	var cumulative *hdrhistogram.Histogram

loop:
	for i := 0; ; i++ {
		select {
		case <-ticker.C:
			var h *hdrhistogram.Histogram
			for _, w := range workers {
				workerLatency := w.getLatencyHistogram()
				workerLatency.Lock()
				m := workerLatency.Merge()
				workerLatency.Rotate()
				workerLatency.Unlock()
				if h == nil {
					h = m
				} else {
					h.Merge(m)
				}
			}

			if cumulative == nil {
				cumulative = h
			} else {
				cumulative.Merge(h)
			}

			p50 := h.ValueAtQuantile(50)
			p95 := h.ValueAtQuantile(95)
			p99 := h.ValueAtQuantile(99)
			pMax := h.ValueAtQuantile(100)

			now := timeutil.Now()
			elapsed := now.Sub(lastNow)
			ops := atomic.LoadUint64(&numOps)
			bytes := atomic.LoadUint64(&numBytes)

			if i%20 == 0 {
				fmt.Println("_elapsed____ops/sec___mb/sec__p50(ms)__p95(ms)__p99(ms)_pMax(ms)")
			}
			fmt.Printf("%8s %10.1f %8.1f %8.1f %8.1f %8.1f %8.1f\n",
				time.Duration(timeutil.Since(start).Seconds()+0.5)*time.Second,
				float64(ops-lastOps)/elapsed.Seconds(),
				float64(bytes-lastBytes)/(1024.0*1024.0)/elapsed.Seconds(),
				time.Duration(p50).Seconds()*1000,
				time.Duration(p95).Seconds()*1000,
				time.Duration(p99).Seconds()*1000,
				time.Duration(pMax).Seconds()*1000,
			)
			lastNow = now
			lastOps = ops
			lastBytes = bytes

		case <-done:
			startElapsed := timeutil.Since(start)
			const totalHeader = "\n_elapsed____ops(total)__mb(total)__avg(ms)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)"
			fmt.Println(totalHeader + `__total`)
			fmt.Printf("%8s %13d %10.1f %8.1f %8.1f %8.1f %8.1f %8.1f\n",
				time.Duration(startElapsed.Seconds())*time.Second,
				atomic.LoadUint64(&numOps),
				float64(atomic.LoadUint64(&numBytes)/(1024.0*1024.0)),
				time.Duration(cumulative.Mean()).Seconds()*1000,
				time.Duration(cumulative.ValueAtQuantile(50)).Seconds()*1000,
				time.Duration(cumulative.ValueAtQuantile(95)).Seconds()*1000,
				time.Duration(cumulative.ValueAtQuantile(99)).Seconds()*1000,
				time.Duration(cumulative.ValueAtQuantile(100)).Seconds()*1000,
			)
			break loop
		}
	}
	return nil

}
