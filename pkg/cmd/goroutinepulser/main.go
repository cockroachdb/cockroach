package main

import (
	"fmt"
	"math"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime/metrics"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/prometheus/client_golang/prometheus"
	prometheusgo "github.com/prometheus/client_model/go"
)

type noopLocker struct{}

func (n noopLocker) Lock() {
	return
}

func (n noopLocker) Unlock() {
	return
}

func main() {
	fmt.Println(os.Args[1:])
	// Usage: $0 <num_goroutines> <pulse_go_duration>.
	n, err := strconv.ParseInt(os.Args[1], 10, 64)
	if err != nil {
		panic(err)
	}
	d, err := time.ParseDuration(os.Args[2])
	if err != nil {
		panic(err)
	}

	// NB: I initially used a channel per consumer goroutine, but
	// it turned out that at high goroutine counts, scheduling latencies
	// became better because the single writer signaling all goroutines
	// sequentially became a bottleneck. The cond here is contrived but
	// exacerbates scheduler overload.
	cond := sync.NewCond(&noopLocker{})

	for i := int64(0); i < n; i++ {
		go func() {
			for {
				cond.Wait()
			}
		}()
	}

	go func() {
		for {
			<-time.After(d)
			cond.Broadcast()
		}
	}()

	go func() {
		for {
			<-time.After(time.Second)
			sl := []metrics.Sample{{Name: "/sched/latencies:seconds"}}
			metrics.Read(sl)
			hist := sl[0].Value.Float64Histogram()

			if true {
				// This code seems broken, outputs bogus small values
				// that tend to get better with more goroutines, no time
				// to look into it now.
				h := prometheus.NewHistogram(prometheus.HistogramOpts{
					Buckets: hist.Buckets,
				})
				var sum float64
				var count uint64
				for i, c := range hist.Counts {
					boundary := hist.Buckets[i]
					if math.IsInf(boundary, -1) {
						boundary = hist.Buckets[i+1]
					} else if math.IsInf(boundary, +1) {
						boundary = hist.Buckets[i-1]
					}

					sum += float64(c) * boundary
					count += c
					for j := uint64(0); j < c; j++ {
						h.Observe(hist.Buckets[i])
					}
				}

				mc := make(chan prometheus.Metric, 1)
				h.Collect(mc)
				var m prometheusgo.Metric
				if err := (<-mc).Write(&m); err != nil {
					panic(err)
				}
				avgH := 1000 * m.Histogram.GetSampleSum() / float64(m.Histogram.GetSampleCount())
				_ = avgH // omitting since avgC is more precise and they end up being the same in practice
				avgC := sum * 1000 / float64(count)
				p50 := metric.ValueAtQuantileWindowed(m.Histogram, 50) * 1000
				p75 := metric.ValueAtQuantileWindowed(m.Histogram, 75) * 1000
				p90 := metric.ValueAtQuantileWindowed(m.Histogram, 90) * 1000
				p99 := metric.ValueAtQuantileWindowed(m.Histogram, 99) * 1000
				p999 := metric.ValueAtQuantileWindowed(m.Histogram, 99.9) * 1000
				p9999 := metric.ValueAtQuantileWindowed(m.Histogram, 99.99) * 1000
				pMax := metric.ValueAtQuantileWindowed(m.Histogram, 100) * 1000

				fmt.Printf(
					"avg\tp50\tp75\tp90\tp99\tp99.9\tp99.99\tpMax\n"+
						"%.2f\t%.2f\t%.2f\t%.2f\tp%.2f\t%.2f\t%.2f\t%.2f [cum ms]\n",
					avgC, p50, p75, p90, p99, p999, p9999, pMax,
				)
			}
		}
	}()

	go func() {
		// Pprof endpoints.
		if err := http.ListenAndServe("localhost:6060", nil); err != nil {
			panic(err)
		}
	}()

	<-(chan struct{})(nil)
}
