// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package resourceattr

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type ResourceAttr struct {
	syncutil.Mutex
	workloads []uint64
	cpuTimes  []float64
	id        int
}

func NewResourceAttr(size int, stopper *stop.Stopper) *ResourceAttr {
	r := &ResourceAttr{
		workloads: make([]uint64, size),
		cpuTimes:  make([]float64, size),
	}

	err := stopper.RunAsyncTask(context.Background(), "resource-attr-reporter", func(ctx context.Context) {
		timer := timeutil.Timer{}
		defer timer.Stop()
		timer.Reset(time.Second * 10)

		for {
			select {
			case <-stopper.ShouldQuiesce():
				return
			case <-ctx.Done():
				return
			case <-timer.C:
				log.Dev.Warningf(ctx, "~~~~~ WORKLOAD SUMMARY:%s", r.SummaryAndClear())
				timer.Reset(time.Second * 10)
			}
		}
	})
	if err != nil {
		panic(err)
	}

	return r
}

func (ra *ResourceAttr) Record(workloadID uint64, cpuTimeNanos float64) {
	ra.Lock()
	defer ra.Unlock()
	ra.workloads[ra.id] = workloadID
	ra.cpuTimes[ra.id] = cpuTimeNanos
	ra.id++
	if ra.id == len(ra.workloads) {
		ra.id = 0
	}
}

type ResourceAttrSum struct {
	workloadCpu map[uint64]float64
}

func (ra *ResourceAttr) SummaryAndClear() *ResourceAttrSum {
	sum := &ResourceAttrSum{
		workloadCpu: make(map[uint64]float64),
	}

	ra.Lock()
	defer ra.Unlock()
	for i := range ra.workloads {
		sum.workloadCpu[ra.workloads[i]] += ra.cpuTimes[i]
		ra.workloads[i] = 0
		ra.cpuTimes[i] = 0
	}
	ra.id = 0
	return sum
}

func (ras *ResourceAttrSum) String() string {
	if len(ras.workloadCpu) == 0 {
		return "(empty)"
	}

	var b strings.Builder
	b.WriteString("\n")
	for workloadID, cpuTime := range ras.workloadCpu {
		fmt.Fprintf(&b, "  Workload %-10d CPU: %0.2f ns\n", workloadID, cpuTime)
	}
	return b.String()
}
