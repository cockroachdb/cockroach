// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package resourceattr

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats/sqlstatsutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const (
	WORKLOAD_ID_UNKNOWN = iota
	WORKLOAD_ID_BULKIO
	WORKLOAD_ID_BACKUP
	WORKLOAD_ID_RESTORE
	WORKLOAD_ID_IMPORT
	WORKLOAD_ID_CDC
	WORKLOAD_ID_JOB
	WORKLOAD_ID_INTERNAL_UNKNOWN
	WORKLOAD_ID_SUBQUERY
	WORKLOAD_ID_BACKFILL
	WORKLOAD_ID_SCHEMA_CHANGE
	WORKLOAD_ID_MVCC_GC
)

var workloadIDToName = map[uint64]string{
	WORKLOAD_ID_UNKNOWN:          "UNKNOWN",
	WORKLOAD_ID_BULKIO:           "BULKIO",
	WORKLOAD_ID_BACKUP:           "BACKUP",
	WORKLOAD_ID_RESTORE:          "RESTORE",
	WORKLOAD_ID_IMPORT:           "IMPORT",
	WORKLOAD_ID_CDC:              "CDC",
	WORKLOAD_ID_JOB:              "JOB",
	WORKLOAD_ID_INTERNAL_UNKNOWN: "INTERNAL_UNKNOWN",
	WORKLOAD_ID_SUBQUERY:         "SUBQUERY",
	WORKLOAD_ID_BACKFILL:         "BACKFILL",
	WORKLOAD_ID_SCHEMA_CHANGE:    "SCHEMA_CHANGE",
	WORKLOAD_ID_MVCC_GC:          "MVCC_GC",
}

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

type workloadEntry struct {
	workloadID uint64
	name       string
	cpuTime    float64
}

func (ras *ResourceAttrSum) String() string {
	if len(ras.workloadCpu) == 0 {
		return "(empty)"
	}

	// Collect entries and find max workload name length for alignment
	entries := make([]workloadEntry, 0, len(ras.workloadCpu))
	maxLen := 0
	for workloadID, cpuTime := range ras.workloadCpu {
		name := workloadName(workloadID)
		entries = append(entries, workloadEntry{
			workloadID: workloadID,
			name:       name,
			cpuTime:    cpuTime,
		})
		if len(name) > maxLen {
			maxLen = len(name)
		}
	}

	// Sort by CPU time descending (highest first)
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].cpuTime > entries[j].cpuTime
	})

	var b strings.Builder
	b.WriteString("\n")
	for _, entry := range entries {
		fmt.Fprintf(&b, "  Workload %-*s CPU: %s\n", maxLen, entry.name, formatCPUTime(entry.cpuTime))
	}
	return b.String()
}

func formatCPUTime(cpuTimeNanos float64) string {
	switch {
	case cpuTimeNanos >= 1e9:
		return fmt.Sprintf("%8.2f s", cpuTimeNanos/1e9)
	case cpuTimeNanos >= 1e6:
		return fmt.Sprintf("%8.2f ms", cpuTimeNanos/1e6)
	case cpuTimeNanos >= 1e3:
		return fmt.Sprintf("%8.2f µs", cpuTimeNanos/1e3)
	default:
		return fmt.Sprintf("%8.2f ns", cpuTimeNanos)
	}
}

func workloadName(workloadID uint64) string {
	if name, ok := workloadIDToName[workloadID]; ok {
		return name
	}
	return fmt.Sprintf("STATEMENT: %s", sqlstatsutil.EncodeStmtFingerprintIDToString(appstatspb.StmtFingerprintID(workloadID)))
}
