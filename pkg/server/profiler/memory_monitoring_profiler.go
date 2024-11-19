// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package profiler

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/server/dumpstore"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
	"github.com/dustin/go-humanize"
)

var memoryMonitoringDumpsEnabled = settings.RegisterBoolSetting(
	settings.SystemVisible,
	"diagnostics.memory_monitoring_dumps.enabled",
	"enable dumping of memory monitoring state at the same time as heap profiles are taken",
	true,
	settings.WithPublic,
)

// MemoryMonitoringProfiler is used to periodically dump the node's memory
// monitoring state. It uses the same heuristics when to dump as the heap
// profiler.
type MemoryMonitoringProfiler struct {
	profiler
}

const memMonitoringFileNamePrefix = "memmonitoring"
const memMonitoringFileNameSuffix = ".txt"

var memMonitoringCombinedFileSize = settings.RegisterByteSizeSetting(
	settings.ApplicationLevel,
	"server.mem_monitoring.total_dump_size_limit",
	"maximum combined disk size of preserved mem monitoring profiles",
	4<<20, // 4MiB
)

// NewMemoryMonitoringProfiler returns a new MemoryMonitoringProfiler. dir is
// the directory in which memory monitoring dumps are to be stored.
func NewMemoryMonitoringProfiler(
	ctx context.Context, dir string, st *cluster.Settings,
) (*MemoryMonitoringProfiler, error) {
	if dir == "" {
		return nil, errors.AssertionFailedf("need to specify dir for MemoryMonitoringProfiler")
	}

	dumpStore := dumpstore.NewStore(dir, memMonitoringCombinedFileSize, st)
	mmp := &MemoryMonitoringProfiler{
		profiler: makeProfiler(
			newProfileStore(dumpStore, memMonitoringFileNamePrefix, memMonitoringFileNameSuffix, st),
			zeroFloor,
			envMemprofInterval,
		),
	}
	log.Infof(ctx, "writing memory monitoring dumps to %s at least every %s", log.SafeManaged(dir), mmp.resetInterval())
	return mmp, nil
}

// MaybeTakeMemoryMonitoringDump takes a memory monitoring dump if the heap is
// big enough.
func (mmp *MemoryMonitoringProfiler) MaybeTakeMemoryMonitoringDump(
	ctx context.Context, curHeap int64, root *mon.BytesMonitor, st *cluster.Settings,
) {
	if !memoryMonitoringDumpsEnabled.Get(&st.SV) {
		return
	}
	mmp.maybeTakeProfile(ctx, curHeap, takeMemoryMonitoringDump, root)
}

// takeMemoryMonitoringDump returns true if and only if the memory monitoring
// dump was taken successfully.
//
// args must contain exactly one argument which is a pointer to the root memory
// monitor.
func takeMemoryMonitoringDump(
	ctx context.Context, path string, args ...interface{},
) (success bool) {
	if len(args) != 1 {
		log.Errorf(ctx, "%v", errors.AssertionFailedf("expected exactly 1 argument (root memory monitor), got %d", len(args)))
		return false
	}
	root, ok := args[0].(*mon.BytesMonitor)
	if !ok {
		log.Errorf(ctx, "%v", errors.AssertionFailedf("expected *mon.BytesMonitor, got %T", args[0]))
		return false
	}
	f, err := os.Create(path)
	if err != nil {
		log.Warningf(ctx, "error creating memory monitoring dump %s: %v", path, err)
		return false
	}
	defer f.Close()
	if err = root.TraverseTree(getMonitorStateCb(f)); err != nil {
		log.Warningf(ctx, "error traversing memory monitoring tree for dump %s: %v", path, err)
		return false
	}
	return true
}

func getMonitorStateCb(f io.Writer) func(state mon.MonitorState) error {
	return func(s mon.MonitorState) error {
		if s.Stopped {
			// Omit monitors that have been stopped.
			return nil
		}
		if s.Used == 0 && s.ReservedUsed == 0 && s.ReservedReserved == 0 {
			// Omit monitors that don't have any memory usage reported.
			return nil
		}
		info := fmt.Sprintf("%s%s %s", strings.Repeat(" ", 4*s.Level), s.Name, humanize.IBytes(uint64(s.Used)))
		if s.ReservedUsed != 0 || s.ReservedReserved != 0 {
			info += fmt.Sprintf(" (%s / %s)", humanize.IBytes(uint64(s.ReservedUsed)), humanize.IBytes(uint64(s.ReservedReserved)))
		}
		if _, err := f.Write([]byte(info)); err != nil {
			return err
		}
		_, err := f.Write([]byte{'\n'})
		return err
	}
}
