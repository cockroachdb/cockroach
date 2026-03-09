// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package liveness

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/dumpstore"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
)

const (
	heartbeatTraceDumpPrefix = "heartbeat_trace"
	heartbeatTraceDumpSuffix = ".txt"
	traceDumpTimeFormat      = "2006-01-02T15_04_05.000"
)

// SlowHeartbeatTraceDumpSizeLimit controls the maximum combined disk size of
// preserved heartbeat trace dumps before old files are garbage collected.
var SlowHeartbeatTraceDumpSizeLimit = settings.RegisterByteSizeSetting(
	settings.SystemOnly,
	"server.liveness_heartbeat.trace_dump.total_size_limit",
	"maximum combined disk size of preserved heartbeat trace dumps",
	64<<20, // 64 MiB
)

// heartbeatTraceDumper writes verbose trace recordings for slow heartbeats to
// disk and manages size-based GC of old dumps via dumpstore.DumpStore.
type heartbeatTraceDumper struct {
	store *dumpstore.DumpStore
}

func newHeartbeatTraceDumper(dir string, st *cluster.Settings) *heartbeatTraceDumper {
	return &heartbeatTraceDumper{
		store: dumpstore.NewStore(dir, SlowHeartbeatTraceDumpSizeLimit, st),
	}
}

// PreFilter implements dumpstore.Dumper. It preserves the latest trace file
// from GC.
func (d *heartbeatTraceDumper) PreFilter(
	_ context.Context, files []os.DirEntry, _ func(string) error,
) (map[int]bool, error) {
	preserved := make(map[int]bool)
	for i := len(files) - 1; i >= 0; i-- {
		if d.CheckOwnsFile(context.Background(), files[i]) {
			preserved[i] = true
			break
		}
	}
	return preserved, nil
}

// CheckOwnsFile implements dumpstore.Dumper.
func (d *heartbeatTraceDumper) CheckOwnsFile(_ context.Context, fi os.DirEntry) bool {
	return strings.HasPrefix(fi.Name(), heartbeatTraceDumpPrefix)
}

func (d *heartbeatTraceDumper) dump(
	ctx context.Context, dur time.Duration, rec tracingpb.Recording,
) {
	// Use a clean context for logging since the original span has been finished.
	ctx = tracing.ContextWithSpan(ctx, nil)
	now := timeutil.Now()
	fileName := fmt.Sprintf("%s.%s.%s%s",
		heartbeatTraceDumpPrefix,
		now.Format(traceDumpTimeFormat),
		dur.Truncate(time.Millisecond),
		heartbeatTraceDumpSuffix)
	path := d.store.GetFullPath(fileName)
	if err := os.WriteFile(path, []byte(rec.String()), 0644); err != nil {
		log.KvExec.Warningf(ctx, "failed to write heartbeat trace dump: %v", err)
		return
	}
	log.KvExec.Infof(ctx, "wrote slow heartbeat trace to %s", path)
	d.store.GC(ctx, now, d)
}
