// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package profiler

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/obs/ash"
	"github.com/cockroachdb/cockroach/pkg/server/dumpstore"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const ashReportPrefix = "ash_report"

var ashReportLookbackWindow = settings.RegisterDurationSettingWithExplicitUnit(
	settings.ApplicationLevel,
	"obs.ash.report.lookback_window",
	"aggregation lookback window for ASH reports triggered by goroutine dumps or CPU profiles",
	60*time.Second,
	settings.PositiveDuration,
)

var ashReportTotalDumpSizeLimit = settings.RegisterByteSizeSetting(
	settings.ApplicationLevel,
	"obs.ash.report.total_dump_size_limit",
	"maximum combined disk size of preserved ASH report files",
	32<<20, // 32MiB
)

// ASHReportProfiler writes aggregated ASH reports to disk when triggered
// by external events (goroutine dumps, CPU profiles). It is not
// threshold-based like the other profilers; instead, it is called
// explicitly after a dump or profile is taken.
type ASHReportProfiler struct {
	dir   string
	store *dumpstore.DumpStore
	st    *cluster.Settings
	// samplesBuf is reused across calls to avoid repeated large
	// allocations when retrieving ASH samples from the ring buffer.
	samplesBuf []ash.ASHSample
	now        func() time.Time
}

// NewASHReportProfiler creates a new ASHReportProfiler. dir is the
// directory in which ASH reports are stored.
func NewASHReportProfiler(
	ctx context.Context, dir string, st *cluster.Settings,
) (*ASHReportProfiler, error) {
	if dir == "" {
		return nil, errors.New("directory to store ASH reports could not be determined")
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	log.Dev.Infof(ctx, "writing ASH reports to %s", log.SafeManaged(dir))
	return &ASHReportProfiler{
		dir:   dir,
		store: dumpstore.NewStore(dir, ashReportTotalDumpSizeLimit, st),
		st:    st,
		now:   timeutil.Now,
	}, nil
}

// WriteReport writes an aggregated ASH report. trigger is included in
// the filename (e.g. "goroutine_dump", "cpu_profile"). Returns true
// if at least one report file was written.
func (p *ASHReportProfiler) WriteReport(ctx context.Context, trigger string) bool {
	now := p.now()

	sampler := ash.GetGlobalSampler()
	if sampler == nil {
		return false
	}

	lookback := ashReportLookbackWindow.Get(&p.st.SV)
	p.samplesBuf = sampler.GetSamples(p.samplesBuf)
	entries := ash.AggregateSamples(p.samplesBuf, now, lookback)

	// Write both text and JSON reports.
	txtOk := p.writeFile(ctx, now, trigger, ".txt", func(f *os.File) error {
		return ash.WriteTextReport(f, entries, now, lookback)
	})
	jsonOk := p.writeFile(ctx, now, trigger, ".json", func(f *os.File) error {
		return ash.WriteJSONReport(f, entries, now, lookback)
	})

	if txtOk || jsonOk {
		p.store.GC(ctx, now, p)
		return true
	}
	return false
}

func (p *ASHReportProfiler) writeFile(
	ctx context.Context, now time.Time, trigger string, suffix string, writeFn func(f *os.File) error,
) bool {
	fileName := fmt.Sprintf(
		"%s.%s.%s%s",
		ashReportPrefix, now.Format(timestampFormat), trigger, suffix,
	)
	path := p.store.GetFullPath(fileName)
	f, err := os.Create(path)
	if err != nil {
		log.Dev.Warningf(ctx, "error creating ASH report %s: %v", path, err)
		return false
	}

	if err := writeFn(f); err != nil {
		log.Dev.Warningf(ctx, "error writing ASH report %s: %v", path, err)
		_ = f.Close()
		_ = os.Remove(path)
		return false
	}
	if err := f.Close(); err != nil {
		log.Dev.Warningf(ctx, "error closing ASH report %s: %v", path, err)
		_ = os.Remove(path)
		return false
	}
	return true
}

// PreFilter is part of the dumpstore.Dumper interface. It preserves the
// most recent pair of ASH report files (one .txt and one .json).
func (p *ASHReportProfiler) PreFilter(
	ctx context.Context, files []os.DirEntry, cleanupFn func(fileName string) error,
) (preserved map[int]bool, _ error) {
	preserved = make(map[int]bool)
	// Preserve the latest .txt and .json files.
	foundTxt, foundJSON := false, false
	for i := len(files) - 1; i >= 0; i-- {
		if !p.CheckOwnsFile(ctx, files[i]) {
			continue
		}
		name := files[i].Name()
		if !foundTxt && strings.HasSuffix(name, ".txt") {
			preserved[i] = true
			foundTxt = true
		}
		if !foundJSON && strings.HasSuffix(name, ".json") {
			preserved[i] = true
			foundJSON = true
		}
		if foundTxt && foundJSON {
			break
		}
	}
	return preserved, nil
}

// CheckOwnsFile is part of the dumpstore.Dumper interface.
func (p *ASHReportProfiler) CheckOwnsFile(_ context.Context, fi os.DirEntry) bool {
	return strings.HasPrefix(fi.Name(), ashReportPrefix+".")
}
