// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tracedumper

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/dumpstore"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/zipper"
	"github.com/cockroachdb/errors"
)

const (
	jobTraceDumpPrefix = "job_trace_dump"
	timeFormat         = "2006-01-02T15_04_05.000"
)

var (
	totalDumpSizeLimit = settings.RegisterByteSizeSetting(
		settings.TenantWritable,
		"server.job_trace.total_dump_size_limit",
		"total size of job trace dumps to be kept. "+
			"Dumps are GC'ed in the order of creation time. The latest dump is "+
			"always kept even if its size exceeds the limit.",
		500<<20, // 500MiB
	)
)

// TraceDumper can be used to dump a zip file containing cluster wide inflight
// trace spans for a particular trace, to a configured dir.
type TraceDumper struct {
	currentTime func() time.Time
	store       *dumpstore.DumpStore
}

// PreFilter is part of the dumpstore.Dumper interface.
func (t *TraceDumper) PreFilter(
	ctx context.Context, files []os.FileInfo, _ func(fileName string) error,
) (preserved map[int]bool, err error) {
	preserved = make(map[int]bool)
	for i := len(files) - 1; i >= 0; i-- {
		// Always preserve the last dump in chronological order.
		if t.CheckOwnsFile(ctx, files[i]) {
			preserved[i] = true
			break
		}
	}
	return
}

// CheckOwnsFile is part of the dumpstore.Dumper interface.
func (t *TraceDumper) CheckOwnsFile(ctx context.Context, fi os.FileInfo) bool {
	return strings.HasPrefix(fi.Name(), jobTraceDumpPrefix)
}

var _ dumpstore.Dumper = &TraceDumper{}

// Dump attempts to dump a trace zip of cluster wide inflight trace spans
// with traceID, to the configured dir.
// The file names are prefixed with the timestamp of when it was written, to
// facilitate GC of older trace zips.
func (t *TraceDumper) Dump(ctx context.Context, name string, traceID int64, ie isql.Executor) {
	err := func() error {
		now := t.currentTime()
		traceZipFile := fmt.Sprintf(
			"%s.%s.%s.zip",
			jobTraceDumpPrefix,
			now.Format(timeFormat),
			name,
		)
		z := zipper.MakeInternalExecutorInflightTraceZipper(ie)
		zipBytes, err := z.Zip(ctx, traceID)
		if err != nil {
			return errors.Wrap(err, "failed to collect inflight trace zip")
		}
		path := t.store.GetFullPath(traceZipFile)
		f, err := os.Create(path)
		if err != nil {
			return errors.Wrapf(err, "error creating file %q for trace dump", path)
		}
		defer f.Close()
		_, err = f.Write(zipBytes)
		if err != nil {
			return errors.Newf("error writing zip file %q for trace dump", path)
		}
		return nil
	}()
	if err != nil {
		log.Errorf(ctx, "failed to dump trace %v", err)
	}
}

// NewTraceDumper returns a TraceDumper.
//
// dir is the directory in which dumps are stored.
func NewTraceDumper(ctx context.Context, dir string, st *cluster.Settings) *TraceDumper {
	if dir == "" {
		return nil
	}

	log.Infof(ctx, "writing job trace dumps to %s", log.SafeManaged(dir))

	td := &TraceDumper{
		currentTime: timeutil.Now,
		store:       dumpstore.NewStore(dir, totalDumpSizeLimit, st),
	}
	return td
}
