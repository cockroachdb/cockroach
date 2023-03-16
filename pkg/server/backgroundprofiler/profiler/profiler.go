// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package profiler

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"regexp"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/server/backgroundprofiler"
	"github.com/cockroachdb/cockroach/pkg/server/dumpstore"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/pprofutil"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/google/pprof/profile"
)

var maxCombinedFileSize = settings.RegisterByteSizeSetting(
	settings.TenantWritable,
	"server.cpu_profiler.total_dump_size_limit",
	"maximum combined disk size of preserved runtime traces",
	128<<20, // 128MiB
)

const timeFormat = "2006-01-02T15_04_05.000"
const CPUProfilerFileNamePrefix = "cpuprofiler."
const RuntimeTraceFileNamePrefix = "runtimetrace."

// BackgroundProfiler is a background service that runs on every node and is
// capable of collecting on-demand CPU and execution profiles.
//
// The first Subscriber initializes the collection of the CPU and execution
// trace profiles. While the profiles are being collected, only Subscribers with
// the same `profileID` are allowed to join the running profiles. The profiles
// are stopped and persisted to local storage when the last Subscriber
// unsubscribes.
type BackgroundProfiler struct {
	nodeID int32

	st        *cluster.Settings
	stopper   *stop.Stopper
	dumpStore *dumpstore.DumpStore

	startProfile   chan struct{}
	startedProfile chan struct{}
	stopProfile    chan struct{}
	res            chan protoutil.Message
	mu             struct {
		syncutil.Mutex

		// profileID is a unique ID that can be associated with the operation that
		// we are profiling. Only subscribers that are executing as part of this
		// operation are allowed to subscribe to the BackgroundProfiler.
		profileID   backgroundprofiler.ProfileID
		subscribers map[backgroundprofiler.SubscriberID]struct{}
	}
}

// Subscribe implements the BackgroundProfiler interface.
func (r *BackgroundProfiler) Subscribe(
	ctx context.Context, subscriber backgroundprofiler.Subscriber,
) (context.Context, func()) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.mu.profileID.IsSet() {
		if subscriber.ProfileID() != r.mu.profileID {
			// TODO(adityamaru): This error needs to make its way back to the Subscriber.
			log.Warningf(ctx, "subscriber with trace ID %d, cannot subscribe to the ongoing trace with ID %d",
				subscriber.ProfileID(), r.mu.profileID)
			return nil, nil
		}
	} else {
		r.mu.profileID = subscriber.ProfileID()
	}

	// Set the profiler labels for the new subscriber.
	labelledCtx, undo := pprofutil.SetProfilerLabels(ctx, []string{fmt.Sprintf("%d", subscriber.ProfileID()),
		subscriber.LabelValue()}...)
	r.mu.subscribers[subscriber.Identifier()] = struct{}{}

	// If this is the first subscriber we should start profiling.
	if len(r.mu.subscribers) == 1 {
		r.startProfile <- struct{}{}
		<-r.startedProfile
	}
	return labelledCtx, undo
}

func (r *BackgroundProfiler) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mu.subscribers = make(map[backgroundprofiler.SubscriberID]struct{})
	r.mu.profileID = 0
}

// Unsubscribe implements the BackgroundProfiler interface.
//
// This method unregisters the Subscriber from the running profile. If this is
// the last Subscriber then this method will stop the running profiles and
// return metadata describing the collected profiles.
func (r *BackgroundProfiler) Unsubscribe(
	subscriber backgroundprofiler.Subscriber,
) (finishedProfiling bool, msg protoutil.Message) {
	r.mu.Lock()
	if _, ok := r.mu.subscribers[subscriber.Identifier()]; !ok {
		r.mu.Unlock()
		return false, nil
	}

	delete(r.mu.subscribers, subscriber.Identifier())

	// If there are no more subscribers, we should stop collecting runtime traces
	// and return the traces we have aggregated so far.
	if len(r.mu.subscribers) != 0 {
		r.mu.Unlock()
		return false, nil
	}

	// Don't hold the lock while stopping the profiles.
	r.mu.Unlock()
	r.stopProfile <- struct{}{}
	profileMessage := <-r.res
	r.reset()
	return true, profileMessage
}

var _ backgroundprofiler.Profiler = &BackgroundProfiler{}

// NewBackgroundProfiler returns an instance of a BackgroundProfiler.
func NewBackgroundProfiler(
	ctx context.Context,
	st *cluster.Settings,
	stopper *stop.Stopper,
	nodeID int32,
	runtimeProfilerDir string,
) *BackgroundProfiler {
	if runtimeProfilerDir == "" {
		return nil
	}

	if err := os.MkdirAll(runtimeProfilerDir, 0755); err != nil {
		// This is possible when running with only in-memory stores; in that case
		// the start-up code sets the output directory to the current directory (.).
		// If running the process from a directory which is not writable, we won't
		// be able to create a subdirectory here.
		log.Warningf(ctx, "cannot create runtime trace dump dir -- runtime traces will be disabled: %v", err)
		return nil
	}

	et := &BackgroundProfiler{
		st:             st,
		stopper:        stopper,
		dumpStore:      dumpstore.NewStore(runtimeProfilerDir, maxCombinedFileSize, st),
		nodeID:         nodeID,
		startProfile:   make(chan struct{}),
		startedProfile: make(chan struct{}),
		stopProfile:    make(chan struct{}),
		res:            make(chan protoutil.Message),
	}
	et.mu.subscribers = make(map[backgroundprofiler.SubscriberID]struct{})
	et.mu.profileID = 0
	return et
}

// CompileTagFilter compiles a filter from the passed in tag. This method is a
// stripped down version of `compileTagFilter` in
// https://github.com/google/pprof/blob/main/internal/driver/driver_focus.go#L80.
func CompileTagFilter(tag string) (func(*profile.Sample) bool, error) {
	if tag == "" {
		return nil, nil
	}

	tagValuePair := strings.SplitN(tag, "=", 2)
	var wantKey string
	if len(tagValuePair) == 2 {
		wantKey = tagValuePair[0]
		tag = tagValuePair[1]
	}

	var rfx []*regexp.Regexp
	for _, tagf := range strings.Split(tag, ",") {
		fx, err := regexp.Compile(tagf)
		if err != nil {
			return nil, errors.Wrap(err, "parsing regexp")
		}
		rfx = append(rfx, fx)
	}
	if wantKey == "" {
		return func(s *profile.Sample) bool {
		matchedrx:
			for _, rx := range rfx {
				for key, vals := range s.Label {
					for _, val := range vals {
						if rx.MatchString(key + ":" + val) {
							continue matchedrx
						}
					}
				}
				return false
			}
			return true
		}, nil
	}
	return func(s *profile.Sample) bool {
		if vals, ok := s.Label[wantKey]; ok {
			for _, rx := range rfx {
				for _, val := range vals {
					if rx.MatchString(val) {
						return true
					}
				}
			}
		}
		return false
	}, nil
}

// filterCPUProfileByLabel removes all samples from the profile, except those
// that match the label regular expression. The filtered profile is written to
// the passed in profileBytesBuf.
func filterCPUProfileByLabel(label string, profileBytesBuf *bytes.Buffer) error {
	p, err := profile.Parse(profileBytesBuf)
	if err != nil {
		return err
	}

	tm, err := CompileTagFilter(label)
	if err != nil {
		return err
	}
	fm, _ := p.FilterSamplesByTag(tm, nil)
	if !fm {
		return nil
	}

	profileBytesBuf.Reset()
	return p.Write(profileBytesBuf)
}

// Start starts the BackgroundProfiler that listens for new Subscribers.
func (r *BackgroundProfiler) Start(ctx context.Context) error {
	return r.stopper.RunAsyncTaskEx(ctx, stop.TaskOpts{
		TaskName: "backgroundprofiler",
		SpanOpt:  stop.ChildSpan,
	}, func(ctx context.Context) {
		ctxDone := ctx.Done()
		for {
			select {
			case <-r.stopper.ShouldQuiesce():
				return
			case <-ctxDone:
				return
			case <-r.startProfile:

				// Start collecting a CPU profile.
				var cpuProfileBuf bytes.Buffer

				// TODO(during review): This is an arbitrarily chosen frequency, the
				// runtime default is a 100Hz. This value will probably require tuning
				// depending on our use case. Egs: profiling a single query execution vs
				// profiling a long running job for a few minutes vs profiling a
				// statement fingerprint.
				runtime.SetCPUProfileRate(500)
				if err := pprof.StartCPUProfile(&cpuProfileBuf); err != nil {
					log.Warningf(ctx, "failed to start CPU profile %+v", err)
				}

				// Start collecting an execution trace.
				//
				// Doing this after we start collecting CPU profiles means that our
				// execution traces will also have CPU samples as events -
				// https://go-review.googlesource.com/c/go/+/400795
				var executionTraceBuf bytes.Buffer
				if err := trace.Start(&executionTraceBuf); err != nil {
					log.Warningf(ctx, "failed to start execution trace %+v", err)
				}

				r.startedProfile <- struct{}{}

				// TODO(during review): We should prevent the trace from growing too big
				// and stop the trace every few seconds. Though the visualizer seems to
				// chunk large traces into 1 second chunks so maybe we don't need to?
				<-r.stopProfile
				trace.Stop()
				pprof.StopCPUProfile()

				// Filter entries from the CPU profile that are not labelled with our
				// profile ID.
				r.mu.Lock()
				filterKey := fmt.Sprintf("%d", r.mu.profileID)
				r.mu.Unlock()
				if err := filterCPUProfileByLabel(filterKey, &cpuProfileBuf); err != nil {
					log.Warningf(ctx, "failed to filer profile labels")
				}

				// Persist the CPU profile + execution trace to local storage.
				now := timeutil.Now()
				cpuProfileName := CPUProfilerFileNamePrefix + now.Format(timeFormat)
				cpuProfilePath := r.dumpStore.GetFullPath(cpuProfileName)
				if err := os.WriteFile(cpuProfilePath, cpuProfileBuf.Bytes(), 0644); err != nil {
					log.Warningf(ctx, "failed to write profiler to %s", cpuProfilePath)
				}

				executionTraceName := RuntimeTraceFileNamePrefix + now.Format(timeFormat)
				executionTracePath := r.dumpStore.GetFullPath(executionTraceName)
				if err := os.WriteFile(executionTracePath, executionTraceBuf.Bytes(), 0644); err != nil {
					log.Warningf(ctx, "failed to write execution trace to %s", executionTracePath)
				}

				// TODO(adityamaru): Define GC semantics of the dumper.

				// Return a summary of the profiles collected.
				r.res <- &Profile{
					NodeID:       r.nodeID,
					RuntimeTrace: executionTracePath,
					CPUProfile:   cpuProfilePath,
				}
			}
		}
	})
}
