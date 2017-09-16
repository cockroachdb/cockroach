// Copyright 2017 The Cockroach Authors.
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
// permissions and limitations under the License.

package server

import (
	"fmt"
	"regexp"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/util/caller"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const (
	logSpyDefaultDuration = 10 * time.Second
	logSpyMaxDuration     = 1 * time.Minute
	logSpyDefaultCount    = 100
	logSpyChanCap         = 4096
)

type logSpyOptions struct {
	Count    int
	Duration time.Duration
	Grep     *regexp.Regexp
}

func (opts *logSpyOptions) validate() error {
	if opts.Count == 0 {
		opts.Count = logSpyDefaultCount
	}
	if opts.Duration == 0 {
		if opts.Count > 0 {
			opts.Duration = logSpyMaxDuration
		} else {
			opts.Duration = logSpyDefaultDuration
		}
	} else if opts.Duration > logSpyMaxDuration {
		return errors.Errorf("duration %s is too large (limit is %s)", opts.Duration, logSpyMaxDuration)
	}
	if opts.Grep == nil {
		return errors.Errorf("options must include a non-nil regexp")
	}
	return nil
}

type logSpyMsg struct {
	entry log.Entry
	err   error
}

type logSpy struct {
	setIntercept func(ctx context.Context, f log.InterceptorFn) error
}

func (spy *logSpy) run(
	ctx context.Context, msgC chan<- logSpyMsg, opts logSpyOptions,
) (dropped int32) {
	defer close(msgC)

	if err := opts.validate(); err != nil {
		msgC <- logSpyMsg{err: err}
		return 0
	}

	ctx, cancel := context.WithTimeout(ctx, opts.Duration)
	defer cancel()

	var countDropped int32
	defer func() {
		dropped = atomic.LoadInt32(&countDropped)
	}()

	// Note that in the code below, this channel is never closed. This is
	// because we don't know when that is safe. This is sketchy in general but
	// OK here since we don't have to guarantee that the channel is fully
	// consumed.
	entries := make(chan log.Entry, logSpyChanCap)

	{
		f, l, _ := caller.Lookup(0)
		entry := log.MakeEntry(
			log.Severity_INFO, timeutil.Now().UnixNano(), f, l,
			fmt.Sprintf("intercepting logs with options %+v", opts))
		entries <- entry
	}

	setInterceptOrErr := func(f log.InterceptorFn) {
		if err := spy.setIntercept(ctx, f); err != nil {
			msgC <- logSpyMsg{err: err}
		}
	}
	setInterceptOrErr(func(entry log.Entry) {
		if re := opts.Grep; re != nil && !re.MatchString(entry.Message) && !re.MatchString(entry.File) {
			return
		}

		select {
		case entries <- entry:
		default:
			// Consumer fell behind, just drop the message.
			atomic.AddInt32(&countDropped, 1)
		}
	})
	defer setInterceptOrErr(nil)

	var count int
	var done <-chan struct{} // set later; assures that we always send header message
	for {
		select {
		case <-done:
			return
		case entry := <-entries:
			msgC <- logSpyMsg{entry: entry}
			count++
			if count >= opts.Count {
				return
			}
			if done == nil {
				done = ctx.Done()
			}
		}
	}
}
