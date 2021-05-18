// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package goroutineui

import (
	"bytes"
	"io"
	"io/ioutil"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/maruel/panicparse/stack"
)

// A Dump wraps a goroutine dump with functionality to output through panicparse.
type Dump struct {
	err error

	now     time.Time
	buckets []*stack.Bucket
}

// NewDump grabs a goroutine dump and associates it with the supplied time.
func NewDump(now time.Time) Dump {
	stacks := log.GetStacks(true /* all */)
	return NewDumpFromBytes(now, stacks.StripMarkers())
}

// NewDumpFromBytes is like NewDump, but treats the supplied bytes as a goroutine
// dump.
func NewDumpFromBytes(now time.Time, b []byte) Dump {
	c, err := stack.ParseDump(bytes.NewReader(b), ioutil.Discard, true /* guesspaths */)
	if err != nil {
		return Dump{err: err}
	}
	return Dump{now: now, buckets: stack.Aggregate(c.Goroutines, stack.AnyValue)}
}

// SortCountDesc rearranges the goroutine buckets such that higher multiplicities
// appear earlier.
func (d Dump) SortCountDesc() {
	sort.Slice(d.buckets, func(i, j int) bool {
		a, b := d.buckets[i], d.buckets[j]
		return len(a.IDs) > len(b.IDs)
	})
}

// SortWaitDesc rearranges the goroutine buckets such that goroutines that have
// longer wait times appear earlier.
func (d Dump) SortWaitDesc() {
	sort.Slice(d.buckets, func(i, j int) bool {
		a, b := d.buckets[i], d.buckets[j]
		return a.SleepMax > b.SleepMax
	})
}

// HTML writes the rendered output of panicparse into the supplied Writer.
func (d Dump) HTML(w io.Writer) error {
	if d.err != nil {
		return d.err
	}
	return writeToHTML(w, d.buckets, d.now)
}

// HTMLString is like HTML, but returns a string. If an error occurs, its string
// representation is returned.
func (d Dump) HTMLString() string {
	var w strings.Builder
	if err := d.HTML(&w); err != nil {
		return err.Error()
	}
	return w.String()
}
