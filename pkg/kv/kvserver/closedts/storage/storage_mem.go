// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/olekukonko/tablewriter"
)

type memStorage struct {
	mu struct {
		syncutil.RWMutex
		buckets []ctpb.Entry
		scale   time.Duration
	}
}

var _ SingleStorage = (*memStorage)(nil)

// NewMemStorage initializes a SingleStorage backed by an in-memory slice that
// represents the given number of buckets, where the i-th bucket holds a closed
// timestamp approximately 2^i*scale in the past.
func NewMemStorage(scale time.Duration, buckets int) SingleStorage {
	m := &memStorage{}
	m.mu.buckets = make([]ctpb.Entry, buckets)
	m.mu.scale = scale
	return m
}

func (m *memStorage) String() string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var buf bytes.Buffer
	tw := tablewriter.NewWriter(&buf)

	header := make([]string, 1+len(m.mu.buckets))
	header[0] = ""
	align := make([]int, 1+len(m.mu.buckets))
	align[0] = tablewriter.ALIGN_LEFT

	for i := range m.mu.buckets {
		header[1+i] = m.mu.buckets[i].ClosedTimestamp.String() + "\nage=" + time.Duration(
			m.mu.buckets[0].ClosedTimestamp.WallTime-m.mu.buckets[i].ClosedTimestamp.WallTime,
		).String() + " (target ≤" + m.bucketMaxAge(i).String() + ")\nepoch=" + fmt.Sprintf("%d", m.mu.buckets[i].Epoch)
		align[1+i] = tablewriter.ALIGN_RIGHT
	}
	tw.SetAutoFormatHeaders(false)
	tw.SetColumnAlignment(align)
	tw.SetHeader(header)
	tw.SetHeaderLine(true)
	tw.SetRowLine(false)
	tw.SetColumnSeparator(" ")
	tw.SetBorder(true)
	tw.SetTrimWhiteSpaceAtEOL(true)
	rangeIDs := make([]roachpb.RangeID, 0, len(m.mu.buckets[0].MLAI))
	for rangeID := range m.mu.buckets[0].MLAI {
		rangeIDs = append(rangeIDs, rangeID)
	}
	sort.Slice(rangeIDs, func(i, j int) bool {
		return rangeIDs[i] < rangeIDs[j]
	})

	row := make([]string, 1+len(m.mu.buckets))
	for _, rangeID := range rangeIDs {
		row[0] = "r" + strconv.FormatInt(int64(rangeID), 10)
		for i, entry := range m.mu.buckets {
			lai, ok := entry.MLAI[rangeID]
			if ok {
				row[1+i] = strconv.FormatInt(int64(lai), 10)
			} else {
				row[1+i] = ""
			}
		}
		tw.Append(row)
	}

	tw.Render()
	return buf.String()
}

func (m *memStorage) bucketMaxAge(index int) time.Duration {
	if index == 0 {
		return 0
	}
	return (1 << uint(index-1)) * m.mu.scale
}

func (m *memStorage) Add(e ctpb.Entry) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := e.ClosedTimestamp.WallTime

	for i := 0; i < len(m.mu.buckets); i++ {
		if time.Duration(now-m.mu.buckets[i].ClosedTimestamp.WallTime) <= m.bucketMaxAge(i) {
			break
		}
		mergedEntry := merge(m.mu.buckets[i], e)
		e = m.mu.buckets[i]
		m.mu.buckets[i] = mergedEntry
	}
}

func (m *memStorage) VisitAscending(f func(ctpb.Entry) (done bool)) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for i := len(m.mu.buckets) - 1; i >= 0; i-- {
		entry := m.mu.buckets[i]
		if entry.Epoch == 0 {
			// Skip empty buckets.
			continue
		}
		if f(entry) {
			return
		}
	}
}

func (m *memStorage) VisitDescending(f func(ctpb.Entry) (done bool)) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for l, i := len(m.mu.buckets), 0; i < l; i++ {
		entry := m.mu.buckets[i]
		// Stop once we hit an empty bucket (which implies that all further buckets
		// are also empty), or once the visitor is satisfied.
		if entry.Epoch == 0 || f(entry) {
			return
		}
	}
}

func (m *memStorage) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := 0; i < len(m.mu.buckets); i++ {
		m.mu.buckets[i] = ctpb.Entry{}
	}
}

func merge(e, ee ctpb.Entry) ctpb.Entry {
	// TODO(tschottdorf): if either of these hit, check that what we're
	// returning has Full set. If we make it past, check that either of
	// them has it set. The first Entry the Storage sees for an epoch must have it
	// set, so the assertions should never fire.
	if e.Epoch < ee.Epoch {
		return ee
	} else if e.Epoch > ee.Epoch {
		return e
	}

	// Epochs match, so we can actually update.

	// Initialize re as a deep copy of e.
	re := e
	re.MLAI = map[roachpb.RangeID]ctpb.LAI{}
	for rangeID, mlai := range e.MLAI {
		re.MLAI[rangeID] = mlai
	}
	// The result is full if either operand is.
	re.Full = e.Full || ee.Full
	// Use the larger of both timestamps with the union of the MLAIs, preferring larger
	// ones on conflict.
	re.ClosedTimestamp.Forward(ee.ClosedTimestamp)
	for rangeID, mlai := range ee.MLAI {
		if cur, found := re.MLAI[rangeID]; !found || cur < mlai {
			re.MLAI[rangeID] = mlai
		}
	}
	return re
}
