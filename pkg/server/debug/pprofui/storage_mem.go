// Copyright 2018 The Cockroach Authors.
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

package pprofui

import (
	"bytes"
	"fmt"
	"io"
	"sort"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
)

type record struct {
	id string
	t  time.Time
	b  []byte
}

// A MemStorage is a Storage implementation that holds recent profiles in memory.
type MemStorage struct {
	mu struct {
		syncutil.Mutex
		records []record // sorted by record.t
	}
	idGen        int32         // accessed atomically
	keepDuration time.Duration // zero for disabled
	keepNumber   int           // zero for disabled
}

var _ Storage = &MemStorage{}

// NewMemStorage creates a MemStorage that retains the most recent n records
// as long as they are less than d old.
//
// Records are dropped only when there is activity (i.e. an old record will
// only be dropped the next time the storage is accessed).
func NewMemStorage(n int, d time.Duration) *MemStorage {
	return &MemStorage{
		keepNumber:   n,
		keepDuration: d,
	}
}

// ID implements Storage.
func (s *MemStorage) ID() string {
	return fmt.Sprint(atomic.AddInt32(&s.idGen, 1))
}

func (s *MemStorage) cleanLocked() {
	if l, m := len(s.mu.records), s.keepNumber; l > m && m != 0 {
		s.mu.records = append([]record(nil), s.mu.records[l-m:]...)
	}
	now := timeutil.Now()
	if pos := sort.Search(len(s.mu.records), func(i int) bool {
		return s.mu.records[i].t.Add(s.keepDuration).After(now)
	}); pos < len(s.mu.records) && s.keepDuration != 0 {
		s.mu.records = append([]record(nil), s.mu.records[pos:]...)
	}
}

// Store implements Storage.
func (s *MemStorage) Store(id string, write func(io.Writer) error) error {
	var b bytes.Buffer
	if err := write(&b); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.records = append(s.mu.records, record{id: id, t: timeutil.Now(), b: b.Bytes()})
	sort.Slice(s.mu.records, func(i, j int) bool {
		return s.mu.records[i].t.Before(s.mu.records[j].t)
	})
	s.cleanLocked()
	return nil
}

// Get implements Storage.
func (s *MemStorage) Get(id string, read func(io.Reader) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, v := range s.mu.records {
		if v.id == id {
			return read(bytes.NewReader(v.b))
		}
	}
	return errors.Errorf("profile not found; it may have expired")
}
