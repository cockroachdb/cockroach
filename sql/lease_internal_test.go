// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql

import (
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestMakeLeaseKey(t *testing.T) {
	defer leaktest.AfterTest(t)

	testData := []struct {
		nodeID     uint32
		descID     ID
		version    uint32
		expiration time.Time
		expected   string
	}{
		{1, 2, 3, time.Date(2015, 10, 20, 15, 22, 10, 0, time.UTC),
			"/1/2/3/2015-10-20 15:22:10+00:00/1"},
		{4, 5, 6, time.Date(2015, 10, 20, 15, 26, 30, 0, time.UTC),
			"/1/5/6/2015-10-20 15:26:30+00:00/4"},
	}
	for _, d := range testData {
		key := makeLeaseKey(d.nodeID, d.descID, d.version, d.expiration.UnixNano())
		if s := prettyKey(key, 1); d.expected != s {
			t.Fatalf("expected %s, but found %s", d.expected, s)
		}
	}
}

func TestMakeLeaseScanKeys(t *testing.T) {
	defer leaktest.AfterTest(t)

	testData := []struct {
		descID        ID
		version       uint32
		expiration    time.Time
		expectedStart string
		expectedEnd   string
	}{
		{1, 2, time.Date(2015, 10, 20, 15, 28, 17, 0, time.UTC),
			"/1/1/2/2015-10-20 15:28:17+00:00", "/1/1/3"},
		{3, 4, time.Date(2015, 10, 20, 15, 32, 48, 0, time.UTC),
			"/1/3/4/2015-10-20 15:32:48+00:00", "/1/3/5"},
	}
	for _, d := range testData {
		startKey, endKey := makeLeaseScanKeys(d.descID, d.version, d.expiration.UnixNano())
		if s := prettyKey(startKey, 1); d.expectedStart != s {
			t.Fatalf("expected %s, but found %s", d.expectedStart, s)
		}
		if s := prettyKey(endKey, 1); d.expectedEnd != s {
			t.Fatalf("expected %s, but found %s", d.expectedEnd, s)
		}
	}
}

func TestLeaseSet(t *testing.T) {
	defer leaktest.AfterTest(t)

	type data struct {
		version    uint32
		expiration int64
	}
	type insert data
	type remove data

	type newest struct {
		version uint32
	}

	testData := []struct {
		op       interface{}
		expected string
	}{
		{newest{0}, "<nil>"},
		{insert{2, 3}, "2:3"},
		{newest{0}, "2:3"},
		{newest{2}, "2:3"},
		{newest{3}, "<nil>"},
		{insert{2, 1}, "2:1 2:3"},
		{newest{0}, "2:3"},
		{newest{2}, "2:3"},
		{newest{3}, "<nil>"},
		{insert{2, 4}, "2:1 2:3 2:4"},
		{newest{0}, "2:4"},
		{newest{2}, "2:4"},
		{newest{3}, "<nil>"},
		{insert{2, 2}, "2:1 2:2 2:3 2:4"},
		{insert{3, 1}, "2:1 2:2 2:3 2:4 3:1"},
		{newest{0}, "3:1"},
		{newest{2}, "2:4"},
		{newest{3}, "3:1"},
		{newest{4}, "<nil>"},
		{insert{1, 1}, "1:1 2:1 2:2 2:3 2:4 3:1"},
		{newest{0}, "3:1"},
		{newest{1}, "1:1"},
		{newest{2}, "2:4"},
		{newest{3}, "3:1"},
		{newest{4}, "<nil>"},
		{remove{0, 0}, "1:1 2:1 2:2 2:3 2:4 3:1"},
		{remove{2, 4}, "1:1 2:1 2:2 2:3 3:1"},
		{remove{3, 1}, "1:1 2:1 2:2 2:3"},
		{remove{1, 1}, "2:1 2:2 2:3"},
		{remove{2, 2}, "2:1 2:3"},
		{remove{2, 3}, "2:1"},
		{remove{2, 1}, ""},
	}

	set := &leaseSet{}
	for i, d := range testData {
		switch op := d.op.(type) {
		case insert:
			s := &LeaseState{}
			s.Version = op.version
			s.expiration = op.expiration
			set.insert(s)
		case remove:
			s := &LeaseState{}
			s.Version = op.version
			s.expiration = op.expiration
			set.remove(s)
		case newest:
			n := set.findNewest(op.version)
			if s := fmt.Sprint(n); d.expected != s {
				t.Fatalf("%d: expected %s, but found %s", i, d.expected, s)
			}
			continue
		}
		if s := set.String(); d.expected != s {
			t.Fatalf("%d: expected %s, but found %s", i, d.expected, s)
		}
	}
}
