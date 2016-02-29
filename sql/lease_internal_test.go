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
// permissions and limitations under the License.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql

import (
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestLeaseSet(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type data struct {
		version    DescriptorVersion
		expiration int64
	}
	type insert data
	type remove data

	type newest struct {
		version DescriptorVersion
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
		{newest{1}, "<nil>"},
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
			s.expiration.Time = time.Unix(0, op.expiration)
			set.insert(s)
		case remove:
			s := &LeaseState{}
			s.Version = op.version
			s.expiration.Time = time.Unix(0, op.expiration)
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
