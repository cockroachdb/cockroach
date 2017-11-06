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

package sql

import (
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"golang.org/x/net/context"
)

var tableNames = map[string]bool{
	"parent1":     true,
	"child1":      true,
	"grandchild1": true,
	"child2":      true,
	"parent2":     true,
}

func parseTestKey(kvDB *client.DB, keyStr string) (roachpb.Key, error) {
	var key []byte
	tokens := strings.Split(keyStr, "/")

	for _, tok := range tokens {
		// Encode the table ID if the token is a table name.
		if tableNames[tok] {
			desc := sqlbase.GetTableDescriptor(kvDB, sqlutils.TestDB, tok)
			key = encoding.EncodeUvarintAscending(key, uint64(desc.ID))
			continue
		}

		// Interleave sentinel.
		if tok == "#" {
			key = encoding.EncodeNotNullDescending(key)
			continue
		}

		// Assume any other value is an unsigned integer.
		tokInt, err := strconv.ParseUint(tok, 10, 64)
		if err != nil {
			return nil, err
		}
		key = encoding.EncodeUvarintAscending(key, tokInt)
	}

	return key, nil
}

func TestFixInterleavePartitions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	sqlutils.CreateTestInterleaveHierarchy(t, sqlDB)

	for i, tc := range []struct {
		table1 string
		table2 string
		// each input is a partition: a slice of Start/EndKeys.
		input    [][2]string
		expected [][2]string
	}{
		// Full table scan. No fixing should happen.
		{
			"parent1", "child1",
			[][2]string{{"parent1/1", "parent1/2"}},
			[][2]string{{"parent1/1", "parent1/2"}},
		},

		// Split didn't happen between child1 rows. No need to fix
		// EndKey.
		{
			"parent1", "child1",
			[][2]string{{"parent1/1", "parent1/1/42"}},
			[][2]string{{"parent1/1", "parent1/1/42"}},
		},

		// Split before child1 row (pid1 = 42, cid1 = 13, cid2 = 37).
		// We want to fix span to everything up to parent row pid1 =
		// 43.
		{
			"parent1", "child1",
			[][2]string{{"parent1/1", "parent1/1/42/#/child1/1/13/37"}},
			[][2]string{{"parent1/1", "parent1/1/43"}},
		},

		// Split before a grandchild1 row (with pid1 = 42, cid1 = 13,
		// cid2 = 37, gcid1 = 100).
		// We want to fix span to everything up to parent row pid1 =
		// 43.
		{
			"parent1", "child1",
			[][2]string{{"parent1/1", "parent1/1/42/#/child1/1/13/37/#/grandchild1/1/100"}},
			[][2]string{{"parent1/1", "parent1/1/43"}},
		},

		// Split on child1 rows with cid1 < 13 (not technically
		// possible, but worth a test).
		{
			"parent1", "child1",
			[][2]string{{"parent1/1", "parent1/1/42/#/child1/1/13"}},
			[][2]string{{"parent1/1", "parent1/1/43"}},
		},

		// Different child table (child2) split before child2 row.
		{
			"parent1", "child2",
			[][2]string{{"parent1/1", "parent1/1/42/#/child2/1/13/37"}},
			[][2]string{{"parent1/1", "parent1/1/43"}},
		},

		// Split before grandchild1 row (gcid = 100) where ancestor is
		// on parent1 (i.e. a join on pid1).
		// We want to include all grandchild1 keys under pid1 = 42,
		// thus we need to modify EndKey to be the parent1 row with
		// pid1 = 43.
		{
			"parent1", "grandchild1",
			[][2]string{{"parent1/1", "parent1/1/42/#/child1/1/13/37/#/grandchild1/1/100"}},
			[][2]string{{"parent1/1", "parent1/1/43"}},
		},

		// Verify the fix to EndKey cascades to the next span.
		{
			"parent1", "grandchild1",
			[][2]string{
				{"parent1/1", "parent1/1/42/#/child1/1/13/37/#/grandchild1/1/100"},
				{"parent1/1/42/#/child1/1/13/37/#/grandchild1/1/100", "parent1/1/44"},
			},
			[][2]string{
				{"parent1/1", "parent1/1/43"},
				{"parent1/1/43", "parent1/1/44"},
			},
		},

		// Verify the fix to EndKey cascades and removes any redundant
		// spans after cascading.

		{
			"parent1", "grandchild1",
			[][2]string{
				{"parent1/1", "parent1/1/42/#/child1/1/13/37/#/grandchild1/1/100"},
				{"parent1/1/42/#/child1/1/13/37/#/grandchild1/1/100", "parent1/1/42/#/child1/1/13/38"},
				{"parent1/1/42/#/child1/1/13/38", "parent1/1/42/#/child1/1/13/38/#/grandchild1/1/200"},
				{"parent1/1/42/#/child1/1/13/38/#/grandchild1/1/200", "parent1/1/44"},
			},
			[][2]string{
				{"parent1/1", "parent1/1/43"},
				{"parent1/1/43", "parent1/1/44"},
			},
		},

		// Verify the fix works even if all spans afterwards are
		// redacted (because they're redundant).
		{
			"parent1", "grandchild1",
			[][2]string{
				{"parent1/1", "parent1/1/42/#/child1/1/13/37/#/grandchild1/1/100"},
				{"parent1/1/42/#/child1/1/13/37/#/grandchild1/1/100", "parent1/1/42/#/child1/1/13/38"},
				{"parent1/1/42/#/child1/1/13/38", "parent1/1/42/#/child1/1/13/38/#/grandchild1/1/200"},
			},
			[][2]string{
				{"parent1/1", "parent1/1/43"},
			},
		},

		// Split before grandchild1 row (gcid = 100) where ancestor is
		// child1.
		// We want to include all granchild1 keys under cid1 = 13, cid2
		// = 37.
		// There is a further ancestor (parent1) which we should ignore
		// since we only care about all grandchild1 rows under our
		// last child1 row (with cid1 = 13, cid2 = 37).
		{
			"child1", "grandchild1",
			[][2]string{{"parent1/1", "parent1/1/42/#/child1/1/13/37/#/grandchild1/1/100"}},
			[][2]string{{"parent1/1", "parent1/1/42/#/child1/1/13/38"}},
		},
	} {
		join, err := newTestJoinNode(kvDB, tc.table1, tc.table2)
		if err != nil {
			t.Fatal(err)
		}

		inSpans := make(roachpb.Spans, len(tc.input))
		for j, spanArgs := range tc.input {
			start, err := parseTestKey(kvDB, spanArgs[0])
			if err != nil {
				t.Fatal(err)
			}
			end, err := parseTestKey(kvDB, spanArgs[1])
			if err != nil {
				t.Fatal(err)
			}
			inSpans[j] = roachpb.Span{
				Key:    start,
				EndKey: end,
			}
		}
		input := []spanPartition{{spans: inSpans}}

		actual, err := fixInterleavePartitions(join, input)

		if len(actual) != 1 {
			t.Fatalf("%d: expected only one span partition from fixInterleavePartitions, got %d", i, len(actual))
		}
		actualSpans := actual[0].spans
		expSpans := make(roachpb.Spans, len(tc.expected))
		for j, spanArgs := range tc.expected {
			start, err := parseTestKey(kvDB, spanArgs[0])
			if err != nil {
				t.Fatal(err)
			}
			end, err := parseTestKey(kvDB, spanArgs[1])
			if err != nil {
				t.Fatal(err)
			}
			expSpans[j] = roachpb.Span{
				Key:    start,
				EndKey: end,
			}
		}

		if len(expSpans) != len(actualSpans) {
			t.Fatalf("%d: expected %d spans, got %d spans", i, len(expSpans), len(actualSpans))
		}

		for j, expected := range expSpans {
			if !expected.EqualValue(actualSpans[j]) {
				t.Errorf("%d: expected span %s, got %s", i, expected, actualSpans[j])
			}
		}
	}
}
