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
	"context"
	"fmt"
	"reflect"
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
	"github.com/pkg/errors"
)

func setTestEqCols(n *joinNode, colNames []string) error {
	left := n.left.plan.(*scanNode)
	right := n.right.plan.(*scanNode)

	n.pred = &joinPredicate{}
	n.mergeJoinOrdering = nil

	for _, colName := range colNames {
		if colName == "" {
			continue
		}

		colFound := false
		for i, leftCol := range left.cols {
			if colName == leftCol.Name {
				n.pred.leftEqualityIndices = append(
					n.pred.leftEqualityIndices,
					i,
				)
				colFound = true
				break
			}
		}
		if !colFound {
			return errors.Errorf("column %s not found in %s", colName, left.desc.Name)
		}

		colFound = false
		for i, rightCol := range right.cols {
			if colName == rightCol.Name {
				n.pred.rightEqualityIndices = append(
					n.pred.rightEqualityIndices,
					i,
				)
				colFound = true
				break
			}
		}
		if !colFound {
			return errors.Errorf("column %s not found in %s", colName, right.desc.Name)
		}
	}

	n.mergeJoinOrdering = computeMergeJoinOrdering(
		planPhysicalProps(n.left.plan),
		planPhysicalProps(n.right.plan),
		n.pred.leftEqualityIndices,
		n.pred.rightEqualityIndices,
	)

	return nil
}

func genPermutations(slice []string) [][]string {
	if len(slice) == 0 {
		return [][]string{{}}
	}

	var out [][]string
	for i, str := range slice {
		recurse := append([]string{}, slice[:i]...)
		recurse = append(recurse, slice[i+1:]...)
		for _, subperms := range genPermutations(recurse) {
			out = append(out, append([]string{str}, subperms...))
		}
	}

	return out
}

var tableNames = map[string]bool{
	"parent1":     true,
	"child1":      true,
	"grandchild1": true,
	"child2":      true,
	"parent2":     true,
}

// Format for any key:
//   <table-name>/<index-id>/<index-col1>/.../#/<table-name>/<index-id>/....
func encodeTestKey(kvDB *client.DB, keyStr string) (roachpb.Key, error) {
	var key []byte
	tokens := strings.Split(keyStr, "/")

	for _, tok := range tokens {
		// Encode the table ID if the token is a table name.
		if tableNames[tok] {
			desc := sqlbase.GetTableDescriptor(kvDB, sqlutils.TestDB, tok)
			key = encoding.EncodeUvarintAscending(key, uint64(desc.ID))
			continue
		}

		// Interleaved sentinel.
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

func decodeTestKey(kvDB *client.DB, key roachpb.Key) (string, error) {
	var out []byte

	keyStr := roachpb.PrettyPrintKey(nil /* valDirs */, key)
	tokens := strings.Split(keyStr, "/")[1:]

	for i := 0; i < len(tokens); i++ {
		tok := tokens[i]
		// We know for certain the next token is the table ID. Need
		// to convert into a table name.
		if tok == "Table" || tok == "#" {
			if tok == "#" {
				out = append(out, []byte("#/")...)
			}

			descID, err := strconv.ParseUint(tokens[i+1], 10, 64)
			if err != nil {
				return "", err
			}

			if err := kvDB.Txn(context.TODO(), func(ctx context.Context, txn *client.Txn) error {
				desc, err := sqlbase.GetTableDescFromID(context.TODO(), txn, sqlbase.ID(descID))
				if err != nil {
					return err
				}

				out = append(out, []byte(desc.Name)...)
				return nil
			}); err != nil {
				return "", err
			}

			// We read an extra token for the table ID.
			i++
		} else {
			// Encode anything else as is.
			out = append(out, []byte(tok)...)
		}

		out = append(out, '/')
	}

	// Omit the last '/'.
	return string(out[:len(out)-1]), nil
}

// See CreateTestInterleavedHierarchy for the longest chain used for the short
// format.
var shortFormTables = [3]string{"parent1", "child1", "grandchild1"}

// shortToLongKey converts the short key format preferred in test cases
//    /1/#/3/4
// to its long form required by parseTestkey
//    parent1/1/1/#/child1/1/3/4
func shortToLongKey(short string) string {
	tableOrder := shortFormTables
	curTableIdx := 0

	var long []byte
	tokens := strings.Split(short, "/")
	// Verify short format starts with '/'.
	if tokens[0] != "" {
		panic("missing '/' token at the beginning of short format")
	}
	// Skip the first element since short format has starting '/'.
	tokens = tokens[1:]

	// Always append parent1.
	long = append(long, []byte(fmt.Sprintf("%s/1/", tableOrder[curTableIdx]))...)
	curTableIdx++

	for _, tok := range tokens {
		// New interleaved table and primary keys follow.
		if tok == "#" {
			if curTableIdx >= len(tableOrder) {
				panic("too many '#' tokens specified in short format (max 2 for child1 and grandchild1)")
			}

			long = append(long, []byte(fmt.Sprintf("#/%s/1/", tableOrder[curTableIdx]))...)
			curTableIdx++

			continue
		}

		long = append(long, []byte(fmt.Sprintf("%s/", tok))...)
	}

	// Remove the last '/'.
	return string(long[:len(long)-1])
}

func TestUseInterleavedJoin(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	sqlutils.CreateTestInterleavedHierarchy(t, sqlDB)

	// Only test cases on the full interleave prefix between the two
	// tables should return true.
	for _, tc := range []struct {
		table1   string
		table2   string
		eqCols   string
		expected bool
	}{
		// Refer to comment above CreateTestInterleavedHierarchy for
		// table schemas.

		// Simple parent-child case.
		// parent1-child1 share interleave prefix (pid1).
		{"parent1", "child1", "pid1", true},
		{"parent1", "child1", "pid1,v", false},
		{"parent1", "child1", "", false},
		{"parent1", "child1", "v", false},
		// Parent-grandchild case.
		// parent1-grandchild1 share interleave prefix (pid1).
		{"parent1", "grandchild1", "pid1", true},
		{"parent1", "grandchild1", "pid1,v", false},
		{"parent1", "grandchild1", "", false},
		{"parent1", "grandchild1", "v", false},
		// Multiple-column interleave prefix.
		// child1-grandchild1 share interleave prefix (pid1, cid1,
		// cid2).
		{"child1", "grandchild1", "pid1,cid1,cid2", true},
		{"child1", "grandchild1", "pid1,cid1,cid2,v", false},
		{"child1", "grandchild1", "", false},
		{"child1", "grandchild1", "v", false},
		// TODO(richardwu): update these once prefix/subset of
		// interleave prefixes are permitted.
		{"child1", "grandchild1", "cid1", false},
		{"child1", "grandchild1", "cid2", false},
		{"child1", "grandchild1", "cid1,v", false},
		{"child1", "grandchild1", "cid2,v", false},
		{"child1", "grandchild1", "cid1,cid2", false},
		{"child1", "grandchild1", "cid1,cid2,v", false},
		{"child1", "grandchild1", "pid1,cid1", false},
		{"child1", "grandchild1", "pid1,cid2", false},
		{"child1", "grandchild1", "pid1,cid1,v", false},
		{"child1", "grandchild1", "pid1,cid2,v", false},
		// Common ancestor example.
		{"child1", "child2", "", false},
		// TODO(richardwu): update this when common ancestor
		// interleaved joins are possible.
		{"child1", "child2", "pid1", false},
	} {
		// Run the subtests with the tables in both positions (left and
		// right).
		for i := 0; i < 2; i++ {
			// Run every permutation of the equality columns (just
			// to ensure mergeJoinOrdering is invariant since we
			// rely on it to correspond with the primary index of
			// the ancestor).
			eqCols := strings.Split(tc.eqCols, ",")
			for _, colNames := range genPermutations(eqCols) {
				testName := fmt.Sprintf("%s-%s-%s", tc.table1, tc.table2, strings.Join(colNames, ","))
				t.Run(testName, func(t *testing.T) {
					join, err := newTestJoinNode(kvDB, tc.table1, tc.table2)
					if err != nil {
						t.Fatal(err)
					}
					join.joinType = sqlbase.InnerJoin

					if err := setTestEqCols(join, colNames); err != nil {
						t.Fatal(err)
					}
					join.mergeJoinOrdering = computeMergeJoinOrdering(
						planPhysicalProps(join.left.plan),
						planPhysicalProps(join.right.plan),
						join.pred.leftEqualityIndices,
						join.pred.rightEqualityIndices,
					)

					actual := useInterleavedJoin(join)

					if tc.expected != actual {
						t.Errorf("expected useInterleaveJoin to return %t, actual %t", tc.expected, actual)
					}
				})
			}
			// Rerun the same subtests but flip the tables
			tc.table1, tc.table2 = tc.table2, tc.table1
		}
	}
}

func TestMaximalJoinPrefix(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	sqlutils.CreateTestInterleavedHierarchy(t, sqlDB)

	testCases := []struct {
		table1    string
		table2    string
		input     string
		expected  string
		truncated bool
	}{
		// Key is already an ancestor prefix.
		{"parent1", "child1", "/2", "/2", false},

		// Key of descendant child1.
		{"parent1", "child1", "/2/#/3/4", "/2", true},

		// Partial key of descendant child1 (only cid1, missing cid2).
		{"parent1", "child1", "/2/#/1/3", "/2", true},

		// Key of descendant grandchild1.
		{"parent1", "grandchild1", "/2/#/3/4/#/5", "/2", true},

		// Key of some descendant child1 is still a descendant key
		// of parent1.
		{"parent1", "grandchild1", "/2/#/3/4", "/2", true},

		// Key is already an ancestor prefix of child1.
		{"child1", "grandchild1", "/2/#/3/4", "/2/#/3/4", false},

		// Key of descendant grandchild1 with ancestor child1:
		// prefix of parent1 retained.
		{"child1", "grandchild1", "/2/#/3/4/#/5", "/2/#/3/4", true},

		// TODO(richardwu): prefix/subset joins and sibiling joins.
	}

	for testIdx, tc := range testCases {
		t.Run(strconv.Itoa(testIdx), func(t *testing.T) {
			join, err := newTestJoinNode(kvDB, tc.table1, tc.table2)
			if err != nil {
				t.Fatal(err)
			}

			input, err := encodeTestKey(kvDB, shortToLongKey(tc.input))
			if err != nil {
				t.Fatal(err)
			}

			ancestor, descendant := join.interleavedNodes()

			// Compute maximal join prefix.
			actualKey, truncated, err := maximalJoinPrefix(ancestor, descendant, input)
			if err != nil {
				t.Fatal(err)
			}

			actual, err := decodeTestKey(kvDB, actualKey)
			if err != nil {
				t.Fatal(err)
			}

			expected := shortToLongKey(tc.expected)

			if expected != actual {
				t.Errorf("unexpected maximal join prefix.\nexpected:\t%s\nactual:\t%s", expected, actual)
			}

			if tc.truncated != truncated {
				t.Errorf("expected maximalJoinPrefix to return %t for truncated, got %t", tc.truncated, truncated)
			}
		})
	}
}

type testPartition struct {
	node  roachpb.NodeID
	spans [][2]string
}

func makeSpanPartitions(kvDB *client.DB, testParts []testPartition) ([]spanPartition, error) {
	spanParts := make([]spanPartition, len(testParts))

	for i, testPart := range testParts {
		spanParts[i].node = testPart.node
		for _, span := range testPart.spans {
			start, err := encodeTestKey(kvDB, shortToLongKey(span[0]))
			if err != nil {
				return nil, err
			}

			end, err := encodeTestKey(kvDB, shortToLongKey(span[1]))
			if err != nil {
				return nil, err
			}

			spanParts[i].spans = append(
				spanParts[i].spans,
				roachpb.Span{Key: start, EndKey: end},
			)
		}
	}

	return spanParts, nil
}

func TestAlignInterleavedSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	sqlutils.CreateTestInterleavedHierarchy(t, sqlDB)

	testCases := []struct {
		table1 string
		table2 string

		ancsParts []testPartition
		descParts []testPartition
		expected  []testPartition
	}{
		// Test that child1 spans get mapped to their corresponding
		// parent1 spans and the descendant span is recursively split
		// to satisfaction.
		{
			table1: "parent1", table2: "child1",

			ancsParts: []testPartition{
				// Test that the next parent row after the
				// last is computed properly if the end key
				// is not a parent1 key.
				{1, [][2]string{{"/1", "/2/#/5"}}},
				// End key is a parent1 key.
				{2, [][2]string{{"/3", "/4"}}},
				{3, [][2]string{{"/4", "/5"}}},
			},

			descParts: []testPartition{
				{4, [][2]string{{"/1/#/7", "/4/#/8"}}},
			},

			expected: []testPartition{
				{1, [][2]string{{"/1/#/7", "/3"}}},
				{2, [][2]string{{"/3", "/4"}}},
				{3, [][2]string{{"/4", "/4/#/8"}}},
			},
		},

		// Test that child spans do not get remapped if they're already
		// on the correct node.
		{
			table1: "parent1", table2: "child1",

			ancsParts: []testPartition{
				{1, [][2]string{{"/1", "/3"}}},
			},

			descParts: []testPartition{
				{1, [][2]string{{"/1/#/7", "/2/#/8"}}},
			},

			expected: []testPartition{
				{1, [][2]string{{"/1/#/7", "/2/#/8"}}},
			},
		},

		// Test that even if the parent1 span does not entirely contain
		// the child1 span, it gets mapped to the relevant parent row
		// correctly.
		{
			table1: "parent1", table2: "child1",

			ancsParts: []testPartition{
				{1, [][2]string{{"/1", "/1/#/5"}}},
			},

			descParts: []testPartition{
				{2, [][2]string{{"/1/#/7", "/1/#/8"}}},
			},

			expected: []testPartition{
				{1, [][2]string{{"/1/#/7", "/1/#/8"}}},
			},
		},

		// Test that multiple child spans mapped to the same nodes
		// are merged and properly ordered.
		{
			table1: "parent1", table2: "child1",

			ancsParts: []testPartition{
				// Multiple spans within each partition.
				{1, [][2]string{
					{"/1", "/1/#/1"},
					{"/1/#/1", "/2"},
				}},
				{2, [][2]string{
					{"/2", "/2/#/1/1/#/8"},
					{"/2/#/1/1/#/8", "/2/#/3/5"},
					{"/2/#/3/5", "/3"},
				}},
			},

			descParts: []testPartition{
				{1, [][2]string{
					// pid1=1 rows should map to node 1.
					{"/1/#/1", "/1/#/2"},
					{"/1/#/7", "/1/#/9"},
					// pid1=2 rows should map to node 2.
					{"/2/#/1", "/2/#/2"},
					{"/2/#/5", "/2/#/8"},
				}},
				{2, [][2]string{
					// pid1=1 rows should map to node 1.
					{"/1/#/2", "/1/#/7"},
					// pid1=2 rows should map to node 2.
					// Overlaps with previous spans in node
					// 1.
					{"/2/#/2", "/2/#/6"},
				}},
				{3, [][2]string{
					// pid1=1 rows should map to node 1.
					{"/1/#/11", "/1/#/13"},
					// pid1=2 rows should map to node 2.
					{"/2/#/11", "/2/#/15"},
				}},
				// pid1=1 and pid=2 rows in a span.
				{4, [][2]string{{"/1/#/15", "/2/#/0/7/#/1"}}},
			},

			expected: []testPartition{
				{1, [][2]string{
					{"/1/#/1", "/1/#/9"},
					{"/1/#/11", "/1/#/13"},
					{"/1/#/15", "/2"},
				}},
				{2, [][2]string{
					{"/2", "/2/#/0/7/#/1"},
					{"/2/#/1", "/2/#/8"},
					{"/2/#/11", "/2/#/15"},
				}},
			},
		},

		// Test with child1 spans having parent1 keys split points.
		{
			table1: "parent1", table2: "child1",

			ancsParts: []testPartition{
				{1, [][2]string{{"/1", "/2"}}},
				{2, [][2]string{{"/2", "/3"}}},
				{3, [][2]string{{"/3", "/4"}}},
			},

			descParts: []testPartition{
				{1, [][2]string{{"/2", "/3"}}},
				// Technically not possible for two partitions
				// to have the same span.
				{3, [][2]string{{"/1", "/2"}}},
				{6, [][2]string{{"/1", "/2"}}},
			},

			expected: []testPartition{
				{1, [][2]string{{"/1", "/2"}}},
				{2, [][2]string{{"/2", "/3"}}},
			},
		},

		// Test child1 span that do not need to be remapped are still
		// split by the next parent1 row after the last.
		{
			table1: "parent1", table2: "child1",

			ancsParts: []testPartition{
				{1, [][2]string{{"/1", "/2"}}},
				{2, [][2]string{{"/2", "/3"}}},
			},

			descParts: []testPartition{
				{1, [][2]string{{"/1", "/3"}}},
			},

			expected: []testPartition{
				{1, [][2]string{{"/1", "/2"}}},
				{2, [][2]string{{"/2", "/3"}}},
			},
		},

		// Test that child1 spans that have no corresponding parent1
		// span are not remapped.
		{
			table1: "parent1", table2: "child1",

			ancsParts: []testPartition{
				{1, [][2]string{{"/1", "/2"}}},
				{2, [][2]string{{"/2", "/3"}}},
			},

			descParts: []testPartition{
				// No corresponding parent span: not remapped.
				{1, [][2]string{{"/4", "/5"}}},
				// Partially no corresponding parent span.
				{2, [][2]string{{"/2", "/4"}}},
			},

			expected: []testPartition{
				{1, [][2]string{{"/4", "/5"}}},
				{2, [][2]string{{"/2", "/4"}}},
			},
		},

		// Test parent-grandchild example.
		{
			table1: "parent1", table2: "grandchild1",

			ancsParts: []testPartition{
				{1, [][2]string{{"/1", "/2/#/1/1/#/5"}}},
				{2, [][2]string{{"/3", "/4"}}},
				{3, [][2]string{{"/4", "/5"}}},
			},

			descParts: []testPartition{
				{4, [][2]string{{"/1/#/42/37/#/5", "/2/#/1/1/#/5"}}},
				// Partial child1 key (instead of grandchild1).
				{5, [][2]string{{"/3/#/1", "/4/#/1/1/#/5"}}},
			},

			expected: []testPartition{
				{1, [][2]string{{"/1/#/42/37/#/5", "/2/#/1/1/#/5"}}},
				{2, [][2]string{{"/3/#/1", "/4"}}},
				{3, [][2]string{{"/4", "/4/#/1/1/#/5"}}},
			},
		},

		// Test child-grandchild example.
		{
			table1: "child1", table2: "grandchild1",

			ancsParts: []testPartition{
				{1, [][2]string{{"/1/#/2/3", "/2"}}},
				{2, [][2]string{{"/2/#/2/3", "/2/#/5/2"}}},
				{3, [][2]string{{"/4", "/5"}}},
				{4, [][2]string{{"/2/#/5/2", "/2/#/6"}}},
			},

			descParts: []testPartition{
				// Starts before any of the child1 spans.
				{5, [][2]string{{"/1", "/1/#/5/6"}}},
				// Starts in between the first and second
				// child1 spans.
				{6, [][2]string{{"/2/#/1/2", "/2/#/5/2/#/7"}}},
			},

			expected: []testPartition{
				{1, [][2]string{{"/1/#/2/3", "/1/#/5/6"}}},
				{2, [][2]string{{"/2/#/2/3", "/2/#/5/2"}}},
				{4, [][2]string{{"/2/#/5/2", "/2/#/5/2/#/7"}}},
				{5, [][2]string{{"/1", "/1/#/2/3"}}},
				{6, [][2]string{{"/2/#/1/2", "/2/#/2/3"}}},
			},
		},
	}

	for testIdx, tc := range testCases {
		t.Run(strconv.Itoa(testIdx), func(t *testing.T) {
			join, err := newTestJoinNode(kvDB, tc.table1, tc.table2)
			if err != nil {
				t.Fatal(err)
			}

			ancsParts, err := makeSpanPartitions(kvDB, tc.ancsParts)
			if err != nil {
				t.Fatal(err)
			}

			descParts, err := makeSpanPartitions(kvDB, tc.descParts)
			if err != nil {
				t.Fatal(err)
			}

			actual, err := alignInterleavedSpans(join, ancsParts, descParts)
			if err != nil {
				t.Fatal(err)
			}

			expected, err := makeSpanPartitions(kvDB, tc.expected)
			if err != nil {
				t.Fatal(err)
			}

			if !reflect.DeepEqual(expected, actual) {
				t.Errorf("unexpected partition results after aligning.\nexpected:\t%v\nactual:\t%v", expected, actual)
			}
		})
	}
}
