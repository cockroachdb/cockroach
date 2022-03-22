// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package partition

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestPrefixSorter(t *testing.T) {

	defer leaktest.AfterTest(t)()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	const (
		local  = true
		remote = false
	)

	testData := []struct {
		// Partition Keys
		// The space-separated keys containing the PARTITION BY LIST values
		partitionKeys string

		// Partition Localities:  true == local, false == remote
		// There must be the same number of entries as partition spans.
		localities []bool

		// Expected string representation of the PrefixSorter, once it's constructed
		expected string

		// Expected results from calling Slice on each possible slice size
		expectedSlices []string
	}{
		{
			partitionKeys:  "[/3] [/1] [/7]",
			localities:     []bool{local, remote, remote},
			expected:       "[(1), remote] [(3), local] [(7), remote]",
			expectedSlices: []string{"(1), (3), (7)"},
		},
		{
			partitionKeys:  "[/-1] [/0/3] [/0] [/0/2] [/0/-1] [/0/2/-1] [/0/2/1] [/0/2/1/0]",
			localities:     []bool{remote, local, remote, local, remote, local, local, remote},
			expected:       "[(0, 2, 1, 0), remote] [(0, 2, -1), local] [(0, 2, 1), local] [(0, -1), remote] [(0, 2), local] [(0, 3), local] [(-1), remote] [(0), remote]",
			expectedSlices: []string{"(0, 2, 1, 0)", "(0, 2, -1), (0, 2, 1)", "(0, -1), (0, 2), (0, 3)", "(-1), (0)"},
		},
		{
			partitionKeys:  "[] [/0] [/0/2] [/0/-1] [/0/2/-1] [/0/2/1] [/0/2/1/0]",
			localities:     []bool{local, remote, local, remote, local, local, remote},
			expected:       "[(0, 2, 1, 0), remote] [(0, 2, -1), local] [(0, 2, 1), local] [(0, -1), remote] [(0, 2), local] [(0), remote] [(), local]",
			expectedSlices: []string{"(0, 2, 1, 0)", "(0, 2, -1), (0, 2, 1)", "(0, -1), (0, 2)", "(0)", "()"},
		},
		{
			partitionKeys:  "[/1] [/1/2] [/1/2/3] [/1/2/3/4] [/1/2/3/4/5] [/1/2/3/4/5/6] [/1/2/3/4/5/6/7] [/1/2/3/4/5/6/7/8] [/1/2/3/4/5/6/7/7]",
			localities:     []bool{remote, local, local, local, local, local, local, local, local},
			expected:       "[(1, 2, 3, 4, 5, 6, 7, 7), local] [(1, 2, 3, 4, 5, 6, 7, 8), local] [(1, 2, 3, 4, 5, 6, 7), local] [(1, 2, 3, 4, 5, 6), local] [(1, 2, 3, 4, 5), local] [(1, 2, 3, 4), local] [(1, 2, 3), local] [(1, 2), local] [(1), remote]",
			expectedSlices: []string{"(1, 2, 3, 4, 5, 6, 7, 7), (1, 2, 3, 4, 5, 6, 7, 8)", "(1, 2, 3, 4, 5, 6, 7)", "(1, 2, 3, 4, 5, 6)", "(1, 2, 3, 4, 5)", "(1, 2, 3, 4)", "(1, 2, 3)", "(1, 2)", "(1)"},
		},
		{
			partitionKeys:  "[/1] [/1/9] [/1/7] [/1/8] [/2] [/1/8/1000] [/1/8/-1] [/1/7/-1] [/1/8/-10000]",
			localities:     []bool{local, remote, remote, remote, remote, remote, remote, remote, remote},
			expected:       "[(1, 7, -1), remote] [(1, 8, -10000), remote] [(1, 8, -1), remote] [(1, 8, 1000), remote] [(1, 7), remote] [(1, 8), remote] [(1, 9), remote] [(1), local] [(2), remote]",
			expectedSlices: []string{"(1, 7, -1), (1, 8, -10000), (1, 8, -1), (1, 8, 1000)", "(1, 7), (1, 8), (1, 9)", "(1), (2)"},
		},
		{
			partitionKeys:  "[] [/1/2/3/4/5/6/7/8/9/10/11/12/13/14/15/16/17/18/19/20/21/22/23/24/25/26/27/28/29/30/31/32/33/34/35/36/37/38/39/40/41/42/43/44/45/46/47/48/49/50]",
			localities:     []bool{local, remote},
			expected:       "[(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50), remote] [(), local]",
			expectedSlices: []string{"(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50)", "()"},
		},
	}

	for i, tc := range testData {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			// Read the partitionKeys and localities entries to make an index that
			// only has the partitions and ps (PrefixSorter) elements populated.
			partKeys := parsePartitionKeys(&evalCtx, tc.partitionKeys)
			partitions := make([]testcat.Partition, len(partKeys))
			localPartitions := util.FastIntSet{}
			for j, partitionKey := range partKeys {
				partitionDatums := make([]tree.Datums, 1)
				partitionDatums[0] = partitionKey
				partitions[j] = testcat.Partition{}
				partitions[j].SetDatums(partitionDatums)

				if tc.localities[j] {
					localPartitions.Add(j)
				}
			}

			// Make the index
			index := &testcat.Index{}
			index.SetPartitions(partitions)
			// Make the PrefixSorter.
			ps := GetSortedPrefixes(index, localPartitions, &evalCtx)

			// Run the tests.
			if res := ps.String(); res != tc.expected {
				t.Errorf("expected  %s  got  %s", tc.expected, res)
			}
			i = 0

			// Get the first slice in the PrefixSorter
			prefixSlice, _, ok := ps.Slice(i)

			for ; ok; prefixSlice, _, ok = ps.Slice(i) {
				if PrefixesToString(prefixSlice) != tc.expectedSlices[i] {
					t.Errorf("expected slice  %s  got  %s", tc.expectedSlices[i],
						PrefixesToString(prefixSlice))
				}
				i++
			}
		})
	}
}

// parsePartitionKeys parses a PARTITION BY LIST representation with integer
// values like:
//   "[/1] [/1/2] [/1/3] [/1/3/5]"
func parsePartitionKeys(evalCtx *tree.EvalContext, str string) []tree.Datums {
	if str == "" {
		return []tree.Datums{}
	}

	s := strings.Split(str, " ")
	var result []tree.Datums
	for i := 0; i < len(s); i++ {
		key := ParsePartitionKey(evalCtx, s[i])
		result = append(result, key)
	}
	return result
}

// ParsePartitionKey parses a partition key in the format [/key1/key2/key3],
// e.g: [/1/2/3]. If no types are passed in, the type is inferred as being an
// int if possible; otherwise a string. If any types are specified, they must be
// specified for every datum.
func ParsePartitionKey(evalCtx *tree.EvalContext, str string, typs ...types.Family) tree.Datums {
	if len(str) < len("[]") {
		panic(str)
	}
	start, expr, end := str[0], str[1:len(str)-1], str[len(str)-1:]
	if start != '[' || end != "]" {
		panic(str)
	}

	// Retrieve the values of the key.
	key := tree.ParsePath(expr)
	if len(typs) == 0 {
		typs = tree.InferTypes(key)
	}

	vals := tree.ParseDatumPath(evalCtx, expr, typs)
	return vals
}
