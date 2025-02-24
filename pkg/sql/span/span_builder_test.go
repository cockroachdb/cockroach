// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package span

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/stretchr/testify/require"
)

func TestBuilder_EncodeConstraintKey(t *testing.T) {
	var (
		colDirs1  = []string{"asc", "asc", "asc"}
		colDirs2  = []string{"desc", "asc", "desc"}
		intDatum1 = tree.NewDInt(1)
		intDatum2 = tree.NewDInt(2)
		textDatum = tree.NewDString("foo")
	)
	for tcIdx, tc := range []struct {
		dirs []string
		in   constraint.Key
		out  string
	}{
		{
			dirs: colDirs1,
			in:   constraint.EmptyKey,
			out:  "/",
		},
		{
			dirs: colDirs1,
			in:   constraint.MakeKey(intDatum1),
			out:  "/1",
		},
		{
			dirs: colDirs2,
			in:   constraint.MakeKey(intDatum1),
			out:  "/1",
		},
		{
			dirs: colDirs1,
			in:   constraint.MakeCompositeKey(intDatum1, intDatum2),
			out:  "/1/2",
		},
		{
			dirs: colDirs1,
			in:   constraint.MakeCompositeKey(intDatum1, textDatum, intDatum2),
			out:  "/1/\"foo\"/2",
		},
		{
			dirs: colDirs2,
			in:   constraint.MakeCompositeKey(intDatum1, textDatum, intDatum2),
			out:  "/1/\"foo\"/2",
		},
	} {
		t.Run(fmt.Sprintf("case %d", tcIdx+1), func(t *testing.T) {
			b := Builder{}
			b.keyAndPrefixCols = make([]fetchpb.IndexFetchSpec_KeyColumn, len(tc.dirs))
			valDirs := make([]encoding.Direction, len(tc.dirs))
			for i, dir := range tc.dirs {
				if dir == "asc" {
					b.keyAndPrefixCols[i].Direction = catenumpb.IndexColumn_ASC
					valDirs[i] = encoding.Ascending
				} else {
					b.keyAndPrefixCols[i].Direction = catenumpb.IndexColumn_DESC
					valDirs[i] = encoding.Descending
				}
			}
			outKey, _, err := b.EncodeConstraintKey(tc.in)
			require.NoError(t, err)
			vals, _ := encoding.PrettyPrintValuesWithTypes(valDirs, outKey)
			require.Equal(t, tc.out, "/"+strings.Join(vals, "/"))
		})
	}
}
