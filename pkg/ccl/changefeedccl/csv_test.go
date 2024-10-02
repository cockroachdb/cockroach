// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func BenchmarkCSVEncodeWideRow(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	// 1024 columns of three runes each.
	benchmarkEncodeCSV(b, 1024, 1024*3, 1024)
}

func BenchmarkCSVEncodeWideRowASCII(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	// 1024 columns of three runes each, all ASCII codepoints.
	benchmarkEncodeCSV(b, 1024, 1024*3, 127)
}

func BenchmarkCSVEncodeWideColumnsASCII(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	// 3 columns of 1024 runes each, all ASCII codepoints.
	benchmarkEncodeCSV(b, 3, 1024*3, 127)
}

func BenchmarkCSVEncodeWideColumns(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	// 3 columns of 1024 runes each.
	benchmarkEncodeCSV(b, 3, 1024*3, 1024)
}

func benchmarkEncodeCSV(b *testing.B, numCols int, numChars int, maxCodepoint int) {
	encoder := newCSVEncoder(changefeedbase.EncodingOptions{Format: changefeedbase.OptFormatCSV})
	ctx := context.Background()
	vals := make([]string, numCols)
	for i := 0; i < numChars; i++ {
		vals[i%numCols] += fmt.Sprintf("%c", i%(maxCodepoint+1))
	}
	datums := tree.Datums{}
	for _, str := range vals {
		datums = append(datums, tree.NewDString(str))
	}
	row := cdcevent.TestingMakeEventRowFromDatums(datums)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := encoder.EncodeValue(ctx, eventContext{}, row, cdcevent.Row{})
		require.NoError(b, err)
	}
}
