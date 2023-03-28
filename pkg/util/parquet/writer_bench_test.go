package parquet

import (
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// BenchmarkParquetWriter benchmarks the Writer.AddData operation.
func BenchmarkParquetWriter(b *testing.B) {
	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))

	// Create a row size of 2KiB.
	numCols := 16
	datumSizeBytes := 128
	sch := newColSchema(numCols)
	for i := 0; i < numCols; i++ {
		sch.columnTypes[i] = types.String
		sch.columnNames[i] = fmt.Sprintf("col%d", i)
	}
	datums := make([]tree.Datum, numCols)
	for i := 0; i < numCols; i++ {
		p := make([]byte, datumSizeBytes)
		_, _ = rng.Read(p)
		tree.NewDBytes(tree.DBytes(p))
		datums[i] = tree.NewDString(string(p))
	}

	fileName := "BenchmarkParquetWriter"
	f, err := os.CreateTemp("", fileName)
	require.NoError(b, err)

	schemaDef, err := NewSchema(sch.columnNames, sch.columnTypes)
	require.NoError(b, err)

	writer, err := NewWriter(schemaDef, f)
	require.NoError(b, err)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := writer.AddData(datums)
		require.NoError(b, err)
	}

	err = writer.Close()
	require.NoError(b, err)
}
