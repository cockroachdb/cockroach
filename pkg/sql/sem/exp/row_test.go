package exp

import (
	"fmt"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestRowBatch(t *testing.T) {
	boolType := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BOOL}
	intType := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT}
	floatType := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_FLOAT}
	stringType := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_STRING}
	b := NewRowBatch(MakeRowBatchLayout(8, boolType, intType, floatType, stringType, stringType))
	fmt.Printf("%s\n", b.Row(0))

	r := b.Row(0)
	r.SetDBool(0, true)
	r.SetDInt(1, 2)
	r.SetDFloat(2, 3.3)
	r.SetDString(3, tree.NewDString("hello"))
	r.SetDString(4, tree.NewDString("world"))
	fmt.Printf("0: %v\n", *r.DBool(0))
	fmt.Printf("1: %v\n", *r.DInt(1))
	fmt.Printf("2: %v\n", *r.DFloat(2))
	fmt.Printf("3: %v\n", *r.DString(3))
	fmt.Printf("3: %v\n", *r.DString(4))
}

type rowContainer struct {
	types    []sqlbase.ColumnType
	ordering sqlbase.ColumnOrdering
	rows     []sqlbase.EncDatumRow
}

func (r *rowContainer) Len() int { return len(r.rows) }
func (r *rowContainer) Less(i, j int) bool {
	c, _ := r.rows[i].Compare(r.types, nil, r.ordering, nil, r.rows[j])
	return c < 0
}
func (r *rowContainer) Swap(i, j int) { panic("unimplemented") }

func randSortedInts(count int) []tree.DInt {
	rng, _ := randutil.NewPseudoRand()
	vals := make([]tree.DInt, count)
	for i := range vals {
		vals[i] = tree.DInt(rng.Int63())
	}
	sort.Slice(vals, func(i, j int) bool {
		return vals[i] < vals[j]
	})
	return vals
}

func BenchmarkEncDatumRowIsSorted(b *testing.B) {
	r := &rowContainer{
		types: []sqlbase.ColumnType{
			{SemanticType: sqlbase.ColumnType_INT},
			{SemanticType: sqlbase.ColumnType_INT},
			{SemanticType: sqlbase.ColumnType_INT},
		},
		ordering: sqlbase.ColumnOrdering{
			{ColIdx: 0, Direction: encoding.Ascending},
			{ColIdx: 1, Direction: encoding.Ascending},
			{ColIdx: 2, Direction: encoding.Ascending},
		},
		rows: make([]sqlbase.EncDatumRow, 100),
	}
	vals := randSortedInts(len(r.rows))
	for i := range r.rows {
		r.rows[i] = sqlbase.EncDatumRow{
			sqlbase.EncDatum{Datum: &vals[i]},
			sqlbase.EncDatum{Datum: &vals[i]},
			sqlbase.EncDatum{Datum: &vals[i]},
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !sort.IsSorted(r) {
			b.Fatal("should be sorted")
		}
	}
}

type batchContainer struct {
	rows    int
	batch   RowBatch
	compare RowCompare
}

func (b *batchContainer) Len() int { return b.rows }
func (b *batchContainer) Less(i, j int) bool {
	return b.compare(b.batch.Row(i), b.batch.Row(j)) < 0
}
func (b *batchContainer) Swap(i, j int) { panic("unimplemented") }

func BenchmarkRowBatchIsSorted(b *testing.B) {
	const rows = 100

	types := []sqlbase.ColumnType{
		{SemanticType: sqlbase.ColumnType_INT},
		{SemanticType: sqlbase.ColumnType_INT},
		{SemanticType: sqlbase.ColumnType_INT},
	}
	ordering := sqlbase.ColumnOrdering{
		{ColIdx: 0, Direction: encoding.Ascending},
		{ColIdx: 1, Direction: encoding.Ascending},
		{ColIdx: 2, Direction: encoding.Ascending},
	}

	c := &batchContainer{
		rows:    rows,
		batch:   NewRowBatch(MakeRowBatchLayout(rows, types...)),
		compare: MakeRowCompare(ordering, types),
	}
	vals := randSortedInts(rows)
	for i := 0; i < rows; i++ {
		r := c.batch.Row(i)
		r.SetDInt(0, vals[i])
		r.SetDInt(1, vals[i])
		r.SetDInt(2, vals[i])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !sort.IsSorted(c) {
			b.Fatal("should be sorted")
		}
	}
}

func BenchmarkRowBatchFastCompareIsSorted(b *testing.B) {
	const rows = 100

	types := []sqlbase.ColumnType{
		{SemanticType: sqlbase.ColumnType_INT},
		{SemanticType: sqlbase.ColumnType_INT},
		{SemanticType: sqlbase.ColumnType_INT},
	}

	c := &batchContainer{
		rows:  rows,
		batch: NewRowBatch(MakeRowBatchLayout(rows, types...)),
		compare: func(a, b Row) int {
			// This is just a hand-coding of the comparison routine used in
			// BenchmarkRowBatchIsSorted that gets rid of the overhead of the
			// indirect function calls.
			av, bv := *a.DInt(0), *b.DInt(0)
			if av < bv {
				return -1
			} else if av > bv {
				return 1
			}
			av, bv = *a.DInt(1), *b.DInt(1)
			if av < bv {
				return -1
			} else if av > bv {
				return 1
			}
			av, bv = *a.DInt(2), *b.DInt(2)
			if av < bv {
				return -1
			} else if av > bv {
				return 1
			}
			return 0
		},
	}
	vals := randSortedInts(rows)
	for i := 0; i < rows; i++ {
		r := c.batch.Row(i)
		r.SetDInt(0, vals[i])
		r.SetDInt(1, vals[i])
		r.SetDInt(2, vals[i])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !sort.IsSorted(c) {
			b.Fatal("should be sorted")
		}
	}
}
