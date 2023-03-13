package changefeedccl

import (
	"fmt"
	"os"
	"testing"

	"github.com/apache/arrow/go/v11/parquet"
	"github.com/apache/arrow/go/v11/parquet/file"
	"github.com/apache/arrow/go/v11/parquet/schema"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func TestParquetBleh(t *testing.T) {
	f, err := os.Create("./blah.txt")
	if err != nil {
		t.Fatal(err)
	}

	numCols := 10
	numRowGroups := 5

	opts := make([]parquet.WriterProperty, 0)
	for i := 0; i < numCols; i++ {
		opts = append(opts, parquet.WithCompressionFor(fmt.Sprintf("col %d", i), 0))
	}

	props := parquet.NewWriterProperties(opts...)

	fields := make([]schema.Node, numCols)
	for i := 0; i < numCols; i++ {
		name := fmt.Sprintf("column_%d", i)
		fields[i], _ = schema.NewPrimitiveNode(name, parquet.Repetitions.Optional, parquet.Types.Int32, -1, 12)
	}
	node, _ := schema.NewGroupNode("schema", parquet.Repetitions.Required, fields, -1)
	sch := schema.NewSchema(node)

	writer := file.NewParquetWriter(f, sch.Root(), file.WithWriterProps(props))

	for rg := 0; rg < numRowGroups; rg++ {
		rgw := writer.AppendRowGroup()
		for col := 0; col < numCols; col++ {
			cw, err := rgw.NextColumn()
			if err != nil {
				t.Fatal(err)
			}
			switch w := cw.(type) {
			case *file.Int32ColumnChunkWriter:
				_, err := w.WriteBatch([]int32{int32(rg)}, []int16{int16(1)}, nil)
				if err != nil {
					t.Fatal(err)
				}
			default:
				if err != nil {
					t.Fatal("non int32")
				}
			}
			err = cw.Close()
			if err != nil {
				t.Fatal("non int32")
			}
		}
		err := rgw.Close()
		if err != nil {
			t.Fatal("non int32")
		}
	}

	err = writer.Close()
	if err != nil {
		t.Fatal(err)
	}
}
