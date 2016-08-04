package pgwire

import (
	"bytes"
	"testing"

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/metric"
)

func TestBinaryDecimal(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// The expected bytes here were found by examining the output of a real
	// Postgres server.
	tests := []struct {
		in     string
		expect []byte
	}{
		{
			in:     "42",
			expect: []byte{0, 0, 0, 10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 42},
		},
		{
			in:     "0",
			expect: []byte{0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			in:     "0.0",
			expect: []byte{0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 1},
		},
		{
			in:     "1",
			expect: []byte{0, 0, 0, 10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1},
		},
		{
			in:     "-1.2",
			expect: []byte{0, 0, 0, 12, 0, 2, 0, 0, 64, 0, 0, 1, 0, 1, 7, 208},
		},
		{
			in:     "123",
			expect: []byte{0, 0, 0, 10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 123},
		},
		{
			in:     "12345",
			expect: []byte{0, 0, 0, 12, 0, 2, 0, 1, 0, 0, 0, 0, 0, 1, 9, 41},
		},
		{
			in:     "12345.1",
			expect: []byte{0, 0, 0, 14, 0, 3, 0, 1, 0, 0, 0, 1, 0, 1, 9, 41, 3, 232},
		},
		{
			in:     "12345.1234",
			expect: []byte{0, 0, 0, 14, 0, 3, 0, 1, 0, 0, 0, 4, 0, 1, 9, 41, 4, 210},
		},
		{
			in:     "12345.12345",
			expect: []byte{0, 0, 0, 16, 0, 4, 0, 1, 0, 0, 0, 5, 0, 1, 9, 41, 4, 210, 19, 136},
		},
		{
			in:     "1.1",
			expect: []byte{0, 0, 0, 12, 0, 2, 0, 0, 0, 0, 0, 1, 0, 1, 3, 232},
		},
		{
			in:     ".1",
			expect: []byte{0, 0, 0, 10, 0, 1, 255, 255, 0, 0, 0, 1, 3, 232},
		},
		{
			in:     ".1234",
			expect: []byte{0, 0, 0, 10, 0, 1, 255, 255, 0, 0, 0, 4, 4, 210},
		},
		{
			in:     ".12345",
			expect: []byte{0, 0, 0, 12, 0, 2, 255, 255, 0, 0, 0, 5, 4, 210, 19, 136},
		},
	}

	buf := writeBuffer{bytecount: metric.NewCounter()}
	dec := new(parser.DDecimal)
	for _, test := range tests {
		buf.wrapped.Reset()

		dec.SetString(test.in)
		buf.writeBinaryDatum(dec)
		if buf.err != nil {
			t.Fatal(buf.err)
		}
		if got := buf.wrapped.Bytes(); !bytes.Equal(got, test.expect) {
			t.Errorf("%q:\n\t%v found,\n\t%v expected", test.in, got, test.expect)
		}
	}
}
