// Copyright 2018 The Cockroach Authors.
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

package workload

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

const (
	rowStartParam = `row-start`
	rowEndParam   = `row-end`
)

// WriteCSVRows writes the specified table rows as a csv. If sizeBytesLimit is >
// 0, it will be used as an approximate upper bound for how much to write. The
// next rowStart is returned (so last row written + 1).
func WriteCSVRows(
	ctx context.Context, w io.Writer, table Table, rowStart, rowEnd int, sizeBytesLimit int64,
) (rowBatchIdx int, err error) {
	bytesWrittenW := &bytesWrittenWriter{w: w}
	csvW := csv.NewWriter(bytesWrittenW)
	var rowStrings []string
	for rowBatchIdx = rowStart; rowBatchIdx < rowEnd; rowBatchIdx++ {
		if sizeBytesLimit > 0 && bytesWrittenW.written > sizeBytesLimit {
			break
		}

		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
		}
		for _, row := range table.InitialRows.Batch(rowBatchIdx) {
			if cap(rowStrings) < len(row) {
				rowStrings = make([]string, len(row))
			} else {
				rowStrings = rowStrings[:len(row)]
			}
			for i, datum := range row {
				rowStrings[i] = datumToCSVString(datum)
			}
			if err := csvW.Write(rowStrings); err != nil {
				return 0, err
			}
		}
	}
	csvW.Flush()
	return rowBatchIdx, csvW.Error()
}

func datumToCSVString(datum interface{}) string {
	if datum == nil {
		return `NULL`
	}
	switch t := datum.(type) {
	case int:
		return strconv.Itoa(t)
	case int64:
		return fmt.Sprint(t)
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case string:
		return t
	default:
		panic(fmt.Sprintf("unsupported type %T: %v", datum, datum))
	}
}

// HandleCSV configures a Generator with url params and outputs the data for a
// single Table as a CSV (optionally limiting the rows via `row-start` and
// `row-end` params). It is intended for use in implementing a
// `net/http.Handler`.
func HandleCSV(w http.ResponseWriter, req *http.Request, prefix string, meta Meta) error {
	ctx := context.Background()
	if err := req.ParseForm(); err != nil {
		return err
	}

	gen := meta.New()
	if f, ok := gen.(Flagser); ok {
		var flags []string
		f.Flags().VisitAll(func(f *pflag.Flag) {
			if vals, ok := req.Form[f.Name]; ok {
				for _, val := range vals {
					flags = append(flags, fmt.Sprintf(`--%s=%s`, f.Name, val))
				}
			}
		})
		if err := f.Flags().Parse(flags); err != nil {
			return errors.Wrapf(err, `parsing parameters %s`, strings.Join(flags, ` `))
		}
	}

	tableName := strings.TrimPrefix(req.URL.Path, prefix)
	var table *Table
	for _, t := range gen.Tables() {
		if t.Name == tableName {
			table = &t
			break
		}
	}
	if table == nil {
		return errors.Errorf(`could not find table %s in generator %s`, tableName, meta.Name)
	}

	rowStart, rowEnd := 0, table.InitialRows.NumBatches
	if vals, ok := req.Form[rowStartParam]; ok && len(vals) > 0 {
		var err error
		rowStart, err = strconv.Atoi(vals[len(vals)-1])
		if err != nil {
			return errors.Wrapf(err, `parsing %s`, rowStartParam)
		}
	}
	if vals, ok := req.Form[rowEndParam]; ok && len(vals) > 0 {
		var err error
		rowEnd, err = strconv.Atoi(vals[len(vals)-1])
		if err != nil {
			return errors.Wrapf(err, `parsing %s`, rowEndParam)
		}
	}

	w.Header().Set(`Content-Type`, `text/csv`)
	_, err := WriteCSVRows(ctx, w, *table, rowStart, rowEnd, -1 /* sizeBytesLimit */)
	return err
}

type bytesWrittenWriter struct {
	w       io.Writer
	written int64
}

func (w *bytesWrittenWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.written += int64(n)
	return n, err
}

// CSVMux returns a mux over http handers for csv data in all tables in the
// given generators.
func CSVMux(metas []Meta) *http.ServeMux {
	mux := http.NewServeMux()
	for _, meta := range metas {
		meta := meta
		prefix := fmt.Sprintf(`/csv/%s/`, meta.Name)
		mux.HandleFunc(prefix, func(w http.ResponseWriter, req *http.Request) {
			if err := HandleCSV(w, req, prefix, meta); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		})
	}
	return mux
}
