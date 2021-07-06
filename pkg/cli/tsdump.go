// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

// TODO(knz): this struct belongs elsewhere.
// See: https://github.com/cockroachdb/cockroach/issues/49509
var debugTimeSeriesDumpOpts = struct {
	format tsDumpFormat
}{
	format: tsDumpText,
}

var debugTimeSeriesDumpCmd = &cobra.Command{
	Use:   "tsdump",
	Short: "dump all the raw timeseries values in a cluster",
	Long: `
Dumps all of the raw timeseries values in a cluster.
`,
	RunE: MaybeDecorateGRPCError(func(cmd *cobra.Command, args []string) error {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var w tsWriter
		switch debugTimeSeriesDumpOpts.format {
		case tsDumpRaw:
			// Special case, we don't go through the text output code.
			conn, _, finish, err := getClientGRPCConn(ctx, serverCfg)
			if err != nil {
				return err
			}
			defer finish()

			tsClient := tspb.NewTimeSeriesClient(conn)
			stream, err := tsClient.DumpRaw(context.Background(), &tspb.DumpRequest{})
			if err != nil {
				return err
			}

			if err := ts.DumpRawTo(stream, os.Stdout); err != nil {
				return err
			}
			return os.Stdout.Sync()

		case tsDumpCSV:
			w = csvTSWriter{w: csv.NewWriter(os.Stdout)}
		case tsDumpTSV:
			cw := csvTSWriter{w: csv.NewWriter(os.Stdout)}
			cw.w.Comma = '\t'
			w = cw
		case tsDumpText:
			w = defaultTSWriter{w: os.Stdout}
		default:
			return errors.Newf("unknown output format: %v", debugTimeSeriesDumpOpts.format)
		}

		conn, _, finish, err := getClientGRPCConn(ctx, serverCfg)
		if err != nil {
			return err
		}
		defer finish()

		tsClient := tspb.NewTimeSeriesClient(conn)
		stream, err := tsClient.Dump(context.Background(), &tspb.DumpRequest{})
		if err != nil {
			return err
		}

		for {
			data, err := stream.Recv()
			if err == io.EOF {
				return w.Flush()
			}
			if err != nil {
				return err
			}
			if err := w.Emit(data); err != nil {
				return err
			}
		}
	}),
}

type tsWriter interface {
	Emit(*tspb.TimeSeriesData) error
	Flush() error
}

type csvTSWriter struct {
	w *csv.Writer
}

func (w csvTSWriter) Emit(data *tspb.TimeSeriesData) error {
	for _, d := range data.Datapoints {
		if err := w.w.Write(
			[]string{data.Name, timeutil.Unix(0, d.TimestampNanos).In(time.UTC).Format(time.RFC3339), data.Source, fmt.Sprint(d.Value)},
		); err != nil {
			return err
		}
	}
	return nil
}

func (w csvTSWriter) Flush() error {
	w.w.Flush()
	return w.w.Error()
}

type defaultTSWriter struct {
	last struct {
		name, source string
	}
	w io.Writer
}

func (w defaultTSWriter) Flush() error { return nil }

func (w defaultTSWriter) Emit(data *tspb.TimeSeriesData) error {
	if w.last.name != data.Name || w.last.source != data.Source {
		w.last.name, w.last.source = data.Name, data.Source
		fmt.Fprintf(w.w, "%s %s\n", data.Name, data.Source)
	}
	for _, d := range data.Datapoints {
		fmt.Fprintf(w.w, "%v %v\n", d.TimestampNanos, d.Value)
	}
	return nil
}

type tsDumpFormat int

const (
	tsDumpText tsDumpFormat = iota
	tsDumpCSV
	tsDumpTSV
	tsDumpRaw
)

// Type implements the pflag.Value interface.
func (m *tsDumpFormat) Type() string { return "string" }

// String implements the pflag.Value interface.
func (m *tsDumpFormat) String() string {
	switch *m {
	case tsDumpCSV:
		return "csv"
	case tsDumpTSV:
		return "tsv"
	case tsDumpText:
		return "text"
	case tsDumpRaw:
		return "raw"
	}
	return ""
}

// Set implements the pflag.Value interface.
func (m *tsDumpFormat) Set(s string) error {
	switch s {
	case "text":
		*m = tsDumpText
	case "csv":
		*m = tsDumpCSV
	case "tsv":
		*m = tsDumpTSV
	case "raw":
		*m = tsDumpRaw
	default:
		return fmt.Errorf("invalid value for --format: %s", s)
	}
	return nil
}
