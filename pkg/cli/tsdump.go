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
	"bufio"
	"context"
	"encoding/csv"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/ts/tsutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

// TODO(knz): this struct belongs elsewhere.
// See: https://github.com/cockroachdb/cockroach/issues/49509
var debugTimeSeriesDumpOpts = struct {
	format       tsDumpFormat
	from, to     timestampValue
	clusterLabel string
}{
	format:       tsDumpText,
	from:         timestampValue{},
	to:           timestampValue(timeutil.Now().Add(24 * time.Hour)),
	clusterLabel: "",
}

var debugTimeSeriesDumpCmd = &cobra.Command{
	Use:   "tsdump",
	Short: "dump all the raw timeseries values in a cluster",
	Long: `
Dumps all of the raw timeseries values in a cluster. Only the default resolution
is retrieved, i.e. typically datapoints older than the value of the
'timeseries.storage.resolution_10s.ttl' cluster setting will be absent from the
output.

When an input file is provided instead (as an argument), this input file
must previously have been created with the --format=raw switch. The command
will then convert it to the --format requested in the current invocation.
`,
	Args: cobra.RangeArgs(0, 1),
	RunE: clierrorplus.MaybeDecorateError(func(cmd *cobra.Command, args []string) error {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var convertFile string
		if len(args) > 0 {
			convertFile = args[0]
		}

		var w tsWriter
		switch debugTimeSeriesDumpOpts.format {
		case tsDumpRaw:
			if convertFile != "" {
				return errors.Errorf("input file is already in raw format")
			}
			// Special case, we don't go through the text output code.
		case tsDumpCSV:
			w = csvTSWriter{w: csv.NewWriter(os.Stdout)}
		case tsDumpTSV:
			cw := csvTSWriter{w: csv.NewWriter(os.Stdout)}
			cw.w.Comma = '\t'
			w = cw
		case tsDumpText:
			w = defaultTSWriter{w: os.Stdout}
		case tsDumpOpenMetrics:
			w = makeOpenMetricsWriter(os.Stdout)
		default:
			return errors.Newf("unknown output format: %v", debugTimeSeriesDumpOpts.format)
		}

		var recv func() (*tspb.TimeSeriesData, error)
		if convertFile == "" {
			// To enable conversion without a running cluster, we want to skip
			// connecting to the server when converting an existing tsdump.
			conn, finish, err := getClientGRPCConn(ctx, serverCfg)
			if err != nil {
				return err
			}
			defer finish()

			names, err := serverpb.GetInternalTimeseriesNamesFromServer(ctx, conn)
			if err != nil {
				return err
			}
			req := &tspb.DumpRequest{
				StartNanos: time.Time(debugTimeSeriesDumpOpts.from).UnixNano(),
				EndNanos:   time.Time(debugTimeSeriesDumpOpts.to).UnixNano(),
				Names:      names,
			}
			tsClient := tspb.NewTimeSeriesClient(conn)

			if debugTimeSeriesDumpOpts.format == tsDumpRaw {
				stream, err := tsClient.DumpRaw(context.Background(), req)
				if err != nil {
					return err
				}

				// Buffer the writes to os.Stdout since we're going to
				// be writing potentially a lot of data to it.
				w := bufio.NewWriter(os.Stdout)
				if err := tsutil.DumpRawTo(stream, w); err != nil {
					return err
				}
				return w.Flush()
			}
			stream, err := tsClient.Dump(context.Background(), req)
			if err != nil {
				return err
			}
			recv = stream.Recv
		} else {
			f, err := os.Open(args[0])
			if err != nil {
				return err
			}
			type tup struct {
				data *tspb.TimeSeriesData
				err  error
			}

			dec := gob.NewDecoder(f)
			gob.Register(&roachpb.KeyValue{})
			decodeOne := func() (*tspb.TimeSeriesData, error) {
				var v roachpb.KeyValue
				err := dec.Decode(&v)
				if err != nil {
					return nil, err
				}

				var data *tspb.TimeSeriesData
				dumper := ts.DefaultDumper{Send: func(d *tspb.TimeSeriesData) error {
					data = d
					return nil
				}}
				if err := dumper.Dump(&v); err != nil {
					return nil, err
				}
				return data, nil
			}

			ch := make(chan tup, 4096)
			go func() {
				// ch is closed when the process exits, so closing channel here is
				// more for extra protection.
				defer close(ch)
				for {
					data, err := decodeOne()
					ch <- tup{data, err}
				}
			}()

			recv = func() (*tspb.TimeSeriesData, error) {
				r := <-ch
				return r.data, r.err
			}
		}

		for {
			data, err := recv()
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

type openMetricsWriter struct {
	out    io.Writer
	labels map[string]string
}

func makeOpenMetricsWriter(out io.Writer) *openMetricsWriter {
	// construct labels
	labelMap := make(map[string]string)
	// Hardcoded values
	labelMap["cluster_type"] = "SELF_HOSTED"
	labelMap["job"] = "cockroachdb"
	labelMap["region"] = "local"
	// Zero values
	labelMap["instance"] = ""
	labelMap["node"] = ""
	labelMap["organization_id"] = ""
	labelMap["organization_label"] = ""
	labelMap["sla_type"] = ""
	labelMap["tenant_id"] = ""
	// Command values
	if debugTimeSeriesDumpOpts.clusterLabel != "" {
		labelMap["cluster"] = debugTimeSeriesDumpOpts.clusterLabel
	} else if serverCfg.ClusterName != "" {
		labelMap["cluster"] = serverCfg.ClusterName
	} else {
		labelMap["cluster"] = fmt.Sprintf("cluster-debug-%d", timeutil.Now().Unix())
	}
	return &openMetricsWriter{out: out, labels: labelMap}
}

var reCrStoreNode = regexp.MustCompile(`^cr\.([^\.]+)\.(.*)$`)
var rePromTSName = regexp.MustCompile(`[^a-z0-9]`)

func (w *openMetricsWriter) Emit(data *tspb.TimeSeriesData) error {
	name := data.Name
	sl := reCrStoreNode.FindStringSubmatch(data.Name)
	labelMap := w.labels
	labelMap["node_id"] = "0"
	if len(sl) != 0 {
		storeNodeKey := sl[1]
		if storeNodeKey == "node" {
			storeNodeKey += "_id"
		}
		labelMap[storeNodeKey] = data.Source
		name = sl[2]
	}
	var l []string
	for k, v := range labelMap {
		l = append(l, fmt.Sprintf("%s=%q", k, v))
	}
	labels := "{" + strings.Join(l, ",") + "}"
	name = rePromTSName.ReplaceAllLiteralString(name, `_`)
	for _, pt := range data.Datapoints {
		if _, err := fmt.Fprintf(
			w.out,
			"%s%s %f %d.%d\n",
			name,
			labels,
			pt.Value,
			// Convert to Unix Epoch in seconds with preserved precision
			// (https://github.com/OpenObservability/OpenMetrics/blob/main/specification/OpenMetrics.md#timestamps).
			pt.TimestampNanos/1e9, pt.TimestampNanos%1e9,
		); err != nil {
			return err
		}
	}
	return nil
}

func (w *openMetricsWriter) Flush() error {
	fmt.Fprintln(w.out, `# EOF`)
	return nil
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
	tsDumpOpenMetrics
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
	case tsDumpOpenMetrics:
		return "openmetrics"
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
	case "openmetrics":
		*m = tsDumpOpenMetrics
	default:
		return fmt.Errorf("invalid value for --format: %s", s)
	}
	return nil
}
