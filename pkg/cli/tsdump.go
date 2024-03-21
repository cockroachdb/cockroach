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
	"bytes"
	"compress/gzip"
	"context"
	"encoding/csv"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/ts/tsutil"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
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
	yaml         string
	targetURL    string
	ddApiKey     string
}{
	format:       tsDumpText,
	from:         timestampValue{},
	to:           timestampValue(timeutil.Now().Add(24 * time.Hour)),
	clusterLabel: "",
	yaml:         "/tmp/tsdump.yaml",
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
			err := createYAML(ctx)
			if err != nil {
				return err
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
		case tsDumpJSON:
			w = makeJSONWriter(debugTimeSeriesDumpOpts.targetURL)
		case tsDumpDatadog:
			w = makeDatadogWriter(
				ctx,
				"https://api.datadoghq.com/api/v2/series",
				false,
				debugTimeSeriesDumpOpts.ddApiKey,
			)
		case tsDumpDatadogInit:
			w = makeDatadogWriter(
				ctx,
				"https://api.datadoghq.com/api/v2/series",
				true,
				debugTimeSeriesDumpOpts.ddApiKey,
			)
		case tsDumpOpenMetrics:
			if debugTimeSeriesDumpOpts.targetURL != "" {
				write := beginHttpRequestWithWritePipe(debugTimeSeriesDumpOpts.targetURL)
				w = makeOpenMetricsWriter(write)
			} else {
				w = makeOpenMetricsWriter(os.Stdout)
			}
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
				w := bufio.NewWriterSize(os.Stdout, 1024*1024)
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

// beginHttpRequestWithWritePipe initiates an HTTP request to the
// `targetURL` argument and returns an `io.Writer` that pipes to the
// request body. This function will return while the request runs
// async.
func beginHttpRequestWithWritePipe(targetURL string) io.Writer {
	read, write := io.Pipe()
	req, err := http.NewRequest("POST", targetURL, read)
	if err != nil {
		panic(err)
	}
	// Start request async while we stream data to the body.
	go func() {
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			fmt.Printf("tsdump: openmetrics: http request error: %s", err)
			panic(err)
		}
		defer resp.Body.Close()
		fmt.Printf("tsdump: openmetrics: http response: %v", resp)
	}()

	return bufio.NewWriterSize(write, 1024*1024)
}

type tsWriter interface {
	Emit(*tspb.TimeSeriesData) error
	Flush() error
}

// datadogWriter can convert our metrics to Datadog format and send
// them via HTTP to the public DD endpoint, assuming an API key is set
// in the CLI flags.
type datadogWriter struct {
	targetURL       string
	buffer          bytes.Buffer
	series          []DatadogSeries
	metricsMetadata map[string]metric.Metadata
	timestamp       int64
	init            bool
	apiKey          string
	// namePrefix sets the string to prepend to all metric names. The
	// names are kept with `.` delimiters.
	namePrefix string
}

func makeDatadogWriter(
	ctx context.Context, targetURL string, init bool, apiKey string,
) *datadogWriter {
	fmt.Printf("Datadog writer init: %t\n", init)
	return &datadogWriter{
		targetURL:  targetURL,
		buffer:     bytes.Buffer{},
		timestamp:  timeutil.Now().Unix(),
		init:       init,
		apiKey:     apiKey,
		namePrefix: "crdb.tsdump.", // Default pre-set prefix to distinguish these uploads.
	}
}

// DatadogPoint is a single metric point in Datadog format
type DatadogPoint struct {
	// Timestamp must be in seconds since Unix epoch.
	Timestamp int64   `json:"timestamp"`
	Value     float64 `json:"value"`
}

// DatadogSeries contains a JSON encoding of a single series object
// that can be send to Datadog.
type DatadogSeries struct {
	Metric    string         `json:"metric"`
	Type      int            `json:"type"`
	Points    []DatadogPoint `json:"points"`
	Resources []struct {
		Name string `json:"name"`
		Type string `json:"type"`
	} `json:"resources"`
	// In order to encode arbitrary key-value pairs, use a `:` delimited
	// tag string like `cluster:dedicated`.
	Tags []string `json:"tags"`
}

// DatadogSubmitMetrics is the top level JSON object that must be sent to Datadog.
// See: https://docs.datadoghq.com/api/latest/metrics/#submit-metrics
type DatadogSubmitMetrics struct {
	Series []DatadogSeries `json:"series"`
}

func (d *datadogWriter) Emit(data *tspb.TimeSeriesData) error {
	series := &DatadogSeries{
		Type:   d.getType(data),
		Points: make([]DatadogPoint, len(data.Datapoints)),
	}

	name := data.Name

	var tags []string
	// Hardcoded values
	tags = append(tags, "cluster_type:SELF_HOSTED")
	tags = append(tags, "job:cockroachdb")
	tags = append(tags, "region:local")

	// Command values
	if debugTimeSeriesDumpOpts.clusterLabel != "" {
		tags = append(tags, fmt.Sprintf("cluster:%s", debugTimeSeriesDumpOpts.clusterLabel))
	} else if serverCfg.ClusterName != "" {
		tags = append(tags, fmt.Sprintf("cluster:%s", serverCfg.ClusterName))
	} else {
		tags = append(tags, fmt.Sprintf("cluster:cluster-debug-%d", d.timestamp))
	}

	sl := reCrStoreNode.FindStringSubmatch(data.Name)
	if len(sl) != 0 {
		storeNodeKey := sl[1]
		if storeNodeKey == "node" {
			storeNodeKey += "_id"
		}
		tags = append(tags, fmt.Sprintf("%s:%s", storeNodeKey, data.Source))
		name = sl[2]
	} else {
		tags = append(tags, "node_id:0")
	}

	series.Tags = tags

	series.Metric = d.namePrefix + name

	// When running in init mode, we insert zeros with the current
	// timestamp in order to populate Datadog's metrics list. Then the
	// user can enable these metrics for historic ingest and load the
	// full dataset. This should only be necessary once globally.
	if d.init {
		series.Points = []DatadogPoint{{
			Value:     0,
			Timestamp: time.Now().Unix(),
		}}
	} else {
		for i, ts := range data.Datapoints {
			series.Points[i].Value = ts.Value
			series.Points[i].Timestamp = ts.TimestampNanos / 1_000_000_000
		}
	}

	fmt.Printf("Appending series: %s with %d points\n", series.Metric, len(series.Points))

	// We append every series directly to the list. This isn't ideal
	// because every series batch that `Emit` is called with will contain
	// around 360 points and the same metric will repeat many many times.
	// This causes us to repeat the metadata collection here. Ideally, we
	// can find the series object for this metric if it already exists
	// and insert the points there.
	d.series = append(d.series, *series)

	// The limit of `100` is an experimentally set heuristic. It can
	// probably be increased. This results in a payload that's generally
	// below 2MB. DD's limit is 5MB.
	if len(d.series) > 100 {
		fmt.Printf(
			"Hit threshold after %d series while processing %s. Sending HTTP Request.\n",
			len(d.series),
			name,
		)
		return d.flushInternal()
	}
	return nil
}

func (d *datadogWriter) Flush() error {
	return d.flushInternal()
}

type DatadogResp struct {
	Errors []string `json:"errors"`
}

func (d *datadogWriter) flushInternal() error {
	var buf bytes.Buffer
	err := json.NewEncoder(&buf).Encode(&DatadogSubmitMetrics{Series: d.series})
	if err != nil {
		return err
	}

	fmt.Printf(
		"tsdump datadog upload: sending payload with %d bytes containing %d series including %s\n",
		buf.Len(),
		len(d.series),
		d.series[0].Metric,
	)

	var zipBuf bytes.Buffer
	g := gzip.NewWriter(&zipBuf)
	_, err = io.Copy(g, &buf)
	if err != nil {
		return err
	}
	err = g.Close()
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", d.targetURL, &zipBuf)
	if err != nil {
		return err
	}
	req.Header.Set("DD-API-KEY", d.apiKey)
	req.Header.Set(server.ContentTypeHeader, "application/json")
	req.Header.Set(httputil.ContentEncodingHeader, "gzip")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	ddResp := DatadogResp{}
	err = json.Unmarshal(respBytes, &ddResp)
	if err != nil {
		return err
	}
	if len(ddResp.Errors) > 0 {
		return errors.Newf("tsdump datadog upload: error response from datadog: %v", ddResp.Errors)
	}
	if resp.StatusCode > 299 {
		return errors.Newf("tsdump datadog upload: bad response status code: %+v", resp)
	}

	d.series = nil
	return nil
}

func (d *datadogWriter) getType(data *tspb.TimeSeriesData) int {
	return 1
	//for name, meta := range d.metricsMetadata {
	//	if strings.HasSuffix(data.Name, name) {
	//		switch meta.MetricType {
	//		case io_prometheus_client.MetricType_COUNTER:
	//			return 1
	//		case io_prometheus_client.MetricType_GAUGE:
	//			return 3
	//		default:
	//			return 0
	//		}
	//	}
	//}
	//return 0
}

var _ tsWriter = &datadogWriter{}

type jsonWriter struct {
	targetURL string
	buffer    bytes.Buffer
	timestamp int64
}

// Format via https://docs.victoriametrics.com/#json-line-format
// {
// // metric contans metric name plus labels for a particular time series
// "metric":{
// "__name__": "metric_name",  // <- this is metric name
//
// // Other labels for the time series
//
// "label1": "value1",
// "label2": "value2",
// ...
// "labelN": "valueN"
// },
//
// // values contains raw sample values for the given time series
// "values": [1, 2.345, -678],
//
// // timestamps contains raw sample UNIX timestamps in milliseconds for the given time series
// // every timestamp is associated with the value at the corresponding position
// "timestamps": [1549891472010,1549891487724,1549891503438]
// }
type victoriaMetricsJSON struct {
	Metric     map[string]string `json:"metric"`
	Values     []float64         `json:"values"`
	Timestamps []int64           `json:"timestamps"`
}

func (o *jsonWriter) Emit(data *tspb.TimeSeriesData) error {
	if o.targetURL == "" {
		return errors.New("No targetURL selected")
	}
	out := &victoriaMetricsJSON{
		Metric:     make(map[string]string, 1),
		Values:     make([]float64, len(data.Datapoints)),
		Timestamps: make([]int64, len(data.Datapoints)),
	}

	name := data.Name

	// Hardcoded values
	out.Metric["cluster_type"] = "SELF_HOSTED"
	out.Metric["job"] = "cockroachdb"
	out.Metric["region"] = "local"
	// Zero values
	out.Metric["instance"] = ""
	out.Metric["node"] = ""
	out.Metric["organization_id"] = ""
	out.Metric["organization_label"] = ""
	out.Metric["sla_type"] = ""
	out.Metric["tenant_id"] = ""
	// Command values
	if debugTimeSeriesDumpOpts.clusterLabel != "" {
		out.Metric["cluster"] = debugTimeSeriesDumpOpts.clusterLabel
	} else if serverCfg.ClusterName != "" {
		out.Metric["cluster"] = serverCfg.ClusterName
	} else {
		out.Metric["cluster"] = fmt.Sprintf("cluster-debug-%d", o.timestamp)
	}

	sl := reCrStoreNode.FindStringSubmatch(data.Name)
	out.Metric["node_id"] = "0"
	if len(sl) != 0 {
		storeNodeKey := sl[1]
		if storeNodeKey == "node" {
			storeNodeKey += "_id"
		}
		out.Metric[storeNodeKey] = data.Source
		name = sl[2]
	}

	name = rePromTSName.ReplaceAllLiteralString(name, `_`)
	out.Metric["__name__"] = name

	for i, ts := range data.Datapoints {
		out.Values[i] = ts.Value
		out.Timestamps[i] = ts.TimestampNanos / 1_000_000
	}

	err := json.NewEncoder(&o.buffer).Encode(out)
	if err != nil {
		return err
	}

	if o.buffer.Len() > 10_000_000 {
		fmt.Printf("Hit threshold. Sending HTTP Request: %s\n", name)
		resp, err := http.Post(o.targetURL, "application/json", &o.buffer)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode > 299 {
			return errors.Newf("bad status: %+v", resp)
		}

		o.buffer = bytes.Buffer{}
		return nil
	}
	return nil
}

func (o *jsonWriter) Flush() error {
	resp, err := http.Post(o.targetURL, "application/json", &o.buffer)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode > 299 {
		return errors.Newf("bad status: %+v", resp)
	}

	o.buffer = bytes.Buffer{}
	return nil
}

var _ tsWriter = &jsonWriter{}

func makeJSONWriter(targetURL string) tsWriter {
	return &jsonWriter{
		targetURL: targetURL,
		timestamp: timeutil.Now().Unix(),
	}
}

type openMetricsWriter struct {
	out    io.Writer
	labels map[string]string
}

// createYAML generates and writes tsdump.yaml to default /tmp or to a specified path
func createYAML(ctx context.Context) (resErr error) {
	file, err := os.OpenFile(debugTimeSeriesDumpOpts.yaml, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	defer file.Close()

	sqlConn, err := makeSQLClient(ctx, "cockroach tsdump", useSystemDb)
	if err != nil {
		return err
	}
	defer func() { resErr = errors.CombineErrors(resErr, sqlConn.Close()) }()

	_, rows, err := sqlExecCtx.RunQuery(
		ctx,
		sqlConn,
		clisqlclient.MakeQuery(`SELECT store_id || ': ' || node_id FROM crdb_internal.kv_store_status`), false)

	if err != nil {
		return err
	}

	var strStoreNodeID string
	for _, row := range rows {
		storeNodeID := row
		strStoreNodeID = strings.Join(storeNodeID, " ")
		strStoreNodeID += "\n"
		_, err := file.WriteString(strStoreNodeID)
		if err != nil {
			return err
		}
	}
	return nil
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
	tsDumpJSON
	// tsDumpDatadog format will send metrics to the public Datadog HTTP
	// endpoint in batches.
	tsDumpDatadog
	// tsDumpDatadogInit will send zero values for all metrics with the
	// current timestamp to Datadog. This pre-populates the custom
	// metrics and lets you enable historical ingestion if you're going
	// to push older timestamps. There's no way to enable historical
	// ingestion if DD doesn't already know your metric name.
	tsDumpDatadogInit
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
	case tsDumpJSON:
		return "json"
	case tsDumpDatadog:
		return "datadog"
	case tsDumpDatadogInit:
		return "datadoginit"
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
	case "json":
		*m = tsDumpJSON
	case "datadog":
		*m = tsDumpDatadog
	case "datadoginit":
		*m = tsDumpDatadogInit

	default:
		return fmt.Errorf("invalid value for --format: %s", s)
	}
	return nil
}
