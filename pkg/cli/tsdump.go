// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/ts/tsdumpmeta"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/ts/tsutil"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/netutil/addr"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/spf13/cobra"
)

const tsDumpAppName = catconstants.InternalAppNamePrefix + " cockroach tsdump"

// TODO(knz): this struct belongs elsewhere.
// See: https://github.com/cockroachdb/cockroach/issues/49509
var debugTimeSeriesDumpOpts = struct {
	format                 tsDumpFormat
	from, to               timestampValue
	clusterLabel           string
	yaml                   string
	targetURL              string
	ddApiKey               string
	ddSite                 string
	httpToken              string
	clusterID              string
	zendeskTicket          string
	organizationName       string
	userName               string
	storeToNodeMapYAMLFile string
	dryRun                 bool
	noOfUploadWorkers      int
	retryFailedRequests    bool
	disableDeltaProcessing bool
	ddMetricInterval       int64  // interval for datadoginit format only
	metricsListFile        string // file containing explicit list of metrics to dump
}{
	format:                 tsDumpText,
	from:                   timestampValue{},
	to:                     timestampValue(timeutil.Now().Add(24 * time.Hour)),
	clusterLabel:           "",
	yaml:                   "/tmp/tsdump.yaml",
	retryFailedRequests:    false,
	disableDeltaProcessing: false, // delta processing enabled by default

	// default to 10 seconds interval for datadoginit.
	// This is based on the scrape interval that is currently set accross all managed clusters
	ddMetricInterval: 10,
}

// hostNameOverride is used to override the hostname for testing purpose.
var hostNameOverride string

// datadogSeriesThreshold holds the threshold for the number of series
// that will be uploaded to Datadog in a single request. We have capped it to 50
// to avoid hitting the Datadog API limits.
var datadogSeriesThreshold = 50

const uploadWorkerErrorMessage = "--upload-workers is set to an invalid value." +
	" please select a value which between 1 and 100."

var debugTimeSeriesDumpCmd = &cobra.Command{
	Use:   "tsdump",
	Short: "dump all the raw timeseries values in a cluster",
	Long: `
Dumps all of the raw timeseries values in a cluster. If the supplied time range
is within the 'timeseries.storage.resolution_10s.ttl', metrics will be dumped
as it is with 10s resolution. If the time range extends outside of the TTL, the
timeseries downsampled to 30m resolution will be dumped for the time beyond
the TTL.

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
		switch cmd := debugTimeSeriesDumpOpts.format; cmd {
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
		case tsDumpJSON:
			w = makeJSONWriter(
				debugTimeSeriesDumpOpts.targetURL,
				debugTimeSeriesDumpOpts.httpToken,
				10_000_000, /* threshold */
				doRequest,
			)
		case tsDumpDatadogInit:
			datadogWriter, err := makeDatadogWriter(
				debugTimeSeriesDumpOpts.ddSite,
				true, /* init */
				debugTimeSeriesDumpOpts.ddApiKey,
				datadogSeriesThreshold,
				hostNameOverride,
				debugTimeSeriesDumpOpts.noOfUploadWorkers,
				false, /* retryFailedRequests not applicable for init */
			)
			if err != nil {
				return err
			}

			return datadogWriter.uploadInitMetrics()
		case tsDumpDatadog:
			if len(args) < 1 {
				return errors.New("no input file provided")
			}

			if debugTimeSeriesDumpOpts.noOfUploadWorkers <= 0 || debugTimeSeriesDumpOpts.noOfUploadWorkers > 100 {
				return errors.New(uploadWorkerErrorMessage)
			}

			datadogWriter, err := makeDatadogWriter(
				debugTimeSeriesDumpOpts.ddSite,
				false, /* init */
				debugTimeSeriesDumpOpts.ddApiKey,
				datadogSeriesThreshold,
				hostNameOverride,
				debugTimeSeriesDumpOpts.noOfUploadWorkers,
				debugTimeSeriesDumpOpts.retryFailedRequests,
			)
			if err != nil {
				return err
			}

			// Handle retry of failed requests if flag is set
			if datadogWriter.isPartialUploadOfFailedRequests {
				return datadogWriter.retryFailedRequests(args[0])
			}

			return datadogWriter.upload(args[0])
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
			conn, finish, err := newClientConn(ctx, serverCfg)
			if err != nil {
				return err
			}
			defer finish()

			target, _ := addr.AddrWithDefaultLocalhost(serverCfg.AdvertiseAddr)
			adminClient := conn.NewAdminClient()

			var names []string
			if debugTimeSeriesDumpOpts.metricsListFile != "" {
				// Use explicit metrics list from file
				names, err = getTimeseriesNamesFromFile(ctx, adminClient, debugTimeSeriesDumpOpts.metricsListFile)
				if err != nil {
					return err
				}
				fmt.Fprintf(os.Stderr, "Using explicit metrics list with %d timeseries names from %s\n",
					len(names), debugTimeSeriesDumpOpts.metricsListFile)
			} else {
				// Fetch all metric names from server
				names, err = serverpb.GetInternalTimeseriesNamesFromServer(ctx, adminClient)
				if err != nil {
					return err
				}
			}
			req := &tspb.DumpRequest{
				StartNanos: time.Time(debugTimeSeriesDumpOpts.from).UnixNano(),
				EndNanos:   time.Time(debugTimeSeriesDumpOpts.to).UnixNano(),
				Names:      names,
				Resolutions: []tspb.TimeSeriesResolution{
					tspb.TimeSeriesResolution_RESOLUTION_30M, tspb.TimeSeriesResolution_RESOLUTION_10S,
				},
			}

			tsClient := conn.NewTimeSeriesClient()
			if debugTimeSeriesDumpOpts.format == tsDumpRaw {
				stream, err := tsClient.DumpRaw(context.Background(), req)
				if err != nil {
					return errors.Wrapf(err, "connecting to %s", target)
				}

				// get the node details so that we can get the SQL port
				statusClient := conn.NewStatusClient()
				resp, err := statusClient.Details(ctx, &serverpb.DetailsRequest{NodeId: "local"})
				if err != nil {
					return err
				}

				// override the server port with the SQL port taken from the DetailsResponse
				// this port should be used to make the SQL connection
				cliCtx.clientOpts.ServerHost, cliCtx.clientOpts.ServerPort, err = net.SplitHostPort(resp.SQLAddress.String())
				if err != nil {
					return err
				}

				// Get store-to-node mapping for metadata
				storeToNodeMap, err := getStoreToNodeMapping(ctx)
				if err != nil {
					return err
				}

				// Create metadata header
				metadata := tsdumpmeta.Metadata{
					Version:        build.BinaryVersion(),
					StoreToNodeMap: storeToNodeMap,
					CreatedAt:      timeutil.Now(),
				}

				// Buffer the writes to os.Stdout since we're going to
				// be writing potentially a lot of data to it.
				w := bufio.NewWriterSize(os.Stdout, 1024*1024)

				// Write embedded metadata first
				if err := tsdumpmeta.Write(w, metadata); err != nil {
					return err
				}

				if err := tsutil.DumpRawTo(stream, w); err != nil {
					return err
				}

				if err = createYAML(ctx); err != nil {
					return err
				}
				return w.Flush()
			}
			stream, err := tsClient.Dump(context.Background(), req)
			if err != nil {
				return errors.Wrapf(err, "connecting to %s", target)
			}
			recv = stream.Recv
		} else {
			f, err := os.Open(args[0])
			if err != nil {
				return err
			}
			defer f.Close()
			type tup struct {
				data *tspb.TimeSeriesData
				err  error
			}

			dec := gob.NewDecoder(f)

			// Try to read embedded metadata first
			embeddedMetadata, metadataErr := tsdumpmeta.Read(dec)
			if metadataErr != nil {
				// No embedded metadata, restart from beginning
				if _, err := f.Seek(0, io.SeekStart); err != nil {
					return err
				}
				dec = gob.NewDecoder(f) // Reset decoder to read from beginning
			} else {
				fmt.Printf("Found embedded store-to-node mapping with %d entries\n", len(embeddedMetadata.StoreToNodeMap))
			}

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
					// Exit the goroutine if we encounter EOF or any error
					if err != nil {
						break
					}
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
				return errors.Wrapf(err, "connecting to %s", serverCfg.AdvertiseAddr)
			}
			if err := w.Emit(data); err != nil {
				return err
			}
		}
	}),
}

func doRequest(req *http.Request) error {
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode > 299 {
		return errors.Newf("tsdump: bad response status: %+v", resp)
	}
	return nil
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

type jsonWriter struct {
	sync.Once
	targetURL string
	buffer    bytes.Buffer
	timestamp int64
	httpToken string
	doRequest func(req *http.Request) error
	threshold int
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
	// Command values
	if debugTimeSeriesDumpOpts.clusterLabel != "" {
		out.Metric["cluster"] = debugTimeSeriesDumpOpts.clusterLabel
	} else if serverCfg.ClusterName != "" {
		out.Metric["cluster"] = serverCfg.ClusterName
	} else {
		out.Metric["cluster"] = fmt.Sprintf("cluster-debug-%d", o.timestamp)
	}
	o.Do(func() {
		fmt.Printf("Cluster label is set to: %s\n", out.Metric["cluster"])
	})

	sl := reCrStoreNode.FindStringSubmatch(data.Name)
	out.Metric["node_id"] = "0"
	if len(sl) != 0 {
		storeNodeKey := sl[1]
		if storeNodeKey == "node" {
			storeNodeKey += "_id"
		}
		out.Metric[storeNodeKey] = data.Source
		// `instance` is used in dashboards to split data by node.
		out.Metric["instance"] = data.Source
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

	if o.buffer.Len() > o.threshold {
		fmt.Printf(
			"tsdump json upload: sending payload with %d bytes\n",
			o.buffer.Len(),
		)
		return o.Flush()
	}
	return nil
}

func (o *jsonWriter) Flush() error {
	req, err := http.NewRequest("POST", o.targetURL, &o.buffer)
	if err != nil {
		return err
	}

	req.Header.Set("X-CRL-TOKEN", o.httpToken)
	err = o.doRequest(req)
	if err != nil {
		return err
	}

	o.buffer = bytes.Buffer{}
	return nil
}

var _ tsWriter = &jsonWriter{}

func makeJSONWriter(
	targetURL string, httpToken string, threshold int, doRequest func(req *http.Request) error,
) tsWriter {
	return &jsonWriter{
		targetURL: targetURL,
		timestamp: timeutil.Now().Unix(),
		httpToken: httpToken,
		threshold: threshold,
		doRequest: doRequest,
	}
}

type openMetricsWriter struct {
	out    io.Writer
	labels map[string]string
}

// createYAML generates and writes tsdump.yaml to default /tmp or to a specified path.
// This file is used for staging the tsdump data into a local database for debugging
func createYAML(ctx context.Context) (resErr error) {
	// Write the YAML file for backward compatibility
	file, err := os.OpenFile(debugTimeSeriesDumpOpts.yaml, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	defer file.Close()

	mapping, err := getStoreToNodeMapping(ctx)
	if err != nil {
		return err
	}

	for storeID, nodeID := range mapping {
		_, err := fmt.Fprintf(file, "%s: %s\n", storeID, nodeID)
		if err != nil {
			return err
		}
	}
	return nil
}

// getStoreToNodeMapping retrieves the store-to-node mapping from the database
func getStoreToNodeMapping(ctx context.Context) (map[string]string, error) {
	sqlConn, err := makeSQLClient(ctx, tsDumpAppName, useSystemDb)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := sqlConn.Close(); closeErr != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to close SQL connection: %v\n", closeErr)
		}
	}()

	_, rows, err := sqlExecCtx.RunQuery(
		ctx,
		sqlConn,
		clisqlclient.MakeQuery(`SELECT store_id, node_id FROM crdb_internal.kv_store_status`), false)

	if err != nil {
		return nil, err
	}

	mapping := make(map[string]string)
	for _, row := range rows {
		if len(row) >= 2 {
			storeID := strings.TrimSpace(row[0])
			nodeID := strings.TrimSpace(row[1])
			mapping[storeID] = nodeID
		}
	}
	return mapping, nil
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

// metricsListEntry represents an entry from the metrics list file,
// which can be either a literal metric name or a regex pattern.
type metricsListEntry struct {
	value   string
	isRegex bool
}

// regexMetaChars contains characters that indicate a line is a regex pattern
// rather than a literal metric name. Metric names only contain alphanumeric
// characters, dots, underscores, and hyphens.
var regexMetaChars = regexp.MustCompile(`[*+?^$|()\[\]{}\\]`)

// isRegexPattern returns true if the line contains regex metacharacters,
// indicating it should be treated as a regex pattern rather than a literal name.
func isRegexPattern(line string) bool {
	return regexMetaChars.MatchString(line)
}

// readMetricsListFile reads a file containing metric names or regex patterns (one per line).
// Lines starting with # are treated as comments and skipped. Inline comments
// (text after #) are also stripped. Empty lines are skipped. If metric names
// include cr.node., cr.store., or cockroachdb. prefixes, they are stripped.
// Lines containing regex metacharacters (*+?^$|()[]{}\) are automatically
// detected and treated as regex patterns.
// Duplicate entries are removed. Returns entries without any prefix.
func readMetricsListFile(filePath string) ([]metricsListEntry, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open metrics list file %s", filePath)
	}
	defer file.Close()

	seen := make(map[string]struct{})
	var entries []metricsListEntry
	scanner := bufio.NewScanner(file)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := scanner.Text()

		// Strip inline comments (anything after #)
		if idx := strings.Index(line, "#"); idx >= 0 {
			line = line[:idx]
		}
		line = strings.TrimSpace(line)

		// Skip empty lines
		if line == "" {
			continue
		}

		// Auto-detect if this is a regex pattern based on metacharacters
		isRegex := isRegexPattern(line)
		if isRegex {
			// Validate the regex
			if _, err := regexp.Compile(line); err != nil {
				return nil, errors.Wrapf(err, "invalid regex pattern on line %d: %s", lineNum, line)
			}
		} else {
			// Strip common prefixes if present (cr.node., cr.store., cockroachdb.)
			line = strings.TrimPrefix(line, "cr.node.")
			line = strings.TrimPrefix(line, "cr.store.")
			line = strings.TrimPrefix(line, "cockroachdb.")
		}

		// Skip duplicates
		if _, exists := seen[line]; exists {
			continue
		}
		seen[line] = struct{}{}
		entries = append(entries, metricsListEntry{value: line, isRegex: isRegex})
	}
	if err := scanner.Err(); err != nil {
		return nil, errors.Wrapf(err, "error reading metrics list file %s", filePath)
	}
	if len(entries) == 0 {
		return nil, errors.Newf("metrics list file %s contains no valid metric names or patterns", filePath)
	}
	return entries, nil
}

// getTimeseriesNamesFromFile reads metric names and regex patterns from a file
// and expands them to internal timeseries names by:
// 1. Querying server metadata to get all available metrics
// 2. Matching regex patterns against all available metric names
// 3. Expanding histogram metrics with quantile suffixes (e.g., -p99, -max)
// 4. Adding cr.node. and cr.store. prefixes
// This mirrors the behavior of serverpb.GetInternalTimeseriesNamesFromServer
// but for an explicit subset of metrics.
func getTimeseriesNamesFromFile(
	ctx context.Context, ac serverpb.RPCAdminClient, filePath string,
) ([]string, error) {
	// Read metric names and patterns from file
	entries, err := readMetricsListFile(filePath)
	if err != nil {
		return nil, err
	}

	// Query server for all metric metadata to determine types
	resp, err := ac.AllMetricMetadata(ctx, &serverpb.MetricMetadataRequest{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to query metric metadata from server")
	}

	// Build list of all available metric names for regex matching
	allMetricNames := make([]string, 0, len(resp.Metadata))
	for name := range resp.Metadata {
		allMetricNames = append(allMetricNames, name)
	}

	// Resolve entries to actual metric names (expand regex patterns)
	resolvedNames := make(map[string]struct{})
	for _, entry := range entries {
		if entry.isRegex {
			// Compile and match against all available metrics
			re, err := regexp.Compile(entry.value)
			if err != nil {
				return nil, errors.Wrapf(err, "invalid regex pattern: %s", entry.value)
			}
			matchCount := 0
			for _, name := range allMetricNames {
				if re.MatchString(name) {
					resolvedNames[name] = struct{}{}
					matchCount++
				}
			}
			if matchCount > 0 {
				fmt.Fprintf(os.Stderr, "Pattern '%s' matched %d metrics\n", entry.value, matchCount)
			} else {
				fmt.Fprintf(os.Stderr, "Warning: pattern '%s' matched no metrics\n", entry.value)
			}
		} else {
			// Literal metric name
			resolvedNames[entry.value] = struct{}{}
		}
	}

	// Convert to sorted slice
	requestedNames := make([]string, 0, len(resolvedNames))
	for name := range resolvedNames {
		requestedNames = append(requestedNames, name)
	}
	sort.Strings(requestedNames)

	// Expand metrics: for histograms add quantile suffixes, otherwise use as-is
	var expanded []string
	for _, name := range requestedNames {
		meta, exists := resp.Metadata[name]
		if exists && meta.MetricType == io_prometheus_client.MetricType_HISTOGRAM {
			// Histogram metric: add all quantile suffixes
			for _, q := range metric.HistogramMetricComputers {
				expanded = append(expanded, name+q.Suffix)
			}
		} else {
			// Non-histogram: use as-is
			expanded = append(expanded, name)
		}
	}

	// Add cr.node. and cr.store. prefixes (both, since we don't know which applies)
	out := make([]string, 0, 2*len(expanded))
	for _, prefix := range []string{"cr.node.", "cr.store."} {
		for _, name := range expanded {
			out = append(out, prefix+name)
		}
	}
	sort.Strings(out)
	return out, nil
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
