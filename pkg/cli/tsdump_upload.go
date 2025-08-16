// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"gopkg.in/yaml.v3"
)

const (
	UploadStatusSuccess        = "Success"
	UploadStatusPartialSuccess = "Partial Success"
	UploadStatusFailure        = "Failed"
	nodeKey                    = "node_id"
	processDeltaEnvVar         = "COCKROACH_TSDUMP_DELTA"
)

var (
	// each site in datadog has a different host name. ddSiteToHostMap
	// holds the mapping of site name to the host name.
	ddSiteToHostMap = map[string]string{
		"us1":     "datadoghq.com",
		"us3":     "us3.datadoghq.com",
		"us5":     "us5.datadoghq.com",
		"eu1":     "datadoghq.eu",
		"ap1":     "ap1.datadoghq.com",
		"us1-fed": "ddog-gov.com",
	}

	datadogDashboardURLFormat = "https://us5.datadoghq.com/dashboard/bif-kwe-gx2/self-hosted-db-console-tsdump?" +
		"tpl_var_cluster=%s&tpl_var_upload_id=%s&tpl_var_upload_day=%d&tpl_var_upload_month=%d&tpl_var_upload_year=%d&from_ts=%d&to_ts=%d"
	zipFileSignature            = []byte{0x50, 0x4B, 0x03, 0x04}
	gzipFileSignature           = []byte{0x1f, 0x8b}
	logMessageFormat            = "tsdump upload to datadog is partially failed for metric: %s"
	partialFailureMessageFormat = "The Tsdump upload to Datadog succeeded but %d metrics partially failed to upload." +
		" These failures can be due to transient network errors.\nMetrics:\n%s\n" +
		"If any of these metrics are critical for your investigation," +
		" Failed requests have been saved to '%s'. You can retry uploading only the failed requests using the --retry-failed-requests flag\n"

	datadogLogsURLFormat         = "https://us5.datadoghq.com/logs?query=cluster_label:%s+upload_id:%s"
	failedRequestsFileNameFormat = "tsdump_failed_requests_%s.json"

	translateMetricType = map[string]*datadogV2.MetricIntakeType{
		"GAUGE":   datadogV2.METRICINTAKETYPE_GAUGE.Ptr(),
		"COUNTER": datadogV2.METRICINTAKETYPE_COUNT.Ptr(),
	}
)

// FailedRequest represents a failed metric upload request that can be retried
type FailedRequest struct {
	MetricSeries []datadogV2.MetricSeries `json:"metric_series"`
	UploadID     string                   `json:"upload_id"`
	Timestamp    time.Time                `json:"timestamp"`
	Error        string                   `json:"error,omitempty"`
}

// FailedRequestsFile represents the structure of the failed requests file
type FailedRequestsFile struct {
	Requests []FailedRequest `json:"requests"`
}

var newTsdumpUploadID = func(uploadTime time.Time) string {
	clusterTagValue := "cluster-debug"
	if debugTimeSeriesDumpOpts.clusterLabel != "" {
		clusterTagValue = debugTimeSeriesDumpOpts.clusterLabel
	}
	return newUploadID(clusterTagValue, uploadTime)
}

// datadogWriter can convert our metrics to Datadog format and send
// them via HTTP to the public DD endpoint, assuming an API key is set
// in the CLI flags.
type datadogWriter struct {
	sync.Once
	uploadID       string
	init           bool
	apiClient      *datadog.APIClient
	apiKey         string
	datadogContext context.Context
	// namePrefix sets the string to prepend to all metric names. The
	// names are kept with `.` delimiters.
	namePrefix        string
	threshold         int
	uploadTime        time.Time
	storeToNodeMap    map[string]string
	metricTypeMap     map[string]string
	noOfUploadWorkers int
	// isPartialUploadOfFailedRequests indicates whether are we retrying failed requests
	isPartialUploadOfFailedRequests bool
	// failedRequestsFileName which captures failed requests.
	failedRequestsFileName string
	// fileMutex protects concurrent access to file which captures failed requests.
	fileMutex syncutil.Mutex
	// hasFailedRequestsInUpload tracks if any failed requests were saved during upload
	hasFailedRequestsInUpload bool
	// cumulativeToDeltaProcessor is used to convert cumulative counter metrics to delta metrics
	cumulativeToDeltaProcessor *CumulativeToDeltaProcessor
}

func makeDatadogWriter(
	ddSite string,
	init bool,
	apiKey string,
	threshold int,
	hostNameOverride string,
	noOfUploadWorkers int,
	isPartialUploadOfFailedRequests bool,
) (*datadogWriter, error) {
	currentTime := getCurrentTime()

	metricTypeMap, err := loadMetricTypesMap(context.Background())
	if err != nil {
		fmt.Printf(
			"error loading metric types map: %v\nThis may lead to some metrics not behaving correctly on Datadog.\n", err)
	}

	ctx := context.WithValue(
		context.Background(),
		datadog.ContextAPIKeys,
		map[string]datadog.APIKey{
			"apiKeyAuth": {
				Key: apiKey,
			},
		},
	)
	host, ok := ddSiteToHostMap[ddSite]
	if !ok {
		return nil, fmt.Errorf("unsupported datadog site '%s'", ddSite)
	}
	ctx = context.WithValue(ctx, datadog.ContextServerVariables, map[string]string{
		"site": host,
	})

	// The Datadog retry configuration is used when we receive error codes
	// 429 and >= 500 from the Datadog.
	configuration := datadog.NewConfiguration()
	configuration.RetryConfiguration.EnableRetry = true
	configuration.RetryConfiguration.BackOffMultiplier = 1
	configuration.RetryConfiguration.BackOffBase = 0.2 // 200ms
	configuration.RetryConfiguration.MaxRetries = 100
	apiClient := datadog.NewAPIClient(configuration)
	if hostNameOverride != "" {
		apiClient.Cfg.Host = hostNameOverride
		apiClient.Cfg.Scheme = "http"
	}

	return &datadogWriter{
		datadogContext:                  ctx,
		apiClient:                       apiClient,
		apiKey:                          apiKey,
		uploadID:                        newTsdumpUploadID(currentTime),
		init:                            init,
		namePrefix:                      "crdb.tsdump.", // Default pre-set prefix to distinguish these uploads.
		threshold:                       threshold,
		uploadTime:                      currentTime,
		storeToNodeMap:                  make(map[string]string),
		metricTypeMap:                   metricTypeMap,
		noOfUploadWorkers:               noOfUploadWorkers,
		isPartialUploadOfFailedRequests: isPartialUploadOfFailedRequests,
		cumulativeToDeltaProcessor:      NewCumulativeToDeltaProcessor(),
	}, nil
}

var getCurrentTime = func() time.Time {
	return timeutil.Now()
}

var getHostname = func() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return hostname
}

// appendTag appends a formatted tag to the series tags.
func appendTag(series *datadogV2.MetricSeries, tagKey, tagValue string) {
	series.Tags = append(series.Tags, fmt.Sprintf("%s:%s", tagKey, tagValue))
}

func (d *datadogWriter) dump(kv *roachpb.KeyValue) (*datadogV2.MetricSeries, error) {
	name, source, res, _, err := ts.DecodeDataKey(kv.Key)
	if err != nil {
		return nil, err
	}
	var idata roachpb.InternalTimeSeriesData
	if err := kv.Value.GetProto(&idata); err != nil {
		return nil, err
	}

	series := &datadogV2.MetricSeries{
		Metric:   name,
		Tags:     []string{},
		Type:     d.resolveMetricType(name),
		Points:   make([]datadogV2.MetricPoint, idata.SampleCount()),
		Interval: datadog.PtrInt64(int64(res.Duration().Seconds())), // convert from time.Duration to number of seconds.
	}

	sl := reCrStoreNode.FindStringSubmatch(name)
	if len(sl) >= 3 {
		// extract the node/store and metric name from the regex match.
		key := sl[1]
		series.Metric = sl[2]

		switch key {
		case "node":
			appendTag(series, nodeKey, source)
		case "store":
			appendTag(series, key, source)
			// We check the node associated with store if store to node mapping
			// is provided as part of --store-to-node-map-file flag. If exists then
			// emit node as tag.
			if nodeID, ok := d.storeToNodeMap[source]; ok {
				appendTag(series, nodeKey, nodeID)
			}
		default:
			appendTag(series, key, source)
		}
	} else {
		// add default node_id as 0 as there is no metric match for the regex.
		appendTag(series, nodeKey, "0")
	}

	isSorted := true
	var previousTimestamp int64
	for i := 0; i < idata.SampleCount(); i++ {
		if idata.IsColumnar() {
			series.Points[i].Timestamp = datadog.PtrInt64(idata.TimestampForOffset(idata.Offset[i]) / 1_000_000_000)
			series.Points[i].Value = datadog.PtrFloat64(idata.Last[i])
		} else {
			series.Points[i].Timestamp = datadog.PtrInt64(idata.TimestampForOffset(idata.Samples[i].Offset) / 1_000_000_000)
			series.Points[i].Value = datadog.PtrFloat64(idata.Samples[i].Sum)
		}

		if !isSorted {
			// if we already found a point out of order, we can skip further checks
			continue
		}

		// Check if timestamps are in ascending order. We cannot assume time series
		// data is sorted because:
		// 1. pkg/ts/tspb/timeseries.go ToInternal() explicitly states "returned slice will not be sorted"
		// 2. pkg/storage/pebble_merge.go sortAndDeduplicateRows/Columns shows storage merge
		//    operations can result in out-of-order data before final sorting
		// 3. Data from different storage slabs may be interleaved during tsdump reads
		currentTimestamp := *series.Points[i].Timestamp
		if i > 0 && previousTimestamp > currentTimestamp {
			isSorted = false
		}
		previousTimestamp = currentTimestamp
	}

	if envutil.EnvOrDefaultInt(processDeltaEnvVar, 0) == 1 {
		if err := d.cumulativeToDeltaProcessor.processCounterMetric(series, isSorted); err != nil {
			return nil, err
		}
	}

	return series, nil
}

// saveFailedRequest saves a failed request to the failed requests file
func (d *datadogWriter) saveFailedRequest(data []datadogV2.MetricSeries, err error) {
	failedRequest := FailedRequest{
		MetricSeries: data,
		UploadID:     d.uploadID,
		Timestamp:    getCurrentTime(),
		Error:        err.Error(),
	}

	// Append to file using streaming JSON
	if appendErr := d.appendFailedRequestToFile(failedRequest); appendErr != nil {
		fmt.Printf("Warning: Failed to save failed request to file: %v\n", appendErr)
		return
	}

	// mark that we have saved requests
	sync.OnceFunc(func() {
		d.hasFailedRequestsInUpload = true
	})()

}

// appendFailedRequestToFile appends a single failed request to the file
func (d *datadogWriter) appendFailedRequestToFile(request FailedRequest) error {
	d.fileMutex.Lock()
	defer d.fileMutex.Unlock()

	fileName := d.failedRequestsFileName
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			fmt.Printf("Warning: Failed to close failed requests file %s: %v\n", file.Name(), err)
		}
	}(file)

	// Write each failed request as a single JSON line (JSONL format)
	requestJSON, err := json.Marshal(request)
	if err != nil {
		return err
	}

	// Append newline to make it proper JSONL format
	_, err = file.Write(append(requestJSON, '\n'))
	return err
}

// streamFailedRequests streams failed requests from file to a channel without loading all into memory
func streamFailedRequests(
	filePath string, ch chan<- *FailedRequest, wg *sync.WaitGroup,
) (int, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			fmt.Printf("failed to close file %s: %v\n", file.Name(), err)
		}
	}(file)

	decoder := json.NewDecoder(file)
	count := 0

	for {
		var request FailedRequest
		err := decoder.Decode(&request)
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("failed to parse failed request line: %v\n", err)
			continue
		}

		// Add to wait group as soon as we read a valid request
		wg.Add(1)
		// Send request to channel for processing
		ch <- &request
		count++

	}
	return count, nil
}

// retryFailedRequests attempts to re-upload failed requests using streaming approach with channels
func (d *datadogWriter) retryFailedRequests(fileName string) error {

	// Create temporary file for requests which are still failing in re-upload.
	tempFile := fileName + ".tmp"
	d.failedRequestsFileName = tempFile

	// Create channel for streaming requests
	requestCh := make(chan *FailedRequest, 100)
	defer close(requestCh)
	var wg sync.WaitGroup

	// State to track retry results
	var retryState struct {
		syncutil.Mutex
		successCount int
		failedCount  int
	}

	// Start workers to retry failed requests
	for i := 0; i < d.noOfUploadWorkers; i++ {
		go func() {
			for failedReq := range requestCh {
				_, err := d.emitDataDogMetrics(failedReq.MetricSeries)
				func() {
					retryState.Lock()
					defer retryState.Unlock()
					if err != nil {
						retryState.failedCount++
					} else {
						retryState.successCount++
					}
				}()
				wg.Done()
			}
		}()
	}

	_, err := streamFailedRequests(fileName, requestCh, &wg)
	if err != nil {
		fmt.Printf("error streaming failed requests: %v\n", err)
	}

	// Wait for all requests to be processed
	wg.Wait()
	fmt.Printf("\nretry completed: %d succeeded, %d still failed\n", retryState.successCount, retryState.failedCount)

	// Replace original file with temp file if there are still failing requests
	if d.hasFailedRequestsInUpload {
		if err := os.Rename(tempFile, fileName); err != nil {
			return fmt.Errorf("failed to update failed requests file: %w", err)
		}
		fmt.Printf("%d requests still failed and have been saved to %s\n", retryState.failedCount, fileName)
		return nil
	}

	// All retries succeeded, remove the original failed requests file
	if err := os.Remove(fileName); err != nil {
		fmt.Printf("failed to remove failed requests file: %v\n", err)
	} else {
		fmt.Println("All retry requests succeeded. Failed requests file has been removed.")
	}
	return nil
}

func (d *datadogWriter) resolveMetricType(metricName string) *datadogV2.MetricIntakeType {
	typeLookupKey := strings.TrimPrefix(metricName, "cr.store.")
	typeLookupKey = strings.TrimPrefix(typeLookupKey, "cr.node.")
	metricType := d.metricTypeMap[typeLookupKey]
	if t, ok := translateMetricType[metricType]; ok {
		return t
	}

	if strings.HasSuffix(metricName, "-count") {
		return datadogV2.METRICINTAKETYPE_COUNT.Ptr()
	}

	if strings.HasSuffix(metricName, "-avg") ||
		strings.HasSuffix(metricName, "-max") ||
		strings.HasSuffix(metricName, "-sum") ||
		strings.HasSuffix(metricName, "-p50") ||
		strings.HasSuffix(metricName, "-p75") ||
		strings.HasSuffix(metricName, "-p90") ||
		strings.HasSuffix(metricName, "-p99") ||
		strings.HasSuffix(metricName, "-p99.9") ||
		strings.HasSuffix(metricName, "-p99.99") ||
		strings.HasSuffix(metricName, "-p99.999") {
		return datadogV2.METRICINTAKETYPE_GAUGE.Ptr()
	}

	return datadogV2.METRICINTAKETYPE_UNSPECIFIED.Ptr()
}

var printLock syncutil.Mutex

func (d *datadogWriter) emitDataDogMetrics(data []datadogV2.MetricSeries) ([]string, error) {
	tags := getUploadTags(d)

	emittedMetrics := make([]string, len(data))
	for i := 0; i < len(data); i++ {
		// If we are retrying failed requests, we don't want to append prefix again to the metrics
		// as initial upload has done that already.
		if !d.isPartialUploadOfFailedRequests {
			data[i].Tags = append(data[i].Tags, tags...)
			data[i].Metric = d.namePrefix + data[i].Metric
		}
		emittedMetrics[i] = data[i].Metric
	}

	// When running in init mode, we insert zeros with the current
	// timestamp in order to populate Datadog's metrics list. Then the
	// user can enable these metrics for historic ingest and load the
	// full dataset. This should only be necessary once globally.
	if d.init {
		for i := 0; i < len(data); i++ {
			data[i].Points = []datadogV2.MetricPoint{{
				Value:     datadog.PtrFloat64(0),
				Timestamp: datadog.PtrInt64(getCurrentTime().Unix()),
			}}
		}
	}

	// Because the log below resets and overwrites the print line in
	// order to avoid an explosion of logs filling the screen, it's
	// necessary to wrap it in a mutex since `emitDataDogMetrics` is
	// called concurrently.
	func() {
		printLock.Lock()
		defer printLock.Unlock()
		if len(data) > 0 {
			fmt.Printf(
				"\033[G\033[Ktsdump datadog upload: uploading metrics containing %d series including %s",
				len(data),
				data[0].Metric,
			)
		} else {
			fmt.Printf(
				"\033[G\033[Ktsdump datadog upload: uploading metrics containing 0 series",
			)
		}
	}()

	err := d.flush(data)
	if err != nil {
		// save failed request for potential retry.
		d.saveFailedRequest(data, err)
	}

	return emittedMetrics, err
}

func getUploadTags(d *datadogWriter) []string {
	var tags []string
	// Hardcoded values
	tags = append(tags, "cluster_type:SELF_HOSTED")

	if debugTimeSeriesDumpOpts.clusterLabel != "" {
		tags = append(tags, makeDDTag("cluster_label", debugTimeSeriesDumpOpts.clusterLabel))
	}
	if debugTimeSeriesDumpOpts.clusterID != "" {
		tags = append(tags, makeDDTag("cluster_id", debugTimeSeriesDumpOpts.clusterID))
	}
	if debugTimeSeriesDumpOpts.zendeskTicket != "" {
		tags = append(tags, makeDDTag("zendesk_ticket", debugTimeSeriesDumpOpts.zendeskTicket))
	}
	if debugTimeSeriesDumpOpts.organizationName != "" {
		tags = append(tags, makeDDTag("org_name", debugTimeSeriesDumpOpts.organizationName))
	}
	if debugTimeSeriesDumpOpts.userName != "" {
		tags = append(tags, makeDDTag("user_name", debugTimeSeriesDumpOpts.userName))
	}

	tags = append(tags, makeDDTag(uploadIDTag, d.uploadID))

	year, month, day := d.uploadTime.Date()
	tags = append(tags, makeDDTag("upload_timestamp", d.uploadTime.Format("2006-01-02 15:04:05")))
	tags = append(tags, makeDDTag("upload_year", strconv.Itoa(year)))
	tags = append(tags, makeDDTag("upload_month", strconv.Itoa(int(month))))
	tags = append(tags, makeDDTag("upload_day", strconv.Itoa(day)))
	return tags
}

func (d *datadogWriter) flush(data []datadogV2.MetricSeries) error {
	if debugTimeSeriesDumpOpts.dryRun {
		return nil
	}

	api := datadogV2.NewMetricsApi(d.apiClient)
	// The retry configuration is used when we receive any error code from upload.
	// We have seen 408 error codes from Datadog when the upload is too large which
	// is not handled by the default retry configuration that Datadog API client provides.
	retryOpts := base.DefaultRetryOptions()
	retryOpts.MaxBackoff = 20 * time.Millisecond
	retryOpts.MaxRetries = 100
	err := error(nil)

	for retryAttempts := retry.Start(retryOpts); retryAttempts.Next(); {
		_, _, err = api.SubmitMetrics(d.datadogContext, datadogV2.MetricPayload{
			Series: data,
		}, datadogV2.SubmitMetricsOptionalParameters{
			ContentEncoding: datadogV2.METRICCONTENTENCODING_GZIP.Ptr(),
		})

		if err == nil {
			return nil
		}
	}
	if err != nil {
		fmt.Printf("error submitting metrics to datadog: %v\n", err)
	}
	return err
}

func (d *datadogWriter) upload(fileName string) error {
	f, err := getFileReader(fileName)
	if err != nil {
		return err
	}

	// Extract directory from input fileName and attach it to failed requests filename
	inputDir := filepath.Dir(fileName)
	failedRequestsBaseName := fmt.Sprintf(failedRequestsFileNameFormat, d.uploadID)
	d.failedRequestsFileName = filepath.Join(inputDir, failedRequestsBaseName)

	if debugTimeSeriesDumpOpts.dryRun {
		fmt.Println("Dry-run mode enabled. Not actually uploading data to Datadog.")
	}

	storeToNodeYamlFile := debugTimeSeriesDumpOpts.storeToNodeMapYAMLFile
	if storeToNodeYamlFile != "" {
		d.populateNodeAndStoreMap(storeToNodeYamlFile)
	}

	dec := gob.NewDecoder(f)
	allMetrics := make(map[string]struct{})
	decodeOne := func() ([]datadogV2.MetricSeries, error) {
		var ddSeries []datadogV2.MetricSeries

		for i := 0; i < d.threshold; i++ {
			var v roachpb.KeyValue
			err := dec.Decode(&v)
			if err != nil {
				return ddSeries, err
			}

			datadogSeries, err := d.dump(&v)
			if err != nil {
				return nil, err
			}
			ddSeries = append(ddSeries, *datadogSeries)
			tags := datadogSeries.Tags
			sort.Strings(tags)
			tagsStr := strings.Join(tags, ",")
			metricCtx := datadogSeries.Metric + "|" + tagsStr
			allMetrics[metricCtx] = struct{}{}
		}

		return ddSeries, nil
	}

	var wg sync.WaitGroup
	ch := make(chan []datadogV2.MetricSeries, 4000)

	// metricsUploadState wraps the failed metrics collection in a mutex since
	// they're collected concurrently.
	var metricsUploadState struct {
		syncutil.Mutex
		// we are taking map here as we want unique metric names which were failed across all tsdump upload requests.
		uploadFailedMetrics     map[string]struct{}
		isSingleUploadSucceeded bool
	}
	metricsUploadState.uploadFailedMetrics = make(map[string]struct{})
	metricsUploadState.isSingleUploadSucceeded = false

	markSuccessOnce := sync.OnceFunc(func() {
		metricsUploadState.isSingleUploadSucceeded = true
	})

	for i := 0; i < d.noOfUploadWorkers; i++ {
		go func() {
			for data := range ch {
				emittedMetrics, err := d.emitDataDogMetrics(data)
				if err != nil {
					func() {
						metricsUploadState.Lock()
						defer metricsUploadState.Unlock()
						for _, metric := range emittedMetrics {
							metricsUploadState.uploadFailedMetrics[metric] = struct{}{}
						}
					}()
				} else {
					func() {
						markSuccessOnce()
					}()
				}
				wg.Done()
			}
		}()
	}

	seriesUploaded := 0
	for {
		data, err := decodeOne()
		seriesUploaded += len(data)
		if err == io.EOF {
			if len(data) != 0 {
				wg.Add(1)
				ch <- data
			}
			break
		}
		if err != nil {
			fmt.Println("failed to decode time series data", err)
			continue
		}
		wg.Add(1)
		ch <- data
	}

	wg.Wait()
	toUnixTimestamp := timeutil.Now().UnixMilli()
	//create timestamp for T-30 days.
	fromUnixTimestamp := toUnixTimestamp - (30 * 24 * 60 * 60 * 1000)
	year, month, day := d.uploadTime.Date()
	dashboardLink := fmt.Sprintf(datadogDashboardURLFormat, debugTimeSeriesDumpOpts.clusterLabel, d.uploadID, day, int(month), year, fromUnixTimestamp, toUnixTimestamp)

	var uploadStatus string
	if metricsUploadState.isSingleUploadSucceeded && d.hasFailedRequestsInUpload {
		uploadStatus = UploadStatusPartialSuccess
	} else if metricsUploadState.isSingleUploadSucceeded {
		uploadStatus = UploadStatusSuccess
	} else {
		uploadStatus = UploadStatusFailure
	}
	fmt.Printf("\nUpload status: %s!\n", uploadStatus)
	fmt.Printf("Uploaded %d series overall\n", seriesUploaded)
	fmt.Printf("Uploaded %d unique series overall\n", len(allMetrics))

	// Estimate cost. The cost of historical metrics ingest is based on how many
	// metrics were active during the upload window. Assuming the entire upload
	// happens during a given hour, that means the cost will be equal to the count
	// of uploaded series times $4.55/100 custom metrics (our rate) divided by
	// 730 hours per month.
	// For a single node upload that has 6500 unique series, that's about $.40
	// per upload.
	estimatedCost := float64(len(allMetrics)) * 4.55 / 100 / 730
	fmt.Printf("Estimated cost of this upload: $%.2f\n", estimatedCost)

	tags := getUploadTags(d)
	api := datadogV2.NewLogsApi(d.apiClient)

	success := metricsUploadState.isSingleUploadSucceeded
	if metricsUploadState.isSingleUploadSucceeded {
		var isDatadogUploadFailed = false
		markDatadogUploadFailedOnce := sync.OnceFunc(func() {
			isDatadogUploadFailed = true
		})
		if len(metricsUploadState.uploadFailedMetrics) != 0 {
			success = false
			// Print capture message if any failed requests were saved
			fmt.Printf(partialFailureMessageFormat, len(metricsUploadState.uploadFailedMetrics), strings.Join(func() []string {
				var failedMetricsList []string
				index := 1
				for metric := range metricsUploadState.uploadFailedMetrics {
					metric = fmt.Sprintf("%d) %s", index, metric)
					failedMetricsList = append(failedMetricsList, metric)
					index++
				}
				return failedMetricsList
			}(), "\n"), d.failedRequestsFileName)

			tags := strings.Join(tags, ",")
			fmt.Println("\nPushing logs of metric upload failures to datadog...")
			for metric := range metricsUploadState.uploadFailedMetrics {
				wg.Add(1)
				go func(metric string) {
					logMessage := fmt.Sprintf(logMessageFormat, metric)

					hostName := getHostname()
					_, _, err := api.SubmitLog(d.datadogContext, []datadogV2.HTTPLogItem{
						{
							Ddsource: datadog.PtrString("tsdump_upload"),
							Ddtags:   datadog.PtrString(tags),
							Message:  logMessage,
							Service:  datadog.PtrString("tsdump_upload"),
							Hostname: datadog.PtrString(hostName),
						},
					}, datadogV2.SubmitLogOptionalParameters{
						ContentEncoding: datadogV2.CONTENTENCODING_GZIP.Ptr(),
					})
					if err != nil {
						markDatadogUploadFailedOnce()
					}
					wg.Done()
				}(metric)
			}

			wg.Wait()
			if isDatadogUploadFailed {
				fmt.Println("Failed to pushed some metrics to datadog logs. Please refer CLI output for all failed metrics.")
			} else {
				fmt.Println("Pushing logs of metric upload failures to datadog...done")
				fmt.Printf("datadog logs for metric upload failures link: %s\n", fmt.Sprintf(datadogLogsURLFormat, debugTimeSeriesDumpOpts.clusterLabel, d.uploadID))
			}
		}
		fmt.Println("\nupload id:", d.uploadID)
		fmt.Printf("datadog dashboard link: %s\n", dashboardLink)
	} else {
		fmt.Println("All metric upload is failed. Please re-upload the Tsdump.")
	}

	eventTags := append(tags, makeDDTag("series_uploaded", strconv.Itoa(seriesUploaded)))
	hostName := getHostname()
	_, _, err = api.SubmitLog(d.datadogContext, []datadogV2.HTTPLogItem{
		{
			Ddsource: datadog.PtrString("tsdump_upload"),
			Ddtags:   datadog.PtrString(strings.Join(eventTags, ",")),
			Message:  fmt.Sprintf("tsdump upload completed: uploaded %d series overall", seriesUploaded),
			Service:  datadog.PtrString("tsdump_upload"),
			Hostname: datadog.PtrString(hostName),
			AdditionalProperties: map[string]string{
				"series_uploaded": strconv.Itoa(seriesUploaded),
				"estimated_cost":  fmt.Sprintf("%g", estimatedCost),
				"duration":        strconv.Itoa(int(getCurrentTime().Sub(d.uploadTime).Nanoseconds())),
				"dry_run":         strconv.FormatBool(debugTimeSeriesDumpOpts.dryRun),
				"success":         strconv.FormatBool(success),
			},
		},
	}, datadogV2.SubmitLogOptionalParameters{
		ContentEncoding: datadogV2.CONTENTENCODING_GZIP.Ptr(),
	})
	if err != nil {
		fmt.Printf("error submitting log to datadog: %v\n", err)
	}

	close(ch)
	return nil
}

func (d *datadogWriter) populateNodeAndStoreMap(fileName string) {
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Printf("error in opening store to node mapping YAML file: %v\n", err)
		return
	}

	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			fmt.Printf("error in clsoing store to node mapping YAML: %v\n", err)
		}
	}(file)

	decoder := yaml.NewDecoder(file)
	if err := decoder.Decode(&d.storeToNodeMap); err != nil {
		fmt.Printf("error decoding store to node mapping file YAML: %v\n", err)
		return
	}
}

// getFileReader returns an io.Reader based on the file type.
func getFileReader(fileName string) (io.Reader, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}

	// Read magic number to detect file type
	buf := make([]byte, 4)
	if _, err := file.Read(buf); err != nil {
		return nil, err
	}

	// Reset the file pointer to the beginning
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	switch {
	case bytes.HasPrefix(buf, zipFileSignature):
		zipReader, err := zip.NewReader(file, fileSize(file))
		if err != nil {
			return nil, err
		}

		if len(zipReader.File) == 0 {
			return nil, fmt.Errorf("zip archive is empty")
		}

		if len(zipReader.File) > 1 {
			fmt.Printf("tsdump datadog upload: warning: more than one file in zip archive, using the first file %s\n", zipReader.File[0].Name)
		}

		firstFile, err := zipReader.File[0].Open()
		if err != nil {
			return nil, err
		}
		return firstFile, nil

	case bytes.HasPrefix(buf, gzipFileSignature):
		gzipReader, err := gzip.NewReader(file)
		if err != nil {
			return nil, err
		}
		return gzipReader, nil

	default:
		return file, nil
	}
}

// fileSize returns the size of the file.
func fileSize(file *os.File) int64 {
	info, err := file.Stat()
	if err != nil {
		return 0
	}
	return info.Size()
}

func loadMetricTypesMap(ctx context.Context) (map[string]string, error) {
	metricLayers, err := generateMetricList(ctx, true /* skipFiltering */)
	if err != nil {
		return nil, errors.Wrap(err, "failed to generate metric list")
	}

	metricTypeMap := make(map[string]string)
	for _, metric := range metricLayers {
		for _, catagory := range metric.Categories {
			for _, m := range catagory.Metrics {
				metricTypeMap[m.Name] = m.Type
			}
		}
	}

	return metricTypeMap, nil
}
