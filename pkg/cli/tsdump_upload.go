// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"archive/zip"
	"bytes"
	"compress/gzip"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const (
	DatadogSeriesTypeUnknown = iota
	DatadogSeriesTypeCounter
	DatadogSeriesTypeRate
	DatadogSeriesTypeGauge
)

var (
	// each site in datadog has a different host name. ddSiteToHostMap
	// holds the mapping of site name to the host name.
	ddSiteToHostMap = map[string]string{
		"us1":     "api.datadoghq.com",
		"us3":     "api.us3.datadoghq.com",
		"us5":     "api.us5.datadoghq.com",
		"eu1":     "api.datadoghq.eu",
		"ap1":     "api.ap1.datadoghq.com",
		"us1-fed": "api.ddog-gov.com",
	}

	targetURLFormat           = "https://%s/api/v2/series"
	datadogDashboardURLFormat = "https://us5.datadoghq.com/dashboard/bif-kwe-gx2/self-hosted-db-console-tsdump?" +
		"tpl_var_cluster=%s&tpl_var_upload_id=%s&from_ts=%d&to_ts=%d"
	zipFileSignature = []byte{0x50, 0x4B, 0x03, 0x04}
)

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

type DatadogResp struct {
	Errors []string `json:"errors"`
}

var newTsdumpUploadID = func() string {
	clusterTagValue := ""
	if debugTimeSeriesDumpOpts.clusterLabel != "" {
		clusterTagValue = debugTimeSeriesDumpOpts.clusterLabel
	} else if serverCfg.ClusterName != "" {
		clusterTagValue = serverCfg.ClusterName
	} else {
		clusterTagValue = fmt.Sprintf("cluster-debug-%d", timeutil.Now().Unix())
	}
	return newUploadID(clusterTagValue)
}

// datadogWriter can convert our metrics to Datadog format and send
// them via HTTP to the public DD endpoint, assuming an API key is set
// in the CLI flags.
type datadogWriter struct {
	sync.Once
	targetURL string
	uploadID  string
	init      bool
	apiKey    string
	// namePrefix sets the string to prepend to all metric names. The
	// names are kept with `.` delimiters.
	namePrefix string
	doRequest  func(req *http.Request) error
	threshold  int
}

func makeDatadogWriter(
	targetURL string,
	init bool,
	apiKey string,
	threshold int,
	doRequest func(req *http.Request) error,
) *datadogWriter {
	return &datadogWriter{
		targetURL:  targetURL,
		uploadID:   newTsdumpUploadID(),
		init:       init,
		apiKey:     apiKey,
		namePrefix: "crdb.tsdump.", // Default pre-set prefix to distinguish these uploads.
		doRequest:  doRequest,
		threshold:  threshold,
	}
}

func doDDRequest(req *http.Request) error {
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
		return errors.Newf("tsdump: error response from datadog: %v", ddResp.Errors)
	}
	if resp.StatusCode > 299 {
		return errors.Newf("tsdump: bad response status code: %+v", resp)
	}
	return nil
}

func dump(kv *roachpb.KeyValue) (*DatadogSeries, error) {
	name, source, _, _, err := ts.DecodeDataKey(kv.Key)
	if err != nil {
		return nil, err
	}
	var idata roachpb.InternalTimeSeriesData
	if err := kv.Value.GetProto(&idata); err != nil {
		return nil, err
	}

	series := &DatadogSeries{
		Metric: name,
		Tags:   []string{},
		Type:   DatadogSeriesTypeUnknown,
		Points: make([]DatadogPoint, idata.SampleCount()),
	}

	sl := reCrStoreNode.FindStringSubmatch(name)
	if len(sl) != 0 {
		storeNodeKey := sl[1]
		if storeNodeKey == "node" {
			storeNodeKey += "_id"
		}
		series.Tags = append(series.Tags, fmt.Sprintf("%s:%s", storeNodeKey, source))
		series.Metric = sl[2]
	} else {
		series.Tags = append(series.Tags, "node_id:0")
	}

	for i := 0; i < idata.SampleCount(); i++ {
		if idata.IsColumnar() {
			series.Points[i].Timestamp = idata.TimestampForOffset(idata.Offset[i]) / 1_000_000_000
			series.Points[i].Value = idata.Last[i]
		} else {
			series.Points[i].Timestamp = idata.TimestampForOffset(idata.Samples[i].Offset) / 1_000_000_000
			series.Points[i].Value = idata.Samples[i].Sum
		}

	}
	return series, nil
}

func (d *datadogWriter) emitDataDogMetrics(data []DatadogSeries) error {
	var tags []string
	// Hardcoded values
	tags = append(tags, "cluster_type:SELF_HOSTED")
	tags = append(tags, "job:cockroachdb")
	tags = append(tags, "region:local")

	if debugTimeSeriesDumpOpts.clusterLabel != "" {
		tags = append(tags, makeDDTag("cluster_label", debugTimeSeriesDumpOpts.clusterLabel))
	}

	tags = append(tags, makeDDTag(uploadIDTag, d.uploadID))

	for i := 0; i < len(data); i++ {
		data[i].Tags = append(data[i].Tags, tags...)
		data[i].Metric = d.namePrefix + data[i].Metric
	}

	// When running in init mode, we insert zeros with the current
	// timestamp in order to populate Datadog's metrics list. Then the
	// user can enable these metrics for historic ingest and load the
	// full dataset. This should only be necessary once globally.
	if d.init {
		for i := 0; i < len(data); i++ {
			data[i].Points = []DatadogPoint{{
				Value:     0,
				Timestamp: timeutil.Now().Unix(),
			}}
		}
	}

	fmt.Printf(
		"\033[G\033[Ktsdump datadog upload: uploading metrics containing %d series including %s",
		len(data),
		data[0].Metric,
	)

	return d.flush(data)
}

func (d *datadogWriter) flush(data []DatadogSeries) error {
	var buf bytes.Buffer
	err := json.NewEncoder(&buf).Encode(&DatadogSubmitMetrics{Series: data})
	if err != nil {
		return err
	}
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

	retryOpts := base.DefaultRetryOptions()
	retryOpts.MaxRetries = 5
	var req *http.Request
	for retry := retry.Start(retryOpts); retry.Next(); {
		req, err = http.NewRequest("POST", d.targetURL, &zipBuf)
		if err != nil {
			return err
		}
		req.Header.Set("DD-API-KEY", d.apiKey)
		req.Header.Set(server.ContentTypeHeader, "application/json")
		req.Header.Set(httputil.ContentEncodingHeader, "gzip")

		err = d.doRequest(req)
		if err == nil {
			return nil
		}
	}
	return err
}

func (d *datadogWriter) upload(fileName string) error {
	f, err := getFileReader(fileName)
	if err != nil {
		return err
	}

	dec := gob.NewDecoder(f)
	gob.Register(&roachpb.KeyValue{})
	decodeOne := func() ([]DatadogSeries, error) {
		var ddSeries []DatadogSeries

		for i := 0; i < d.threshold; i++ {
			var v roachpb.KeyValue
			err := dec.Decode(&v)
			if err != nil {
				return ddSeries, err
			}

			datadogSeries, err := dump(&v)
			if err != nil {
				return nil, err
			}
			ddSeries = append(ddSeries, *datadogSeries)
		}

		return ddSeries, nil
	}

	var wg sync.WaitGroup
	ch := make(chan []DatadogSeries, 4000)
	var errorsInDDUpload []string
	for i := 0; i < 1000; i++ {
		go func() {
			for data := range ch {
				err := d.emitDataDogMetrics(data)
				if err != nil {
					errorsInDDUpload = append(errorsInDDUpload,
						fmt.Sprintf("retries exhausted for datadog upload for series %s with error %v\n", data[0].Metric, err))
					wg.Done()
					return
				}
				wg.Done()
			}
		}()
	}

	for {
		data, err := decodeOne()
		if err == io.EOF {
			if len(data) != 0 {
				wg.Add(1)
				ch <- data
			}
			break
		}
		wg.Add(1)
		ch <- data
	}

	wg.Wait()
	toUnixTimestamp := timeutil.Now().UnixMilli()
	//create timestamp for T-30 days.
	fromUnixTimestamp := toUnixTimestamp - (30 * 24 * 60 * 60 * 1000)
	dashboardLink := fmt.Sprintf(datadogDashboardURLFormat, debugTimeSeriesDumpOpts.clusterLabel, d.uploadID, fromUnixTimestamp, toUnixTimestamp)

	if len(errorsInDDUpload) != 0 {
		fmt.Printf("\n%d upload errors occurred:\n%s\n", len(errorsInDDUpload), strings.Join(errorsInDDUpload, "\n"))
	}
	fmt.Println("\nupload id:", d.uploadID)
	fmt.Printf("datadog dashboard link: %s\n", dashboardLink)

	close(ch)
	return nil
}

// getFileReader returns an io.Reader based on the file type.
func getFileReader(fileName string) (io.Reader, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}

	// Check if the file is a zip file by reading its magic number
	buf := make([]byte, 4)
	if _, err := file.Read(buf); err != nil {
		return nil, err
	}

	// Reset the file pointer to the beginning
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	// Check for zip file signature
	if bytes.HasPrefix(buf, zipFileSignature) {
		zipReader, err := zip.NewReader(file, fileSize(file))
		if err != nil {
			return nil, err
		}

		if len(zipReader.File) > 0 {
			if len(zipReader.File) > 1 {
				fmt.Printf("tsdump datadog upload: warning: more than one file in zip archive, using the first file %s\n", zipReader.File[0].Name)
			}
			firstFile, err := zipReader.File[0].Open()
			if err != nil {
				return nil, err
			}
			return firstFile, nil
		}
		return nil, fmt.Errorf("zip archive is empty")
	}

	return file, nil
}

// fileSize returns the size of the file.
func fileSize(file *os.File) int64 {
	info, err := file.Stat()
	if err != nil {
		return 0
	}
	return info.Size()
}
