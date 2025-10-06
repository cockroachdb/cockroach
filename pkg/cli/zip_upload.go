// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/system"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

type profileUploadEvent struct {
	Start       string   `json:"start"`
	End         string   `json:"end"`
	Attachments []string `json:"attachments"`
	Tags        string   `json:"tags_profiler"`
	Family      string   `json:"family"`
	Version     string   `json:"version"`
}

type uploadZipArtifactFunc func(ctx context.Context, uuid string, debugDirPath string) error

const (
	// default flag values
	defaultDDSite       = "us5"
	defaultGCPProjectID = "arjun-sandbox-424904" // TODO: change this project ID

	// datadog env vars
	datadogSiteEnvVar   = "DD_SITE"
	datadogAPIKeyEnvVar = "DD_API_KEY"
	datadogAPPKeyEnvVar = "DD_APP_KEY"

	// datadog HTTP headers
	datadogAPIKeyHeader = "DD-API-KEY"
	datadogAppKeyHeader = "DD-APPLICATION-KEY"

	// the path pattern to search for specific artifacts in the debug zip directory
	zippedProfilePattern = "nodes/*/*.pprof"
	zippedLogsPattern    = "nodes/*/logs/*"

	// this is not the pprof version, but the version of the profile
	// upload format supported by datadog
	profileVersion = "4"
	profileFamily  = "go"

	// names of mandatory tag
	nodeIDTag   = "node_id"
	uploadIDTag = "upload_id"
	clusterTag  = "cluster"

	// datadog endpoint URLs
	datadogProfileUploadURLTmpl = "https://intake.profile.%s/v1/input"
	datadogCreateArchiveURLTmpl = "https://%s/api/v2/logs/config/archives"

	// datadog archive attributes
	ddArchiveType            = "archives"
	ddArchiveDestinationType = "gcs"
	ddArchiveQuery           = "-*" // will make sure to not archive any live logs
	ddArchiveBucketName      = "debugzip-archives"
	ddArchiveDefaultClient   = "datadog-archive" // TODO(arjunmahishi): make this a flag also

	gcsPathTimeFormat = "dt=20060102/hour=15"
	zipUploadRetries  = 5
)

var debugZipUploadOpts = struct {
	include              []string
	ddAPIKey             string
	ddAPPKey             string
	ddSite               string
	clusterName          string
	gcpProjectID         string
	tags                 []string
	from, to             timestampValue
	logFormat            string
	maxConcurrentUploads int
}{
	maxConcurrentUploads: system.NumCPU() * 4,
}

// This is the list of all supported artifact types. The "possible values" part
// in the help text is generated from this list. So, make sure to keep this updated
// var zipArtifactTypes = []string{"profiles", "logs"}
// TODO(arjunmahishi): Removing the profiles upload for now. It has started
// failing for some reason. Will fix this later
var zipArtifactTypes = []string{"logs"}

// uploadZipArtifactFuncs is a registry of handler functions for each artifact type.
// While adding/removing functions from here, make sure to update
// the zipArtifactTypes list as well
var uploadZipArtifactFuncs = map[string]uploadZipArtifactFunc{
	"profiles": uploadZipProfiles,
	"logs":     uploadZipLogs,
}

// default datadog tags. Source has to be "cockroachdb" for the logs to be
// ingested correctly. This will make sure that the logs pass through the right
// pipeline which enriches the logs with more fields.
var defaultDDTags = []string{"service:CRDB-SH", "env:debug", "source:cockroachdb"}

func runDebugZipUpload(cmd *cobra.Command, args []string) error {
	if err := validateZipUploadReadiness(); err != nil {
		return err
	}

	// a unique ID for this upload session. This should be used to tag all the artifacts uploaded in this session
	uploadID := newUploadID(debugZipUploadOpts.clusterName)

	// override the list of artifacts to upload if the user has provided any
	artifactsToUpload := zipArtifactTypes
	if len(debugZipUploadOpts.include) > 0 {
		artifactsToUpload = debugZipUploadOpts.include
	}

	// run the upload functions for each artifact type. This can run sequentially.
	// All the concurrency is contained within the upload functions.
	for _, artType := range artifactsToUpload {
		if err := uploadZipArtifactFuncs[artType](cmd.Context(), uploadID, args[0]); err != nil {
			// Log the error and continue with the next artifact
			fmt.Printf("Failed to upload %s: %s\n", artType, err)
		}
	}

	fmt.Println("Upload ID:", uploadID)
	return nil
}

func validateZipUploadReadiness() error {
	var (
		includeLookup     = map[string]struct{}{}
		artifactsToUpload = zipArtifactTypes
	)

	if len(debugZipUploadOpts.include) > 0 {
		artifactsToUpload = debugZipUploadOpts.include
	}
	for _, inc := range artifactsToUpload {
		if _, ok := includeLookup[inc]; ok {
			// if the artifact type is already included, ignore the duplicate and
			// continue
			continue
		}

		includeLookup[inc] = struct{}{}
	}

	if debugZipUploadOpts.ddAPIKey == "" {
		return fmt.Errorf("datadog API key is required for uploading profiles")
	}

	if debugZipUploadOpts.clusterName == "" {
		return fmt.Errorf("cluster name is required for uploading profiles")
	}

	// validate the artifact types provided and fail early if any of them are not supported
	for _, artType := range debugZipUploadOpts.include {
		if _, ok := uploadZipArtifactFuncs[artType]; !ok {
			return fmt.Errorf("unsupported artifact type '%s'", artType)
		}
	}

	// validate the datadog site name
	if _, ok := ddSiteToHostMap[debugZipUploadOpts.ddSite]; !ok {
		return fmt.Errorf("unsupported datadog site '%s'", debugZipUploadOpts.ddSite)
	}

	// special validations when logs are to be uploaded
	_, ok := log.FormatParsers[debugZipUploadOpts.logFormat]
	_, shouldUploadLogs := includeLookup["logs"]
	if shouldUploadLogs {
		if !ok {
			return fmt.Errorf("unsupported log format '%s'", debugZipUploadOpts.logFormat)
		}

		if debugZipUploadOpts.ddAPPKey == "" {
			return fmt.Errorf("datadog APP key is required for uploading logs")
		}
	}

	return nil
}

func uploadZipProfiles(ctx context.Context, uploadID string, debugDirPath string) error {
	paths, err := expandPatterns([]string{path.Join(debugDirPath, zippedProfilePattern)})
	if err != nil {
		return err
	}

	pathsByNode := make(map[string][]string)
	for _, path := range paths {
		nodeID := filepath.Base(filepath.Dir(path))
		if _, ok := pathsByNode[nodeID]; !ok {
			pathsByNode[nodeID] = []string{}
		}

		pathsByNode[nodeID] = append(pathsByNode[nodeID], path)
	}

	retryOpts := base.DefaultRetryOptions()
	retryOpts.MaxRetries = zipUploadRetries
	var req *http.Request
	for nodeID, paths := range pathsByNode {
		for retry := retry.Start(retryOpts); retry.Next(); {
			req, err = newProfileUploadReq(
				ctx, paths, appendUserTags(
					append(
						defaultDDTags, makeDDTag(nodeIDTag, nodeID), makeDDTag(uploadIDTag, uploadID),
						makeDDTag(clusterTag, debugZipUploadOpts.clusterName),
					), // system generated tags
					debugZipUploadOpts.tags..., // user provided tags
				),
			)
			if err != nil {
				continue
			}

			if _, err = doUploadReq(req); err == nil {
				break
			}
		}

		if err != nil {
			return fmt.Errorf("failed to upload profiles of node %s: %w", nodeID, err)
		}

		fmt.Fprintf(os.Stderr, "Uploaded profiles of node %s to datadog (%s)\n", nodeID, strings.Join(paths, ", "))
		fmt.Fprintf(os.Stderr, "Explore the profiles on datadog: "+
			"https://{{ datadog domain }}/profiling/explorer?query=%s:%s\n", uploadIDTag, uploadID)
	}

	return nil
}

func newProfileUploadReq(
	ctx context.Context, profilePaths []string, tags []string,
) (*http.Request, error) {
	var (
		body  bytes.Buffer
		mw    = multipart.NewWriter(&body)
		now   = timeutil.Now()
		event = &profileUploadEvent{
			Version: profileVersion,
			Family:  profileFamily,
			Tags:    strings.Join(tags, ","),

			// Ideally, we should be calculating the start and end times based on the
			// timestamp encoded in the pprof file. But, datadog doesn't seem to
			// support uploading profiles that are older than a certain period. So, we
			// are using a 5-second window around the current time.
			Start: now.Add(time.Second * -5).Format(time.RFC3339Nano),
			End:   now.Format(time.RFC3339Nano),
		}
	)

	for _, profilePath := range profilePaths {
		fileName := filepath.Base(profilePath)
		event.Attachments = append(event.Attachments, fileName)

		f, err := mw.CreateFormFile(fileName, fileName)
		if err != nil {
			return nil, err
		}

		data, err := os.ReadFile(profilePath)
		if err != nil {
			return nil, err
		}

		if _, err := f.Write(data); err != nil {
			return nil, err
		}
	}

	f, err := mw.CreatePart(textproto.MIMEHeader{
		httputil.ContentDispositionHeader: []string{`form-data; name="event"; filename="event.json"`},
		httputil.ContentTypeHeader:        []string{httputil.JSONContentType},
	})
	if err != nil {
		return nil, err
	}

	if err := json.NewEncoder(f).Encode(event); err != nil {
		return nil, err
	}

	if err := mw.Close(); err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, makeDDURL(datadogProfileUploadURLTmpl), &body)
	if err != nil {
		return nil, err
	}

	req.Header.Set(httputil.ContentTypeHeader, mw.FormDataContentType())
	req.Header.Set(datadogAPIKeyHeader, debugZipUploadOpts.ddAPIKey)
	return req, nil
}

func processLogFile(
	uploadID, debugDirPath string, file fileInfo, uploadFn func(logUploadSig),
) (time.Time, time.Time, error) {
	var (
		pathParts                            = strings.Split(strings.TrimPrefix(file.path, debugDirPath), "/")
		inputEditMode                        = log.SelectEditMode(false /* redactable */, false /* redactInput */)
		nodeID                               = pathParts[2]
		fileName                             = path.Base(file.path)
		logBuffer                            = &bytes.Buffer{}
		localMinTimestamp, localMaxTimestamp = time.Time{}, time.Time{}
		prevTargetPath                       = ""
	)

	stream, err := newFileLogStream(
		file, time.Time(debugZipUploadOpts.from), time.Time(debugZipUploadOpts.to),
		inputEditMode, debugZipUploadOpts.logFormat,
	)
	if err != nil {
		return localMinTimestamp, localMaxTimestamp, err
	}

	for e, ok := stream.peek(); ok; e, ok = stream.peek() {
		currentTimestamp := timeutil.Unix(0, e.Time)
		if localMinTimestamp.IsZero() || currentTimestamp.Before(localMinTimestamp) {
			localMinTimestamp = currentTimestamp
		}
		localMaxTimestamp = currentTimestamp

		// The target path is constructed like this:
		// <cluster-name>/<upload-id>/dt=20210901/hour=15/<node_id>/<filename>
		currTargetPath := path.Join(
			debugZipUploadOpts.clusterName, uploadID,
			timeutil.Unix(0, e.Time).Format(gcsPathTimeFormat), nodeID, fileName,
		)

		if prevTargetPath != "" && prevTargetPath != currTargetPath {
			// we've found a new hour, so we need to send the logs of the
			// previous hour for upload
			uploadFn(logUploadSig{
				key:    prevTargetPath,
				nodeID: nodeID,
				data:   logBuffer.Bytes(),
			})

			logBuffer.Reset()
		}

		rawLine, err := logEntryToJSON(e, appendUserTags(
			append(
				defaultDDTags, makeDDTag(uploadIDTag, uploadID), makeDDTag(nodeIDTag, nodeID),
				makeDDTag(clusterTag, debugZipUploadOpts.clusterName),
			), // system generated tags
			debugZipUploadOpts.tags..., // user provided tags
		))
		if err != nil {
			fmt.Println(err)
			continue
		}

		_, err = logBuffer.Write(append(rawLine, []byte("\n")...))
		if err != nil {
			fmt.Println(err)
			continue
		}

		stream.pop()
		prevTargetPath = currTargetPath
	}

	// upload the remaining logs
	if logBuffer.Len() > 0 {
		uploadFn(logUploadSig{
			key:    prevTargetPath,
			data:   logBuffer.Bytes(),
			nodeID: nodeID,
		})
		logBuffer.Reset()
	}

	return localMinTimestamp, localMaxTimestamp, nil
}

func logReaderPool(
	size int, debugDirPath, uploadID string, uploadFn func(logUploadSig),
) (func() (time.Time, time.Time), error) {
	paths, err := expandPatterns([]string{path.Join(debugDirPath, zippedLogsPattern)})
	if err != nil {
		return nil, err
	}

	filePattern := regexp.MustCompile(logFilePattern)
	files, err := findLogFiles(
		paths, filePattern, nil, groupIndex(filePattern, "program"),
	)
	if err != nil {
		return nil, err
	}

	filesChan := make(chan fileInfo, len(files))
	wg := sync.WaitGroup{}
	for _, file := range files {
		filesChan <- file
	}
	wg.Add(len(files))

	logTimeRange := struct {
		syncutil.Mutex
		min, max time.Time
	}{}

	for i := 0; i < size; i++ {
		go func() {
			for file := range filesChan {
				fileMinTimestamp, fileMaxTimestamp, err := processLogFile(
					uploadID, debugDirPath, file, uploadFn,
				)
				if err != nil {
					fmt.Println("Failed to upload logs:", err)
				} else {
					if !fileMinTimestamp.IsZero() && !fileMaxTimestamp.IsZero() {
						// consolidate the min and max timestamps. This is done in an
						// anonymous function because the lock + update + unlock has to be
						// done atomically. The linter will complain if there are if conditions
						// in between the lock and unlock.
						func() {
							logTimeRange.Lock()
							defer logTimeRange.Unlock()

							if logTimeRange.min.IsZero() || fileMinTimestamp.Before(logTimeRange.min) {
								logTimeRange.min = fileMinTimestamp
							}

							if fileMaxTimestamp.After(logTimeRange.max) {
								logTimeRange.max = fileMaxTimestamp
							}
						}()
					}
				}

				wg.Done()
			}
		}()
	}

	wait := func() (time.Time, time.Time) {
		wg.Wait() // wait for all the reads to complete
		close(filesChan)
		return logTimeRange.min, logTimeRange.max
	}

	return wait, nil
}

func uploadZipLogs(ctx context.Context, uploadID string, debugDirPath string) error {
	var (
		// both the channels are buffered to keep the workers busy
		gcsWorkChan = make(chan logUploadSig, debugZipUploadOpts.maxConcurrentUploads*2)
		doneChan    = make(chan logUploadStatus, debugZipUploadOpts.maxConcurrentUploads*2)
		writerGroup = sync.WaitGroup{}
		totalSize   = 0
		nodeLookup  = make(map[string]struct{})
	)

	go func() {
		for sig := range doneChan {
			if _, ok := nodeLookup[sig.nodeID]; !ok {
				nodeLookup[sig.nodeID] = struct{}{}
				fmt.Fprintf(os.Stderr, "Uploading logs for node %s\n", sig.nodeID)
			}

			if sig.err != nil {
				fmt.Fprintln(os.Stderr, "error while uploading logs:", sig.err)
			} else {
				totalSize += sig.uploadSize
			}

			writerGroup.Done()
		}
	}()

	// queueForUpload is responsible for receiving the logs from the
	// logReaderPool and queuing them for upload. Currently, it just adds the
	// logs to the gcsWorkChan but this can be extended to add work to more than
	// on worker pool. In the near future, this will extend support to datadog
	// logs API
	queueForUpload := func(sig logUploadSig) {
		writerGroup.Add(1)
		gcsWorkChan <- sig
	}

	startGCSWriterPool(debugZipUploadOpts.maxConcurrentUploads, gcsWorkChan, doneChan)
	waitForReads, err := logReaderPool(
		debugZipUploadOpts.maxConcurrentUploads, debugDirPath, uploadID, queueForUpload,
	)
	if err != nil {
		return err
	}

	// block until all the logs queued for upload
	firstEventTime, lastEventTime := waitForReads()

	writerGroup.Wait()
	close(gcsWorkChan)
	close(doneChan)

	if totalSize != 0 {
		fmt.Fprintf(os.Stderr, "Upload complete! Total size: %s\n", humanReadableSize(totalSize))

		if err := setupDDArchive(
			ctx, path.Join(debugZipUploadOpts.clusterName, uploadID), uploadID,
		); err != nil {
			return errors.Wrap(err, "failed to setup datadog archive")
		}

		printRehydrationSteps(uploadID, uploadID, firstEventTime, lastEventTime)
	}

	return nil
}

type ddArchivePayload struct {
	Type       string              `json:"type"`
	Attributes ddArchiveAttributes `json:"attributes"`
}

type ddArchiveAttributes struct {
	Name        string               `json:"name"`
	Query       string               `json:"query"`
	Destination ddArchiveDestination `json:"destination"`
}

type ddArchiveDestination struct {
	Type        string               `json:"type"`
	Path        string               `json:"path"`
	Bucket      string               `json:"bucket"`
	Integration ddArchiveIntegration `json:"integration"`
}

type ddArchiveIntegration struct {
	ProjectID   string `json:"project_id"`
	ClientEmail string `json:"client_email"`
}

func setupDDArchive(ctx context.Context, pathPrefix, archiveName string) error {
	rawPayload, err := json.Marshal(struct {
		Data ddArchivePayload `json:"data"`
	}{
		Data: ddArchivePayload{
			Type: ddArchiveType,
			Attributes: ddArchiveAttributes{
				Name:  archiveName,
				Query: ddArchiveQuery,
				Destination: ddArchiveDestination{
					Type:   ddArchiveDestinationType,
					Bucket: ddArchiveBucketName,
					Path:   pathPrefix,
					Integration: ddArchiveIntegration{
						ProjectID: debugZipUploadOpts.gcpProjectID,
						ClientEmail: fmt.Sprintf(
							"%s@%s.iam.gserviceaccount.com",
							ddArchiveDefaultClient, debugZipUploadOpts.gcpProjectID,
						),
					},
				},
			},
		},
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(
		ctx, http.MethodPost, makeDDURL(datadogCreateArchiveURLTmpl), bytes.NewReader(rawPayload),
	)
	if err != nil {
		return err
	}

	req.Header.Set(httputil.ContentTypeHeader, httputil.JSONContentType)
	req.Header.Set(datadogAPIKeyHeader, debugZipUploadOpts.ddAPIKey)
	req.Header.Set(datadogAppKeyHeader, debugZipUploadOpts.ddAPPKey)

	if _, err := doUploadReq(req); err != nil {
		return fmt.Errorf("failed to create datadog archive: %w", err)
	}

	return nil
}

type logUploadSig struct {
	key    string
	nodeID string
	data   []byte
}

type logUploadStatus struct {
	err        error
	uploadSize int
	nodeID     string
}

// startGCSWriterPool creates a worker pool that can concurrently write the
// logs to GCS. This function only orchestrates the upload process. This pool
// is terminated when the workChan is closed
func startGCSWriterPool(size int, workChan <-chan logUploadSig, doneChan chan<- logUploadStatus) {
	for i := 0; i < size; i++ {
		go func() {
			for sig := range workChan {
				doneChan <- logUploadStatus{
					err:        writeLogsToGCS(context.Background(), sig),
					uploadSize: len(sig.data),
					nodeID:     sig.nodeID,
				}
			}
		}()
	}
}

// writeLogsToGCS is a function that writes the logs to GCS.
// The key in the gcsWorkerSig is the target path where the logs should be
// uploaded.
//
//	Example: "<cluster-name>/<upload-id>/dt=20210901/hour=15/<node_id>/<filename>"
//
// Each path will be uploaded as a separate file. The final file name will be
// randomly generated just be for uploading. This function only does the actual
// writing to GCS. The concurrency has to be handled by the caller.
var writeLogsToGCS = func(ctx context.Context, sig logUploadSig) error {
	gcsClient, closeGCSClient, err := newGCSClient(ctx)
	if err != nil {
		return err
	}
	defer closeGCSClient()

	filename := path.Join(sig.key, fmt.Sprintf(
		"archive_%s_%s_%s.json.gz",
		newRandStr(6, true /* numericOnly */), newRandStr(4, true), newRandStr(22, false),
	))

	retryOpts := base.DefaultRetryOptions()
	retryOpts.MaxRetries = zipUploadRetries

	for retry := retry.Start(retryOpts); retry.Next(); {
		objectWriter := gcsClient.Bucket(ddArchiveBucketName).Object(filename).NewWriter(ctx)
		w := gzip.NewWriter(objectWriter)
		_, err = w.Write(sig.data)
		if err != nil {
			continue
		}

		if err = w.Close(); err != nil {
			continue
		}

		if err = objectWriter.Close(); err != nil {
			continue
		}

		// if there was no error, we can break out of this loop
		break
	}

	return err
}

func newGCSClient(ctx context.Context) (*storage.Client, func(), error) {
	tokenSource, err := google.DefaultTokenSource(ctx)
	if err != nil {
		return nil, nil, err
	}

	gcsClient, err := storage.NewClient(ctx, option.WithTokenSource(tokenSource))
	if err != nil {
		return nil, nil, err
	}

	return gcsClient, func() {
		// return a function that already handles the closing error
		if err := gcsClient.Close(); err != nil {
			fmt.Println(err)
		}
	}, nil
}

type ddLogEntry struct {
	logpb.Entry

	Date      string `json:"date"`
	Timestamp int64  `json:"timestamp"`
	Channel   string `json:"channel"`
	Severity  string `json:"severity"`

	// fields to be omitted
	Message any    `json:"message,omitempty"`
	Time    string `json:"time,omitempty"`
	Tags    string `json:"tags,omitempty"`
}

// logEntryToJSON converts a logpb.Entry to a JSON byte slice and also
// transform a few fields to use the correct types. The JSON format is based on
// the specification provided by datadog.
// Refer: https://gist.github.com/ckelner/edc0e4efe4fa110f6b6b61f69d580171
func logEntryToJSON(e logpb.Entry, tags []string) ([]byte, error) {
	var message any = e.Message
	if strings.HasPrefix(e.Message, "{") {
		// If the message is already a JSON object, we don't want to escape it
		// by wrapping it in quotes. Instead, we want to include it as a nested
		// object in the final JSON output. So, we can override the Message field
		// with the json.RawMessage instead of string. This will prevent the
		// message from being escaped.
		message = json.RawMessage(e.Message)
	}

	date := timeutil.Unix(0, e.Time).Format(time.RFC3339)
	timestamp := e.Time / 1e9

	return json.Marshal(struct {
		// override the following fields in the embedded logpb.Entry struct
		Timestamp  int64      `json:"timestamp"`
		Date       string     `json:"date"`
		Message    any        `json:"message"`
		Tags       []string   `json:"tags"`
		ID         string     `json:"_id"`
		Attributes ddLogEntry `json:"attributes"`
	}{
		Timestamp: timestamp,
		Date:      date,
		Message:   message,
		Tags:      tags,
		ID:        newRandStr(24, false /* numericOnly */),
		Attributes: ddLogEntry{
			Entry:     e,
			Date:      date,
			Timestamp: timestamp,
			Channel:   e.Channel.String(),
			Severity:  e.Severity.String(),

			// remove the below fields via the omitempty tag
			Time: "",
			Tags: "",
		},
	})
}

// appendUserTags will make sure there are no duplicates in the final list of tags.
// In case of duplicates, the user provided tags will take precedence.
func appendUserTags(systemTags []string, tags ...string) []string {
	tagsMap := make(map[string]string)
	for _, tag := range systemTags {
		split := strings.Split(tag, ":")
		if len(split) != 2 {
			tagsMap[tag] = ""
			continue
		}

		tagsMap[split[0]] = split[1]
	}

	for _, tag := range tags {
		split := strings.Split(tag, ":")
		if len(split) != 2 {
			tagsMap[tag] = ""
			continue
		}

		tagsMap[split[0]] = split[1]
	}

	var finalList []string
	for key, value := range tagsMap {
		if value == "" {
			finalList = append(finalList, key)
			continue
		}

		finalList = append(finalList, fmt.Sprintf("%s:%s", key, value))
	}

	sort.Strings(finalList)
	return finalList
}

// makeDDTag is a simple convenience function to make a tag string in the key:value format.
// This is just to make the code more readable.
func makeDDTag(key, value string) string {
	return fmt.Sprintf("%s:%s", key, value)
}

// doUploadReq is a variable that holds the function that makes the actual HTTP request.
// There is also some error handling logic in this function. This is a variable so that
// we can mock this function in the tests.
var doUploadReq = func(req *http.Request) ([]byte, error) {
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err := resp.Body.Close(); err != nil {
			fmt.Println("failed to close response body:", err)
		}
	}()

	rawBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// treat all non-2xx status codes as errors
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("status code: %s, err message: %s", resp.Status, string(rawBody))
	}

	return rawBody, nil
}

// a wrapper around timestamp to make the tests more deterministic.
// Everything is converted to lowercase and spaces are replaced with hyphens. Because,
// datadog will do this anyway and we want to make sure the UUIDs match when we generate the
// explore/dashboard links.
var newUploadID = func(cluster string) string {
	currentTime := timeutil.Now()
	formattedTime := currentTime.Format("20060102150405")
	return strings.ToLower(
		strings.ReplaceAll(
			fmt.Sprintf("%s-%s", cluster, formattedTime), " ", "-",
		),
	)
}

// newRandStr generates a random alphanumeric string of the given length. This is used
// for the _id field in the log entries and for the name of the archives
var newRandStr = func(length int, numericOnly bool) string {
	charSet := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	if numericOnly {
		charSet = "0123456789"
	}

	r := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	b := make([]byte, length)
	for i := range b {
		b[i] = charSet[r.Intn(len(charSet))]
	}
	return string(b)
}

func printRehydrationSteps(uploadID, archiveName string, from, to time.Time) {
	msg := `
The logs have been added to an archive and are ready for rehydration (ingestion). This has to be
triggered manually for now. This will be automated as soon as the datadog API supports it.

Follow these steps to trigger rehydration:

  1. Open this link in your browser: https://us5.datadoghq.com/logs/pipelines/historical-views/add
  2. In "Select Time Range" section, select the time range from "%s" to "%s" or a subset of it
  3. In "Select Archive" section, select the archive "%s"
  4. In "Name Historical Index", enter the name "%s"
  5. Click on "Rehydrate From Archive"

You will receive an email notification once the rehydration is complete.
`

	// Month data year
	timeFormat := "Jan 2 2006"
	from = from.Truncate(time.Hour)            // round down to the nearest hour
	to = to.Add(time.Hour).Truncate(time.Hour) // round up to the nearest hour
	fmt.Fprintf(
		os.Stderr, msg, from.Format(timeFormat), to.Format(timeFormat), archiveName, uploadID,
	)
}

// makeDDURL constructe the final datadog URL by replacing the site
// placeholder in the template. This is a simple convenience
// function. It assumes that the site is valid. This assumption is
// fine because we are validating the site early on in the flow.
func makeDDURL(tmpl string) string {
	return fmt.Sprintf(tmpl, ddSiteToHostMap[debugZipUploadOpts.ddSite])
}

// humanReadableSize converts the given number of bytes to a human readable
// format. Lowest unit is bytes and the highest unit is petabytes.
func humanReadableSize(bytes int) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
