// Copyright 2024 The Cockroach Authors.
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
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/system"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/spf13/cobra"
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
	datadogAPIKeyHeader = "DD-API-KEY"

	// the path pattern to search for specific artifacts in the debug
	// zip directory
	zippedProfilePattern = "nodes/*/*.pprof"
	zippedLogsPattern    = "nodes/*/logs/*"

	profileFamily = "go"
	profileFormat = "pprof"

	// this is not the pprof version, but the version of the profile
	// upload format supported by datadog
	profileVersion = "4"

	// names of mandatory tag
	nodeIDTag   = "node_id"
	uploadIDTag = "upload_id"
	clusterTag  = "cluster"

	// datadog URL templates
	datadogProfileUploadURL = "https://intake.profile.%s/v1/input"
	datadogLogUploadURL     = "https://http-intake.logs.%s/api/v2/logs"

	maxLinesPerChunk = 1000
)

var debugZipUploadOpts = struct {
	include              []string
	ddAPIKey             string
	ddSite               string
	clusterName          string
	tags                 []string
	from, to             timestampValue
	logFormat            string
	isDryRun             bool
	maxConcurrentUploads int
}{
	maxConcurrentUploads: system.NumCPU(),
}

// This is the list of all supported artifact types. The "possible values" part
// in the help text is generated from this list. So, make sure to keep this updated
var zipArtifactTypes = []string{"profiles", "logs"}

// uploadZipArtifactFuncs is a registry of handler functions for each artifact type.
// While adding/removing functions from here, make sure to update
// the zipArtifactTypes list as well
var uploadZipArtifactFuncs = map[string]uploadZipArtifactFunc{
	"profiles": uploadZipProfiles,
	"logs":     uploadZipLogs,
}

// default datadog tags
var defaultDDTags = []string{"service:CRDB-SH", "env:debug"}

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

	// run the upload functions
	// TODO(arjunmahishi): Make this concurrent once there are multiple artifacts to upload
	for _, artType := range artifactsToUpload {
		if err := uploadZipArtifactFuncs[artType](cmd.Context(), uploadID, args[0]); err != nil {
			return err
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
			return fmt.Errorf(
				"the list of artifacts to upload has duplicates: [%s]", strings.Join(artifactsToUpload, ", "),
			)
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

	// validate the log format if logs are to be uploaded
	_, ok := log.FormatParsers[debugZipUploadOpts.logFormat]
	_, shouldUploadLogs := includeLookup["logs"]
	if shouldUploadLogs && !ok {
		return fmt.Errorf("unsupported log format '%s'", debugZipUploadOpts.logFormat)
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

	for nodeID, paths := range pathsByNode {
		req, err := newProfileUploadReq(
			ctx, paths, appendUserTags(
				append(
					defaultDDTags, makeDDTag(nodeIDTag, nodeID), makeDDTag(uploadIDTag, uploadID),
					makeDDTag(clusterTag, debugZipUploadOpts.clusterName),
				), // system generated tags
				debugZipUploadOpts.tags..., // user provided tags
			),
		)
		if err != nil {
			return err
		}

		resp, err := doUploadReq(req)
		if err != nil {
			return err
		}
		defer func() {
			if err := resp.Body.Close(); err != nil {
				fmt.Println("failed to close response body:", err)
			}
		}()

		if resp.StatusCode != http.StatusOK {
			errMsg := fmt.Sprintf(
				"Failed to upload profiles of node %s to datadog (%s)",
				nodeID, (strings.Join(paths, ", ")),
			)
			if resp.Body != nil {
				body, err := io.ReadAll(resp.Body)
				if err != nil {
					return err
				}

				return fmt.Errorf("%s: %s", errMsg, string(body))
			}

			return fmt.Errorf("%s: %s", errMsg, resp.Status)
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

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, makeDDURL(datadogProfileUploadURL), &body)
	if err != nil {
		return nil, err
	}

	req.Header.Set(httputil.ContentTypeHeader, mw.FormDataContentType())
	req.Header.Set(datadogAPIKeyHeader, debugZipUploadOpts.ddAPIKey)
	return req, nil
}

func uploadZipLogs(ctx context.Context, uuid string, debugDirPath string) error {
	paths, err := expandPatterns([]string{path.Join(debugDirPath, zippedLogsPattern)})
	if err != nil {
		return err
	}

	filePattern := regexp.MustCompile(logFilePattern)
	files, err := findLogFiles(
		paths, filePattern, nil, groupIndex(filePattern, "program"),
	)
	if err != nil {
		return err
	}

	var reqQueue []*http.Request
	for _, file := range files {
		pathParts := strings.Split(strings.TrimPrefix(file.path, debugDirPath), "/")
		inputEditMode := log.SelectEditMode(false /* redactable */, false /* redactInput */)
		stream, err := newFileLogStream(
			file, time.Time(debugZipUploadOpts.from), time.Time(debugZipUploadOpts.to),
			inputEditMode, debugZipUploadOpts.logFormat,
		)
		if err != nil {
			return err
		}

		var chunk [][]byte
		for e, ok := stream.peek(); ok; e, ok = stream.peek() {
			rawLine, err := logEntryToJSON(e, appendUserTags(
				append(
					defaultDDTags, makeDDTag(uploadIDTag, uuid), makeDDTag(nodeIDTag, pathParts[2]),
					makeDDTag(clusterTag, debugZipUploadOpts.clusterName),
				), // system generated tags
				debugZipUploadOpts.tags..., // user provided tags
			))
			if err != nil {
				return err
			}

			chunk = append(chunk, rawLine)
			if len(chunk) == maxLinesPerChunk {
				req, err := newLogUploadReq(ctx, chunk)
				if err != nil {
					return err
				}

				reqQueue = append(reqQueue, req)
				chunk = nil
			}

			stream.pop()
		}

		// upload the remaining lines if any
		if len(chunk) > 0 {
			req, err := newLogUploadReq(ctx, chunk)
			if err != nil {
				return err
			}
			reqQueue = append(reqQueue, req)
		}

		if err := stream.error(); err != nil {
			if err.Error() == "EOF" {
				continue
			}
			return err
		}
	}

	if len(reqQueue) == 0 {
		return nil
	}

	// now that the requests to be made are ready, we can fan out the work across
	// multiple go routines. The number of go routines is capped at the number of
	// CPUs. It is possible that the number of requests are less than the number
	// of CPUs. In that case, we will use the number of requests as the number of
	// workers (one request per worker).
	numOfWorkers := min(debugZipUploadOpts.maxConcurrentUploads, len(reqQueue))
	reqChan := make(chan *http.Request, len(reqQueue))
	errChan := make(chan error, len(reqQueue))
	for _, req := range reqQueue {
		reqChan <- req
	}

	for i := 0; i < numOfWorkers; i++ {
		go func() {
			for req := range reqChan {
				resp, err := doUploadReq(req)
				if err != nil {
					errChan <- err
					continue
				}

				if resp.StatusCode/100 != 2 {
					body := []byte{}
					if resp.Body != nil {
						body, err = io.ReadAll(resp.Body)
						if err != nil {
							errChan <- err
							continue
						}

						if err := resp.Body.Close(); err != nil {
							errChan <- err
							continue
						}
					}

					errChan <- fmt.Errorf("failed to send logs: %s", string(body))
					continue
				}

				errChan <- nil
			}
		}()
	}

	// wait for all the requests to finish
	for i := 0; i < len(reqQueue); i++ {
		if err := <-errChan; err != nil {
			// kill all the go routines by closing the channels and return the error
			close(reqChan)
			close(errChan)
			return err
		}

		fmt.Fprintf(os.Stderr, "Log upload progress: %.2f%%\n", (float32(i+1)/float32(len(reqQueue)))*100)
	}

	close(reqChan)
	close(errChan)

	fmt.Fprintf(os.Stderr, "Uploaded logs to datadog. Explore them here:")
	fmt.Fprintf(os.Stderr, "https://{{ datadog domain }}/logs?query=%s:%s\n", uploadIDTag, uuid)
	return nil
}

func newLogUploadReq(ctx context.Context, logLines [][]byte) (*http.Request, error) {
	var (
		compressedLogs       bytes.Buffer
		compressedLogsWriter = gzip.NewWriter(&compressedLogs)
	)

	if _, err := compressedLogsWriter.Write([]byte("[")); err != nil {
		return nil, err
	}

	if _, err := compressedLogsWriter.Write(bytes.Join(logLines, []byte(",\n"))); err != nil {
		return nil, err
	}

	if _, err := compressedLogsWriter.Write([]byte("]")); err != nil {
		return nil, err
	}

	if err := compressedLogsWriter.Close(); err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(
		ctx, http.MethodPost, makeDDURL(datadogLogUploadURL), &compressedLogs,
	)
	if err != nil {
		return nil, err
	}

	req.Header.Set(httputil.ContentTypeHeader, httputil.JSONContentType)
	req.Header.Set(httputil.ContentEncodingHeader, httputil.GzipEncoding)
	req.Header.Set(datadogAPIKeyHeader, debugZipUploadOpts.ddAPIKey)
	return req, nil
}

// logEntryToJSON converts a logpb.Entry to a JSON byte slice and also
// transform a few fields to use the correct types
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

	return json.Marshal(struct {
		logpb.Entry

		// override the following fields in the embedded logpb.Entry struct
		Timestamp int64  `json:"time"`
		Channel   string `json:"channel"`
		Severity  string `json:"severity"`
		Message   any    `json:"message"`
		Tags      string `json:"ddtags,omitempty"`
	}{
		Entry:     e,
		Timestamp: e.Time / 1e9,
		Channel:   e.Channel.String(),
		Severity:  e.Severity.String(),
		Message:   message,
		Tags:      strings.Join(tags, ","),
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

// doUploadReq is a variable that holds the function that literally just sends the request.
// This is useful to mock the datadog API's response in tests.
var doUploadReq = func(req *http.Request) (*http.Response, error) {
	if debugZipUploadOpts.isDryRun {
		return dryRunRequest(req)
	}

	return http.DefaultClient.Do(req)
}

func dryRunRequest(req *http.Request) (*http.Response, error) {
	switch req.URL.Path {
	default:
		return &http.Response{StatusCode: http.StatusAccepted}, nil
	case "/api/v2/logs":
		// dump the request to stdout and return a 202 Accepted response
		gReader, err := gzip.NewReader(req.Body)
		if err != nil {
			return nil, err
		}

		var body bytes.Buffer
		if _, err := body.ReadFrom(gReader); err != nil {
			return nil, err
		}

		fmt.Println("==== BEGIN REQUEST BODY ====")
		fmt.Println(body.String())
		fmt.Println("==== END REQUEST BODY ====")

		return &http.Response{StatusCode: http.StatusAccepted}, nil
	}
}

// a wrapper around uuid.MakeV4().String() to make the tests more deterministic.
// Everything is converted to lowercase and spaces are replaced with hyphens. Because,
// datadog will do this anyway and we want to make sure the UUIDs match when we generate the
// explore/dashboard links.
var newUploadID = func(cluster string) string {
	return strings.ToLower(
		strings.ReplaceAll(
			fmt.Sprintf("%s-%s", cluster, uuid.NewV4().Short()), " ", "-",
		),
	)
}

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
)

// makeDDURL constructe the final datadog URL by replacing the site
// placeholder in the template. This is a simple convenience
// function. It assumes that the site is valid. This assumption is
// fine because we are validating the site early on in the flow.
func makeDDURL(tmpl string) string {
	return fmt.Sprintf(tmpl, ddSiteToHostMap[debugZipUploadOpts.ddSite])
}
