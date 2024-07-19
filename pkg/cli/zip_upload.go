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
	// zip director
	zippedProfilePattern = "nodes/*/*.pprof"
	zippedLogsPattern    = "nodes/*/logs/*"

	profileFamily = "go"
	profileFormat = "pprof"

	// this is not the pprof version, but the version of the profile
	// upload format supported by datadog
	profileVersion = "4"

	// names of mandatory tag
	nodeIDTag  = "node_id"
	uuidTag    = "uuid"
	clusterTag = "cluster"

	// datadog URL templates
	datadogProfileUploadURL = "https://intake.profile.%s/v1/input"
	datadogLogUploadURL     = "https://http-intake.logs.%s/api/v2/logs"

	logLinePerChunk = 100
)

var debugZipUploadOpts = struct {
	include     []string
	ddAPIKey    string
	ddSite      string
	clusterName string
	tags        []string
	from, to    timestampValue
	logFormat   string
}{}

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

// default tags
var ddProfileTags = []string{"service:CRDB-SH", "env:debug"}

func runDebugZipUpload(cmd *cobra.Command, args []string) error {
	if err := validateZipUploadReadiness(); err != nil {
		return err
	}

	// a unique ID for this upload session. This should be used to tag
	// all the artifacts uploaded in this session
	uploadID := newUploadID()

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
		includeLookup     = map[string]bool{}
		artifactsToUpload = zipArtifactTypes
	)

	if len(debugZipUploadOpts.include) > 0 {
		artifactsToUpload = debugZipUploadOpts.include
	}
	for _, inc := range artifactsToUpload {
		includeLookup[inc] = true
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
	if _, ok := log.FormatParsers[debugZipUploadOpts.logFormat]; !ok && includeLookup["logs"] {
		return fmt.Errorf("unsupported log format '%s'", debugZipUploadOpts.logFormat)
	}

	return nil
}

func uploadZipProfiles(ctx context.Context, uuid string, debugDirPath string) error {
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
					ddProfileTags, makeDDTag(nodeIDTag, nodeID), makeDDTag(uuidTag, uuid),
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

		fmt.Printf("Uploaded profiles of node %s to datadog (%s)\n", nodeID, strings.Join(paths, ", "))
		fmt.Printf("Explore this profile on datadog: "+
			"https://{{ datadog domain }}/profiling/explorer?query=uuid:%s\n", uuid)
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

	upload := func(logLines [][]byte) error {
		req, err := newLogUploadReq(ctx, logLines)
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

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		if resp.StatusCode != http.StatusAccepted {
			return fmt.Errorf("failed to send logs, status code: %d, response: %s", resp.StatusCode, string(body))
		}

		return nil
	}

	for _, file := range files {
		inputEditMode := log.SelectEditMode(false /* redactable */, false /* redactInput */)
		stream, err := newFileLogStream(
			file, time.Time(debugZipUploadOpts.from), time.Time(debugZipUploadOpts.to),
			inputEditMode, debugZipUploadOpts.logFormat,
		)
		if err != nil {
			return err
		}

		chunk := [][]byte{}
		for e, ok := stream.peek(); ok; e, ok = stream.peek() {
			rawLine, err := logEntryToJSON(e)
			if err != nil {
				return err
			}

			chunk = append(chunk, rawLine)
			if len(chunk) == logLinePerChunk {
				if err := upload(chunk); err != nil {
					return err
				}

				// fmt.Println(string(bytes.Join(chunk, []byte(",\n"))))
				chunk = nil
			}

			stream.pop()
		}

		// upload the remaining lines if any
		if len(chunk) > 0 {
			if err := upload(chunk); err != nil {
				return err
			}

			// fmt.Println(string(bytes.Join(chunk, []byte(",\n"))))
		}

		if err := stream.error(); err != nil {
			if err.Error() == "EOF" {
				return nil
			}
			return err
		}
	}

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
// convert the 'time' field from nanoseconds to seconds
func logEntryToJSON(e logpb.Entry) ([]byte, error) {
	if strings.HasPrefix(e.Message, "{") {
		// If the message is already a JSON object, we don't want to escape it
		// by wrapping it in quotes. Instead, we want to include it as a nested
		// object in the final JSON output. So, we can override the Message field
		// with the json.RawMessage instead of string. This will prevent the
		// message from being escaped.
		return json.Marshal(struct {
			Timestamp int64           `json:"time"`
			Channel   string          `json:"channel"`
			Message   json.RawMessage `json:"message,omitempty"`
			logpb.Entry
		}{
			Timestamp: e.Time / 1e9,       // convert nanoseconds to seconds
			Channel:   e.Channel.String(), // use the string representation of the channel
			Message:   json.RawMessage(e.Message),
			Entry:     e,
		})
	}

	return json.Marshal(struct {
		Timestamp int64  `json:"time"`
		Channel   string `json:"channel"`
		logpb.Entry
	}{
		Timestamp: e.Time / 1e9,       // convert nanoseconds to seconds
		Channel:   e.Channel.String(), // use the string representation of the channel
		Entry:     e,
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
	return http.DefaultClient.Do(req)
}

// a wrapper around uuid.MakeV4().String() to make the tests more deterministic.
// Everything is converted to lowercase and spaces are replaced with hyphens. Because,
// datadog will do this anyway and we want to make sure the UUIDs match when we generate the
// explore/dashboard links.
var newUploadID = func() string {
	return strings.ToLower(
		strings.ReplaceAll(
			fmt.Sprintf("%s-%s", debugZipUploadOpts.clusterName, uuid.NewV4().Short()), " ", "-",
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
