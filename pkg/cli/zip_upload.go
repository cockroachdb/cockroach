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
	"time"

	"cloud.google.com/go/storage"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/system"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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
	datadogCreateArchiveURLTmpl = "https://api.%s/api/v2/logs/config/archives"

	// datadog archive attributes
	ddArchiveType            = "archives"
	ddArchiveDestinationType = "gcs"
	ddArchiveQuery           = "-*" // will make sure to not archive any live logs
	ddArchiveBucketName      = "test-log-upload-arj"
	ddArchiveDefaultClient   = "datadog-archive" // TODO(arjunmahishi): make this a flag also
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
	reporter             *zipUploadReporter
}{
	maxConcurrentUploads: system.NumCPU(),
	reporter:             newReporter(os.Stderr),
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

		if resp.StatusCode/100 != 2 {
			errMsg := fmt.Sprintf(
				"Failed to upload profiles of node %s to datadog (%s)",
				nodeID, (strings.Join(paths, ", ")),
			)

			if resp.Body != nil {
				defer func() {
					if err := resp.Body.Close(); err != nil {
						fmt.Println("failed to close response body:", err)
					}
				}()

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

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, makeDDURL(datadogProfileUploadURLTmpl), &body)
	if err != nil {
		return nil, err
	}

	req.Header.Set(httputil.ContentTypeHeader, mw.FormDataContentType())
	req.Header.Set(datadogAPIKeyHeader, debugZipUploadOpts.ddAPIKey)
	return req, nil
}

func uploadZipLogs(ctx context.Context, uploadID string, debugDirPath string) error {
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

	// chunkMap holds the mapping of target path names to the chunks of log lines
	chunkMap := make(map[string][][]byte)
	firstEventTime, lastEventTime := time.Time{}, time.Time{}
	gcsPathPrefix := path.Join(debugZipUploadOpts.clusterName, uploadID)
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

		for e, ok := stream.peek(); ok; e, ok = stream.peek() {
			if firstEventTime.IsZero() {
				firstEventTime = timeutil.Unix(0, e.Time) // from the first log entry
			}
			lastEventTime = timeutil.Unix(0, e.Time) // from the last log entry

			// The target path is constructed like this: <cluster-name>/<upload-id>/dt=20210901/hour=15
			targetPath := path.Join(
				gcsPathPrefix, timeutil.Unix(0, e.Time).Format("dt=20060102/hour=15"),
			)
			rawLine, err := logEntryToJSON(e, appendUserTags(
				append(
					defaultDDTags, makeDDTag(uploadIDTag, uploadID), makeDDTag(nodeIDTag, pathParts[2]),
					makeDDTag(clusterTag, debugZipUploadOpts.clusterName),
				), // system generated tags
				debugZipUploadOpts.tags..., // user provided tags
			))
			if err != nil {
				return err
			}

			if _, ok := chunkMap[targetPath]; !ok {
				chunkMap[targetPath] = [][]byte{}
			}

			// TODO(arjunmahishi): Can this map hold all the data? We might be able
			// to start the upload here itself. So that we don't have to keep
			// everything in memory. But since we are running this on our mac books
			// for now, we can afford to keep everything in memory.
			chunkMap[targetPath] = append(chunkMap[targetPath], rawLine)
			stream.pop()
		}

		if err := stream.error(); err != nil {
			if err.Error() == "EOF" {
				continue
			}
			return err
		}
	}

	if err := writeLogsToGCS(ctx, chunkMap); err != nil {
		return err
	}

	if err := setupDDArchive(ctx, gcsPathPrefix, uploadID); err != nil {
		return errors.Wrap(err, "failed to setup datadog archive")
	}

	printRehydrationSteps(uploadID, uploadID, firstEventTime, lastEventTime)
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

	resp, err := doUploadReq(req)
	if err != nil {
		return err
	}

	if resp.StatusCode/100 != 2 {
		if resp.Body != nil {
			defer func() {
				if err := resp.Body.Close(); err != nil {
					fmt.Println("failed to close response body:", err)
				}
			}()

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				return err
			}

			return fmt.Errorf("status code: %s, body: %s", resp.Status, string(body))
		}
	}

	return nil
}

type gcsWorkerSig struct {
	key  string
	data []byte
}

// writeLogsToGCS is a function that concurrently writes the logs to GCS.
// The chunkMap is expected to be a map of time-base paths to luist of log lines.
//
//	Example: {
//	  "dt=20210901/hour=15": [[]byte, []byte, ...],
//	}
//
// Each path will be uploaded as a separate file. The final file name will be
// randomly generated just be for uploading.
var writeLogsToGCS = func(ctx context.Context, chunkMap map[string][][]byte) error {
	gcsClient, closeGCSClient, err := newGCSClient(ctx)
	if err != nil {
		return err
	}
	defer closeGCSClient()

	// The concurrency can be easily managed with something like sync/errors.Group
	// But using channels gives us two advantages:
	// 1. We can fail fast i.e exit as soon as one upload fails
	// 2. We can monitor the progress as the uploads happen and report it to the
	// CLI user (this seem like a non-feature but this is super useful when the
	// upload times are long)
	workChan := make(chan gcsWorkerSig, len(chunkMap))
	doneChan := make(chan error, len(chunkMap))
	for key, lines := range chunkMap {
		workChan <- gcsWorkerSig{key, bytes.Join(lines, []byte("\n"))}
	}

	noOfWorkers := min(system.NumCPU(), len(chunkMap))
	for i := 0; i < noOfWorkers; i++ {
		go func() {
			for sig := range workChan {
				filename := path.Join(sig.key, fmt.Sprintf(
					"archive_%s_%s_%s.json.gz",
					newRandStr(6, true /* numericOnly */), newRandStr(4, true), newRandStr(22, false),
				))

				objectWriter := gcsClient.Bucket(ddArchiveBucketName).Object(filename).NewWriter(ctx)
				w := gzip.NewWriter(objectWriter)
				_, err := w.Write(sig.data)
				if err != nil {
					doneChan <- err
					return
				}

				if err := w.Close(); err != nil {
					doneChan <- err
					return
				}

				if err := objectWriter.Close(); err != nil {
					doneChan <- err
					return
				}

				doneChan <- nil
			}
		}()
	}

	report := newReporter(os.Stderr).newReport("logs")
	doneCount := 0.0
	for i := 0; i < len(chunkMap); i++ {
		if err := <-doneChan; err != nil {
			// stop everything and return the error
			close(workChan)
			close(doneChan)
			return err
		}

		doneCount++
		report((doneCount / float64(len(chunkMap))) * 100)
	}

	close(workChan)
	close(doneChan)
	return nil
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

// doUploadReq is a variable that holds the function that literally just sends the request.
// This is useful to mock the datadog API's response in tests.
var doUploadReq = func(req *http.Request) (*http.Response, error) {
	return http.DefaultClient.Do(req)
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

// zipUploadReporter is a simple concurrency-safe logger that can be used to
// report the progress on the upload of each artifact type in the debug zip.
// The log printed by this is updated in-place as the progress changes.
// Usage pattern:
//
//	reporter := newReporter(os.Stderr)
//	report := reporter.newReport("profiles")
//	report(50) // 50% progress
//	report(75) // 50% progress
//	report(100) // 100% progress
type zipUploadReporter struct {
	syncutil.RWMutex
	reports   map[string]float64
	output    string
	logWriter io.Writer
}

func (r *zipUploadReporter) print() {
	r.RLock()
	defer r.RUnlock()

	// move the cursor to the top
	currentLines := strings.Count(r.output, "\n")
	if currentLines > 0 {
		fmt.Fprintf(r.logWriter, "\033[%dA", currentLines)
	}

	reports := []string{}
	for name := range r.reports {
		reports = append(reports, name)
	}
	sort.Strings(reports)

	var outputBuilder strings.Builder
	for _, name := range reports {
		progress := r.reports[name]
		outputBuilder.WriteString(fmt.Sprintf("%s upload progress: %.2f%%\n", name, progress))
	}

	r.output = outputBuilder.String()
	fmt.Fprint(r.logWriter, r.output)
}

func (r *zipUploadReporter) newReport(name string) func(float64) {
	r.Lock()
	defer r.print()
	defer r.Unlock()

	r.reports[name] = 0
	return func(progress float64) {
		r.Lock()
		defer r.print()
		defer r.Unlock()

		r.reports[name] = progress
	}
}

func newReporter(logWriter io.Writer) *zipUploadReporter {
	return &zipUploadReporter{
		reports:   make(map[string]float64),
		logWriter: logWriter,
	}
}
