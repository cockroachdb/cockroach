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
	"bytes"
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
	zippedLogsPattern    = "nodes/*/logs/*"
	zippedProfilePattern = "nodes/*/*.pprof"
	profileFamily        = "go"
	profileFormat        = "pprof"

	// this is not the pprof version, but the version of the profile upload format supported by datadog
	profileVersion = "4"
)

var debugZipUploadOpts = struct {
	from, to           time.Time
	include            []string
	logFilePattern     *regexp.Regexp
	format             string
	ddAPIKey           string
	ddProfileUploadURL string
	customerName       string
	tags               []string
}{
	logFilePattern:     regexp.MustCompile(logFilePattern),
	ddProfileUploadURL: "https://intake.profile.datadoghq.com/v1/input",
}

// This is the list of all supported artifact types. The "possible values" part
// in the help text is generated from this list. So, make sure to keep this updated
var zipArtifactTypes = []string{"profiles"}

// uploadZipArtifactFuncs is a registry of handler functions for each artifact type.
// While adding/removing functions from here, make sure to update
// the zipArtifactTypes list as well
var uploadZipArtifactFuncs = map[string]uploadZipArtifactFunc{
	"profiles": uploadZipProfiles,
}

var ddProfileTags = []string{"service:CRDB-SH", "env:debug"}

func runDebugZipUpload(cmd *cobra.Command, args []string) error {
	if err := validateZipUploadReadiness(); err != nil {
		return err
	}

	// a unique ID for this upload session. This should be used to tag all the artifacts uploaded in this session
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
	if debugZipUploadOpts.ddAPIKey == "" {
		return fmt.Errorf("datadog API key is required for uploading profiles")
	}

	if debugZipUploadOpts.customerName == "" {
		return fmt.Errorf("customer name is required for uploading profiles")
	}

	// validate the artifact types provided and fail early if any of them are not supported
	for _, artType := range debugZipUploadOpts.include {
		if _, ok := uploadZipArtifactFuncs[artType]; !ok {
			return fmt.Errorf("unsupported artifact type '%s'", artType)
		}
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
				append(ddProfileTags, "nodeID:"+nodeID, "uuid:"+uuid, "customer:"+debugZipUploadOpts.customerName),
				debugZipUploadOpts.tags...,
			),
		)
		if err != nil {
			return err
		}

		resp, err := doUploadProfileReq(req)
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
		"Content-Disposition": []string{`form-data; name="event"; filename="event.json"`},
		"Content-Type":        []string{"application/json"},
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

	req, err := http.NewRequestWithContext(ctx, "POST", debugZipUploadOpts.ddProfileUploadURL, &body)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("DD-API-KEY", debugZipUploadOpts.ddAPIKey)
	return req, nil
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

// doUploadProfileReq is a variable that holds the function that literally just sends the request.
// This is useful to mock the datadog API's response in tests.
var doUploadProfileReq = func(req *http.Request) (*http.Response, error) {
	return http.DefaultClient.Do(req)
}

// a wrapper around uuid.MakeV4().String() to make the tests more deterministic
var newUploadID = func() string {
	return fmt.Sprintf("%s-%s", debugZipUploadOpts.customerName, uuid.NewV4().String())
}
