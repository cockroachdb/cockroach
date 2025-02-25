// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//
// run-pgo-build triggers a run of a special roachtest job in TeamCity on the
// current branch, waits for the job to complete, downloads the associated
// artifacts, extracts all the CPU (.pprof) profiles, merges them, and places
// the merged result in a location on disk.
package main

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/pprof/profile"
)

const (
	buildConfigID = "Cockroach_Nightlies_RoachtestGceForceProfile"
)

var (
	branch    = os.Getenv("TC_BUILD_BRANCH")
	serverURL = os.Getenv("TC_SERVER_URL")
	username  = os.Getenv("TC_API_USER")
	password  = os.Getenv("TC_API_PASSWORD")

	outFile = flag.String("out", "", "where to store the result pprof profile")
)

type Build struct {
	ID         int64
	FinishDate string
	Status     string
}

type ReadAtCloser interface {
	io.ReaderAt
	io.ReadCloser
}

func doRequest(req *http.Request) (*Build, error) {
	req.SetBasicAuth(username, password)
	req.Header.Add("Accept", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("failed to send HTTP request; got status code %d", resp.StatusCode)
	}
	defer resp.Body.Close()
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, resp.Body); err != nil {
		return nil, err
	}
	var ret Build
	if err := json.Unmarshal(buf.Bytes(), &ret); err != nil {
		return nil, err
	}
	return &ret, nil
}

func queueBuild(buildConfigID, branch string) (*Build, error) {
	reqStruct := struct {
		BuildTypeID string `json:"buildTypeId,omitempty"`
		BranchName  string `json:"branchName,omitempty"`
	}{
		BuildTypeID: buildConfigID,
		BranchName:  branch,
	}
	reqJson, err := json.Marshal(reqStruct)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", fmt.Sprintf("https://%s/httpAuth/app/rest/buildQueue", serverURL), bytes.NewReader(reqJson))
	if err != nil {
		return nil, err
	}
	req.Header.Add("Content-Type", "application/json")
	return doRequest(req)
}

func getBuild(id int64) (*Build, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("https://%s/httpAuth/app/rest/builds/id:%d?fields=id,finishDate,status", serverURL, id), nil)
	if err != nil {
		return nil, err
	}
	return doRequest(req)
}

// downloadArtifacts downloads the artifacts.zip archive for the given build and
// returns the ReadAtCloser corresponding to it. The file will be stored on
// disk in the given tmpDir. The caller is responsible for closing the file.
// The returned int64 is the length of the file.
func downloadArtifacts(buildID int64, tmpDir string) (ReadAtCloser, int64, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("https://%s/repository/downloadAll/%s/%d:id", serverURL, buildConfigID, buildID), nil)
	if err != nil {
		return nil, 0, err
	}
	req.SetBasicAuth(username, password)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	if resp.StatusCode >= 400 {
		return nil, 0, fmt.Errorf("failed to download artifacts: got status code %d", resp.StatusCode)
	}
	defer resp.Body.Close()

	zipFile, err := os.Create(filepath.Join(tmpDir, "artifacts.zip"))
	if err != nil {
		return nil, 0, err
	}
	zipLength, err := io.Copy(zipFile, resp.Body)
	if err != nil {
		zipFile.Close()
		return nil, 0, err
	}
	_, err = zipFile.Seek(0, 0)
	if err != nil {
		zipFile.Close()
		return nil, 0, err
	}

	return zipFile, zipLength, nil
}

// processArtifactsZip processes a file named artifacts.zip inside the larger
// artifacts zip archive. This is confusing, but the `.pprof` files for any
// given node are inside a sub-zip archive named `artifacts.zip`, which is one
// of the files in the giant zip archive that we fetch for the build. This
// function processes that sub-zip archive and extracts all the CPU (.pprof)
// files.
//
// The .pprof files will be parsed and put into the pprofFilesChan.
// wg.Done() will be called when this function completes.
func processArtifactsZip(f *zip.File, profilesChan chan *profile.Profile, wg *sync.WaitGroup) {
	archive, err := f.Open()
	if err != nil {
		panic(err)
	}
	defer archive.Close()
	var archiveBuf bytes.Buffer
	_, err = io.Copy(&archiveBuf, archive)
	if err != nil {
		panic(err)
	}
	zipReader, err := zip.NewReader(bytes.NewReader(archiveBuf.Bytes()), int64(f.UncompressedSize64))
	if err != nil {
		panic(err)
	}
	for _, file := range zipReader.File {
		if strings.HasSuffix(file.FileHeader.Name, ".pprof") &&
			strings.Contains(file.FileHeader.Name, "/cpuprof.") &&
			file.UncompressedSize64 > 0 {
			pprofFile, err := file.Open()
			if err != nil {
				panic(err)
			}
			//nolint:deferloop TODO(#137605)
			defer pprofFile.Close()
			prof, err := profile.Parse(pprofFile)
			if err != nil {
				panic(err)
			}
			profilesChan <- prof
		}
	}
	wg.Done()
}

// processPprofFiles reads all the parsed profiles from the given channel,
// merges all of the profiles, and dumps the results profile to a final
// location. wg.Done() will be called at the end of this function.
func processPprofFiles(profilesChan chan *profile.Profile, wg *sync.WaitGroup) {
	var profiles []*profile.Profile
	for prof := range profilesChan {
		profiles = append(profiles, prof)
	}
	if len(profiles) == 0 {
		panic("expected to find a profile; found none")
	}
	res, err := profile.Merge(profiles)
	if err != nil {
		panic(err)
	}
	w, err := os.Create(*outFile)
	if err != nil {
		panic(err)
	}
	defer w.Close()
	err = res.Write(w)
	if err != nil {
		panic(err)
	}
	wg.Done()
}

func main() {
	flag.Parse()

	if branch == "" || serverURL == "" || username == "" || password == "" {
		panic("ensure credentials, server URL, and branch are supplied")
	}
	if *outFile == "" {
		panic("must supply -out")
	}

	tmpDir, err := os.MkdirTemp("", "run-pgo-build")
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()

	build, err := queueBuild(buildConfigID, branch)
	if err != nil {
		panic(err)
	}
	fmt.Printf("queued build with ID %d\n", build.ID)

	for {
		if build.FinishDate != "" {
			break
		}
		time.Sleep(3 * time.Minute)
		lookupID := build.ID
		newBuild, err := getBuild(lookupID)
		if err != nil {
			fmt.Printf("failed to get build %d; got error %+v -- this will be retried\n", lookupID, err)
		} else {
			build = newBuild
		}
	}

	if build.Status != "SUCCESS" {
		panic(fmt.Sprintf("expected build to succeed; got status %s for build %+v", build.Status, build))
	}

	zipFile, zipLength, err := downloadArtifacts(build.ID, tmpDir)
	if err != nil {
		panic(err)
	}
	defer zipFile.Close()

	zipReader, err := zip.NewReader(zipFile, zipLength)
	if err != nil {
		panic(err)
	}

	// wg tracks the progress of loading the profiles, readWg tracks the
	// progress of reading them.
	var wg, readWg sync.WaitGroup
	// All parsed profiles will be sent to profilesChan.
	profilesChan := make(chan *profile.Profile)
	for _, file := range zipReader.File {
		if strings.HasSuffix(file.FileHeader.Name, "/artifacts.zip") {
			wg.Add(1)
			go processArtifactsZip(file, profilesChan, &wg)
		}
	}
	readWg.Add(1)
	go processPprofFiles(profilesChan, &readWg)
	wg.Wait()
	close(profilesChan)
	readWg.Wait()
}
