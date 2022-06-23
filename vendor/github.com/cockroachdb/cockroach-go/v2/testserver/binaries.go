// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package testserver

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"mime"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"time"

	"github.com/gofrs/flock"
)

const (
	latestSuffix     = "LATEST"
	finishedFileMode = 0555
	writingFileMode  = 0600 // Allow reads so that another process can check if there's a flock.
)

const (
	linuxUrlpat  = "https://binaries.cockroachdb.com/cockroach-v%s.linux-amd64.tgz"
	macUrlpat    = "https://binaries.cockroachdb.com/cockroach-v%s.darwin-10.9-amd64.tgz"
	winUrlpat    = "https://binaries.cockroachdb.com/cockroach-v%s.windows-6.2-amd64.zip"
	sourceUrlPat = "https://binaries.cockroachdb.com/cockroach-v%s.src.tgz)"
)

// updatesUrl is used to get the info of the latest stable version of CRDB.
// Note that it may return a withdrawn version, but the risk is low for local tests here.
const updatesUrl = "https://register.cockroachdb.com/api/updates"

var muslRE = regexp.MustCompile(`(?i)\bmusl\b`)

// GetDownloadResponse return the http response of a CRDB download.
// It creates the url for downloading a CRDB binary for current runtime OS,
// makes a request to this url, and return the response.
func GetDownloadResponse(nonStable bool) (*http.Response, string, error) {
	goos := runtime.GOOS
	if goos == "linux" {
		goos += func() string {
			// Detect which C library is present on the system. See
			// https://unix.stackexchange.com/a/120381.
			cmd := exec.Command("ldd", "--version")
			out, err := cmd.Output()
			if err != nil {
				log.Printf("%s: %s: out=%q err=%v", testserverMessagePrefix, cmd.Args, out, err)
			} else if muslRE.Match(out) {
				return "-musl"
			}
			return "-gnu"
		}()
	}
	binaryName := fmt.Sprintf("cockroach.%s-%s", goos, runtime.GOARCH)
	if runtime.GOOS == "windows" {
		binaryName += ".exe"
	}

	var dbUrl string
	var err error

	latestStableVersion := ""
	// For the latest (beta) CRDB, we use the `edge-binaries.cockroachdb.com` host.
	if nonStable {
		u := &url.URL{
			Scheme: "https",
			Host:   "edge-binaries.cockroachdb.com",
			Path:   path.Join("cockroach", fmt.Sprintf("%s.%s", binaryName, latestSuffix)),
		}
		dbUrl = u.String()
	} else {
		// For the latest stable CRDB, we use the url provided in the CRDB release page.
		dbUrl, latestStableVersion, err = getLatestStableVersionInfo()
		if err != nil {
			return nil, "", err
		}
	}

	log.Printf("GET %s", dbUrl)
	response, err := http.Get(dbUrl)
	if err != nil {
		return nil, "", err
	}

	if response.StatusCode != 200 {
		return nil, "", fmt.Errorf("error downloading %s: %d (%s)", dbUrl,
			response.StatusCode, response.Status)
	}
	return response, latestStableVersion, nil
}

// downloadBinary saves the latest version of CRDB into a local binary file,
// and returns the path for this local binary.
// To download the latest STABLE version of CRDB, set `nonStable` to false.
// To download the bleeding edge version of CRDB, set `nonStable` to true.
func downloadBinary(tc *TestConfig, nonStable bool) (string, error) {
	response, latestStableVersion, err := GetDownloadResponse(nonStable)
	if err != nil {
		return "", err
	}

	defer func() { _ = response.Body.Close() }()

	filename, err := GetDownloadFilename(response, nonStable, latestStableVersion)
	if err != nil {
		return "", err
	}

	localFile := filepath.Join(os.TempDir(), filename)
	for {
		info, err := os.Stat(localFile)
		if os.IsNotExist(err) {
			// File does not exist: download it.
			break
		}
		if err != nil {
			return "", err
		}
		// File already present: check mode.
		if info.Mode().Perm() == finishedFileMode {
			return localFile, nil
		}

		localFileLock := flock.New(localFile)
		// If there's a process downloading the binary, local file cannot be flocked.
		locked, err := localFileLock.TryLock()
		if err != nil {
			return "", err
		}

		if locked {
			// If local file can be locked, it means the previous download was
			// killed in the middle. Delete local file and re-download.
			log.Printf("previous download failed in the middle, deleting and re-downloading")
			if err := os.Remove(localFile); err != nil {
				log.Printf("failed to remove partial download %s: %v", localFile, err)
				return "", err
			}
			break
		}

		log.Printf("waiting for download of %s", localFile)
		time.Sleep(time.Millisecond * 10)
	}

	output, err := os.OpenFile(localFile, os.O_WRONLY|os.O_CREATE|os.O_EXCL, writingFileMode)
	if err != nil {
		return "", fmt.Errorf("error creating %s: %w", localFile, err)
	}

	// Assign a flock to the local file.
	// If the downloading process is killed in the middle,
	// the lock will be automatically dropped.
	localFileLock := flock.New(localFile)

	if _, err := localFileLock.TryLock(); err != nil {
		return "", err
	}

	defer func() { _ = localFileLock.Unlock() }()

	if tc.IsTest && tc.StopDownloadInMiddle {
		log.Printf("download process killed")
		output.Close()
		return "", errStoppedInMiddle
	}

	var downloadMethod func(*http.Response, *os.File, string) error

	if nonStable {
		downloadMethod = downloadBinaryFromResponse
	} else {
		if runtime.GOOS == "windows" {
			downloadMethod = downloadBinaryFromZip
		} else {
			downloadMethod = downloadBinaryFromTar
		}
	}
	log.Printf("saving %s to %s, this may take some time", response.Request.URL, localFile)
	if err := downloadMethod(response, output, localFile); err != nil {
		if !errors.Is(err, errStoppedInMiddle) {
			if err := os.Remove(localFile); err != nil {
				log.Printf("failed to remove %s: %s", localFile, err)
			}
		}
		return "", err
	}

	if err := localFileLock.Unlock(); err != nil {
		return "", err
	}

	if err := output.Close(); err != nil {
		return "", err
	}

	return localFile, nil
}

// GetDownloadFilename returns the local filename of the downloaded CRDB binary file.
func GetDownloadFilename(
	response *http.Response, nonStableDB bool, latestStableVersion string,
) (string, error) {
	if nonStableDB {
		const contentDisposition = "Content-Disposition"
		_, disposition, err := mime.ParseMediaType(response.Header.Get(contentDisposition))
		if err != nil {
			return "", fmt.Errorf("error parsing %s headers %s: %w", contentDisposition, response.Header, err)
		}
		filename, ok := disposition["filename"]
		if !ok {
			return "", fmt.Errorf("content disposition header %s did not contain filename", disposition)
		}
		return filename, nil
	}
	filename := fmt.Sprintf("cockroach-%s", latestStableVersion)
	if runtime.GOOS == "windows" {
		filename += ".exe"
	}
	return filename, nil
}

// getLatestStableVersionInfo returns the latest stable CRDB's download URL,
// and the formatted corresponding version number. The download URL is based
// on the runtime OS.
// Note that it may return a withdrawn version, but the risk is low for local tests here.
func getLatestStableVersionInfo() (string, string, error) {
	resp, err := http.Get(updatesUrl)
	if err != nil {
		return "", "", err
	}
	var respJson map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&respJson); err != nil {
		return "", "", err
	}
	latestStableVersion, ok := respJson["version"]
	if !ok {
		return "", "", fmt.Errorf("api/updates response is of wrong format")
	}
	var downloadUrl string
	switch runtime.GOOS {
	case "linux":
		downloadUrl = fmt.Sprintf(linuxUrlpat, latestStableVersion)
	case "darwin":
		downloadUrl = fmt.Sprintf(macUrlpat, latestStableVersion)
	case "windows":
		downloadUrl = fmt.Sprintf(winUrlpat, latestStableVersion)
	}
	latestStableVerFormatted := strings.ReplaceAll(latestStableVersion, ".", "-")
	return downloadUrl, latestStableVerFormatted, nil
}

// downloadBinaryFromResponse copies the http response's body directly into a local binary.
func downloadBinaryFromResponse(response *http.Response, output *os.File, filePath string) error {
	if _, err := io.Copy(output, response.Body); err != nil {
		return fmt.Errorf("problem saving %s to %s: %w", response.Request.URL, filePath, err)
	}

	// Download was successful, add the rw bits.
	if err := output.Chmod(finishedFileMode); err != nil {
		return err
	}

	return nil
}

// downloadBinaryFromTar writes the binary compressed in a tar from a http response
// to a local file.
// It is created because the download url from the release page only provides the tar.gz/zip
// for a pre-compiled binary.
func downloadBinaryFromTar(response *http.Response, output *os.File, filePath string) error {
	// Unzip the tar file from the response's body.
	gzf, err := gzip.NewReader(response.Body)
	if err != nil {
		return fmt.Errorf("cannot read tar from response body: %w", err)
	}
	// Read the files from the tar.
	tarReader := tar.NewReader(gzf)
	for {
		header, err := tarReader.Next()

		// No more file from tar to read.
		if err == io.EOF {
			return fmt.Errorf("cannot find the binary from tar")
		}

		if err != nil {
			return fmt.Errorf("cannot untar: %w", err)
		}

		// Only copy the cockroach binary.
		// The header.Name is of the form "zip_name/file_name".
		// We extract the file name.
		splitHeaderName := strings.Split(header.Name, "/")
		fileName := splitHeaderName[len(splitHeaderName)-1]
		if fileName == "cockroach" {
			// Copy the binary to desired path.
			if _, err := io.Copy(output, tarReader); err != nil {
				return fmt.Errorf(
					"problem saving %s to %s: %w",
					response.Request.URL, filePath,
					err,
				)
			}
			if err := output.Chmod(finishedFileMode); err != nil {
				return err
			}
			return nil
		}

	}
	return nil
}

// downloadBinaryFromZip writes the binary compressed in a zip from a http response
// to a local file.
// It is created because the download url from the release page only provides the tar.gz/zip
// for a pre-compiled binary.
func downloadBinaryFromZip(response *http.Response, output *os.File, filePath string) error {
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return fmt.Errorf("cannot read zip from response body: %w", err)
	}

	zipReader, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		log.Fatal(err)
	}

	findFile := false
	// Read all the files from zip archive.
	for _, zipFile := range zipReader.File {
		splitHeaderName := strings.Split(zipFile.Name, "/")
		fileName := splitHeaderName[len(splitHeaderName)-1]
		fmt.Printf("filename=%s", fileName)
		if fileName == "cockroach" {
			findFile = true
			if err := readZipFile(zipFile, output); err != nil {
				return fmt.Errorf("problem saving %s to %s: %w",
					response.Request.URL,
					filePath,
					err)
			}
			if err := output.Chmod(finishedFileMode); err != nil {
				return err
			}
		}
	}
	if !findFile {
		return fmt.Errorf("cannot find the binary from zip")
	}

	return nil
}

func readZipFile(zf *zip.File, target *os.File) error {
	f, err := zf.Open()
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err = io.Copy(target, f); err != nil {
		return err
	}
	return nil
}
