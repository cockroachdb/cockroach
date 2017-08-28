// Copyright 2016 The Cockroach Authors.
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

package acceptance

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"

	"github.com/mholt/archiver"
	"github.com/pkg/errors"
)

// fetchAndCacheBinary downloads the given version (e.g. "v1.0.5") from the
// official website, and skips the download if the archive is already present.
//
// Returns the path to the (executable) binary
func fetchAndCacheBinary(destDir, version string) (string, error) {
	goOs := runtime.GOOS
	goArch := runtime.GOARCH

	switch goOs {
	case "darwin":
		goOs = "darwin-10.9"
	case "linux":
	default:
		return "", errors.Errorf("unsupported GOOS: %s", goOs)
	}

	_ = os.MkdirAll(destDir, 0755)

	base := fmt.Sprintf("cockroach-%s.%s-%s", version, goOs, goArch)

	v := url.Values{}
	v.Add("ci", "true")
	u := url.URL{
		Scheme:   "https",
		Host:     "binaries.cockroachdb.com",
		Path:     base + ".tgz",
		RawQuery: v.Encode(),
	}

	if _, err := os.Stat(destDir); err != nil && !os.IsNotExist(err) {
		return "", err
	}

	destTGZ := filepath.Join(destDir, u.Path)
	destFile, err := os.Create(destTGZ)
	if err != nil {
		return "", err
	}

	resp, err := http.Get(u.String())
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := ioutil.ReadAll(resp.Body)
		return "", errors.Errorf("unexpected HTTP response: %d\n%s", resp.StatusCode, body)
	}
	if _, err := io.Copy(destFile, resp.Body); err != nil {
		return "", errors.Wrapf(err, "while downloading %s to %s", u, destTGZ)
	}

	if err := archiver.TarGz.Open(destTGZ, destDir); err != nil {
		return "", errors.Wrapf(err, "while decompressing %s to %s", destTGZ, destDir)
	}

	binary := filepath.Join(destDir, base, "cockroach")
	if _, err := os.Stat(binary); err != nil && !os.IsNotExist(err) {
		return "", errors.Wrap(err, "checking downloaded binary")
	}

	if err := os.Chmod(binary, 0755); err != nil {
		return "", errors.Wrap(err, "during chmod")
	}

	return binary, nil
}
