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
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

// fetchAndCacheBinary downloads the given version (e.g. "v1.0.5") from the
// official website, and skips the download if the archive is already present.
//
// Returns the path to the (executable) binary
func fetchAndCacheBinary(ctx context.Context, destDir, version string) (string, error) {
	goos := runtime.GOOS

	switch goos {
	case "darwin":
		goos += "-10.9"
	case "linux":
	default:
		return "", errors.Errorf("unsupported GOOS: %s", goos)
	}

	_ = os.MkdirAll(destDir, 0755)

	base := fmt.Sprintf("cockroach-%s.%s-%s", version, goos, runtime.GOARCH)

	v := url.Values{}
	v.Add("ci", "true")
	u := url.URL{
		Scheme:   "https",
		Host:     "binaries.cockroachdb.com",
		Path:     base + ".tgz",
		RawQuery: v.Encode(),
	}

	destFileName := filepath.Join(destDir, base)

	if _, err := os.Stat(destFileName); err == nil {
		return destFileName, nil // cache hit
	} else if !os.IsNotExist(err) {
		return "", err
	}

	log.Infof(ctx, "downloading %s to %s", u.String(), destFileName)
	destFile, err := os.Create(destFileName)
	if err != nil {
		return "", err
	}
	defer destFile.Close()

	resp, err := http.Get(u.String())
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := ioutil.ReadAll(resp.Body)
		return "", errors.Errorf("unexpected HTTP response: %d\n%s", resp.StatusCode, body)
	}
	gzf, err := gzip.NewReader(resp.Body)
	if err != nil {
		return "", err
	}

	tarReader := tar.NewReader(gzf)

	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
		switch header.Typeflag {
		case tar.TypeDir:
			continue
		case tar.TypeReg:
			base := filepath.Base(header.Name)
			if base != "cockroach" {
				log.Warningf(ctx, "skipping file %s", header.Name)
				continue
			}
			if _, err := io.Copy(destFile, tarReader); err != nil {
				return "", errors.Wrapf(err, "while downloading %s to %s", &u, destFileName)
			}
		default:
			return "", errors.Errorf("unknown tar header %+v", header)
		}
	}

	if _, err := os.Stat(destFileName); err != nil && !os.IsNotExist(err) {
		return "", errors.Wrap(err, "checking downloaded binary")
	}

	if err := os.Chmod(destFileName, 0755); err != nil {
		return "", errors.Wrap(err, "during chmod")
	}

	return destFileName, nil
}
