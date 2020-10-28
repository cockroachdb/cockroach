// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package binfetcher

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
)

// Options are the options to Download().
type Options struct {
	Binary  string
	Version string

	// These are optional but should usually be set.
	Dir    string // empty for OS temp dir
	GOOS   string // empty for build-time GOOS
	GOARCH string // empty for build-time GOOS

	// These are optional and rarely need to be set.
	Component string  // empty for auto-inferred; this is the "loadgen" in "loadgen/kv"
	Suffix    string  // empty for auto-inferred; either empty, ".zip", or ".tgz"
	URL       url.URL // empty for auto-inferred
}

func (opts *Options) init() error {
	if opts.Dir == "" {
		opts.Dir = os.TempDir()
	}
	if opts.GOOS == "" {
		opts.GOOS = runtime.GOOS
	}
	if opts.GOARCH == "" {
		opts.GOARCH = runtime.GOARCH
	}
	if opts.Component == "" {
		switch opts.Binary {
		case "cockroach":
			opts.Component = "cockroach"
		default:
			opts.Component = filepath.Dir(opts.Binary)
			opts.Binary = filepath.Base(opts.Binary)
		}
	}

	var suffix string // only used when Binary == "cockroach"
	switch opts.GOOS {
	case "darwin":
		suffix = ".tgz"
	case "linux":
		suffix = ".tgz"
	case "windows":
		suffix = ".zip"
	default:
		return errors.Errorf("unsupported GOOS: %s", opts.GOOS)
	}

	if opts.URL == (url.URL{}) {
		switch opts.Version[0] {
		case 'v':
			if opts.Binary != "cockroach" {
				// Only the main binary has versions.
				return errors.Errorf("invalid binary %q for version %s", opts.Binary, opts.Version)
			}

			goos := opts.GOOS
			if opts.GOOS == "darwin" {
				goos += "-10.9"
			} else if opts.GOOS == "windows" {
				goos += "-6.2"
			}

			if opts.Suffix == "" {
				opts.Suffix = suffix
			}

			base := fmt.Sprintf("%s-%s.%s-%s", opts.Binary, opts.Version, goos, opts.GOARCH)
			opts.URL = url.URL{
				Scheme: "https",
				Host:   "binaries.cockroachdb.com",
				Path:   base + opts.Suffix, // no component
			}
		default:
			modGOOS := opts.GOOS
			var base string
			if opts.Binary == "cockroach" {
				if opts.GOOS == "linux" {
					modGOOS += "-gnu"
				}
				base = fmt.Sprintf("%s.%s-%s.%s", opts.Binary, modGOOS, opts.GOARCH, opts.Version)
			} else {
				if opts.GOOS != "linux" {
					return errors.Errorf("%s is not available for %s", opts.Binary, opts.GOOS)
				}
				base = fmt.Sprintf("%s.%s", opts.Binary, opts.Version)
			}
			opts.URL = url.URL{
				Scheme: "https",
				Host:   "edge-binaries.cockroachdb.com",
				Path:   filepath.Join(opts.Component, base),
			}
		}

		v := url.Values{}
		v.Add("ci", "true")
		opts.URL.RawQuery = v.Encode()
	}
	return nil
}

func (opts Options) filename() string {
	return filepath.Base(opts.URL.Path[:len(opts.URL.Path)-len(opts.Suffix)])
}

// Downloading binaries may take some time, so give ourselves
// some room before the timeout expires.
var httpClient = httputil.NewClientWithTimeout(300 * time.Second)

// Download downloads the binary for the given version, and skips the download
// if the archive is already present in `destDir`.
//
// Do not use this to download the cockroach binary. It won't work for v20.2+
// releases, which include the geos libraries along with the binary. On
// roachprod, use `roachprod stage` instead.
//
// `version` can be:
//
// - a SHA from the master branch, e.g. bd828feaa309578142fe7ad2d89ee1b70adbd52d
// - the string "LATEST" for the most recent SHA from the master branch. Note that
//   caching is disabled in that case.
//
// Returns the path to the (executable) binary.
func Download(ctx context.Context, opts Options) (string, error) {
	if err := opts.init(); err != nil {
		return "", err
	}

	_ = os.MkdirAll(opts.Dir, 0755)
	destFileName := filepath.Join(opts.Dir, opts.filename())

	if stat, err := os.Stat(destFileName); err != nil {
		if !oserror.IsNotExist(err) {
			return "", err
		}
	} else if stat.Size() > 0 && opts.Version != "LATEST" {
		return destFileName, nil // cache hit
	}

	log.Infof(ctx, "downloading %s to %s", opts.URL.String(), destFileName)
	resp, err := httpClient.Get(ctx, opts.URL.String())
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := ioutil.ReadAll(resp.Body)
		return "", errors.Errorf("unexpected HTTP response from %s: %d\n%s", opts.URL.String(), resp.StatusCode, body)
	}
	if opts.Version == "LATEST" {
		log.Infof(ctx, "LATEST redirected to %s", resp.Request.URL.String())
	}

	destFile, err := os.Create(destFileName)
	if err != nil {
		return "", err
	}
	defer destFile.Close()

	switch {
	case strings.HasSuffix(opts.URL.Path, ".tgz"):
		r, err := gzip.NewReader(resp.Body)
		if err != nil {
			return "", err
		}
		if err := untar(r, destFile); err != nil {
			_ = destFile.Truncate(0)
			return "", err
		}
	case strings.HasSuffix(opts.URL.Path, ".zip"):
		if err := unzip(resp.Body, destFile); err != nil {
			_ = destFile.Truncate(0)
			return "", err
		}
	default:
		if _, err := io.Copy(destFile, resp.Body); err != nil {
			_ = destFile.Truncate(0)
			return "", err
		}
	}

	if stat, err := os.Stat(destFileName); err != nil && !oserror.IsNotExist(err) {
		return "", errors.Wrap(err, "checking downloaded binary")
	} else if stat.Size() == 0 {
		return "", errors.Errorf("%s is unexpectedly empty", destFileName)
	}

	if err := os.Chmod(destFileName, 0755); err != nil {
		return "", errors.Wrap(err, "during chmod")
	}

	return destFileName, nil
}
