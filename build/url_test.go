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

// +build check,urlcheck

package build_test

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"go/build"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/ghemawat/stream"
)

// Number of concurrent HTTP requests during the test
const concurrentRequests = 20

// Source: https://mathiasbynens.be/demo/url-regex
const urlRE = `https?://[^ \t\n/$.?#].[^ \t\n)]*(\)\.aspx)?`

var re = regexp.MustCompile(urlRE)

var ignored = []string{
	// Invalid URLs
	"http://%s",
	"http://127.0.0.1",
	"http://HOST:PORT/",
	"http://\"",
	"https://localhost",
	"https://myroach:8080",
	"http://localhost",
	"https://127.0.0.1",
	"https://\"",
	"https://edge-binaries.cockroachdb.com/cockroach/cockroach.linux-gnu-amd64.${var.cockroach_sha}",
	"https://edge-binaries.cockroachdb.com/examples-go/block_writer.${var.block_writer_sha}",
	"https://edge-binaries.cockroachdb.com/examples-go/photos.${var.photos_sha}",
	"https://github.com/cockroachdb/cockroach/commits/%s",
	"https://github.com/cockroachdb/cockroach/issues/%d",
	// These are AWS endpoints.
	"http://metadata/",
	"http://instance-data/latest/meta-data/public-ipv4",
	// All these are auto-generated and thus valid by definition.
	"https://registry.yarnpkg.com/",
	// XML namespace identifiers, these are not really HTTP endpoints.
	"https://www.w3.org/",
	"http://www.bohemiancoding.com/sketch/ns",
	"http://www.w3.org/",
	// These report 404 for non-API GETs.
	"http://ignored:ignored@errors.cockroachdb.com/",
	"https://cockroachdb.github.io/distsqlplan/",
	"https://ignored:ignored@errors.cockroachdb.com/",
	"https://register.cockroachdb.com",
	"https://www.googleapis.com/auth",
	// GitHub reports 404 for private repos without auth
	"https://github.com/cockroachlabs/registration/",
	// Regexp not too smart about HTML...
	`http://www.cockroachlabs.com">http://www.cockroachlabs.com</a></span`,
}

func testURLLiveness(t *testing.T, pkg *build.Package) {
	t.Parallel()

	cmd, stderr, filter, err := dirCmd(pkg.Dir, "git", "grep", "-nE", urlRE)
	if err != nil {
		t.Fatal(err)
	}

	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	uniqueURLs := map[string][]string{}

	if err := stream.ForEach(filter, func(s string) {
		for _, match := range re.FindAllString(s, -1) {
			// Remove punctuation after the URL
			match = strings.TrimRight(match, ".,;\\\">`]")
			// Remove the HTML target
			n := strings.LastIndex(match, "#")
			if n != -1 {
				match = match[:n]
			}
			// Add the source reference to the list
			l, _ := uniqueURLs[match]
			l = append(l, s)
			uniqueURLs[match] = l
		}
	}); err != nil {
		t.Error(err)
	}

	if err := cmd.Wait(); err != nil {
		if out := stderr.String(); len(out) > 0 {
			t.Fatalf("err=%s, stderr=%s", err, out)
		}
	}

	var wg sync.WaitGroup
	var sem = make(chan struct{}, concurrentRequests)

	// This test doesn't care that https certificates are invalid.
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: tr}

	for url, locs := range uniqueURLs {
		ignore := false
		for _, i := range ignored {
			if strings.HasPrefix(url, i) {
				ignore = true
				break
			}
		}
		if ignore {
			continue
		}

		sem <- struct{}{}
		wg.Add(1)
		go func(url string, locs []string) {
			defer func() { wg.Done(); <-sem }()

			fmt.Fprintf(os.Stderr, "Checking %s...\n", url)
			var buf bytes.Buffer
			fmt.Fprintln(&buf, "Found in:")
			for _, loc := range locs {
				fmt.Fprintln(&buf, loc)
			}

			resp, err := client.Head(url)
			if err != nil {
				t.Errorf("while accessing %s: %v\n%s", url, err, buf.String())
				return
			}
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				if resp.StatusCode == 405 {
					// The server doesn't like HEAD. Try GET.
					resp, err = client.Get(url)
					if err != nil {
						t.Errorf("while accessing %s: %v\n%s", url, err, buf.String())
						return
					}
					if resp.StatusCode >= 200 || resp.StatusCode < 300 {
						return
					}
				}

				t.Errorf("while accessing %s: %s\n%s", url, resp.Status, buf.String())
			}
		}(url, locs)
	}

	wg.Wait()
}
