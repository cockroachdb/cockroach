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

package main

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/ghemawat/stream"
)

// Number of concurrent HTTP requests during the test
const concurrentRequests = 20

// Number of times to retry requests that time out
const timeoutRetries = 3

// Source: https://mathiasbynens.be/demo/url-regex
const urlRE = `https?://[^ \t\n/$.?#].[^ \t\n]*`

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
	"https://storage.googleapis.com/golang/go${GOVERSION}",
	"http://s3.amazonaws.com/cockroach/cockroach/cockroach.$(",
	"https://binaries.cockroachdb.com/cockroach-${BETA_TAG}",
	"https://edge-binaries.cockroachdb.com/cockroach/cockroach.linux-gnu-amd64.${var.cockroach_sha}",
	"https://edge-binaries.cockroachdb.com/examples-go/block_writer.${var.block_writer_sha}",
	"https://edge-binaries.cockroachdb.com/examples-go/photos.${var.photos_sha}",
	"https://github.com/cockroachdb/cockroach/commits/%s",
	"https://github.com/cockroachdb/cockroach/issues/%d",
	// These are cloud provider metadata endpoints.
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
	// These require authentication.
	"https://console.aws.amazon.com/",
	"https://github.com/cockroachlabs/registration/",
	"https://index.docker.io/v1/",
	// Regexp not too smart about HTML...
	`http://www.cockroachlabs.com">http://www.cockroachlabs.com</a></span`,
}

func fatalf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format, args...)
	os.Exit(1)
}

var errorMessages []string

func errorf(format string, args ...interface{}) {
	errorMessages = append(errorMessages, fmt.Sprintf(format, args...))
}

// chompUnbalanced truncates s before the first right rune to appear without an
// earlier left rune.
// Example: chompUnbalanced('(', ')', '(real) cool) garbage') -> '(real) cool'
func chompUnbalanced(left, right rune, s string) string {
	depth := 0
	for i, c := range s {
		switch c {
		case left:
			depth++
		case right:
			if depth == 0 {
				return s[:i]
			}
			depth--
		}
	}
	return s
}

func checkURL(client *http.Client, url string) error {
	resp, err := client.Head(url)
	if err != nil {
		return err
	}
	if _, err := io.Copy(ioutil.Discard, resp.Body); err != nil {
		fatalf("fatal: %s\n", err)
	}
	resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}

	if resp.StatusCode == 403 || resp.StatusCode == 405 {
		// The server doesn't like HEAD. Try GET. This is obviously the
		// correct strategy for 405 Method Not ALlowed responses. This is a
		// less-correct strategy for 403 Forbidden responses, but we link to
		// several misconfigured S3 buckets that return 403 Forbidden for HEAD
		// requests but not for GET requests.
		resp, err = client.Get(url)
		if err != nil {
			return err
		}
		if _, err := io.Copy(ioutil.Discard, resp.Body); err != nil {
			fatalf("fatal: %s\n", err)
		}
		resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
	}

	return errors.New(resp.Status)
}

func main() {
	cmd := exec.Command("git", "grep", "-nE", urlRE)
	stdout, err := cmd.StdoutPipe()
	filter := stream.ReadLines(stdout)
	if err != nil {
		fatalf("fatal: %s\n", err)
	}
	stderr := new(bytes.Buffer)
	cmd.Stderr = stderr

	if err := cmd.Start(); err != nil {
		fatalf("fatal: %s\n", err)
	}

	uniqueURLs := map[string][]string{}

	if err := stream.ForEach(filter, func(s string) {
		for _, match := range re.FindAllString(s, -1) {
			// Discard any characters after the first unbalanced ')' or ']'.
			match = chompUnbalanced('(', ')', match)
			match = chompUnbalanced('[', ']', match)
			// Remove punctuation after the URL
			match = strings.TrimRight(match, ".,;\\\">`]")
			// Remove the HTML target
			n := strings.LastIndex(match, "#")
			if n != -1 {
				match = match[:n]
			}
			// Add the source reference to the list
			l := uniqueURLs[match]
			if len(s) > 150 {
				s = s[:150] + "..."
			}
			l = append(l, s)
			uniqueURLs[match] = l
		}
	}); err != nil {
		fatalf("fatal: %s\n", err)
	}

	if err := cmd.Wait(); err != nil {
		if out := stderr.String(); len(out) > 0 {
			fatalf("err=%s, stderr=%s", err, out)
		}
	}

	var wg sync.WaitGroup
	var sem = make(chan struct{}, concurrentRequests)

	// This test doesn't care that https certificates are invalid.
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{
		Transport: tr,
		Timeout:   5 * time.Second,
	}

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

			urlError := func(message interface{}) {
				var buf bytes.Buffer
				fmt.Fprintf(&buf, "%s: %s\n", url, message)
				for _, loc := range locs {
					fmt.Fprintln(&buf, "    ", loc)
				}
				errorf(buf.String())
			}

			for i := uint(0); i < timeoutRetries; i++ {
				err := checkURL(client, url)
				if netErr, ok := err.(net.Error); !(ok && netErr.Timeout()) {
					if err != nil {
						urlError(err)
					}
					return
				}
				time.Sleep((1 << i) * time.Second)
			}

			urlError(fmt.Sprintf("timed out on %d separate tries, giving up", timeoutRetries))
		}(url, locs)
	}

	wg.Wait()

	if len(errorMessages) > 0 {
		for _, message := range errorMessages {
			fmt.Print(message)
		}
		fmt.Printf("%d errors\n", len(errorMessages))
		fmt.Println("FAIL")
		os.Exit(1)
	}
	fmt.Println("PASS")
}
