// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package urlcheck

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/ghemawat/stream"
)

// maxConcurrentRequests specifies the maximum number of concurrent HTTP
// requests during the test.
const maxConcurrentRequests = 20

// timeoutRetries specifies the number of times that requests which time out
// will be retried.
const timeoutRetries = 3

// URLRE is the regular expression to use to extract URLs from
// the input stream.
// Source: https://mathiasbynens.be/demo/url-regex
const URLRE = `https?://[^ \t\n/$.?#].[^ \t\n"<]*`

var re = regexp.MustCompile(URLRE)

var ignored = []string{
	// These are invalid URLs.
	"http://%s",
	"http://127.0.0.1",
	"http://\"",
	"http://HOST:PORT/",
	"http://localhost",
	"http://s3.amazonaws.com/cockroach/cockroach/cockroach.$(",
	"https://127.0.0.1",
	"https://\"",
	"https://binaries.cockroachdb.com/cockroach-${BETA_TAG}",
	"https://edge-binaries.cockroachdb.com/cockroach/cockroach.linux-gnu-amd64.${var.cockroach_sha}",
	"https://edge-binaries.cockroachdb.com/examples-go/block_writer.${var.block_writer_sha}",
	"https://edge-binaries.cockroachdb.com/examples-go/photos.${var.photos_sha}",
	"https://binaries.cockroachdb.com/cockroach-${FORWARD_REFERENCE_VERSION}.linux-amd64.tgz",
	"https://binaries.cockroachdb.com/cockroach-${BIDIRECTIONAL_REFERENCE_VERSION}.linux-amd64.tgz",
	"https://github.com/cockroachdb/cockroach/commits/%s",
	"https://github.com/cockroachdb/cockroach/issues/%d",
	"https://ignored:ignored@ignored/ignored",
	"https://localhost",
	"https://myroach:8080",
	"https://storage.googleapis.com/golang/go${GOVERSION}",
	"https://storage.googleapis.com/SOME_BUCKET/SHA-bar.txt",
	"https://%s.blob.core.windows.net/%s",
	"https://roachfixtureseastus.blob.core.windows.net/$container",
	"https://roachfixtureswestus.blob.core.windows.net/$container",
	"https://example.com/",
	"https://server-url-not-found-in-env.com",
	"https://go.crdb.dev/issue-v/([0-9]+)/",
	"https://www.cockroachlabs.com/docs/_version",
	`https://github.com/cockroachdb/cockroach/commit/(\w+)\W`,
	"https://api.github.com/search/issues?q=%s&per_page=100",
	"https://api.github.com/gists/",
	"https://cluster.example.com",
	// These are cloud provider metadata endpoints.
	"http://instance-data/latest/meta-data/public-ipv4",
	"http://instance-data.ec2.internal/",
	"http://metadata/",
	"http://169.254.169.254/metadata/",
	"http://metadata.google.internal/computeMetadata/v1/instance/",
	// These are auto-generated and thus valid by definition.
	"https://registry.yarnpkg.com/",
	// XML namespace identifiers, these are not really HTTP endpoints.
	"http://www.bohemiancoding.com/sketch/ns",
	"http://www.w3.org/",
	"https://www.w3.org/",
	"http://maven.apache.org/POM/4.0.0",
	// These report 40x for non-API or non-browser GETs.
	"http://ignored:ignored@errors.cockroachdb.com/",
	"https://cockroachdb.github.io/distsqlplan/",
	"https://ignored:ignored@errors.cockroachdb.com/",
	"https://register.cockroachdb.com",
	"https://www.googleapis.com/auth",
	"https://ubuntu.pkgs.org/22.04/ubuntu-main-amd64/tcpdump_4.99.1-3build2_amd64.deb.html",
	"https://segment.com/docs/connections/spec/track/",
	"https://www.cloudping.co/",
	"https://cloud.ibm.com",
	"https://s3-us-west-2.amazonaws.com/confluent.cloud/confluent-cli/archives",
	"https://ignored@errors.cockroachdb.com/api/sentry/v2/1111",
	"https://ignored@errors.cockroachdb.com/api/sentrydev/v2/1111",
	// These require authentication.
	"https://console.aws.amazon.com/",
	"https://github.com/cockroachlabs/registration/",
	"https://api.github.com/repos/cockroachdb/cockroach/issues",
	"https://index.docker.io/v1/",
	"https://github.com/cockroachlabs/managed-service",
	"https://github.com/cockroachlabs/support/",
	"https://storage.googleapis.com/cockroach-test-artifacts",
	"https://www.googleapis.com/compute/v1/projects/",
	"https://www.figma.com/proto/",
	"https://sentry.io/api/0/organizations/cockroach-labs/issues/",
	"https://observablehq.com/",
	"https://cockroachlabs.udemy.com/course/general-onboarding/learn/lecture/22164146",
	"https://support.cockroachlabs.com",
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

func head200(client *http.Client, url string) (bool, error) {
	resp, err := client.Head(url)
	if err != nil {
		return false, err
	}
	if err := resp.Body.Close(); err != nil {
		return false, err
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return true, nil
	}
	return false, nil
}

func checkURL(client *http.Client, url string) error {
	if ok, err := head200(client, url); err != nil {
		return err
	} else if ok {
		return nil
	}
	// The server doesn't like HEAD. Try GET. This is obviously the correct
	// strategy for a 405 Method Not Allowed error, but a less-correct strategy
	// for any other error. Still, we link to several misconfigured servers that
	// return 403 Forbidden or 500 Internal Server Error for HEAD requests, but
	// not for GET requests.
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	if err := resp.Body.Close(); err != nil {
		return err
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}

	return errors.Newf("%s", errors.Safe(resp.Status))
}

func checkURLWithRetries(client *http.Client, url string) error {
	for i := 0; i < timeoutRetries; i++ {
		err := existsURL(client, url)
		if netErr := (net.Error)(nil); errors.As(err, &netErr) && netErr.Timeout() {
			// Back off exponentially if we hit a timeout.
			time.Sleep((1 << uint(i)) * time.Second)
			continue
		}
		return err
	}
	return fmt.Errorf("timed out on %d separate tries, giving up", timeoutRetries)
}

// CheckURLsFromGrepOutput runs the specified cmd, which should be
// grepping using the URLRE regular expression defined above.
// The optional `cache` is used to skip URLs that have already been checked within a designated TTL (e.g., ~7 days).
func CheckURLsFromGrepOutput(cmd *exec.Cmd, cache map[string]struct{}) ([]string, error) {
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}
	filter := stream.ReadLines(stdout)
	stderr := new(bytes.Buffer)
	cmd.Stderr = stderr

	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}

	uniqueURLs, err := getURLs(filter)
	if err != nil {
		log.Fatal(err)
	}

	if err := cmd.Wait(); err != nil {
		log.Fatalf("err=%s, stderr=%s", err, stderr.String())
	}
	if cache == nil {
		cache = map[string]struct{}{}
	}
	checkedURLs, failedURLs, err := checkURLs(uniqueURLs, cache)

	log.Printf("checked %d; failed %d; (%d unique)\n", len(checkedURLs), len(failedURLs), len(uniqueURLs))
	return checkedURLs, err
}

// getURLs extracts URLs from the given filter.
func getURLs(filter stream.Filter) (map[string][]string, error) {
	uniqueURLs := map[string][]string{}

	if err := stream.ForEach(filter, func(s string) {
	outer:
		for _, match := range re.FindAllString(s, -1) {
			// Discard any characters after the first unbalanced ')' or ']'.
			match = chompUnbalanced('(', ')', match)
			match = chompUnbalanced('[', ']', match)
			// Remove punctuation after the URL.
			match = strings.TrimRight(match, ".,;\\\">`]")
			// Remove the HTML target.
			n := strings.LastIndexByte(match, '#')
			if n != -1 {
				match = match[:n]
			}
			// Add the source reference to the list.
			if len(s) > 150 {
				s = s[:150] + "..."
			}
			for _, ig := range ignored {
				if strings.HasPrefix(match, ig) {
					continue outer
				}
				if _, err := IsValidHTTPURL(match); err != nil {
					log.Printf("skipping invalid URL %q: %v in %q\n", match, err, s)
					continue outer
				}
				if strings.Contains(match, "{}") || strings.Contains(match, "$(") ||
					strings.Contains(match, "${") || strings.Contains(match, "{{") ||
					strings.Contains(match, "$ARCH") {
					log.Printf("skipping templated URL %q in %q\n", match, s)
					continue outer
				}
			}
			// remove trailing ":", "'" which amount to a typo
			match = strings.TrimSuffix(strings.TrimSuffix(match, ":"), "'")
			uniqueURLs[match] = append(uniqueURLs[match], s)
		}
	}); err != nil {
		return nil, err
	}

	return uniqueURLs, nil
}

// checkURLs checks the provided unique URLs
func checkURLs(
	uniqueURLs map[string][]string, cache map[string]struct{},
) ([]string, []string, error) {
	sem := make(chan struct{}, maxConcurrentRequests)

	client := newClient()
	limit, untilReset, err := checkGithubRateLimit(client)
	if err != nil {
		return []string{}, []string{}, err
	}
	log.Printf("GitHub API rate limit: %d requests remaining; time until reset: %s\n", limit, untilReset)
	checkedURLs := []string{}
	failedURLs := []string{}
	errs := []error{}
	var mu syncutil.Mutex
	var wg sync.WaitGroup

	for url, locs := range uniqueURLs {
		if _, ok := cache[url]; ok {
			log.Printf("Skipping cached %s\n", url)
			continue
		}
		sem <- struct{}{} // concurrency limiter
		wg.Add(1)

		// nolint:deferunlockcheck
		go func(url string, locs []string) {
			defer func() { <-sem; wg.Done() }()
			log.Printf("Checking %s", url)
			if err := checkURLWithRetries(client, url); err != nil {
				var buf bytes.Buffer
				fmt.Fprintf(&buf, "%s : %s\n", url, err)
				for _, loc := range locs {
					fmt.Fprintln(&buf, "    ", loc)
				}
				mu.Lock()
				errs = append(errs, errors.Newf("%s", buf.String()))
				failedURLs = append(failedURLs, url)
				mu.Unlock()
			} else {
				mu.Lock()
				checkedURLs = append(checkedURLs, url)
				mu.Unlock()
			}
		}(url, locs)
	}
	wg.Wait()

	if len(errs) > 0 {
		var buf bytes.Buffer
		for _, err := range errs {
			fmt.Fprintln(&buf, err)
		}
		fmt.Fprintf(&buf, "%d errors\n", len(errs))
		return checkedURLs, failedURLs, errors.Newf("%s", buf.String())
	}
	return checkedURLs, failedURLs, nil
}

type authTransport struct {
	base  http.RoundTripper
	token string
	ua    string
}

func newClient() *http.Client {
	token := os.Getenv("GITHUB_API_TOKEN")
	if token == "" {
		panic("set GITHUB_API_TOKEN")
	}
	// N.B. we set custom User-Agent header to avoid being blocked.
	// E.g., as of 08/25/25, Wikipedia blocks default UAs [1].
	// [1] https://phabricator.wikimedia.org/T400119
	ua := "URLChecker/1.0 (https://example.com/myapp; myapp@example.com)"
	base := &http.Transport{
		// This test doesn't care that https certificates are invalid.
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	return &http.Client{
		Transport: &authTransport{base: base, token: token, ua: ua},
		Timeout:   time.Minute,
	}
}

func (t *authTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Clone to avoid mutating the caller’s request
	r := req.Clone(req.Context())

	// Add auth for github requests.
	if r.URL.Host == "api.github.com" {
		if t.token != "" {
			r.Header.Set("Authorization", "Bearer "+t.token) // or "token "+t.token
		}
		r.Header.Set("Accept", "application/vnd.github+json")
		r.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	}
	if t.ua != "" {
		r.Header.Set("User-Agent", t.ua)
	}

	return t.base.RoundTrip(r)
}

func IsValidHTTPURL(raw string) (bool, error) {
	u, err := url.ParseRequestURI(raw)
	if err != nil {
		return false, err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return false, errors.New("unsupported scheme (want http/https)")
	}
	if u.Host == "" {
		return false, errors.New("missing host")
	}
	host := u.Hostname()
	if net.ParseIP(host) == nil && !strings.Contains(host, ".") {
		return false, errors.Newf("host(%q) is missing domain", host)
	}
	return true, nil
}

func existsURL(client *http.Client, rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return err
	}
	host := strings.ToLower(u.Host)
	switch host {
	case "github.com", "www.github.com":
		return existsGitHubSitePath(client, u)
	case "raw.githubusercontent.com":
		return existsRawContent(client, u)
	case "gist.github.com", "www.gist.github.com":
		return existsGist(client, u)
	default:
		// Fallback
		return checkURL(client, rawURL)
	}
}

func splitPath(p string) []string {
	p = strings.Trim(p, "/")
	if p == "" {
		return nil
	}
	return strings.Split(p, "/")
}

func escapePathSegments(segments []string) string {
	escaped := make([]string, 0, len(segments))
	for _, s := range segments {
		escaped = append(escaped, url.PathEscape(s))
	}
	return strings.Join(escaped, "/")
}
