package engflow

import (
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"

	bes "github.com/cockroachdb/cockroach/pkg/build/bazel/bes"
	//lint:ignore SA1019
	gproto "github.com/golang/protobuf/proto"
	"golang.org/x/net/http2"
)

type testResultWithMetadata struct {
	run, shard, attempt int32
	testResult          *bes.TestResult
}

type TestResultWithXml struct {
	Label               string
	Run, Shard, Attempt int32
	TestResult          *bes.TestResult
	TestXml             string
	Err                 error
}

type InvocationInfo struct {
	TestResults map[string][]*TestResultWithXml
}

func getHttpClient(certFile, keyFile string) (*http.Client, error) {
	cer, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	config := &tls.Config{
		Certificates: []tls.Certificate{cer},
	}
	transport := &http2.Transport{
		TLSClientConfig: config,
	}
	httpClient := &http.Client{
		Transport: transport,
	}
	return httpClient, nil
}

func downloadTestXml(client *http.Client, uri string) (string, error) {
	url := strings.ReplaceAll(uri, "bytestream://", "https://")
	url = strings.ReplaceAll(url, "/blobs/", "/api/v0/blob/")
	resp, err := client.Get(url)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	xml, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(xml), nil
}

func fetchTestXml(
	httpClient *http.Client,
	label string,
	testResult testResultWithMetadata,
	ch chan *TestResultWithXml,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	for _, output := range testResult.testResult.TestActionOutput {
		if output.Name == "test.xml" {
			xml, err := downloadTestXml(httpClient, output.GetUri())
			ch <- &TestResultWithXml{
				Label:      label,
				Run:        testResult.run,
				Shard:      testResult.shard,
				Attempt:    testResult.attempt,
				TestXml:    xml,
				Err:        err,
				TestResult: testResult.testResult,
			}
		}
	}
}

// LoadTestResults parses the test results out of the given event stream file, returning a
// map of test results keyed by label.
// Note the TestResultWithXml struct contains an Err field. This is the error
// (if any) from fetching the test.xml for this test run. This must be checked
// *in addition to* the err return value from the function.
func LoadTestResults(
	eventStreamFile io.Reader,
	certFile string,
	keyFile string) (*InvocationInfo, error) {
	httpClient, err := getHttpClient(certFile, keyFile)
	if err != nil {
		return nil, err
	}

	content, err := io.ReadAll(eventStreamFile)
	if err != nil {
		return nil, err
	}
	buf := gproto.NewBuffer(content)
	ret := &InvocationInfo{
		TestResults: make(map[string][]*TestResultWithXml),
	}
	testResults := make(map[string][]testResultWithMetadata)

	for {
		var event bes.BuildEvent
		err := buf.DecodeMessage(&event)
		if err != nil {
			// This is probably OK: just no more stuff left in the buffer.
			break
		}
		switch id := event.Id.Id.(type) {
		case *bes.BuildEventId_TestResult:
			res := testResultWithMetadata{
				run:        id.TestResult.Run,
				shard:      id.TestResult.Shard,
				attempt:    id.TestResult.Attempt,
				testResult: event.GetTestResult(),
			}
			testResults[id.TestResult.Label] = append(testResults[id.TestResult.Label], res)
		}
	}

	unread := buf.Unread()
	if len(unread) != 0 {
		return nil, fmt.Errorf("didn't read entire file: %d bytes remaining", len(unread))
	}

	// Download test xml's.
	ch := make(chan *TestResultWithXml)
	var wg sync.WaitGroup
	for label, results := range testResults {
		for _, result := range results {
			wg.Add(1)
			go fetchTestXml(httpClient, label, result, ch, &wg)
		}
	}
	go func(wg *sync.WaitGroup) {
		wg.Wait()
		close(ch)
	}(&wg)

	// Collect test xml's.
	for result := range ch {
		ret.TestResults[result.Label] = append(ret.TestResults[result.Label], result)
	}

	for _, slice := range ret.TestResults {
		slices.SortFunc(slice, func(a, b *TestResultWithXml) int {
			// First Shard, then Run, then Attempt.
			if a.Run < b.Run {
				return -1
			} else if a.Run > b.Run {
				return 1
			} else if a.Shard < b.Shard {
				return -1
			} else if a.Shard > b.Shard {
				return 1
			} else if a.Attempt < b.Attempt {
				return -1
			} else if a.Attempt > b.Attempt {
				return 1
			}
			return 0
		})
	}

	return ret, nil
}
